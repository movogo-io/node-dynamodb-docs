import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import type { ReadOptions, TransactionItem, Written } from '@movogo-io/docs/driver'
import type { KeyRange } from '@movogo-io/docs/schema'
import { dbRequest, requestsInFlightDefault, requestTimeoutMsDefault } from './aws.js'

export class Driver {
    readonly #bounds

    constructor(bounds?: { requestsInFlightMax?: number; requestTimeoutMs?: number }) {
        this.#bounds = {
            requestsInFlightMax: bounds?.requestsInFlightMax ?? requestsInFlightDefault,
            requestTimeoutMs: bounds?.requestTimeoutMs ?? requestTimeoutMsDefault,
        }
    }

    connect(context: Context) {
        return Promise.try(
            () => new Connection(context, consistentReads(context.env), this.#bounds),
        )
    }
}

type Context = {
    env?: Environment
    log?: {
        debug: (message: string, error: unknown, fields: unknown) => void
    }
    signal?: AbortSignal
}

type Environment = {
    AWS_REGION?: string
    AWS_DEFAULT_REGION?: string
    AWS_ACCESS_KEY_ID?: string
    AWS_SECRET_ACCESS_KEY?: string
    AWS_SESSION_TOKEN?: string
    AWS_DYNAMODB_ENDPOINT?: string
    TABLE_PREFIX?: string
    TABLE_POSTFIX?: string
    AWS_DYNAMODB_BILLING_METHOD?: string
    AWS_DYNAMODB_RCU?: string
    AWS_DYNAMODB_WCU?: string
    AWS_DYNAMODB_POINT_IN_TIME_RECOVERY?: string
    AWS_DYNAMODB_READ_CONSISTENCY?: string
}

const throttleAttemptsMax = 8
// TransactWriteItems takes at most 100 operations.
const transactionItemsMax = 100
// BatchGetItem reads at most 100 keys, and answers at most 16MB; whatever it
// could not read comes back as unprocessed keys with a 200, not as an error.
const batchGetKeysMax = 100
// Four requests in flight reads 400 keys at once, a burst of connections no
// larger than the one the store opens reading a list through `get`.
const batchGetRequestsInFlightMax = 4
const backoffDelayMsBase = 100
const backoffDelayMsMax = 3200
const tableCreationAttemptsMax = 60
const tableCreationDelayMs = 1000

type WriteOptions = { now: number; expiresAt?: number; newRevision?: unknown }

class Connection {
    // What the store may assume of this connection, so it bounds nothing itself.
    readonly requestsInFlightMax
    readonly transactionItemsMax = transactionItemsMax
    // `add` and `update` store the revision the store hands them, as a
    // transaction stores an item's.
    readonly acceptsNewRevision = true
    readonly #context
    readonly #consistentRead
    readonly #requestTimeoutMs

    constructor(
        context: Context,
        consistentRead: boolean,
        bounds: { requestsInFlightMax: number; requestTimeoutMs: number },
    ) {
        this.#context = context
        this.#consistentRead = consistentRead
        this.requestsInFlightMax = bounds.requestsInFlightMax
        this.#requestTimeoutMs = bounds.requestTimeoutMs
    }

    async add(
        table: string,
        partition: string,
        key: string,
        document: unknown,
        options: WriteOptions,
    ): Promise<Written> {
        try {
            const { Attributes } = await this.#request<UpdateResponse>('UpdateItem', {
                ...addItem(
                    this.#tableName(table),
                    partition,
                    key,
                    options.newRevision ?? randomUUID().replaceAll('-', ''),
                    document,
                    options.expiresAt,
                    isoOf(options.now),
                    options.now,
                ),
                ReturnValues: 'ALL_NEW',
            })
            return writtenOf(Attributes)
        } catch (e) {
            if (isErrorType(e, 'ResourceNotFoundException')) {
                await this.#createTable(table)
                await setTimeout(tableCreationDelayMs, undefined, { signal: this.#context.signal })
                return this.add(table, partition, key, document, options)
            }
            if (isErrorType(e, 'ResourceInUseException')) {
                this.#context.log?.debug(
                    'Table in use; retrying assuming it is being created.',
                    e,
                    {
                        table,
                    },
                )
                await setTimeout(tableCreationDelayMs, undefined, { signal: this.#context.signal })
                return this.add(table, partition, key, document, options)
            }
            if (isErrorType(e, 'ConditionalCheckFailedException')) {
                throw conflict()
            }
            // A transaction holds the item; the revision this write assumes, or the
            // absence it asserts, is about to be stale.
            if (isErrorType(e, 'TransactionConflictException')) {
                throw conflict()
            }
            throw e
        }
    }

    async get(table: string, partition: string, key: string, options?: ReadOptions) {
        try {
            const result = await this.#request<{
                Item?: {
                    [key: string]: AttributeValue
                }
            }>(
                'GetItem',
                {
                    TableName: this.#tableName(table),
                    ...(this.#consistent(options) && { ConsistentRead: true }),
                    Key: itemKey(partition, key),
                },
                this.#context.signal,
            )

            if (!result.Item) {
                throw notFound()
            }

            return rowOf(result.Item)
        } catch (e) {
            if (isErrorType(e, 'ResourceNotFoundException')) {
                throw notFound()
            }
            if (isErrorType(e, 'ResourceInUseException')) {
                throw notFound()
            }
            throw e
        }
    }

    // Every row among `refs` that exists, or a throw. A batch that comes back
    // with unprocessed keys is retried until it is whole, because the store
    // cannot tell a short answer from documents that are gone.
    async getMany(
        table: string,
        refs: readonly { partition: string; key: string }[],
        options?: ReadOptions,
    ) {
        const batches = []
        for (let start = 0; start < refs.length; start += batchGetKeysMax) {
            batches.push(refs.slice(start, start + batchGetKeysMax))
        }
        const rows = []
        for (let start = 0; start < batches.length; start += batchGetRequestsInFlightMax) {
            const read = await Promise.all(
                batches
                    .slice(start, start + batchGetRequestsInFlightMax)
                    .map(batch => this.#batchGet(table, batch, options)),
            )
            rows.push(...read.flat())
        }
        return rows
    }

    async *getPartitions(table: string) {
        try {
            let lastEvaluatedKey: unknown
            const seen = new Set<string>()

            for (;;) {
                const result = await this.#request<QueryResponse>(
                    'Scan',
                    {
                        TableName: this.#tableName(table),
                        ...(this.#consistentRead && { ConsistentRead: true }),
                        ProjectionExpression: '#p',
                        ExpressionAttributeNames: {
                            '#p': 'partition',
                        },
                        ...(lastEvaluatedKey !== undefined && {
                            ExclusiveStartKey: lastEvaluatedKey,
                        }),
                    },
                    this.#context.signal,
                )

                if (result.Items !== undefined) {
                    for (const item of result.Items) {
                        const partition = item.partition?.S
                        if (!partition || seen.has(partition)) {
                            continue
                        }
                        seen.add(partition)
                        yield partition
                    }
                }

                if (!result.LastEvaluatedKey) {
                    break
                }
                lastEvaluatedKey = result.LastEvaluatedKey
            }
        } catch (e) {
            if (isErrorType(e, 'ResourceNotFoundException')) {
                return
            }
            if (isErrorType(e, 'ResourceInUseException')) {
                return
            }
            throw e
        }
    }

    async *getPartition(table: string, partition: string, range?: KeyRange, options?: ReadOptions) {
        if (range && 'before' in range && range.before === '') {
            return
        }
        try {
            let lastEvaluatedKey: unknown

            for (;;) {
                const result = await this.#request<QueryResponse>(
                    'Query',
                    {
                        TableName: this.#tableName(table),
                        ...(this.#consistent(options) && { ConsistentRead: true }),
                        ...(lastEvaluatedKey !== undefined && {
                            ExclusiveStartKey: lastEvaluatedKey,
                        }),
                        ...queryFromRange(partition, range),
                    },
                    this.#context.signal,
                )

                if (result.Items !== undefined) {
                    yield* result.Items.map(item => {
                        const key = item.key?.S
                        if (!key || (range && !matchRange(range)(key))) {
                            return undefined
                        }

                        // A partition's rows carry no partition: the caller named it.
                        const { partition: _, ...row } = rowOf(item)
                        return row
                    }).filter(i => !!i)
                }

                if (!result.LastEvaluatedKey) {
                    break
                }
                lastEvaluatedKey = result.LastEvaluatedKey
            }
        } catch (e) {
            if (isErrorType(e, 'ResourceNotFoundException')) {
                return undefined
            }
            if (isErrorType(e, 'ResourceInUseException')) {
                return undefined
            }
            throw e
        }
    }

    async update(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        document: unknown,
        options: WriteOptions,
    ): Promise<Written> {
        try {
            const { Attributes } = await this.#request<UpdateResponse>('UpdateItem', {
                ...updateItem(
                    this.#tableName(table),
                    partition,
                    key,
                    currentRevision,
                    options.newRevision ?? randomUUID().replaceAll('-', ''),
                    document,
                    options,
                    isoOf(options.now),
                ),
                ReturnValues: 'ALL_NEW',
            })
            return writtenOf(Attributes)
        } catch (e) {
            if (isErrorType(e, 'ConditionalCheckFailedException')) {
                throw conflict()
            }
            if (isErrorType(e, 'TransactionConflictException')) {
                throw conflict()
            }
            if (isErrorType(e, 'ResourceInUseException')) {
                this.#context.log?.debug(
                    'Table in use; retrying assuming it is being created.',
                    e,
                    {
                        table,
                    },
                )
                await setTimeout(tableCreationDelayMs, undefined, { signal: this.#context.signal })
                return this.update(table, partition, key, currentRevision, document, options)
            }
            if (isErrorType(e, 'ResourceNotFoundException')) {
                throw conflict()
            }
            throw e
        }
    }

    async delete(
        table: string,
        partition: string,
        key: string,
        currentRevision: unknown,
        options: { now: number },
    ) {
        try {
            await this.#request('DeleteItem', {
                ...deleteItem(this.#tableName(table), partition, key),
                ...liveRevisionCondition(currentRevision, options.now),
            })
        } catch (e) {
            if (isErrorType(e, 'ConditionalCheckFailedException')) {
                throw conflict()
            }
            if (isErrorType(e, 'TransactionConflictException')) {
                throw conflict()
            }
            if (isErrorType(e, 'ResourceInUseException')) {
                throw conflict()
            }
            if (isErrorType(e, 'ResourceNotFoundException')) {
                throw conflict()
            }
            throw e
        }
    }

    async transact(items: TransactionItem[], options: { now: number }) {
        if (transactionItemsMax < items.length) {
            throw transactionTooLarge(items, 'limit of 100 operations per transaction')
        }
        const timestamp = isoOf(options.now)
        const request = {
            TransactItems: items.map(item => this.#transactItem(item, options.now, timestamp)),
            ClientRequestToken: randomUUID(),
        }
        for (let attempt = 1; ; attempt++) {
            try {
                await this.#request('TransactWriteItems', request)
                return
            } catch (e) {
                await this.#recoverTransaction(e, items, attempt)
            }
        }
    }

    close() {
        return Promise.resolve()
    }

    // The service-wide setting, or the caller asking for this one read.
    #consistent(options: ReadOptions | undefined) {
        return this.#consistentRead || options?.consistent === true
    }

    async #batchGet(
        table: string,
        refs: readonly { partition: string; key: string }[],
        options?: ReadOptions,
    ) {
        const tableName = this.#tableName(table)
        const rows = []
        let keys: { [key: string]: AttributeValue }[] = refs.map(ref =>
            itemKey(ref.partition, ref.key),
        )
        for (let attempt = 1; ; attempt++) {
            try {
                const result = await this.#request<BatchGetResponse>(
                    'BatchGetItem',
                    {
                        RequestItems: {
                            [tableName]: {
                                Keys: keys,
                                ...(this.#consistent(options) && { ConsistentRead: true }),
                            },
                        },
                    },
                    this.#context.signal,
                )
                rows.push(...(result.Responses?.[tableName] ?? []).map(item => rowOf(item)))
                const unprocessed = result.UnprocessedKeys?.[tableName]?.Keys ?? []
                if (unprocessed.length === 0) {
                    return rows
                }
                if (throttleAttemptsMax <= attempt) {
                    throw new Error(
                        `Reading ${unprocessed.length.toString()} of ${refs.length.toString()} keys of table ${tableName} kept being throttled.`,
                    )
                }
                await backoff(attempt, this.#context.signal)
                keys = unprocessed
            } catch (e) {
                // A table that does not exist yet holds none of the documents.
                if (isErrorType(e, 'ResourceNotFoundException')) {
                    return []
                }
                if (isErrorType(e, 'ResourceInUseException')) {
                    return []
                }
                throw e
            }
        }
    }

    async #recoverTransaction(e: unknown, items: TransactionItem[], attempt: number) {
        const reasons = cancellationReasons(e)
        if (reasons) {
            if (
                reasons.some(
                    r => r.Code === 'ConditionalCheckFailed' || r.Code === 'TransactionConflict',
                )
            ) {
                throw conflict()
            }
            if (isThrottledTransaction(reasons) && attempt < throttleAttemptsMax) {
                await backoff(attempt, this.#context.signal)
                return
            }
            throw e
        }
        const limit = exceededLimit(e)
        if (limit) {
            throw transactionTooLarge(items, limit)
        }
        if (isErrorType(e, 'TransactionInProgressException') && attempt < throttleAttemptsMax) {
            await backoff(attempt, this.#context.signal)
            return
        }
        // Nobody knows whether it was applied. The request carries the same
        // token on every attempt, so sending it again applies it at most once;
        // it is never a conflict, which would have the store write it anew.
        if (isUnanswered(e) && attempt < throttleAttemptsMax) {
            await backoff(attempt, this.#context.signal)
            return
        }
        if (isErrorType(e, 'ResourceNotFoundException') && attempt < tableCreationAttemptsMax) {
            for (const table of new Set(items.map(item => item.table))) {
                await this.#createTable(table)
            }
            await setTimeout(tableCreationDelayMs, undefined, { signal: this.#context.signal })
            return
        }
        if (isErrorType(e, 'ResourceInUseException') && attempt < tableCreationAttemptsMax) {
            this.#context.log?.debug('Table in use; retrying assuming it is being created.', e, {
                tables: [...new Set(items.map(item => item.table))].join(','),
            })
            await setTimeout(tableCreationDelayMs, undefined, { signal: this.#context.signal })
            return
        }
        throw e
    }

    #transactItem(item: TransactionItem, nowSeconds: number, timestamp: string) {
        const tableName = this.#tableName(item.table)
        switch (item.op) {
            case 'add':
                return {
                    Update: addItem(
                        tableName,
                        item.partition,
                        item.key,
                        item.newRevision,
                        item.document,
                        item.expiresAt,
                        timestamp,
                        nowSeconds,
                    ),
                }
            case 'put':
                return {
                    Update: putUpdateItem(
                        tableName,
                        item.partition,
                        item.key,
                        item.newRevision,
                        item.document,
                        item.expiresAt,
                        timestamp,
                        true,
                    ),
                }
            case 'update':
                return {
                    Update: updateItem(
                        tableName,
                        item.partition,
                        item.key,
                        item.revision,
                        item.newRevision,
                        item.document,
                        { now: nowSeconds, expiresAt: item.expiresAt },
                        timestamp,
                    ),
                }
            case 'delete':
                return {
                    Delete: {
                        ...deleteItem(tableName, item.partition, item.key),
                        ...liveRevisionCondition(item.revision, nowSeconds),
                    },
                }
            case 'clear':
                return {
                    Delete: deleteItem(tableName, item.partition, item.key),
                }
            case 'check':
                return {
                    ConditionCheck: {
                        TableName: tableName,
                        Key: itemKey(item.partition, item.key),
                        ...liveRevisionCondition(item.revision, nowSeconds),
                    },
                }
        }
    }

    // `signal` is given for reads only; see RequestOptions.
    async #request<T>(target: string, body: unknown, signal?: AbortSignal): Promise<T> {
        for (let attempt = 1; ; attempt++) {
            try {
                return await dbRequest<T>(this.#context.env, target, body, {
                    signal,
                    timeoutMs: this.#requestTimeoutMs,
                    requestsInFlightMax: this.requestsInFlightMax,
                })
            } catch (e) {
                if (throttleAttemptsMax <= attempt || !isThrottledRequest(e)) {
                    throw e
                }
                await backoff(attempt, this.#context.signal)
            }
        }
    }

    async #createTable(table: string) {
        const tableOptions =
            this.#context.env?.AWS_DYNAMODB_BILLING_METHOD === 'PROVISIONED'
                ? {
                      BillingMode: 'PROVISIONED',
                      ProvisionedThroughput: {
                          ReadCapacityUnits: Number(this.#context.env.AWS_DYNAMODB_RCU ?? '1'),
                          WriteCapacityUnits: Number(this.#context.env.AWS_DYNAMODB_WCU ?? '1'),
                      },
                  }
                : {
                      BillingMode: 'PAY_PER_REQUEST',
                  }

        try {
            await this.#request('CreateTable', {
                TableName: this.#tableName(table),
                AttributeDefinitions: [
                    { AttributeName: 'partition', AttributeType: 'S' },
                    { AttributeName: 'key', AttributeType: 'S' },
                ],
                KeySchema: [
                    { AttributeName: 'partition', KeyType: 'HASH' },
                    { AttributeName: 'key', KeyType: 'RANGE' },
                ],
                ...tableOptions,
            })
        } catch (e) {
            if (isErrorType(e, 'ResourceInUseException')) {
                return
            }
            throw e
        }
        await this.#waitForActive(table)
        await this.#request('UpdateTimeToLive', {
            TableName: this.#tableName(table),
            TimeToLiveSpecification: { AttributeName: 'expiresAt', Enabled: true },
        })
        if (this.#context.env?.AWS_DYNAMODB_POINT_IN_TIME_RECOVERY === 'true') {
            await this.#enablePointInTimeRecovery(table)
        }
    }

    // Continuous backups keep being provisioned for a few seconds after the table is active.
    async #enablePointInTimeRecovery(table: string) {
        for (let attempt = 1; ; attempt++) {
            try {
                await this.#request('UpdateContinuousBackups', {
                    TableName: this.#tableName(table),
                    PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true },
                })
                return
            } catch (e) {
                if (
                    tableCreationAttemptsMax <= attempt ||
                    !isErrorType(e, 'ContinuousBackupsUnavailableException')
                ) {
                    throw e
                }
                await setTimeout(tableCreationDelayMs, undefined, { signal: this.#context.signal })
            }
        }
    }

    async #waitForActive(table: string) {
        for (let attempt = 1; ; attempt++) {
            const { Table } = await this.#request<{ Table?: { TableStatus?: string } }>(
                'DescribeTable',
                { TableName: this.#tableName(table) },
            )
            if (Table?.TableStatus === 'ACTIVE') {
                return
            }
            if (tableCreationAttemptsMax <= attempt) {
                throw new Error(`Table ${this.#tableName(table)} did not become active.`)
            }
            await setTimeout(tableCreationDelayMs, undefined, { signal: this.#context.signal })
        }
    }

    #tableName(table: string) {
        return `${this.#context.env?.TABLE_PREFIX ?? ''}${table}${this.#context.env?.TABLE_POSTFIX ?? ''}`
    }
}

type BatchGetResponse = {
    Responses?: {
        [table: string]: {
            [key: string]: AttributeValue
        }[]
    }
    UnprocessedKeys?: {
        [table: string]: {
            Keys?: {
                [key: string]: AttributeValue
            }[]
        }
    }
}

type UpdateResponse = {
    Attributes?: {
        [key: string]: AttributeValue
    }
}

type QueryResponse = {
    Items?: {
        [key: string]: AttributeValue
    }[]
    LastEvaluatedKey?: {
        [key: string]: AttributeValue
    }
    Count?: number
    ScannedCount?: number
}

type AttributeValue = {
    S?: string
    N?: string
    B?: string
    SS?: string[]
    NS?: string[]
    BS?: string[]
    M?: { [key: string]: AttributeValue }
    L?: AttributeValue[]
    NULL?: boolean
    BOOL?: boolean
}

// An `add` is an Update, not a Put: a Put would write `seq` afresh, where an
// add over an expired item continues its count.
function addItem(
    tableName: string,
    partition: string,
    key: string,
    newRevision: unknown,
    document: unknown,
    expiresAt: number | undefined,
    timestamp: string,
    nowSeconds: number,
) {
    return withCondition(
        putUpdateItem(
            tableName,
            partition,
            key,
            newRevision,
            document,
            expiresAt,
            timestamp,
            false,
        ),
        absentOrExpiredCondition(nowSeconds),
    )
}

// What a write stored, from the attributes DynamoDB answers a write with.
function writtenOf(attributes: { [key: string]: AttributeValue } | undefined): Written {
    if (attributes === undefined) {
        throw new Error('UpdateItem answered without the attributes of the item it wrote.')
    }
    const { revision, seq, updatedAt } = rowOf(attributes)
    return { revision, seq, updatedAt }
}

function rowOf(item: { [key: string]: AttributeValue }) {
    return {
        partition: item.partition?.S ?? '',
        key: item.key?.S ?? '',
        revision: item.revision?.S as unknown,
        document: JSON.parse(item.document?.S ?? '{}') as unknown,
        // Every item this driver and @riddance/dynamodb-docs ever wrote carries
        // both; the fallbacks exist for hand-built items (test mocks) only.
        seq: Number(item.seq?.N ?? '0'),
        updatedAt: item.updated?.S ?? item.created?.S ?? '1970-01-01T00:00:00.000Z',
        ...expiresAtOf(item),
    }
}

function expiresAtOf(item: { expiresAt?: AttributeValue }) {
    const seconds = item.expiresAt?.N
    if (seconds === undefined) {
        return {}
    }
    return { expiresAt: Number(seconds) }
}

function updateItem(
    tableName: string,
    partition: string,
    key: string,
    revision: unknown,
    newRevision: unknown,
    document: unknown,
    options: WriteOptions,
    timestamp: string,
) {
    const condition = liveRevisionCondition(revision, options.now)
    return {
        TableName: tableName,
        Key: itemKey(partition, key),
        UpdateExpression: updateExpression(options.expiresAt),
        ConditionExpression: condition.ConditionExpression,
        ExpressionAttributeValues: {
            ...condition.ExpressionAttributeValues,
            ':one': { N: '1' },
            ':newRevision': { S: newRevision as string },
            ':updated': { S: timestamp },
            ':document': { S: JSON.stringify(document) },
            ...(options.expiresAt !== undefined && {
                ':expiresAt': { N: options.expiresAt.toString() },
            }),
        },
    }
}

function updateExpression(expiresAt: number | undefined) {
    const set = 'ADD seq :one SET revision = :newRevision, updated = :updated, document = :document'
    if (expiresAt === undefined) {
        return `${set} REMOVE expiresAt`
    }
    return `${set}, expiresAt = :expiresAt`
}

// A transaction's unconditional put. A Put item would write `seq` afresh, so
// it is an Update instead: `seq` continues from the item it replaces, live or
// expired, and a new item starts at 0. A plain ADD would start it at 1.
// `created` is kept for a put over an item, and stamped anew for an add, which
// begins the document anew.
function putUpdateItem(
    tableName: string,
    partition: string,
    key: string,
    newRevision: unknown,
    document: unknown,
    expiresAt: number | undefined,
    timestamp: string,
    keepCreated: boolean,
) {
    const created = keepCreated ? 'if_not_exists(created, :updated)' : ':updated'
    const set = `SET seq = if_not_exists(seq, :minusOne) + :one, revision = :newRevision, updated = :updated, created = ${created}, document = :document`
    return {
        TableName: tableName,
        Key: itemKey(partition, key),
        UpdateExpression:
            expiresAt === undefined ? `${set} REMOVE expiresAt` : `${set}, expiresAt = :expiresAt`,
        ExpressionAttributeValues: {
            ':minusOne': { N: '-1' },
            ':one': { N: '1' },
            ':newRevision': { S: newRevision as string },
            ':updated': { S: timestamp },
            ':document': { S: JSON.stringify(document) },
            ...(expiresAt !== undefined && { ':expiresAt': { N: expiresAt.toString() } }),
        },
    }
}

// A delete removes the item, `seq` included: a document re-added under the
// key starts its count at 0, and a key never written stays absent.
function deleteItem(tableName: string, partition: string, key: string) {
    return {
        TableName: tableName,
        Key: itemKey(partition, key),
    }
}

function withCondition<T extends { ExpressionAttributeValues: { [name: string]: AttributeValue } }>(
    item: T,
    condition: {
        ConditionExpression: string
        ExpressionAttributeValues: { [name: string]: AttributeValue }
    },
) {
    return {
        ...item,
        ConditionExpression: condition.ConditionExpression,
        ExpressionAttributeValues: {
            ...item.ExpressionAttributeValues,
            ...condition.ExpressionAttributeValues,
        },
    }
}

function absentOrExpiredCondition(nowSeconds: number) {
    return {
        ConditionExpression: 'attribute_not_exists(revision) OR expiresAt <= :nowSeconds',
        ExpressionAttributeValues: {
            ':nowSeconds': { N: nowSeconds.toString() },
        },
    }
}

function liveRevisionCondition(revision: unknown, nowSeconds: number) {
    return {
        ConditionExpression:
            'revision = :oldRevision AND (attribute_not_exists(expiresAt) OR :nowSeconds < expiresAt)',
        ExpressionAttributeValues: {
            ':oldRevision': { S: revision as string },
            ':nowSeconds': { N: nowSeconds.toString() },
        },
    }
}

// Reads are eventually consistent unless the service opts in for all of them:
// a strongly consistent read costs twice the read units, and the store asks
// for one per call, with `{ consistent: true }`, wherever a stale answer would
// be acted on unchecked. The env var is the service-wide override. Under it a
// read that finds no row is made twice: the store confirms an absence
// consistently and cannot tell this one already was.
function consistentReads(env: Environment | undefined) {
    const consistency = env?.AWS_DYNAMODB_READ_CONSISTENCY ?? 'EVENTUAL'
    if (consistency === 'STRONG') {
        return true
    }
    if (consistency === 'EVENTUAL') {
        return false
    }
    throw new Error(
        `AWS_DYNAMODB_READ_CONSISTENCY must be STRONG or EVENTUAL, not '${consistency}'.`,
    )
}

// `created` and `updated` come from the context clock the store hands down as
// `options.now`, at second precision, never from the wall clock: the memory
// driver stamps the same instant, so a test sees one value from both.
function isoOf(seconds: number) {
    return new Date(seconds * 1000).toISOString()
}

function itemKey(partition: string, key: string) {
    return {
        partition: { S: partition },
        key: { S: key },
    }
}

function queryFromRange(partition: string, range?: KeyRange) {
    if (!range) {
        return {
            KeyConditionExpression: '#p = :p',
            ExpressionAttributeNames: {
                '#p': 'partition',
            },
            ExpressionAttributeValues: {
                ':p': { S: partition },
            },
        }
    }
    if ('withPrefix' in range) {
        // Every key begins with '', and DynamoDB rejects an empty key condition value.
        if (range.withPrefix === '') {
            return queryFromRange(partition)
        }
        return {
            KeyConditionExpression: '#p = :p AND begins_with(#k, :withPrefix)',
            ExpressionAttributeNames: {
                '#p': 'partition',
                '#k': 'key',
            },
            ExpressionAttributeValues: {
                ':p': { S: partition },
                ':withPrefix': { S: range.withPrefix },
            },
        }
    }
    if ('before' in range || 'after' in range) {
        // Every key is at or after '', and DynamoDB rejects an empty key condition value.
        const after = range.after === '' ? undefined : range.after
        const { before } = range
        if (after === undefined && before === undefined) {
            return queryFromRange(partition)
        }
        return {
            KeyConditionExpression: `#p = :p and ${keyCondition(after, before)}`,
            ExpressionAttributeNames: {
                '#p': 'partition',
                '#k': 'key',
            },
            ExpressionAttributeValues: {
                ':p': { S: partition },
                ...(before !== undefined && { ':before': { S: before } }),
                ...(after !== undefined && { ':after': { S: after } }),
            },
        }
    }
    throw new Error('Unsupported range.')
}

function keyCondition(after: string | undefined, before: string | undefined) {
    if (after === undefined) {
        return '#k < :before'
    }
    if (before === undefined) {
        return ':after <= #k'
    }
    return '#k between :after and :before'
}

function matchRange(range?: KeyRange) {
    if (!range) {
        return () => true
    }
    if ('withPrefix' in range) {
        return (key: string) => key.startsWith(range.withPrefix)
    }
    if ('before' in range || 'after' in range) {
        const { after, before } = range
        if (after !== undefined) {
            if (before !== undefined) {
                return (key: string) => after <= key && key < before
            }
            return (key: string) => after <= key
        }
        if (before !== undefined) {
            return (key: string) => key < before
        }
    }
    return none
}

const none = () => false

// Equal jitter. Attempts 1-7 sleep about 7 seconds in total, past the 5 seconds DynamoDB
// says must elapse before a retry can complete a TransactionInProgressException inline.
async function backoff(attempt: number, signal: AbortSignal | undefined) {
    const delayMs = Math.min(backoffDelayMsMax, backoffDelayMsBase * 2 ** (attempt - 1))
    await setTimeout(delayMs / 2 + Math.random() * (delayMs / 2), undefined, { signal })
}

function conflict() {
    return Object.assign(new Error('Conflict'), { status: 409, statusCode: 409 })
}

function notFound() {
    return Object.assign(new Error('Not found'), { status: 404, statusCode: 404 })
}

function isThrottledRequest(error: unknown) {
    return (
        isErrorType(error, 'ProvisionedThroughputExceededException') ||
        isErrorType(error, 'ThrottlingException') ||
        isErrorType(error, 'RequestLimitExceeded')
    )
}

function isUnanswered(error: unknown) {
    if (!Error.isError(error)) {
        return false
    }
    if ('unanswered' in error) {
        return true
    }
    return 'response' in error && statusOf(error.response) >= 500
}

function statusOf(response: unknown) {
    if (typeof response !== 'object' || response === null || !('status' in response)) {
        return 0
    }
    return typeof response.status === 'number' ? response.status : 0
}

// Recognized by `isTransactionTooLarge` of @movogo-io/docs by its code.
function transactionTooLarge(items: TransactionItem[], limit: string) {
    return Object.assign(
        new Error(
            `Transaction of ${String(items.length)} operations on ${[...new Set(items.map(item => `'${item.table}'`))].join(', ')} exceeds the ${limit}.`,
        ),
        { code: 'docs.transaction_too_large' },
    )
}

function isErrorType(error: unknown, type: string) {
    if (!Error.isError(error)) {
        return false
    }
    if (!('response' in error)) {
        return false
    }
    const { response } = error as { response: { status: number; body: string } }
    try {
        const body = JSON.parse(response.body) as { __type?: string[] }
        return body.__type?.includes(type) ?? false
    } catch {
        return false
    }
}

// DynamoDB refuses an oversized transaction as invalid input, and refuses it
// again on every retry. Its message names neither the tables nor the
// operations, so the error names them and the limit. It does not carry
// DynamoDB's reply: the one to a transaction over 100 items echoes the items,
// documents included, into whatever log prints the error.
function exceededLimit(error: unknown) {
    if (!isErrorType(error, 'ValidationException')) {
        return undefined
    }
    const message = messageOf(error)
    if (message.includes('Transaction payload size cannot exceed')) {
        const measured = /Payload Size: (\d+)/u.exec(message)?.[1]
        if (measured) {
            return `limit of 4 MB per transaction: DynamoDB measured ${measured} bytes`
        }
        return 'limit of 4 MB per transaction'
    }
    if (/Item size (?:to update )?has exceeded the maximum allowed size/u.test(message)) {
        return 'limit of 400 KB per item'
    }
    if (
        message.includes(
            "at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100",
        )
    ) {
        return 'limit of 100 operations per transaction'
    }
    return undefined
}

function messageOf(error: unknown) {
    const { response } = error as { response: { body: string } }
    const body = JSON.parse(response.body) as { message?: string }
    return body.message ?? ''
}

function isThrottledTransaction(reasons: { Code?: string }[]) {
    const codes = reasons.map(r => r.Code).filter(code => code !== 'None')
    return (
        codes.length !== 0 &&
        codes.every(code => code === 'ThrottlingError' || code === 'ProvisionedThroughputExceeded')
    )
}

function cancellationReasons(error: unknown) {
    if (!isErrorType(error, 'TransactionCanceledException')) {
        return undefined
    }
    const { response } = error as { response: { body: string } }
    try {
        const body = JSON.parse(response.body) as {
            CancellationReasons?: { Code?: string }[]
        }
        return body.CancellationReasons ?? []
    } catch {
        return []
    }
}
