import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import type { TransactionItem } from '@movogo-io/docs/driver'
import type { KeyRange } from '@movogo-io/docs/schema'
import { dbRequest } from './aws.js'

export class Driver {
    connect(context: Context) {
        return Promise.resolve(new Connection(context))
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
}

const throttleAttemptsMax = 8
const backoffDelayMsBase = 100
const backoffDelayMsMax = 3200
const tableCreationAttemptsMax = 60
const tableCreationDelayMs = 1000

type WriteOptions = { now: number; expiresAt?: number }

class Connection {
    readonly #context

    constructor(context: Context) {
        this.#context = context
    }

    async add(
        table: string,
        partition: string,
        key: string,
        document: unknown,
        options: WriteOptions,
    ): Promise<unknown> {
        const revision = randomUUID().replaceAll('-', '')
        try {
            await this.#request('PutItem', {
                ...putItem(
                    this.#tableName(table),
                    partition,
                    key,
                    revision,
                    document,
                    options.expiresAt,
                    new Date().toISOString(),
                ),
                ...absentOrExpiredCondition(options.now),
            })
        } catch (e) {
            if (isErrorType(e, 'ResourceNotFoundException')) {
                await this.#createTable(table)
                await setTimeout(tableCreationDelayMs)
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
                await setTimeout(tableCreationDelayMs)
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
        return revision
    }

    async get(table: string, partition: string, key: string) {
        try {
            const result = await this.#request<{
                Item?: {
                    [key: string]: AttributeValue
                }
            }>('GetItem', {
                TableName: this.#tableName(table),
                Key: itemKey(partition, key),
            })

            if (!result.Item) {
                throw notFound()
            }

            return {
                partition,
                key,
                revision: result.Item.revision?.S as unknown,
                document: JSON.parse(result.Item.document?.S ?? '{}') as unknown,
                ...expiresAtOf(result.Item),
            }
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

    async *getPartitions(table: string) {
        try {
            let lastEvaluatedKey: unknown
            const seen = new Set<string>()

            for (;;) {
                const result = await this.#request<QueryResponse>('Scan', {
                    TableName: this.#tableName(table),
                    ProjectionExpression: '#p',
                    ExpressionAttributeNames: {
                        '#p': 'partition',
                    },
                    ...(lastEvaluatedKey !== undefined && {
                        ExclusiveStartKey: lastEvaluatedKey,
                    }),
                })

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

    async *getPartition(table: string, partition: string, range?: KeyRange) {
        if (range && 'before' in range && range.before === '') {
            return
        }
        try {
            let lastEvaluatedKey: unknown

            for (;;) {
                const result = await this.#request<QueryResponse>('Query', {
                    TableName: this.#tableName(table),
                    ...(lastEvaluatedKey !== undefined && {
                        ExclusiveStartKey: lastEvaluatedKey,
                    }),
                    ...queryFromRange(partition, range),
                })

                if (result.Items !== undefined) {
                    yield* result.Items.map(item => {
                        const key = item.key?.S
                        if (!key || (range && !matchRange(range)(key))) {
                            return undefined
                        }

                        return {
                            partition: item.partition?.S ?? '',
                            key,
                            revision: item.revision?.S as unknown,
                            document: JSON.parse(item.document?.S ?? '{}') as unknown,
                            ...expiresAtOf(item),
                        }
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
    ): Promise<unknown> {
        const newRevision = randomUUID().replaceAll('-', '')
        try {
            await this.#request(
                'UpdateItem',
                updateItem(
                    this.#tableName(table),
                    partition,
                    key,
                    currentRevision,
                    newRevision,
                    document,
                    options,
                    new Date().toISOString(),
                ),
            )
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
                await setTimeout(tableCreationDelayMs)
                return this.update(table, partition, key, currentRevision, document, options)
            }
            if (isErrorType(e, 'ResourceNotFoundException')) {
                throw conflict()
            }
            throw e
        }
        return newRevision
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
        const timestamp = new Date().toISOString()
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
                await backoff(attempt)
                return
            }
            throw e
        }
        if (isErrorType(e, 'TransactionInProgressException') && attempt < throttleAttemptsMax) {
            await backoff(attempt)
            return
        }
        if (isErrorType(e, 'ResourceNotFoundException') && attempt < tableCreationAttemptsMax) {
            for (const table of new Set(items.map(item => item.table))) {
                await this.#createTable(table)
            }
            await setTimeout(tableCreationDelayMs)
            return
        }
        if (isErrorType(e, 'ResourceInUseException') && attempt < tableCreationAttemptsMax) {
            this.#context.log?.debug('Table in use; retrying assuming it is being created.', e, {
                tables: [...new Set(items.map(item => item.table))].join(','),
            })
            await setTimeout(tableCreationDelayMs)
            return
        }
        throw e
    }

    #transactItem(item: TransactionItem, nowSeconds: number, timestamp: string) {
        const tableName = this.#tableName(item.table)
        switch (item.op) {
            case 'add':
                return {
                    Put: {
                        ...putItem(
                            tableName,
                            item.partition,
                            item.key,
                            item.newRevision,
                            item.document,
                            item.expiresAt,
                            timestamp,
                        ),
                        ...absentOrExpiredCondition(nowSeconds),
                    },
                }
            case 'put':
                return {
                    Put: putItem(
                        tableName,
                        item.partition,
                        item.key,
                        item.newRevision,
                        item.document,
                        item.expiresAt,
                        timestamp,
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

    async #request<T>(target: string, body: unknown): Promise<T> {
        for (let attempt = 1; ; attempt++) {
            try {
                return await dbRequest<T>(this.#context.env, target, body)
            } catch (e) {
                if (throttleAttemptsMax <= attempt || !isThrottledRequest(e)) {
                    throw e
                }
                await backoff(attempt)
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
                await setTimeout(tableCreationDelayMs)
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
            await setTimeout(tableCreationDelayMs)
        }
    }

    #tableName(table: string) {
        return `${this.#context.env?.TABLE_PREFIX ?? ''}${table}${this.#context.env?.TABLE_POSTFIX ?? ''}`
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

function putItem(
    tableName: string,
    partition: string,
    key: string,
    revision: unknown,
    document: unknown,
    expiresAt: number | undefined,
    timestamp: string,
) {
    return {
        TableName: tableName,
        Item: {
            ...itemKey(partition, key),
            revision: { S: revision as string },
            created: { S: timestamp },
            updated: { S: timestamp },
            seq: { N: '0' },
            document: { S: JSON.stringify(document) },
            ...(expiresAt !== undefined && { expiresAt: { N: expiresAt.toString() } }),
        },
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

function deleteItem(tableName: string, partition: string, key: string) {
    return {
        TableName: tableName,
        Key: itemKey(partition, key),
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
async function backoff(attempt: number) {
    const delayMs = Math.min(backoffDelayMsMax, backoffDelayMsBase * 2 ** (attempt - 1))
    await setTimeout(delayMs / 2 + Math.random() * (delayMs / 2))
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
