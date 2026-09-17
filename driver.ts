import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import type { TransactionItem } from '@movogo-io/docs/driver'
import { dbRequest } from './lib/aws.js'
import type { KeyRange } from './schema.js'

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
    TABLE_PREFIX?: string
    TABLE_POSTFIX?: string
    AWS_DYNAMODB_BILLING_METHOD?: string
    AWS_DYNAMODB_RCU?: string
    AWS_DYNAMODB_WCU?: string
}

const throttleAttemptsMax = 8
const backoffDelayMsBase = 100
const backoffDelayMsMax = 3200
const tableCreationAttemptsMax = 60
const tableCreationDelayMs = 1000

class Connection {
    readonly #context

    constructor(context: Context) {
        this.#context = context
    }

    async add(table: string, partition: string, key: string, document: unknown): Promise<unknown> {
        const revision = randomUUID().replaceAll('-', '')
        try {
            await this.#request('PutItem', {
                ...putItem(
                    this.#tableName(table),
                    partition,
                    key,
                    revision,
                    document,
                    new Date().toISOString(),
                ),
                ConditionExpression: 'attribute_not_exists(revision)',
            })
        } catch (e) {
            if (isErrorType(e, 'ResourceNotFoundException')) {
                await this.#createTable(table)
                await setTimeout(tableCreationDelayMs)
                return this.add(table, partition, key, document)
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
                return this.add(table, partition, key, document)
            }
            if (isErrorType(e, 'ConditionalCheckFailedException')) {
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
                    new Date().toISOString(),
                ),
            )
        } catch (e) {
            if (isErrorType(e, 'ConditionalCheckFailedException')) {
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
                return this.update(table, partition, key, currentRevision, document)
            }
            if (isErrorType(e, 'ResourceNotFoundException')) {
                throw conflict()
            }
            throw e
        }
        return newRevision
    }

    async delete(table: string, partition: string, key: string, currentRevision?: unknown) {
        try {
            await this.#request(
                'DeleteItem',
                deleteItem(this.#tableName(table), partition, key, currentRevision),
            )
        } catch (e) {
            if (isErrorType(e, 'ConditionalCheckFailedException')) {
                throw conflict()
            }
            if (isErrorType(e, 'ResourceInUseException')) {
                return
            }
            if (isErrorType(e, 'ResourceNotFoundException')) {
                if (currentRevision) {
                    throw conflict()
                }
                return
            }
            throw e
        }
    }

    async transact(items: TransactionItem[]) {
        const now = new Date().toISOString()
        const request = {
            TransactItems: items.map(item => this.#transactItem(item, now)),
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

    #transactItem(item: TransactionItem, now: string) {
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
                            now,
                        ),
                        ConditionExpression: 'attribute_not_exists(revision)',
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
                        now,
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
                        now,
                    ),
                }
            case 'delete':
                return {
                    Delete: deleteItem(tableName, item.partition, item.key, item.revision),
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
                        ...revisionCondition(item.revision),
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
    now: string,
) {
    return {
        TableName: tableName,
        Item: {
            ...itemKey(partition, key),
            revision: { S: revision as string },
            created: { S: now },
            updated: { S: now },
            seq: { N: '0' },
            document: { S: JSON.stringify(document) },
        },
    }
}

function updateItem(
    tableName: string,
    partition: string,
    key: string,
    revision: unknown,
    newRevision: unknown,
    document: unknown,
    now: string,
) {
    return {
        TableName: tableName,
        Key: itemKey(partition, key),
        UpdateExpression:
            'ADD seq :one SET revision = :newRevision, updated = :now, document = :document',
        ConditionExpression: 'revision = :oldRevision',
        ExpressionAttributeValues: {
            ':one': { N: '1' },
            ':oldRevision': { S: revision as string },
            ':newRevision': { S: newRevision as string },
            ':now': { S: now },
            ':document': { S: JSON.stringify(document) },
        },
    }
}

function deleteItem(tableName: string, partition: string, key: string, revision?: unknown) {
    return {
        TableName: tableName,
        Key: itemKey(partition, key),
        ...(revision !== undefined && revisionCondition(revision)),
    }
}

function revisionCondition(revision: unknown) {
    return {
        ConditionExpression: 'revision = :oldRevision',
        ExpressionAttributeValues: {
            ':oldRevision': { S: revision as string },
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
        const terms = ['#p = :p']
        if (range.after) {
            if (range.before) {
                terms.push('#k between :after and :before')
            } else {
                terms.push(':after <= #k')
            }
        } else if (range.before) {
            terms.push('#k < :before')
        }
        return {
            KeyConditionExpression: terms.join(' and '),
            ExpressionAttributeNames: {
                '#p': 'partition',
                '#k': 'key',
            },
            ExpressionAttributeValues: {
                ':p': { S: partition },
                ':before': range.before && { S: range.before },
                ':after': range.after && { S: range.after },
            },
        }
    }
    throw new Error('Unsupported range.')
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
        if (after) {
            if (before) {
                return (key: string) => after <= key && key < before
            }
            return (key: string) => after <= key
        }
        if (before) {
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
    const e = new Error('Conflict')
    ;(e as unknown as { status: number }).status = 409
    return e
}

function notFound() {
    const e = new Error('Not found')
    ;(e as unknown as { status: number }).status = 404
    return e
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
