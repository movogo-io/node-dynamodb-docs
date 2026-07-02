import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
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

class Connection {
    readonly #context

    constructor(context: Context) {
        this.#context = context
    }

    async add(table: string, partition: string, key: string, document: unknown): Promise<unknown> {
        const revision = randomUUID().replaceAll('-', '')
        try {
            const now = new Date().toISOString()
            await dbRequest(this.#context.env, 'PutItem', {
                TableName: this.#tableName(table),
                Item: {
                    partition: { S: partition },
                    key: { S: key },
                    revision: { S: revision },
                    created: { S: now },
                    updated: { S: now },
                    seq: { N: '0' },
                    document: { S: JSON.stringify(document) },
                },
                ConditionExpression: 'attribute_not_exists(revision)',
            })
        } catch (e) {
            if (isErrorType(e, 'ResourceNotFoundException')) {
                await this.#createTable(table)
                await setTimeout(1000)
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
                await setTimeout(1000)
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
            const result = await dbRequest<{
                Item?: {
                    [key: string]: AttributeValue
                }
            }>(this.#context.env, 'GetItem', {
                TableName: this.#tableName(table),
                Key: {
                    partition: { S: partition },
                    key: { S: key },
                },
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
                const result = await dbRequest<QueryResponse>(this.#context.env, 'Scan', {
                    TableName: this.#tableName(table),
                    ProjectionExpression: '#p',
                    ExpressionAttributeNames: {
                        '#p': 'partition',
                    },
                    ...(lastEvaluatedKey !== undefined && {
                        ExclusiveStartKey: lastEvaluatedKey,
                    }),
                })

                for (const item of result.Items ?? []) {
                    const partition = item.partition?.S
                    if (!partition || seen.has(partition)) {
                        continue
                    }
                    seen.add(partition)
                    yield partition
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
                const result = await dbRequest<QueryResponse>(this.#context.env, 'Query', {
                    TableName: this.#tableName(table),
                    ...(lastEvaluatedKey !== undefined && {
                        ExclusiveStartKey: lastEvaluatedKey,
                    }),
                    ...queryFromRange(partition, range),
                })

                for (const item of result.Items ?? []) {
                    const key = item.key?.S
                    if (!key || (range && !matchRange(range)(key))) {
                        continue
                    }

                    yield {
                        partition: item.partition?.S ?? '',
                        key,
                        revision: item.revision?.S as unknown,
                        document: JSON.parse(item.document?.S ?? '{}') as unknown,
                    }
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
            const now = new Date().toISOString()
            await dbRequest(this.#context.env, 'UpdateItem', {
                TableName: this.#tableName(table),
                Key: {
                    partition: { S: partition },
                    key: { S: key },
                },
                UpdateExpression:
                    'ADD seq :one SET revision = :newRevision, updated = :now, document = :document',
                ConditionExpression: 'revision = :oldRevision',
                ExpressionAttributeValues: {
                    ':one': { N: '1' },
                    ':oldRevision': { S: currentRevision as string },
                    ':newRevision': { S: newRevision },
                    ':now': { S: now },
                    ':document': { S: JSON.stringify(document) },
                },
            })
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
                await setTimeout(1000)
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
            const deleteParams = {
                TableName: this.#tableName(table),
                Key: {
                    partition: { S: partition },
                    key: { S: key },
                },
                ...(currentRevision !== undefined && {
                    ConditionExpression: 'revision = :oldRevision',
                    ExpressionAttributeValues: {
                        ':oldRevision': { S: currentRevision as string },
                    },
                }),
            }

            await dbRequest(this.#context.env, 'DeleteItem', deleteParams)
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
                } else {
                    return
                }
            }
            throw e
        }
    }

    close() {
        return Promise.resolve()
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
            await dbRequest(this.#context.env, 'CreateTable', {
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
            } else {
                return (key: string) => after <= key
            }
        }
        if (before) {
            return (key: string) => key < before
        }
    }
    return none
}

const none = () => false

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

function isErrorType(error: unknown, type: string) {
    if (!(error instanceof Error)) {
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
