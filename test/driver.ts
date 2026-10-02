import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { createServer, type IncomingHttpHeaders, type ServerResponse } from 'node:http'
import { setTimeout } from 'node:timers/promises'
import { isConflict, isTransactionTooLarge, transactEach } from '@movogo-io/docs'
import { declaredLimits, setDriver } from '@movogo-io/docs/driver'
import { docs } from '@movogo-io/docs/indexed'
import { harness } from '@movogo-io/docs/test/harness'
import { dbRequest, localAwsEnv, type LocalEnv } from '../lib/aws.js'
import { Driver } from '../lib/dynamo-driver.js'

const context: { env: LocalEnv & { TABLE_PREFIX: string }; on?: undefined } = {
    env: {
        ...(await localAwsEnv()),
        TABLE_PREFIX: 'DocsTests.',
    },
}

const now = 4_000_000_000
// What a write at `now`, and one at `now + 60`, stamps as `created`/`updated`.
const updated = { S: '2096-10-02T07:06:40.000Z' }
const updatedLater = { S: '2096-10-02T07:07:40.000Z' }

type Schema = {
    IndexTestDocs: { [partition: string]: { [key: string]: { unitId: string } } }
    BulkTestDocs: { [partition: string]: { [key: string]: { name: string; count: number } } }
}

const schema = docs<Schema>()
const byUnit = schema.index(
    'IndexTestDocs',
    'byUnit',
    r => r.document.unitId,
    r => r.key,
)

describe('driver', () => {
    harness(
        (message, runner) => {
            it(message, runner).timeout(30_000)
        },
        new Driver(),
        () => context,
    )

    it('rejects a read consistency it does not know', async () => {
        await assert.rejects(
            new Driver().connect({
                env: { ...context.env, AWS_DYNAMODB_READ_CONSISTENCY: 'strong' },
            }),
            { message: "AWS_DYNAMODB_READ_CONSISTENCY must be STRONG or EVENTUAL, not 'strong'." },
        )
    })

    it('reads eventually consistently unless told otherwise', async () => {
        const mock = await createMockDynamo()
        try {
            const connection = await new Driver().connect({
                env: {
                    AWS_REGION: 'eu-north-1',
                    AWS_ACCESS_KEY_ID: 'mock',
                    AWS_SECRET_ACCESS_KEY: 'mock',
                    AWS_DYNAMODB_ENDPOINT: mock.baseUrl,
                    TABLE_PREFIX: 'Mock.',
                },
            })
            await connection.get('Docs', 'p', 'k')
            await connection.getMany('Docs', [{ partition: 'p', key: 'k' }])
            await Array.fromAsync(connection.getPartition('Docs', 'p'))
            await Array.fromAsync(connection.getPartitions('Docs'))

            assert.deepStrictEqual(mock.requests, [
                {
                    target: 'DynamoDB_20120810.GetItem',
                    body: {
                        TableName: 'Mock.Docs',
                        Key: { partition: { S: 'p' }, key: { S: 'k' } },
                    },
                },
                {
                    target: 'DynamoDB_20120810.BatchGetItem',
                    body: {
                        RequestItems: {
                            'Mock.Docs': { Keys: [{ partition: { S: 'p' }, key: { S: 'k' } }] },
                        },
                    },
                },
                {
                    target: 'DynamoDB_20120810.Query',
                    body: {
                        TableName: 'Mock.Docs',
                        KeyConditionExpression: '#p = :p',
                        ExpressionAttributeNames: { '#p': 'partition' },
                        ExpressionAttributeValues: { ':p': { S: 'p' } },
                    },
                },
                {
                    target: 'DynamoDB_20120810.Scan',
                    body: {
                        TableName: 'Mock.Docs',
                        ProjectionExpression: '#p',
                        ExpressionAttributeNames: { '#p': 'partition' },
                    },
                },
            ])
        } finally {
            await mock.close()
        }
    })

    it('names the transaction limit DynamoDB refuses, without retrying', async () => {
        const mock = await createMockDynamo(() => ({
            status: 400,
            body: {
                __type: 'com.amazon.coral.validate#ValidationException',
                message:
                    'Transaction payload size cannot exceed 4MB. Payload Size: : Transaction payload size cannot exceed 4MB. Payload Size: 4306456',
            },
        }))
        try {
            const connection = await new Driver().connect({
                env: {
                    AWS_REGION: 'eu-north-1',
                    AWS_ACCESS_KEY_ID: 'mock',
                    AWS_SECRET_ACCESS_KEY: 'mock',
                    AWS_DYNAMODB_ENDPOINT: mock.baseUrl,
                    TABLE_PREFIX: 'Mock.',
                },
            })

            await assert.rejects(
                connection.transact(
                    [
                        {
                            op: 'add',
                            table: 'Docs',
                            partition: 'p',
                            key: 'k1',
                            document: {},
                            newRevision: 'r1',
                        },
                        {
                            op: 'add',
                            table: 'Docs.byUnit',
                            partition: 'u',
                            key: 'k1',
                            document: {},
                            newRevision: 'r1',
                        },
                    ],
                    { now },
                ),
                {
                    message:
                        "Transaction of 2 operations on 'Docs', 'Docs.byUnit' exceeds the limit of 4 MB per transaction: DynamoDB measured 4306456 bytes.",
                },
            )
            assert.strictEqual(mock.requests.length, 1)
        } finally {
            await mock.close()
        }
    })

    it('names the item limit DynamoDB refuses, without retrying', async () => {
        const mock = await createMockDynamo(() => ({
            status: 400,
            body: {
                __type: 'com.amazon.coral.validate#ValidationException',
                message: 'Item size to update has exceeded the maximum allowed size',
            },
        }))
        try {
            const connection = await new Driver().connect({
                env: {
                    AWS_REGION: 'eu-north-1',
                    AWS_ACCESS_KEY_ID: 'mock',
                    AWS_SECRET_ACCESS_KEY: 'mock',
                    AWS_DYNAMODB_ENDPOINT: mock.baseUrl,
                    TABLE_PREFIX: 'Mock.',
                },
            })

            await assert.rejects(
                connection.transact(
                    [
                        {
                            op: 'add',
                            table: 'Docs',
                            partition: 'p',
                            key: 'k1',
                            document: {},
                            newRevision: 'r1',
                        },
                    ],
                    { now },
                ),
                {
                    message:
                        "Transaction of 1 operations on 'Docs' exceeds the limit of 400 KB per item.",
                },
            )
            assert.strictEqual(mock.requests.length, 1)
        } finally {
            await mock.close()
        }
    })

    it('reads the refusal of a transaction DynamoDB counts over 100 items, which it mislabels as compressed', async () => {
        const echoedItems = Array.from(
            { length: 101 },
            (_, i) =>
                `TransactWriteItem(put=Put(item={document=AttributeValue(s={"name":"Jane Doe ${String(i)}"})}))`,
        ).join(', ')
        const mock = await createMockDynamo((_target, headers) => ({
            status: 400,
            headers: headers['accept-encoding']?.includes('gzip')
                ? { 'content-encoding': 'gzip' }
                : undefined,
            body: {
                __type: 'com.amazon.coral.validate#ValidationException',
                message: `1 validation error detected: Value '[${echoedItems}]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100`,
            },
        }))
        try {
            const connection = await new Driver().connect({
                env: {
                    AWS_REGION: 'eu-north-1',
                    AWS_ACCESS_KEY_ID: 'mock',
                    AWS_SECRET_ACCESS_KEY: 'mock',
                    AWS_DYNAMODB_ENDPOINT: mock.baseUrl,
                    TABLE_PREFIX: 'Mock.',
                },
            })

            await assert.rejects(
                connection.transact(
                    Array.from({ length: 100 }, (_, i) => ({
                        op: 'add',
                        table: 'Docs',
                        partition: 'p',
                        key: `k${String(i)}`,
                        document: { name: `Jane Doe ${String(i)}` },
                        newRevision: `r${String(i)}`,
                    })),
                    { now },
                ),
                (e: unknown) => {
                    assert.deepStrictEqual(
                        e,
                        Object.assign(
                            new Error(
                                "Transaction of 100 operations on 'Docs' exceeds the limit of 100 operations per transaction.",
                            ),
                            { code: 'docs.transaction_too_large' },
                        ),
                    )
                    return true
                },
            )
            assert.strictEqual(mock.requests.length, 1)
        } finally {
            await mock.close()
        }
    })

    it('passes other invalid input on, reduced to what the driver reads', async () => {
        const mock = await createMockDynamo(() => ({
            status: 400,
            body: {
                __type: 'com.amazon.coral.validate#ValidationException',
                message: `${'A'.repeat(200)} {"name":"Jane Doe"} ${'B'.repeat(300)}`,
                echoedRequest: { name: 'Jane Doe' },
            },
        }))
        try {
            const connection = await new Driver().connect({
                env: {
                    AWS_REGION: 'eu-north-1',
                    AWS_ACCESS_KEY_ID: 'mock',
                    AWS_SECRET_ACCESS_KEY: 'mock',
                    AWS_DYNAMODB_ENDPOINT: mock.baseUrl,
                    TABLE_PREFIX: 'Mock.',
                },
            })

            await assert.rejects(
                connection.transact(
                    [
                        {
                            op: 'add',
                            table: 'Docs',
                            partition: 'p',
                            key: 'k1',
                            document: {},
                            newRevision: 'r1',
                        },
                    ],
                    { now },
                ),
                (e: unknown) => {
                    assert.deepStrictEqual(
                        e,
                        Object.assign(new Error('Error fetching DynamoDB'), {
                            response: {
                                url: mock.baseUrl,
                                status: 400,
                                body: JSON.stringify({
                                    __type: 'com.amazon.coral.validate#ValidationException',
                                    message: `${'A'.repeat(128)}…${'B'.repeat(256)}`,
                                }),
                            },
                            target: 'DynamoDB_20120810.TransactWriteItems',
                        }),
                    )
                    return true
                },
            )
        } finally {
            await mock.close()
        }
    })

    it('opens no more connections than its bound, however many requests are in flight', async () => {
        const mock = await createMockDynamo(async (target, headers) => {
            await setTimeout(5)
            return mockResponse(target, headers)
        })
        try {
            const connection = await new Driver({ requestsInFlightMax: 4 }).connect({
                env: mockEnv(mock.baseUrl),
            })

            const rows = await Promise.all(
                Array.from({ length: 100 }, (_, i) => connection.get('Docs', 'p', `k${String(i)}`)),
            )

            assert.strictEqual(rows.length, 100)
            assert.strictEqual(mock.connections.opened, 4)
        } finally {
            await mock.close()
        }
    })

    it('declares its bound on requests in flight and its transaction limit', async () => {
        const bounded = await new Driver({ requestsInFlightMax: 8 }).connect({ env: mockEnv('') })
        const plain = await new Driver().connect({ env: mockEnv('') })

        assert.strictEqual(bounded.requestsInFlightMax, 8)
        assert.strictEqual(plain.requestsInFlightMax, 64)
        assert.strictEqual(bounded.transactionItemsMax, 100)
    })

    it('gives up a request that is not answered in time', async () => {
        const mock = await createMockDynamo(neverAnswered)
        try {
            const connection = await new Driver({ requestTimeoutMs: 50 }).connect({
                env: mockEnv(mock.baseUrl),
            })

            await assert.rejects(connection.get('Docs', 'p', 'k'), {
                message: 'DynamoDB did not answer within 50 ms.',
                unanswered: true,
            })
        } finally {
            await mock.close()
        }
    })

    it('aborts a read with the signal of the context, and leaves a write to finish', async () => {
        const mock = await createMockDynamo()
        try {
            const connection = await new Driver().connect({
                env: mockEnv(mock.baseUrl),
                signal: AbortSignal.abort(),
            })

            await assert.rejects(connection.get('Docs', 'p', 'k'), { name: 'AbortError' })
            await connection.delete('Docs', 'p', 'k', 'r', { now })

            assert.deepStrictEqual(
                mock.requests.map(request => request.target),
                ['DynamoDB_20120810.DeleteItem'],
            )
        } finally {
            await mock.close()
        }
    })

    it('sends a transaction again under the same token when its reply is lost', async () => {
        let requested = 0
        const mock = await createMockDynamo(() => {
            ++requested
            if (requested === 1) {
                return {
                    status: 500,
                    body: {
                        __type: 'com.amazonaws.dynamodb.v20120810#InternalServerError',
                        message: 'Internal server error',
                    },
                }
            }
            return { status: 200, body: {} }
        })
        try {
            const connection = await new Driver().connect({ env: mockEnv(mock.baseUrl) })

            await connection.transact(
                [{ op: 'check', table: 'Docs', partition: 'p', key: 'k', revision: 'r' }],
                { now },
            )

            const tokens = mock.requests.map(
                request => (request.body as { ClientRequestToken: string }).ClientRequestToken,
            )
            assert.strictEqual(tokens.length, 2)
            assert.strictEqual(tokens[0], tokens[1])
        } finally {
            await mock.close()
        }
    })

    it('refuses more operations than a transaction takes before sending any', async () => {
        const mock = await createMockDynamo()
        try {
            const connection = await new Driver().connect({ env: mockEnv(mock.baseUrl) })

            await assert.rejects(
                connection.transact(
                    Array.from({ length: 101 }, (_, i) => ({
                        op: 'check',
                        table: 'Docs',
                        partition: 'p',
                        key: `k${String(i)}`,
                        revision: 'r',
                    })),
                    { now },
                ),
                {
                    message:
                        "Transaction of 101 operations on 'Docs' exceeds the limit of 100 operations per transaction.",
                    code: 'docs.transaction_too_large',
                },
            )
            assert.deepStrictEqual(mock.requests, [])
        } finally {
            await mock.close()
        }
    })

    it('backs off a throttled transaction of many items', async () => {
        const codes = Array.from({ length: 100 }, (_, i) => (i === 99 ? 'ThrottlingError' : 'None'))
        let requested = 0
        const mock = await createMockDynamo(target => {
            if (target !== 'DynamoDB_20120810.TransactWriteItems') {
                return mockResponse(target, {})
            }
            ++requested
            if (requested === 1) {
                return {
                    status: 400,
                    body: {
                        __type: 'com.amazonaws.dynamodb.v20120810#TransactionCanceledException',
                        message: `Transaction cancelled, please refer cancellation reasons for specific reasons [${codes.join(', ')}]`,
                        CancellationReasons: codes.map(Code =>
                            Code === 'None'
                                ? { Code }
                                : {
                                      Code,
                                      Message:
                                          'Throughput exceeds the current capacity of your table or index.',
                                  },
                        ),
                    },
                }
            }
            return { status: 200, body: {} }
        })
        try {
            const connection = await new Driver().connect({
                env: {
                    AWS_REGION: 'eu-north-1',
                    AWS_ACCESS_KEY_ID: 'mock',
                    AWS_SECRET_ACCESS_KEY: 'mock',
                    AWS_DYNAMODB_ENDPOINT: mock.baseUrl,
                    TABLE_PREFIX: 'Mock.',
                },
            })

            await connection.transact(
                codes.map((_, i) => ({
                    op: 'check',
                    table: 'Docs',
                    partition: 'p',
                    key: `k${String(i)}`,
                    revision: 'r',
                })),
                { now },
            )

            assert.strictEqual(requested, 2)
        } finally {
            await mock.close()
        }
    })

    it('sees a lost race in a transaction of many items as a conflict', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()

        await assert.rejects(
            connection.transact(
                Array.from({ length: 30 }, (_, i) => ({
                    op: 'check',
                    table: 'IndexTestDocs',
                    partition,
                    key: `k${String(i)}`,
                    revision: randomUUID(),
                })),
                { now },
            ),
            isConflict,
        )
    })

    it('reads seq 0 and the epoch from an item written without them', async () => {
        const mock = await createMockDynamo()
        try {
            const connection = await new Driver().connect({
                env: {
                    AWS_REGION: 'eu-north-1',
                    AWS_ACCESS_KEY_ID: 'mock',
                    AWS_SECRET_ACCESS_KEY: 'mock',
                    AWS_DYNAMODB_ENDPOINT: mock.baseUrl,
                    TABLE_PREFIX: 'Mock.',
                },
            })
            assert.deepStrictEqual(await connection.get('Docs', 'p', 'k'), {
                partition: 'p',
                key: 'k',
                revision: 'r',
                document: {},
                seq: 0,
                updatedAt: '1970-01-01T00:00:00.000Z',
            })
        } finally {
            await mock.close()
        }
    })

    it('reads strongly consistently when the caller asks for that one read', async () => {
        const mock = await createMockDynamo()
        try {
            const connection = await new Driver().connect({
                env: {
                    AWS_REGION: 'eu-north-1',
                    AWS_ACCESS_KEY_ID: 'mock',
                    AWS_SECRET_ACCESS_KEY: 'mock',
                    AWS_DYNAMODB_ENDPOINT: mock.baseUrl,
                    TABLE_PREFIX: 'Mock.',
                },
            })
            await connection.get('Docs', 'p', 'k', { consistent: true })
            await connection.getMany('Docs', [{ partition: 'p', key: 'k' }], { consistent: true })
            await Array.fromAsync(
                connection.getPartition('Docs', 'p', undefined, { consistent: true }),
            )
            await connection.get('Docs', 'p', 'k', { consistent: false })
            await connection.get('Docs', 'p', 'k')

            assert.deepStrictEqual(mock.requests, [
                {
                    target: 'DynamoDB_20120810.GetItem',
                    body: {
                        TableName: 'Mock.Docs',
                        ConsistentRead: true,
                        Key: { partition: { S: 'p' }, key: { S: 'k' } },
                    },
                },
                {
                    target: 'DynamoDB_20120810.BatchGetItem',
                    body: {
                        RequestItems: {
                            'Mock.Docs': {
                                Keys: [{ partition: { S: 'p' }, key: { S: 'k' } }],
                                ConsistentRead: true,
                            },
                        },
                    },
                },
                {
                    target: 'DynamoDB_20120810.Query',
                    body: {
                        TableName: 'Mock.Docs',
                        ConsistentRead: true,
                        KeyConditionExpression: '#p = :p',
                        ExpressionAttributeNames: { '#p': 'partition' },
                        ExpressionAttributeValues: { ':p': { S: 'p' } },
                    },
                },
                {
                    target: 'DynamoDB_20120810.GetItem',
                    body: {
                        TableName: 'Mock.Docs',
                        Key: { partition: { S: 'p' }, key: { S: 'k' } },
                    },
                },
                {
                    target: 'DynamoDB_20120810.GetItem',
                    body: {
                        TableName: 'Mock.Docs',
                        Key: { partition: { S: 'p' }, key: { S: 'k' } },
                    },
                },
            ])
        } finally {
            await mock.close()
        }
    })

    it('reads strongly consistently when the service opts in', async () => {
        const mock = await createMockDynamo()
        try {
            const connection = await new Driver().connect({
                env: {
                    AWS_REGION: 'eu-north-1',
                    AWS_ACCESS_KEY_ID: 'mock',
                    AWS_SECRET_ACCESS_KEY: 'mock',
                    AWS_DYNAMODB_ENDPOINT: mock.baseUrl,
                    TABLE_PREFIX: 'Mock.',
                    AWS_DYNAMODB_READ_CONSISTENCY: 'STRONG',
                },
            })
            await connection.get('Docs', 'p', 'k')
            await connection.getMany('Docs', [{ partition: 'p', key: 'k' }])
            await Array.fromAsync(connection.getPartition('Docs', 'p'))
            await Array.fromAsync(connection.getPartitions('Docs'))

            assert.deepStrictEqual(mock.requests, [
                {
                    target: 'DynamoDB_20120810.GetItem',
                    body: {
                        TableName: 'Mock.Docs',
                        ConsistentRead: true,
                        Key: { partition: { S: 'p' }, key: { S: 'k' } },
                    },
                },
                {
                    target: 'DynamoDB_20120810.BatchGetItem',
                    body: {
                        RequestItems: {
                            'Mock.Docs': {
                                Keys: [{ partition: { S: 'p' }, key: { S: 'k' } }],
                                ConsistentRead: true,
                            },
                        },
                    },
                },
                {
                    target: 'DynamoDB_20120810.Query',
                    body: {
                        TableName: 'Mock.Docs',
                        ConsistentRead: true,
                        KeyConditionExpression: '#p = :p',
                        ExpressionAttributeNames: { '#p': 'partition' },
                        ExpressionAttributeValues: { ':p': { S: 'p' } },
                    },
                },
                {
                    target: 'DynamoDB_20120810.Scan',
                    body: {
                        TableName: 'Mock.Docs',
                        ConsistentRead: true,
                        ProjectionExpression: '#p',
                        ExpressionAttributeNames: { '#p': 'partition' },
                    },
                },
            ])
        } finally {
            await mock.close()
        }
    })

    it('reads a whole partition through an empty prefix', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        await connection.add('DocsTests', partition, 'a', { n: 1 }, { now })
        await connection.add('DocsTests', partition, 'b', { n: 2 }, { now })
        assert.deepStrictEqual(
            await Array.fromAsync(
                connection.getPartition('DocsTests', partition, { withPrefix: '' }),
                r => r.key,
            ),
            ['a', 'b'],
        )
    }).timeout(30_000)

    // The conformance harness covers what `getMany` answers; what is DynamoDB's
    // alone is the 100-key limit of BatchGetItem, which a list longer than that
    // must be split across, and the expiry riding along on each item.
    it('reads more keys than one batch holds, across partitions', async () => {
        const connection = await new Driver().connect(context)
        const partitions = [randomUUID(), randomUUID()]
        const refs = Array.from({ length: 101 }, (_, i) => ({
            partition: partitions[i % 2] ?? '',
            key: `k${i.toString().padStart(3, '0')}`,
        }))
        await Promise.all(
            refs.map(ref =>
                connection.add('DocsTests', ref.partition, ref.key, { n: ref.key }, { now }),
            ),
        )

        const found = await connection.getMany('DocsTests', [
            ...refs,
            { partition: partitions[0] ?? '', key: 'absent' },
        ])
        assert.deepStrictEqual(
            found.map(row => row.key).sort((a, b) => a.localeCompare(b)),
            refs.map(ref => ref.key).sort((a, b) => a.localeCompare(b)),
        )
        assert.deepStrictEqual(
            found.map(row => JSON.stringify(row.document)).sort((a, b) => a.localeCompare(b)),
            refs.map(ref => JSON.stringify({ n: ref.key })).sort((a, b) => a.localeCompare(b)),
        )
    }).timeout(120_000)

    it('reads the expiry of a batch, and nothing of a table never written to', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        await connection.add('TtlTestDocs', partition, 'a', { n: 1 }, { now, expiresAt: now + 60 })
        await connection.add('TtlTestDocs', partition, 'b', { n: 2 }, { now })

        assert.deepStrictEqual(
            (
                await connection.getMany('TtlTestDocs', [
                    { partition, key: 'a' },
                    { partition, key: 'b' },
                ])
            )
                .map(row => [row.key, row.expiresAt])
                .sort(([a], [b]) => String(a).localeCompare(String(b))),
            [
                ['a', now + 60],
                ['b', undefined],
            ],
        )
        assert.deepStrictEqual(
            await connection.getMany(`Absent${randomUUID().replaceAll('-', '')}`, [
                { partition, key: 'a' },
            ]),
            [],
        )
    }).timeout(60_000)

    it('stores the expiry as a numeric top-level attribute', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const key = randomUUID()
        const { revision } = await connection.add(
            'TtlTestDocs',
            partition,
            key,
            { data: 'x' },
            { now, expiresAt: now + 60 },
        )

        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: { N: '4000000060' },
            document: { S: '{"data":"x"}' },
            seq: { N: '0' },
            created: updated,
            updated,
        })

        const { revision: updatedRevision } = await connection.update(
            'TtlTestDocs',
            partition,
            key,
            revision,
            { data: 'y' },
            { now, expiresAt: now + 120 },
        )
        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: { N: '4000000120' },
            document: { S: '{"data":"y"}' },
            seq: { N: '1' },
            created: updated,
            updated,
        })

        await connection.update(
            'TtlTestDocs',
            partition,
            key,
            updatedRevision,
            { data: 'z' },
            { now: now + 60 },
        )
        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: undefined,
            document: { S: '{"data":"z"}' },
            seq: { N: '2' },
            created: updated,
            updated: updatedLater,
        })
    }).timeout(60_000)

    it('answers a write with what it stored', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const key = randomUUID()
        const added = await connection.add('TtlTestDocs', partition, key, { n: 1 }, { now })
        assert.deepStrictEqual(added, {
            revision: added.revision,
            seq: 0,
            updatedAt: updated.S,
        })
        const updatedRow = await connection.update(
            'TtlTestDocs',
            partition,
            key,
            added.revision,
            { n: 2 },
            { now: now + 60 },
        )
        assert.deepStrictEqual(updatedRow, {
            revision: updatedRow.revision,
            seq: 1,
            updatedAt: updatedLater.S,
        })
        assert.notStrictEqual(updatedRow.revision, added.revision)
        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: undefined,
            document: { S: '{"n":2}' },
            seq: { N: '1' },
            created: updated,
            updated: updatedLater,
        })
    }).timeout(60_000)

    it('removes a deleted item outright, and re-adds its key from seq 0', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const key = randomUUID()
        const added = await connection.add('TtlTestDocs', partition, key, { n: 1 }, { now })
        const { revision } = await connection.update(
            'TtlTestDocs',
            partition,
            key,
            added.revision,
            { n: 2 },
            { now },
        )
        await connection.delete('TtlTestDocs', partition, key, revision, { now })

        assert.deepStrictEqual(await rawItem(partition, key), undefined)
        assert.deepStrictEqual(await rawPartition(partition), [])
        await assert.rejects(connection.get('TtlTestDocs', partition, key), { statusCode: 404 })
        assert.deepStrictEqual(
            await Array.fromAsync(connection.getPartition('TtlTestDocs', partition)),
            [],
        )
        assert.deepStrictEqual(await connection.getMany('TtlTestDocs', [{ partition, key }]), [])

        const reAdded = await connection.add(
            'TtlTestDocs',
            partition,
            key,
            { n: 3 },
            { now: now + 60 },
        )
        assert.deepStrictEqual(reAdded, {
            revision: reAdded.revision,
            seq: 0,
            updatedAt: updatedLater.S,
        })
        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: undefined,
            document: { S: '{"n":3}' },
            seq: { N: '0' },
            created: updatedLater,
            updated: updatedLater,
        })
    }).timeout(60_000)

    it('removes transacted deletes and clears outright, creating nothing for a clear', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const key = randomUUID()
        const clearedKey = randomUUID()
        const unwrittenKey = randomUUID()
        const { revision } = await connection.add('TtlTestDocs', partition, key, { n: 1 }, { now })
        await connection.add('TtlTestDocs', partition, clearedKey, { n: 1 }, { now })
        await connection.transact(
            [
                { op: 'delete', table: 'TtlTestDocs', partition, key, revision },
                { op: 'clear', table: 'TtlTestDocs', partition, key: clearedKey },
                { op: 'clear', table: 'TtlTestDocs', partition, key: unwrittenKey },
            ],
            { now },
        )

        assert.deepStrictEqual(await rawItem(partition, key), undefined)
        assert.deepStrictEqual(await rawItem(partition, clearedKey), undefined)
        assert.deepStrictEqual(await rawItem(partition, unwrittenKey), undefined)
        assert.deepStrictEqual(await rawPartition(partition), [])
    }).timeout(60_000)

    it('continues seq over an expired item', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const key = randomUUID()
        const added = await connection.add(
            'TtlTestDocs',
            partition,
            key,
            { n: 1 },
            { now, expiresAt: now + 60 },
        )
        await connection.update(
            'TtlTestDocs',
            partition,
            key,
            added.revision,
            { n: 2 },
            { now, expiresAt: now + 60 },
        )
        const reAdded = await connection.add(
            'TtlTestDocs',
            partition,
            key,
            { n: 3 },
            { now: now + 60 },
        )
        assert.deepStrictEqual(reAdded, {
            revision: reAdded.revision,
            seq: 2,
            updatedAt: updatedLater.S,
        })
        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: undefined,
            document: { S: '{"n":3}' },
            seq: { N: '2' },
            created: updatedLater,
            updated: updatedLater,
        })
    }).timeout(60_000)

    it('continues seq through updates and a transacted put', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const key = randomUUID()
        const added = await connection.add('TtlTestDocs', partition, key, { n: 1 }, { now })
        await connection.update('TtlTestDocs', partition, key, added.revision, { n: 2 }, { now })
        assert.deepStrictEqual((await rawItem(partition, key))?.seq, { N: '1' })
        await connection.transact(
            [
                {
                    op: 'put',
                    table: 'TtlTestDocs',
                    partition,
                    key,
                    document: { n: 3 },
                    newRevision: randomUUID(),
                },
            ],
            { now: now + 60 },
        )
        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: undefined,
            document: { S: '{"n":3}' },
            seq: { N: '2' },
            created: updated,
            updated: updatedLater,
        })
    }).timeout(60_000)

    it('stores the expiry from transactions', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const key = randomUUID()
        await connection.transact(
            [
                {
                    op: 'put',
                    table: 'TtlTestDocs',
                    partition,
                    key,
                    document: { data: 'x' },
                    newRevision: randomUUID(),
                    expiresAt: now + 60,
                },
            ],
            { now },
        )

        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: { N: '4000000060' },
            document: { S: '{"data":"x"}' },
            seq: { N: '0' },
            created: updated,
            updated,
        })

        await connection.transact(
            [
                {
                    op: 'put',
                    table: 'TtlTestDocs',
                    partition,
                    key,
                    document: { data: 'y' },
                    newRevision: randomUUID(),
                },
            ],
            { now: now + 60 },
        )

        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: undefined,
            document: { S: '{"data":"y"}' },
            seq: { N: '1' },
            created: updated,
            updated: updatedLater,
        })
    }).timeout(60_000)

    it('enables time to live on created tables', async () => {
        const connection = await new Driver().connect(context)
        await connection.add('TtlTestDocs', randomUUID(), randomUUID(), {}, { now })

        const { TimeToLiveDescription } = await dbRequest<{
            TimeToLiveDescription?: { AttributeName?: string; TimeToLiveStatus?: string }
        }>(context.env, 'DescribeTimeToLive', { TableName: 'DocsTests.TtlTestDocs' })

        assert.strictEqual(TimeToLiveDescription?.AttributeName, 'expiresAt')
        assert.ok(
            ['ENABLING', 'ENABLED'].includes(TimeToLiveDescription.TimeToLiveStatus ?? ''),
            TimeToLiveDescription.TimeToLiveStatus,
        )
    }).timeout(60_000)

    it('enables point-in-time recovery on created tables when configured', async () => {
        const connection = await new Driver().connect({
            env: { ...context.env, AWS_DYNAMODB_POINT_IN_TIME_RECOVERY: 'true' },
        })
        await connection.add('PitrTestDocs', randomUUID(), randomUUID(), {}, { now })

        const { ContinuousBackupsDescription } = await dbRequest<{
            ContinuousBackupsDescription?: {
                PointInTimeRecoveryDescription?: { PointInTimeRecoveryStatus?: string }
            }
        }>(context.env, 'DescribeContinuousBackups', { TableName: 'DocsTests.PitrTestDocs' })

        assert.strictEqual(
            ContinuousBackupsDescription?.PointInTimeRecoveryDescription?.PointInTimeRecoveryStatus,
            'ENABLED',
        )
    }).timeout(60_000)

    it('leaves point-in-time recovery off by default', async () => {
        const connection = await new Driver().connect(context)
        await connection.add('TtlTestDocs', randomUUID(), randomUUID(), {}, { now })

        const { ContinuousBackupsDescription } = await dbRequest<{
            ContinuousBackupsDescription?: {
                PointInTimeRecoveryDescription?: { PointInTimeRecoveryStatus?: string }
            }
        }>(context.env, 'DescribeContinuousBackups', { TableName: 'DocsTests.TtlTestDocs' })

        assert.strictEqual(
            ContinuousBackupsDescription?.PointInTimeRecoveryDescription?.PointInTimeRecoveryStatus,
            'DISABLED',
        )
    }).timeout(60_000)

    it('moves index entries when the indexed value changes', async () => {
        const previous = setDriver(new Driver())
        try {
            await using tables = schema.tables(context)
            await using index = byUnit(context)
            const partition = randomUUID()
            const key = randomUUID()
            const firstUnit = randomUUID()
            const secondUnit = randomUUID()

            const revision = await tables.IndexTestDocs.partition(partition).add(key, {
                unitId: firstUnit,
            })
            assert.deepStrictEqual(await index.partition(firstUnit).first(key), {
                key,
                revision,
                document: { unitId: firstUnit },
                source: { partition, key },
            })

            const movedRevision = await tables.IndexTestDocs.partition(partition).update(
                key,
                revision,
                { unitId: secondUnit },
            )
            assert.deepStrictEqual(await index.partition(firstUnit).first(key), undefined)
            assert.deepStrictEqual(await index.partition(secondUnit).first(key), {
                key,
                revision: movedRevision,
                document: { unitId: secondUnit },
                source: { partition, key },
            })
        } finally {
            setDriver(previous)
        }
    }).timeout(90_000)

    it('answers the bounds it declares to the store', async () => {
        const previous = setDriver(new Driver({ requestsInFlightMax: 4 }))
        try {
            assert.deepStrictEqual(await declaredLimits(context), {
                requestsInFlightMax: 4,
                transactionItemsMax: 100,
            })
        } finally {
            setDriver(previous)
        }
    })

    it('has the store write in windows of the bound it declares', async () => {
        const previous = setDriver(new Driver({ requestsInFlightMax: 4 }))
        try {
            await using stored = schema.tables(context)
            const partition = randomUUID()
            const keys = Array.from({ length: 10 }, (_, i) => `k${String(i)}`)
            let running = 0
            let runningPeak = 0

            await transactEach<Schema, string>(context, keys, async (tx, key) => {
                running += 1
                runningPeak = Math.max(runningPeak, running)
                try {
                    await tx.BulkTestDocs.partition(partition).add(key, { name: key, count: 1 })
                    await setTimeout(20)
                } finally {
                    running -= 1
                }
            })

            assert.strictEqual(runningPeak, 4)
            assert.deepStrictEqual(
                await Array.fromAsync(
                    stored.BulkTestDocs.partition(partition).getAll(),
                    row => row.key,
                ),
                ['k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9'],
            )
        } finally {
            setDriver(previous)
        }
    }).timeout(90_000)

    it('has the store apply two units that write one document in item order', async () => {
        const previous = setDriver(new Driver())
        try {
            await using stored = schema.tables(context)
            const partition = randomUUID()
            await stored.BulkTestDocs.partition(partition).add('k', { name: '', count: 0 })
            const runs: string[] = []

            await transactEach<Schema, string>(
                context,
                ['first', 'second'],
                async (tx, name) => {
                    runs.push(name)
                    const row = await tx.BulkTestDocs.partition(partition).get('k', {
                        consistent: true,
                    })
                    await tx.BulkTestDocs.partition(partition).update('k', row.revision, {
                        name,
                        count: row.document.count + 1,
                    })
                },
                { retries: 0 },
            )

            assert.deepStrictEqual(runs, ['first', 'second', 'second'])
            assert.deepStrictEqual(
                (await stored.BulkTestDocs.partition(partition).get('k', { consistent: true }))
                    .document,
                { name: 'second', count: 2 },
            )
        } finally {
            setDriver(previous)
        }
    }).timeout(90_000)

    it('has the store refuse a unit of more than 100 operations and commit the others', async () => {
        const previous = setDriver(new Driver())
        try {
            await using stored = schema.tables(context)
            const partition = randomUUID()

            await assert.rejects(
                transactEach<Schema, number>(context, [1, 101], async (tx, count) => {
                    for (let i = 0; i !== count; ++i) {
                        await tx.BulkTestDocs.partition(partition).add(
                            `k${String(count)}-${String(i)}`,
                            { name: 'a', count: i },
                        )
                    }
                }),
                isTransactionTooLarge,
            )

            assert.deepStrictEqual(
                await Array.fromAsync(
                    stored.BulkTestDocs.partition(partition).getAll(),
                    row => row.key,
                ),
                ['k1-0'],
            )
        } finally {
            setDriver(previous)
        }
    }).timeout(90_000)

    it('pages ordered ranges past one megabyte', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const filler = 'x'.repeat(250_000)
        for (const key of ['03', '01', '05', '02', '04']) {
            await connection.add('LargeTestDocs', partition, key, { key, filler }, { now })
        }

        const rows = await Array.fromAsync(connection.getPartition('LargeTestDocs', partition))
        assert.deepStrictEqual(
            rows.map(row => row.key),
            ['01', '02', '03', '04', '05'],
        )
        assert.deepStrictEqual(
            rows.map(row => row.document),
            ['01', '02', '03', '04', '05'].map(key => ({ key, filler })),
        )
    }).timeout(120_000)
})

// The item as DynamoDB holds it, or undefined when there is none at all.
async function rawItem(partition: string, key: string) {
    const { Item } = await dbRequest<{
        Item?: {
            expiresAt?: { N: string }
            document?: { S: string }
            seq?: { N: string }
            created?: { S: string }
            updated?: { S: string }
        }
    }>(context.env, 'GetItem', {
        TableName: 'DocsTests.TtlTestDocs',
        Key: { partition: { S: partition }, key: { S: key } },
        ProjectionExpression: 'expiresAt, #d, seq, created, updated',
        ExpressionAttributeNames: { '#d': 'document' },
        ConsistentRead: true,
    })
    if (Item === undefined) {
        return undefined
    }
    return {
        expiresAt: Item.expiresAt,
        document: Item.document,
        seq: Item.seq,
        created: Item.created,
        updated: Item.updated,
    }
}

function mockEnv(baseUrl: string) {
    return {
        AWS_REGION: 'eu-north-1',
        AWS_ACCESS_KEY_ID: 'mock',
        AWS_SECRET_ACCESS_KEY: 'mock',
        AWS_DYNAMODB_ENDPOINT: baseUrl,
        TABLE_PREFIX: 'Mock.',
    }
}

// Every item of the partition as DynamoDB holds it, whatever its attributes.
async function rawPartition(partition: string) {
    const { Items } = await dbRequest<{ Items?: { key?: { S: string } }[] }>(context.env, 'Query', {
        TableName: 'DocsTests.TtlTestDocs',
        KeyConditionExpression: '#p = :p',
        ExpressionAttributeNames: { '#p': 'partition' },
        ExpressionAttributeValues: { ':p': { S: partition } },
        ConsistentRead: true,
    })
    return (Items ?? []).map(item => item.key?.S)
}

// Answers every read with an empty result and records what was asked, so a
// test can see the request shape; nothing here reaches a real account. A
// responder may answer later, or never: `close` drops what is still open.
async function createMockDynamo(
    respond: (
        target: string | string[] | undefined,
        headers: IncomingHttpHeaders,
    ) => MockResponse | Promise<MockResponse> = mockResponse,
) {
    const requests: { target: string | string[] | undefined; body: unknown }[] = []
    const replies: Promise<void>[] = []
    const connections = { opened: 0 }
    const server = await new Promise<ReturnType<typeof createServer>>((resolve, reject) => {
        const s = createServer((req, res) => {
            req.setEncoding('utf-8')
            let body = ''
            req.on('data', (chunk: string) => {
                body += chunk
            })
            req.on('end', () => {
                requests.push({ target: req.headers['x-amz-target'], body: JSON.parse(body) })
                replies.push(reply(res, respond(req.headers['x-amz-target'], req.headers)))
            })
        })
        s.on('connection', () => {
            connections.opened += 1
        })
        s.on('error', reject)
        s.listen(0, '127.0.0.1', () => {
            resolve(s)
        })
    })
    const address = server.address()
    if (address === null || typeof address === 'string') {
        throw new Error('Mock server did not bind to a TCP port.')
    }
    return {
        baseUrl: `http://127.0.0.1:${address.port.toString()}/`,
        requests,
        connections,
        close: () =>
            new Promise<void>((resolve, reject) => {
                server.closeAllConnections()
                server.close(err => {
                    if (err) {
                        reject(err)
                    } else {
                        resolve()
                    }
                })
            }),
    }
}

type MockResponse = { status: number; headers?: { [name: string]: string }; body: unknown }

async function reply(res: ServerResponse, response: MockResponse | Promise<MockResponse>) {
    try {
        const { status, headers, body } = await response
        res.writeHead(status, { 'content-type': 'application/json', ...headers })
        res.end(JSON.stringify(body))
    } catch (e) {
        res.destroy(Error.isError(e) ? e : undefined)
    }
}

function neverAnswered() {
    return new Promise<MockResponse>(() => {
        // Left pending: the request is never answered.
    })
}

function mockResponse(
    target: string | string[] | undefined,
    _headers: IncomingHttpHeaders,
): MockResponse {
    if (target === 'DynamoDB_20120810.GetItem') {
        return {
            status: 200,
            body: {
                Item: {
                    partition: { S: 'p' },
                    key: { S: 'k' },
                    revision: { S: 'r' },
                    document: { S: '{}' },
                },
            },
        }
    }
    if (target === 'DynamoDB_20120810.BatchGetItem') {
        return { status: 200, body: { Responses: {} } }
    }
    return { status: 200, body: { Items: [] } }
}
