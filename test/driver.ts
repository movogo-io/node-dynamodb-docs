import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import { setDriver } from '@movogo-io/docs/driver'
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

type Schema = {
    IndexTestDocs: { [partition: string]: { [key: string]: { unitId: string } } }
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
        const revision = await connection.add(
            'TtlTestDocs',
            partition,
            key,
            { data: 'x' },
            { now, expiresAt: now + 60 },
        )

        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: { N: '4000000060' },
            document: { S: '{"data":"x"}' },
        })

        const updatedRevision = await connection.update(
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
        })

        await connection.update(
            'TtlTestDocs',
            partition,
            key,
            updatedRevision,
            { data: 'z' },
            { now },
        )
        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: undefined,
            document: { S: '{"data":"z"}' },
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

async function rawItem(partition: string, key: string) {
    const { Item } = await dbRequest<{
        Item?: { expiresAt?: { N: string }; document?: { S: string } }
    }>(context.env, 'GetItem', {
        TableName: 'DocsTests.TtlTestDocs',
        Key: { partition: { S: partition }, key: { S: key } },
        ProjectionExpression: 'expiresAt, #d',
        ExpressionAttributeNames: { '#d': 'document' },
    })
    return { expiresAt: Item?.expiresAt, document: Item?.document }
}

// Answers every read with an empty result and records what was asked, so a
// test can see the request shape; nothing here reaches a real account.
async function createMockDynamo() {
    const requests: { target: string | string[] | undefined; body: unknown }[] = []
    const server = await new Promise<ReturnType<typeof createServer>>((resolve, reject) => {
        const s = createServer((req, res) => {
            req.setEncoding('utf-8')
            let body = ''
            req.on('data', (chunk: string) => {
                body += chunk
            })
            req.on('end', () => {
                requests.push({ target: req.headers['x-amz-target'], body: JSON.parse(body) })
                res.writeHead(200, { 'content-type': 'application/json' })
                res.end(JSON.stringify(mockResponse(req.headers['x-amz-target'])))
            })
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
        close: () =>
            new Promise<void>((resolve, reject) => {
                server.close(err => {
                    err ? reject(err) : resolve()
                })
            }),
    }
}

function mockResponse(target: string | string[] | undefined) {
    if (target === 'DynamoDB_20120810.GetItem') {
        return {
            Item: {
                partition: { S: 'p' },
                key: { S: 'k' },
                revision: { S: 'r' },
                document: { S: '{}' },
            },
        }
    }
    if (target === 'DynamoDB_20120810.BatchGetItem') {
        return { Responses: {} }
    }
    return { Items: [] }
}
