import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
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
