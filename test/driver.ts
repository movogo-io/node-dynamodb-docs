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

    it('rejects expiresAt in milliseconds', async () => {
        const connection = await new Driver().connect(context)
        await assert.rejects(
            connection.add('TtlTestDocs', randomUUID(), randomUUID(), {
                expiresAt: Date.UTC(2030, 0, 1),
            }),
            /epoch seconds/u,
        )
    })

    it('projects numeric expiresAt to a top-level attribute', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const key = randomUUID()
        const revision = await connection.add('TtlTestDocs', partition, key, {
            expiresAt: 1_893_456_000,
        })

        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: { N: '1893456000' },
            document: { S: '{"expiresAt":1893456000}' },
        })

        const updatedRevision = await connection.update('TtlTestDocs', partition, key, revision, {
            expiresAt: 1_893_542_400,
        })
        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: { N: '1893542400' },
            document: { S: '{"expiresAt":1893542400}' },
        })

        await connection.update('TtlTestDocs', partition, key, updatedRevision, {
            expiresAt: '2030-01-01T00:00:00Z',
        })
        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: undefined,
            document: { S: '{"expiresAt":"2030-01-01T00:00:00Z"}' },
        })
    }).timeout(60_000)

    it('projects expiresAt from transactions', async () => {
        const connection = await new Driver().connect(context)
        const partition = randomUUID()
        const key = randomUUID()
        await connection.transact([
            {
                op: 'put',
                table: 'TtlTestDocs',
                partition,
                key,
                document: { expiresAt: 1_893_456_000 },
                newRevision: randomUUID(),
            },
        ])

        assert.deepStrictEqual(await rawItem(partition, key), {
            expiresAt: { N: '1893456000' },
            document: { S: '{"expiresAt":1893456000}' },
        })
    }).timeout(60_000)

    it('enables time to live on created tables', async () => {
        const connection = await new Driver().connect(context)
        await connection.add('TtlTestDocs', randomUUID(), randomUUID(), {})

        const { TimeToLiveDescription } = await dbRequest<{
            TimeToLiveDescription?: { AttributeName?: string; TimeToLiveStatus?: string }
        }>(context.env, 'DescribeTimeToLive', { TableName: 'DocsTests.TtlTestDocs' })

        assert.strictEqual(TimeToLiveDescription?.AttributeName, 'expiresAt')
        assert.ok(
            ['ENABLING', 'ENABLED'].includes(TimeToLiveDescription.TimeToLiveStatus ?? ''),
            TimeToLiveDescription.TimeToLiveStatus,
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
            await connection.add('LargeTestDocs', partition, key, { key, filler })
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
