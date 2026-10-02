// MEASUREMENT SCRIPT. Finds the bound on requests in flight the driver should
// default to (`requestsInFlightDefault` in ../lib/aws.ts), by running the same
// burst against a real table at several bounds and reporting, for each, what
// the bound is there to prevent and what it costs:
//
//   TABLE_PREFIX=staging.measure. AWS_PROFILE=staging node bin/measure-in-flight.js \
//       [--rows 500] [--bounds 8,16,32,64] [--alongside https://host/path]...
//
// For every bound it runs two bursts of `--rows` concurrent operations on one
// partition, the shape of a tenant-sized list in one handler: reads (GetItem),
// then single-row transactions (TransactWriteItems). Each burst runs in a
// process of its own, so its connections are cold: every one pays its TLS
// handshake and its DNS lookup, as after a deploy. `--alongside` fetches that
// URL once per row at the same moment through fetch's own unbounded
// dispatcher, standing in for what else a handler sends at a cold start (its
// emits, a sibling call): lookups are a resource of the whole process.
//
// It prints one JSON line per burst and a table at the end:
//   sockets        connections the driver opened; at most the bound
//   connectErrors  failed connections by code; EBUSY or ENOTFOUND here is the outage
//   statuses       replies by HTTP status; a 400 is a throttle, retried by the driver
//   failed         operations that threw after the driver's own retries
//   wallMs         the whole burst; p50Ms, p99Ms, maxMs per operation, waits included
//
// It needs TABLE_PREFIX, so it cannot write beside a service's tables by
// accident, and credentials from the environment or the AWS_PROFILE section of
// the credentials file. It creates the table `<TABLE_PREFIX>InFlightMeasure`
// on first use, removes its rows when done, and leaves the empty table:
//   AWS dynamodb delete-table --table-name <TABLE_PREFIX>InFlightMeasure
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { subscribe } from 'node:diagnostics_channel'
import { pathToFileURL } from 'node:url'
import { parseArgs } from 'node:util'
import { localAwsEnv } from '../lib/aws.js'
import { Driver } from '../lib/dynamo-driver.js'

const table = 'InFlightMeasure'
const partition = 'tenant'
const workloads = ['reads', 'transactions']

async function main() {
    const { values } = parseArgs({
        options: {
            rows: { type: 'string', default: '500' },
            bounds: { type: 'string', default: '8,16,32,64' },
            alongside: { type: 'string', multiple: true, default: [] },
            run: { type: 'string' },
        },
    })
    const rows = positive(values.rows, '--rows')
    if (values.run !== undefined) {
        console.log(JSON.stringify(await burst(values.run, rows, values.alongside)))
        return
    }
    const bounds = values.bounds.split(',').map(bound => positive(bound, '--bounds'))

    const connection = await new Driver().connect({ env: await measureEnv() })
    await seed(connection, rows)
    try {
        const results: unknown[] = []
        for (const bound of bounds) {
            for (const workload of workloads) {
                const line = await inOwnProcess(
                    `${workload}:${String(bound)}`,
                    rows,
                    values.alongside,
                )
                console.log(line)
                results.push(JSON.parse(line))
            }
        }
        console.table(results)
    } finally {
        await removeRows(connection)
    }
}

type Connection = Awaited<ReturnType<Driver['connect']>>

async function measureEnv() {
    const prefix = process.env.TABLE_PREFIX
    if (prefix === undefined || prefix === '') {
        throw new Error('TABLE_PREFIX is required, e.g. staging.measure.')
    }
    return { ...(await localAwsEnv(undefined, process.env.AWS_PROFILE)), TABLE_PREFIX: prefix }
}

// The first write creates the table, which takes seconds; the rest follow it.
async function seed(connection: Connection, rows: number) {
    const [first, ...rest] = keysOf(rows)
    if (first === undefined) {
        return
    }
    await put(connection, first)
    await Promise.all(rest.map(key => put(connection, key)))
}

async function removeRows(connection: Connection) {
    const stored = await Array.fromAsync(connection.getPartition(table, partition))
    await Promise.all(
        stored.map(row =>
            connection.delete(table, partition, row.key, row.revision, { now: now() }),
        ),
    )
}

function inOwnProcess(run: string, rows: number, alongside: readonly string[]) {
    return new Promise<string>((resolve, reject) => {
        const child = spawn(
            process.execPath,
            [
                import.meta.filename,
                '--run',
                run,
                '--rows',
                String(rows),
                ...alongside.flatMap(url => ['--alongside', url]),
            ],
            { stdio: ['ignore', 'pipe', 'inherit'] },
        )
        let output = ''
        child.stdout.setEncoding('utf-8')
        child.stdout.on('data', (chunk: string) => {
            output += chunk
        })
        child.on('error', reject)
        child.on('close', code => {
            if (code !== 0) {
                reject(new Error(`The burst ${run} exited with code ${String(code)}.`))
                return
            }
            resolve(output.trim())
        })
    })
}

async function burst(run: string, rows: number, alongside: readonly string[]) {
    const [workload, bound] = run.split(':', 2)
    const requestsInFlightMax = positive(bound, '--run')
    const counted = countConnections()
    const connection = await new Driver({ requestsInFlightMax }).connect({
        env: await measureEnv(),
    })
    const operation = workload === 'reads' ? get : put

    const started = performance.now()
    const [timings, others] = await Promise.all([
        Promise.all(keysOf(rows).map(key => timed(() => operation(connection, key)))),
        Promise.all(
            alongside.flatMap(url => keysOf(rows).map(() => timed(async () => await fetch(url)))),
        ),
    ])
    const wallMs = Math.round(performance.now() - started)

    const durations = timings.map(timing => timing.ms).toSorted((a, b) => a - b)
    const failures = timings.flatMap(timing => timing.failure ?? [])
    return {
        workload,
        bound: requestsInFlightMax,
        rows,
        sockets: counted.sockets(),
        connectErrors: counted.connectErrors(),
        statuses: counted.statuses(),
        failed: failures.length,
        failures: [...new Set(failures)].slice(0, 5),
        alongsideFailed: others.filter(other => other.failure !== undefined).length,
        wallMs,
        p50Ms: percentile(durations, 0.5),
        p99Ms: percentile(durations, 0.99),
        maxMs: durations.at(-1),
    }
}

async function get(connection: Connection, key: string) {
    await connection.get(table, partition, key)
}

async function put(connection: Connection, key: string) {
    await connection.transact(
        [
            {
                op: 'put',
                table,
                partition,
                key,
                document: { measured: true },
                newRevision: randomUUID(),
            },
        ],
        { now: now() },
    )
}

async function timed(operation: () => Promise<unknown>) {
    const started = performance.now()
    try {
        await operation()
        return { ms: Math.round(performance.now() - started) }
    } catch (e) {
        return { ms: Math.round(performance.now() - started), failure: describe(e) }
    }
}

// The cause is where a failed lookup or connection names its code.
function describe(e: unknown): string {
    if (!Error.isError(e)) {
        return String(e)
    }
    if (e.cause === undefined) {
        return e.message
    }
    return `${e.message} (${describe(e.cause)})`
}

// What fetch's own dispatchers report: only the driver sends through the
// bounded one, so with `--alongside` the sockets include the other fetches'.
function countConnections() {
    let sockets = 0
    const connectErrors: { [code: string]: number } = {}
    const statuses: { [status: string]: number } = {}
    subscribe('undici:client:connected', () => {
        sockets += 1
    })
    subscribe('undici:client:connectError', message => {
        const code = codeOf(message)
        connectErrors[code] = (connectErrors[code] ?? 0) + 1
    })
    subscribe('undici:request:headers', message => {
        const status = statusOf(message)
        statuses[status] = (statuses[status] ?? 0) + 1
    })
    return {
        sockets: () => sockets,
        connectErrors: () => connectErrors,
        statuses: () => statuses,
    }
}

function codeOf(message: unknown) {
    if (typeof message !== 'object' || message === null || !('error' in message)) {
        return 'unknown'
    }
    const { error } = message
    if (typeof error !== 'object' || error === null || !('code' in error)) {
        return 'unknown'
    }
    return String(error.code)
}

function statusOf(message: unknown) {
    if (typeof message !== 'object' || message === null || !('response' in message)) {
        return 'unknown'
    }
    const { response } = message
    if (typeof response !== 'object' || response === null || !('statusCode' in response)) {
        return 'unknown'
    }
    return String(response.statusCode)
}

function percentile(sorted: readonly number[], fraction: number) {
    return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))]
}

function keysOf(rows: number) {
    return Array.from({ length: rows }, (_, i) => `row-${String(i).padStart(6, '0')}`)
}

function positive(value: string | undefined, what: string) {
    const parsed = Number(value)
    if (!Number.isSafeInteger(parsed) || parsed < 1) {
        throw new Error(`${what} takes positive whole numbers, not '${String(value)}'.`)
    }
    return parsed
}

function now() {
    return Math.floor(Date.now() / 1000)
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
    await main()
}
