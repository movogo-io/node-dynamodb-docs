import { fetchJson } from '@riddance/fetch'
import { SignatureV4 } from '@smithy/signature-v4'
import { createHash, createHmac } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'

export type LocalEnv = {
    AWS_REGION: string
    AWS_ACCESS_KEY_ID: string
    AWS_SECRET_ACCESS_KEY: string
    AWS_SESSION_TOKEN?: string
}

let cachedConfigLines: string[] | undefined

export async function localAwsEnv(region?: string, profile?: string): Promise<LocalEnv> {
    let { AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN } = {
        AWS_REGION: region ?? process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
        AWS_SESSION_TOKEN: process.env.AWS_SESSION_TOKEN,
    }
    if (AWS_REGION && AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY) {
        return { AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN }
    }
    const configLines =
        cachedConfigLines ??
        (
            await readFile(
                process.env.AWS_SHARED_CREDENTIALS_FILE ?? join(homedir(), '.aws', 'credentials'),
                'ascii',
            )
        )
            .split('\n')
            .map(line => line.trim())
            .filter(line => !!line && !line.startsWith('#'))
    // eslint-disable-next-line require-atomic-updates, unicorn/no-top-level-assignment-in-function
    cachedConfigLines = configLines

    let sectionBeginIx = -1
    const section = `[${profile ?? 'default'}]`
    sectionBeginIx = configLines.indexOf(section)
    if (sectionBeginIx === -1) {
        sectionBeginIx = configLines.indexOf('[default]')
    }
    if (sectionBeginIx === -1) {
        throw new Error('Section not found.')
    }
    const sectionEndIx = configLines.findIndex(
        (line, ix) => ix > sectionBeginIx && line.startsWith('['),
    )
    const sectionLines = configLines
        .slice(sectionBeginIx + 1, sectionEndIx === -1 ? undefined : sectionEndIx)
        .map(line => line.split('='))
        .map(([k, v]) => [k?.trim(), v?.trim()])
    AWS_REGION ??= sectionLines.find(([k]) => k === 'region')?.[1]
    AWS_ACCESS_KEY_ID = sectionLines.find(([k]) => k === 'aws_access_key_id')?.[1]
    AWS_SECRET_ACCESS_KEY = sectionLines.find(([k]) => k === 'aws_secret_access_key')?.[1]
    AWS_SESSION_TOKEN = sectionLines.find(([k]) => k === 'aws_session_token')?.[1]
    if (!AWS_REGION || !AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY) {
        throw new Error('Incomplete AWS credentials file.')
    }
    return { AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN }
}

export function dbRequest<T>(env: Partial<LocalEnv> | undefined, target: string, body: unknown) {
    return awsStringRequest<T>(
        {
            AWS_REGION: env?.AWS_REGION ?? missing('AWS_REGION'),
            AWS_ACCESS_KEY_ID: env?.AWS_ACCESS_KEY_ID ?? missing('AWS_ACCESS_KEY_ID'),
            AWS_SECRET_ACCESS_KEY: env?.AWS_SECRET_ACCESS_KEY ?? missing('AWS_SECRET_ACCESS_KEY'),
            AWS_SESSION_TOKEN: env?.AWS_SESSION_TOKEN,
        },
        'POST',
        'dynamodb',
        '/',
        JSON.stringify(body),
        'application/json',
        'DynamoDB_20120810.' + target,
    )
}

function missing(what?: string): never {
    throw new Error(what ? `Missing ${what}.` : 'Missing.')
}

async function awsStringRequest<T>(
    env: LocalEnv,
    method: string,
    service: string,
    path: string,
    body: string,
    contentType: string,
    target: string,
) {
    const signer = new SignatureV4({
        service,
        region: env.AWS_REGION,
        sha256: AwsHash,
        credentials: {
            accessKeyId: env.AWS_ACCESS_KEY_ID,
            secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
            sessionToken: env.AWS_SESSION_TOKEN,
        },
    })
    const uri = new URL(`https://${service}.${env.AWS_REGION}.amazonaws.com${path}`)
    const query: { [key: string]: string } = {}
    uri.searchParams.forEach((value, key) => {
        query[key] = value
    })
    const { headers } = await signer.sign({
        method,
        protocol: 'https:',
        hostname: uri.hostname,
        path: uri.pathname,
        query,
        headers: {
            host: uri.hostname,
            'content-type': contentType,
            accept: 'application/json',
            ...(target && { 'X-Amz-Target': target }),
        },
        body,
    })
    return await fetchJson<T>(
        uri,
        {
            method,
            headers,
            body,
        },
        'Error fetching DynamoDB',
        { target },
    )
}

type SourceData = string | ArrayBuffer | ArrayBufferView

class AwsHash {
    readonly #secret?: SourceData
    #hash: ReturnType<typeof createHash> | ReturnType<typeof createHmac>

    constructor(secret?: SourceData) {
        this.#secret = secret
        this.#hash = makeHash(this.#secret)
    }

    digest() {
        return Promise.resolve(this.#hash.digest())
    }

    reset() {
        this.#hash = makeHash(this.#secret)
    }

    update(chunk: Uint8Array) {
        this.#hash.update(new Uint8Array(Buffer.from(chunk)))
    }
}

function makeHash(secret?: SourceData) {
    return secret ? createHmac('sha256', castSourceData(secret)) : createHash('sha256')
}

function castSourceData(data: SourceData) {
    if (Buffer.isBuffer(data)) {
        return data
    }
    if (typeof data === 'string') {
        return Buffer.from(data)
    }
    if (ArrayBuffer.isView(data)) {
        return Buffer.from(data.buffer, data.byteOffset, data.byteLength)
    }
    return Buffer.from(data)
}
