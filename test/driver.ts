import { harness } from '@movogo-io/docs/test/harness'
import { Driver } from '../driver.js'
import { localAwsEnv } from '../lib/aws.js'

const context = {
    env: {
        ...(await localAwsEnv()),
        TABLE_PREFIX: 'DocsTests.',
    },
}

describe('driver', () => {
    harness(it, new Driver(), () => context)
})
