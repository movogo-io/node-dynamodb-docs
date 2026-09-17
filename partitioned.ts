import { setDriver } from '@riddance/docs/driver'
import { Driver } from './driver.js'

export * from '@riddance/docs'
export * from '@riddance/docs/indexed'

// eslint-disable-next-line unicorn/no-top-level-side-effects
setDriver(new Driver())
