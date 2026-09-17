import { setDriver } from '@movogo-io/docs/driver'
import { Driver } from './driver.js'

export * from '@movogo-io/docs'
export * from '@movogo-io/docs/indexed'

// eslint-disable-next-line unicorn/no-top-level-side-effects
setDriver(new Driver())
