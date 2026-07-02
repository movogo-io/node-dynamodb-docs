import { setDriver } from '@riddance/docs/driver'
import { Driver } from './driver.js'

export * from '@riddance/docs'
export * from '@riddance/docs/indexed'

setDriver(new Driver())
