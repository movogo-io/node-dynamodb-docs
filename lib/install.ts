import { setDriver } from '@movogo-io/docs/driver'
import { Driver } from './dynamo-driver.js'

setDriver(new Driver())
