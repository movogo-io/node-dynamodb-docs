# @movogo-io/dynamodb-docs

The **DynamoDB driver** for `@movogo-io/docs`. Importing any of its entry points (`@movogo-io/dynamodb-docs`, `/indexed`, `/driver`) registers the driver, and the `@movogo-io/docs` API is re-exported from the matching entry point. `@movogo-io/docs` is a peer dependency: the service pins its version, and exactly one copy must be installed, or the driver registers on a copy the service does not use. Services never depend on this package directly; it is substituted for the in-memory driver at deployment.

## Configuration

The driver reads these from the context's `env`:

- `AWS_REGION` (or `AWS_DEFAULT_REGION`), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`.
- `AWS_DYNAMODB_ENDPOINT`: optional full URL overriding the regional endpoint, e.g. `http://localhost:8000/` for DynamoDB Local.
- `TABLE_PREFIX` / `TABLE_POSTFIX`: wrapped around every schema table name, e.g. `staging.Rentals`.
- `AWS_DYNAMODB_BILLING_METHOD`: `PROVISIONED` with `AWS_DYNAMODB_RCU` / `AWS_DYNAMODB_WCU`, otherwise pay per request.
- `AWS_DYNAMODB_POINT_IN_TIME_RECOVERY`: `true` enables point-in-time recovery on every table the driver creates. Like time to live, it is only applied on creation; enable it on existing tables once with `aws dynamodb update-continuous-backups --table-name <name> --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true`.
- `AWS_DYNAMODB_READ_CONSISTENCY`: `STRONG` makes every read strongly consistent, at twice the read cost; unset or `EVENTUAL` keeps DynamoDB's default eventually consistent reads. A revision-fenced write is correct either way, since a stale read conflicts and is retried. A read that looks at a row written milliseconds earlier and acts on what it sees asks for its own consistency: every read of `@movogo-io/docs` 0.2.0 takes a trailing `{ consistent: true }`, which this driver sends as `ConsistentRead` for that call whatever the env var says. `@movogo-io/sagas` 0.3.0, `@movogo-io/idempotency` and `@movogo-io/audit` built against docs 0.2.0 ask for it on the reads that need it (lease claims, fencing checks, `revisions` and erasures right after a write), so the env var is no longer required for them; it remains the override for a service that wants every read strong.

Tables are created on first write, so no provisioning step is needed. A table's first write waits for the table to become active, which takes several seconds.

## Connections, timeouts and lost replies

The driver bounds what it holds in flight, so nothing above it has to. Node's `fetch` opens a connection whenever all the ones it has are busy, without limit: 200 concurrent requests open 200 connections, each with a TLS handshake and a DNS lookup on a cold start, and a burst over a tenant-sized list has failed a production request outright with `getaddrinfo EBUSY`. The driver sends through a dispatcher of its own, capped at `requestsInFlightMax` connections; a request past the cap waits for one. The connection it hands the store declares that number as `requestsInFlightMax`, and `transactionItemsMax` (100), so the store sizes its own work from what the driver enforces instead of assuming it. It also declares `acceptsNewRevision`: `add` and `update` store the revision the store hands them, as a transaction stores an item's, so docs 0.2.5 writes a table whose write extensions add nothing to the write without a transaction.

`new Driver({ requestsInFlightMax, requestTimeoutMs })` sets both; the defaults are 64 and 10 seconds. The 64 was set with `bin/measure-in-flight.ts` against staging: a burst of 500 cold operations finishes in about a third of the time it takes at 16, while a higher bound gains little, and at 256 the burst is slower again because it spends its time opening connections. It is also an order of magnitude below the 600 connections, each with its own DNS lookup, that one handler opened at once in the production failure.

- **Every attempt has a timeout**, its wait for a connection included. A request that is not answered in time is given up with an error marked `unanswered`, and its connection is freed: without it, a request stalled on a socket that died while the process was frozen holds a connection for every later invocation.
- **The signal of the context aborts reads, and the waits between attempts.** It never aborts a write that was sent: that write may have been applied, and aborting it would report as failed what committed.
- **A transaction whose reply was lost is sent again under the same token.** A reply is lost when DynamoDB answers a 5xx, the connection fails, or the timeout passes. DynamoDB applies a token at most once, so the resend commits the transaction or learns that it already did. It is never reported as a conflict, which would have the store build and write it anew. A single write (`add`, `update`, `delete`) whose reply is lost throws, as before: nobody knows whether it was applied.
- **More than 100 operations are refused before anything is sent**, with the error `isTransactionTooLarge` of `@movogo-io/docs` recognizes; so is a transaction DynamoDB itself refuses for its size.

## Batch reads

`findEach` in `@movogo-io/docs` hands the driver a whole list of keys, which it reads through `BatchGetItem`: at most 100 keys per request, at most four requests in flight. DynamoDB answers a request it could not finish — a throttle, or 16MB of items — with a 200 and the keys it skipped, so the driver resubmits those with the same backoff it uses for throttled requests, and throws once the attempts are spent. It never answers with part of a list: the store cannot tell a short answer from documents that have been deleted, and would report the difference as missing.

## Expiry

Document expiry is declared in the service through `schema.expiry(...)` from `@movogo-io/docs`; see that package's instructions. The driver stores the expiry it is handed as a numeric `expiresAt` item attribute in epoch seconds, removes the attribute when a write carries no expiry, and enables DynamoDB time to live on that attribute for every table it creates, so expired items are eventually deleted without a sweeper. Expired items that DynamoDB has not yet removed are hidden by `@movogo-io/docs`, not by the driver.

Tables created before version 0.2.0 need time to live enabled once:

```sh
aws dynamodb update-time-to-live --table-name <TABLE_PREFIX>Rentals<TABLE_POSTFIX> --time-to-live-specification Enabled=true,AttributeName=expiresAt
```

## Sequence and timestamps

Every row `@movogo-io/docs` 0.2.0 answers carries `seq` and `updatedAt`, read back from the `seq`, `updated` and `created` item attributes this driver has always written. `seq` is a per-item write counter: the first `add` of a key starts it at 0, and every later write adds one to it, `add` and a transaction's `put` included, over a live item or an expired one DynamoDB has not yet swept. A delete, and a transaction's `delete` or `clear`, removes the item outright, so a document re-added under the key starts at 0 again, and a `clear` of a key never written creates nothing. `add` and `update` answer the revision, `seq` and `updatedAt` they stored, read from the item DynamoDB answers the write with; a transaction answers nothing, so `@movogo-io/docs` reports what the driver stores for a write over the row it read beforehand. `created` and `updated` are stamped from the context clock the store hands down, at second precision, never from the wall clock; the in-memory driver stamps the same instant, so a test that fixes `context.now` sees one `updatedAt` from both. A conditional write that fails changes none of them.

## Tests

`test/driver.ts` runs the `@movogo-io/docs` driver conformance harness plus DynamoDB-specific checks against a real account, using the local AWS credentials and the table prefix `DocsTests.`. Delete the `DocsTests.*` tables afterwards.
