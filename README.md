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
