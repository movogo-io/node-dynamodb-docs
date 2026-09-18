# @movogo-io/dynamodb-docs

The **DynamoDB driver** for `@movogo-io/docs`. Importing any of its entry points (`@movogo-io/dynamodb-docs`, `/indexed`, `/driver`) registers the driver, and the `@movogo-io/docs` API is re-exported from the matching entry point. `@movogo-io/docs` is a peer dependency: the service pins its version, and exactly one copy must be installed, or the driver registers on a copy the service does not use. Services never depend on this package directly; it is substituted for the in-memory driver at deployment.

## Configuration

The driver reads these from the context's `env`:

- `AWS_REGION` (or `AWS_DEFAULT_REGION`), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`.
- `AWS_DYNAMODB_ENDPOINT`: optional full URL overriding the regional endpoint, e.g. `http://localhost:8000/` for DynamoDB Local.
- `TABLE_PREFIX` / `TABLE_POSTFIX`: wrapped around every schema table name, e.g. `staging.Rentals`.
- `AWS_DYNAMODB_BILLING_METHOD`: `PROVISIONED` with `AWS_DYNAMODB_RCU` / `AWS_DYNAMODB_WCU`, otherwise pay per request.
- `AWS_DYNAMODB_POINT_IN_TIME_RECOVERY`: `true` enables point-in-time recovery on every table the driver creates. Like time to live, it is only applied on creation; enable it on existing tables once with `aws dynamodb update-continuous-backups --table-name <name> --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true`.

Tables are created on first write, so no provisioning step is needed. A table's first write waits for the table to become active, which takes several seconds.

## Batch reads

`findEach` in `@movogo-io/docs` hands the driver a whole list of keys, which it reads through `BatchGetItem`: at most 100 keys per request, at most four requests in flight. DynamoDB answers a request it could not finish — a throttle, or 16MB of items — with a 200 and the keys it skipped, so the driver resubmits those with the same backoff it uses for throttled requests, and throws once the attempts are spent. It never answers with part of a list: the store cannot tell a short answer from documents that have been deleted, and would report the difference as missing.

## Expiry

Document expiry is declared in the service through `schema.expiry(...)` from `@movogo-io/docs`; see that package's instructions. The driver stores the expiry it is handed as a numeric `expiresAt` item attribute in epoch seconds, removes the attribute when a write carries no expiry, and enables DynamoDB time to live on that attribute for every table it creates, so expired items are eventually deleted without a sweeper. Expired items that DynamoDB has not yet removed are hidden by `@movogo-io/docs`, not by the driver.

Tables created before version 0.2.0 need time to live enabled once:

```sh
aws dynamodb update-time-to-live --table-name <TABLE_PREFIX>Rentals<TABLE_POSTFIX> --time-to-live-specification Enabled=true,AttributeName=expiresAt
```

## Tests

`test/driver.ts` runs the `@movogo-io/docs` driver conformance harness plus DynamoDB-specific checks against a real account, using the local AWS credentials and the table prefix `DocsTests.`. Delete the `DocsTests.*` tables afterwards.
