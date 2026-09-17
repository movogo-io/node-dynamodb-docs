# Overview

This package is the **DynamoDB driver** for `@movogo-io/docs`. Importing any of its entry points (`@movogo-io/dynamodb-docs`, `/indexed`, `/driver`) registers the driver, and the `@movogo-io/docs` API is re-exported from the matching entry point, so a service can import `tables`, `withTransaction`, and `docs` either from `@movogo-io/docs` or from here. `@movogo-io/docs` is a peer dependency: the service pins its version, and exactly one copy must be installed, or the driver registers on a copy the service does not use.

## Configuration

The driver reads these from the context's `env`:

- `AWS_REGION` (or `AWS_DEFAULT_REGION`), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN`.
- `AWS_DYNAMODB_ENDPOINT`: optional full URL overriding the regional endpoint, e.g. `http://localhost:8000/` for DynamoDB Local.
- `TABLE_PREFIX` / `TABLE_POSTFIX`: wrapped around every schema table name, e.g. `staging.Rentals`.
- `AWS_DYNAMODB_BILLING_METHOD`: `PROVISIONED` with `AWS_DYNAMODB_RCU` / `AWS_DYNAMODB_WCU`, otherwise pay per request.

Tables are created on first write, so no provisioning step is needed. A table's first write waits for the table to become active, which takes several seconds.

## Time to live

A document whose top-level `expiresAt` is a **number** is treated as expiring at that epoch time in **seconds**; the driver projects it to a numeric `expiresAt` attribute on the item, and DynamoDB deletes the item after that time. Updating the document to one without a numeric `expiresAt` removes the attribute, so the item no longer expires. A string `expiresAt` (e.g. an ISO date) is an ordinary document field and does not expire anything.

- The value must be a non-negative integer below `100000000000`; a millisecond timestamp is rejected, since it would never expire.
- Expiry is asynchronous and can lag by hours. An expired item can still be read until it is deleted, so readers must check `expiresAt` themselves where it matters.
- Index entries carry the document and its `expiresAt`, so they expire with it.
- The driver enables time to live on the tables it creates. Tables created before version 0.1.1 need it enabled once, on the `expiresAt` attribute:

```sh
aws dynamodb update-time-to-live --table-name <TABLE_PREFIX>Rentals<TABLE_POSTFIX> --time-to-live-specification Enabled=true,AttributeName=expiresAt
```
