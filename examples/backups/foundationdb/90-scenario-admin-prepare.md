# Scenario: admin prepares the FoundationDB backup strategy

This narrative covers the one-time admin setup that lets tenants in the
cluster back up `apps.cozystack.io/FoundationDB` applications without
authoring strategy CRDs themselves.

## Goal

- Make a cluster-scoped `strategy.backups.cozystack.io/FoundationDB`
  available.
- Bind the `apps.cozystack.io/FoundationDB` Kind to it via a `BackupClass`.
- Hand off to tenants so they can `kubectl apply` a `BackupJob` against
  any FoundationDB instance they own.

## Steps

1. **Apply the strategy** — `01-create-strategy.sh`.
   - Renders a `FoundationDB` strategy CR that templates blob-store
     coordinates from BackupClass parameters and references a per-app
     `{{ .Application.metadata.name }}-fdb-backup-creds` Secret in the
     application namespace.
2. **Apply the BackupClass** — `02-create-backupclass.sh`.
   - The parameters are placeholders (`REPLACE_ME`) until step 03 reads
     the real bucket coordinates and patches them in. Applying the class
     early is fine: any BackupJob that runs against unfilled parameters
     fails fast with a `accountName is required` validation error - the
     intentional "fail-loud" behaviour for a half-configured tenant.

## Verification

```
kubectl get foundationdbs.strategy.backups.cozystack.io
kubectl get backupclasses.backups.cozystack.io foundationdb-default
```

The strategy CR should report no error conditions, and the BackupClass
should list one strategy entry pointing at it.
