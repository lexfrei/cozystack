# Scenario: tenant restores from a FoundationDB Backup

This narrative covers both restore variants exercised by the demo's
`06..07` scripts. Both are mechanically the same: the driver materialises
a `FoundationDBRestore` CR with `destinationClusterName` pointed at the
target FoundationDB cluster and the same blob-store coordinates the source
Backup wrote with.

## Variants

### In-place — `06-restore-in-place.sh`

- `RestoreJob.spec.targetApplicationRef` is left empty; the driver
  resolves it to `Backup.spec.applicationRef`.
- The FoundationDB operator pauses the source cluster, clears the
  keyspace, and replays the backup via `fdbrestore`. Anything written
  after the backup point is **lost** — this is exactly what an in-place
  restore is supposed to do.

### To-copy — `07-restore-to-copy.sh`

- `RestoreJob.spec.targetApplicationRef` names a freshly-provisioned
  `apps.cozystack.io/FoundationDB` (the script creates `fdb-dst` for
  exactly this purpose).
- The driver creates the `FoundationDBRestore` against
  `destinationClusterName=foundationdb-fdb-dst` (the operator-side cluster
  carries the `foundationdb-` release prefix; the driver applies it
  automatically).
- The source cluster is **not** touched. The verification step in the
  script reads the sentinel key off the destination cluster as the
  positive proof.

## When to pick which

| Variant | Best for |
|---|---|
| `in-place` | Recover from data corruption / accidental deletion on a live cluster you intend to keep using under the same name. |
| `to-copy`  | Disaster-recovery drills, branch databases, side-by-side validation, or migrating to a new FDB version. The source stays online. |

## Verification

For either variant, watch the RestoreJob and the operator-side restore:

```
kubectl -n <ns> get restorejobs.backups.cozystack.io <name> -o yaml
kubectl -n <ns> get foundationdbrestores.apps.foundationdb.org \
  -l backups.cozystack.io/owned-by.BackupJobName=<name>
```

A successful restore reports `status.phase: Succeeded` on the RestoreJob
and `status.state: Completed` on the operator-side
`FoundationDBRestore`.
