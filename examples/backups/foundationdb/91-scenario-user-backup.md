# Scenario: tenant runs a FoundationDB backup

This narrative covers the per-tenant backup flow that the demo's
`03..05` scripts encode.

## Goal

Take a Cozystack `Backup` artefact of a running FoundationDB application,
backed by a discrete blob-store directory the operator's `backup_agent`
streamed into.

## Steps

1. **Provision a Bucket and project credentials** — `03-create-bucket.sh`.
   - Creates the Cozystack `Bucket` CR.
   - Reads the resulting `bucket-<name>-backup` BucketInfo Secret to
     extract S3 endpoint + access keys + bucket name.
   - Materialises per-app `<app>-fdb-backup-creds` Secrets in the
     application namespace. The Secret carries a `blob_credentials.json`
     payload in the FoundationDB operator's expected shape:
     ```json
     {
       "accounts": {
         "<api_key>@<endpoint-host>:<port>": {
           "api_key": "<access_key>",
           "secret":  "<secret_key>"
         }
       }
     }
     ```
   - Patches the BackupClass with the resolved `accountName`, `bucket`,
     `region`, and `secureConnection` parameters.
2. **Provision the FoundationDB and write some data** —
   `04-create-foundationdb-src.sh`.
   - Renders the chart with `backup.enabled=false`: the new BackupClass
     flow is out-of-chart.
   - Writes `/backup-demo/sentinel` so the restore flows can witness the
     value land on a restored cluster.
3. **Submit the BackupJob** — `05-create-backupjob.sh`.
   - The driver creates a `FoundationDBBackup` CR labelled by the
     BackupJob, sets `backupState=Running`, and waits for the operator to
     reconcile + the `backup_agent` to land a full snapshot
     (`status.backupDetails.snapshotTime > 0`).
   - Stamps a Cozystack `Backup` artefact (same name as the BackupJob).
     `Backup.spec.driverMetadata` carries the per-run blob path so the
     RestoreJob path can rebuild the blob-store config from it.

## Verification

```
kubectl -n <ns> get backupjobs.backups.cozystack.io <name> -o yaml
kubectl -n <ns> get backups.backups.cozystack.io      <name> -o yaml
kubectl -n <ns> get foundationdbbackups.apps.foundationdb.org \
  -l backups.cozystack.io/owned-by.BackupJobName=<name>
```

The BackupJob should report `phase: Succeeded`. The operator-side
FoundationDBBackup carries
`status.backupDetails.{running: true, snapshotTime: <int>, url: <s3-url>}`.
