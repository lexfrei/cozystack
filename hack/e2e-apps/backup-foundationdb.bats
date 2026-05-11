#!/usr/bin/env bats

# End-to-end backup + to-copy restore for the FoundationDB strategy.
# Drives the same flow as examples/backups/foundationdb/ but inline so the
# bats file is self-contained and doesn't depend on the example scripts
# staying in lockstep with the controller dispatch.
#
# Why to-copy and not in-place: an in-place FoundationDBRestore wipes the
# source cluster's keyspace before replaying the backup. To-copy leaves
# the source untouched and lets the assertion at the end witness the
# sentinel key landing on a separate cluster - a stronger restore proof,
# and matches the mariadb e2e's reasoning.
#
# Scope: this e2e runs entirely inside `tenant-root` so the FDB pods can
# reach `seaweedfs-s3` in the same namespace via the permissive
# `allow-internal-communication` CiliumNetworkPolicy. The cross-tenant
# variant (target FDB in `tenant-test`, seaweedfs in `tenant-root`) is
# blocked by the `${tenant}-egress` CiliumClusterwideNetworkPolicy and
# stays a manual / dev-cluster flow.
#
# Prereqs in the cluster:
#   - cozystack apps: FoundationDB + Bucket
#   - foundationdb-operator (apps.foundationdb.org) reachable from
#     tenant-root, plus FoundationDBBackup/FoundationDBRestore CRDs
#   - seaweedfs deployed in tenant-root (cozystack default)
#   - backup-controller and backupstrategy-controller running with the
#     FoundationDB dispatch case wired (see
#     internal/backupcontroller/foundationdbstrategy_controller.go)

NAMESPACE='tenant-root'
SRC='fdb-src'
DST='fdb-dst'
BUCKET='foundationdb-backups'
BUCKET_USER='backup'
BUCKET_ACCESS="bucket-${BUCKET}-${BUCKET_USER}"
STRATEGY_NAME='foundationdb-strategy-default'
BACKUPCLASS_NAME='foundationdb-default'
BACKUPJOB_NAME='fdb-src-adhoc'
RESTOREJOB_NAME='fdb-src-to-fdb-dst'

print_log() {
  echo "===== $1 ====="
}

apply_in_ns() {
  kubectl apply -n "${NAMESPACE}" -f -
}

# Run fdbcli against a running cluster_controller pod. The operator-side
# cluster name carries the "foundationdb-" release prefix.
fdbcli_exec() {
  local app="$1" script="$2"
  local cluster_name="foundationdb-${app}"
  local pod
  pod=$(kubectl -n "${NAMESPACE}" get pods \
    -l "foundationdb.org/fdb-cluster-name=${cluster_name},foundationdb.org/fdb-process-class=cluster_controller" \
    --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [ -n "${pod}" ] || { echo "no running cluster_controller pod for ${cluster_name}" >&2; return 1; }
  kubectl -n "${NAMESPACE}" exec -i "${pod}" -c foundationdb -- \
    fdbcli --cluster-file=/var/dynamic-conf/fdb.cluster --exec "${script}"
}

# Bats invokes teardown() after every @test, including failed ones. Every
# deletion is best-effort: --ignore-not-found absorbs a never-applied
# step, and `|| true` keeps a transient apiserver hiccup from masking the
# real assertion failure in the test body.
teardown() {
  print_log "Teardown: cleaning up FoundationDB backup test resources"

  # Drop the per-run Cozystack jobs and the resulting Backup artefact in
  # one pass. The operator-side FoundationDBBackup / FoundationDBRestore
  # CRs survive RestoreJob deletion (the driver labels them for
  # idempotent ensure-by-label semantics, not OwnerReferences), so reap
  # them by label too - otherwise a re-run would reuse stale CRs.
  kubectl -n "${NAMESPACE}" delete --ignore-not-found --wait=false \
    "restorejob.backups.cozystack.io/${RESTOREJOB_NAME}" \
    "backupjob.backups.cozystack.io/${BACKUPJOB_NAME}" \
    "backup.backups.cozystack.io/${BACKUPJOB_NAME}" || true
  for owner in "${BACKUPJOB_NAME}" "${RESTOREJOB_NAME}"; do
    kubectl -n "${NAMESPACE}" delete --ignore-not-found --wait=false \
      foundationdbbackups.apps.foundationdb.org,foundationdbrestores.apps.foundationdb.org \
      -l "backups.cozystack.io/owned-by.BackupJobName=${owner}" || true
  done

  # FoundationDB apps (HelmReleases). Flux uninstalls the chart, which
  # drops the FoundationDBCluster CR + PVCs.
  kubectl -n "${NAMESPACE}" delete --ignore-not-found --wait=false \
    "hr/foundationdb-${SRC}" "hr/foundationdb-${DST}" || true

  # Bucket app HR - removing it tears down the COSI BucketClaim/BucketAccess
  # and the per-user credentials Secret.
  kubectl -n "${NAMESPACE}" delete --ignore-not-found --wait=false \
    "hr/bucket-${BUCKET}" || true

  # Per-app blob credentials Secrets materialised by Step 0.
  kubectl -n "${NAMESPACE}" delete --ignore-not-found \
    "secret/${SRC}-fdb-backup-creds" \
    "secret/${DST}-fdb-backup-creds" || true

  # Cluster-scoped: BackupClass + FoundationDB strategy.
  kubectl delete --ignore-not-found \
    "backupclass.backups.cozystack.io/${BACKUPCLASS_NAME}" \
    "foundationdbs.strategy.backups.cozystack.io/${STRATEGY_NAME}" || true

  rm -f /tmp/foundationdb-bucket-info.json
}

@test "FoundationDB backup + to-copy restore" {
  print_log "Step 0: Bucket + blob_credentials.json Secrets"
  apply_in_ns <<EOF
apiVersion: apps.cozystack.io/v1alpha1
kind: Bucket
metadata:
  name: ${BUCKET}
spec:
  users:
    ${BUCKET_USER}:
      readonly: false
EOF
  kubectl -n "${NAMESPACE}" wait "hr/bucket-${BUCKET}" --for=condition=ready --timeout=300s
  timeout 300 sh -ec "until kubectl -n ${NAMESPACE} get bucketaccesses.objectstorage.k8s.io ${BUCKET_ACCESS} >/dev/null 2>&1; do sleep 2; done"
  kubectl -n "${NAMESPACE}" wait "bucketaccesses.objectstorage.k8s.io/${BUCKET_ACCESS}" \
    --for=jsonpath='{.status.accessGranted}'=true --timeout=300s

  kubectl -n "${NAMESPACE}" get secret "${BUCKET_ACCESS}" \
    -o jsonpath='{.data.BucketInfo}' | base64 -d > /tmp/foundationdb-bucket-info.json
  ACCESS=$(jq -r '.spec.secretS3.accessKeyID' /tmp/foundationdb-bucket-info.json)
  SECRETKEY=$(jq -r '.spec.secretS3.accessSecretKey' /tmp/foundationdb-bucket-info.json)
  COSI_BUCKET=$(jq -r '.spec.bucketName' /tmp/foundationdb-bucket-info.json)
  COSI_ENDPOINT=$(jq -r '.spec.secretS3.endpoint' /tmp/foundationdb-bucket-info.json)
  # FDB's backup_agent treats accountName as "<api_key>@<endpoint-host:port>";
  # strip scheme to derive the host[:port] portion. seaweedfs-s3 in tenant-root
  # is reachable in-cluster as seaweedfs-s3.tenant-root:8333 by default.
  ENDPOINT_HOSTPORT=${COSI_ENDPOINT#http://}
  ENDPOINT_HOSTPORT=${ENDPOINT_HOSTPORT#https://}
  ACCOUNT_NAME="${ACCESS}@${ENDPOINT_HOSTPORT}"
  SECURE_CONNECTION="0"
  case "${COSI_ENDPOINT}" in https://*) SECURE_CONNECTION="1" ;; esac

  for app in "${SRC}" "${DST}"; do
    creds_json=$(jq -nc \
      --arg account "${ACCOUNT_NAME}" \
      --arg key "${ACCESS}" \
      --arg secret "${SECRETKEY}" \
      '{accounts: {($account): {api_key: $key, secret: $secret}}}')
    kubectl -n "${NAMESPACE}" create secret generic "${app}-fdb-backup-creds" \
      --from-literal="blob_credentials.json=${creds_json}" \
      --dry-run=client -o yaml | kubectl apply -f -
  done

  print_log "Step 1: source FoundationDB + sentinel"
  apply_in_ns <<EOF
apiVersion: apps.cozystack.io/v1alpha1
kind: FoundationDB
metadata:
  name: ${SRC}
spec:
  cluster:
    version: "7.3.63"
    processCounts:
      storage: 1
      stateless: -1
      cluster_controller: 1
    redundancyMode: "single"
    storageEngine: "ssd-2"
    faultDomain:
      key: "foundationdb.org/none"
      valueFrom: "\$FDB_ZONE_ID"
  storage:
    size: "1Gi"
    storageClass: ""
  resourcesPreset: "small"
  backup:
    enabled: false
  monitoring:
    enabled: true
  imageType: "unified"
  automaticReplacements: true
EOF
  kubectl -n "${NAMESPACE}" wait "hr/foundationdb-${SRC}" --for=condition=ready --timeout=300s
  timeout 600 sh -ec "until kubectl -n ${NAMESPACE} get foundationdbclusters.apps.foundationdb.org foundationdb-${SRC} -o jsonpath='{.status.health.available}' | grep -q true; do sleep 10; done"
  fdbcli_exec "${SRC}" "writemode on; set /backup-demo/sentinel 'round-trip'"

  print_log "Step 2: FoundationDB strategy + BackupClass"
  kubectl apply -f - <<EOF
apiVersion: strategy.backups.cozystack.io/v1alpha1
kind: FoundationDB
metadata:
  name: ${STRATEGY_NAME}
spec:
  template:
    blobStoreConfiguration:
      accountName: "{{ .Parameters.accountName }}"
      bucket: "{{ .Parameters.bucket }}"
      urlParameters:
        - "secure_connection={{ .Parameters.secureConnection }}"
        - "region=us-east-1"
    snapshotPeriodSeconds: 600
    customParameters:
      - "--blob_credentials=/var/fdb-blob-credentials/blob_credentials.json"
    backupDeploymentPodTemplateSpec:
      spec:
        containers:
          - name: foundationdb
            volumeMounts:
              - name: blob-credentials
                mountPath: /var/fdb-blob-credentials
                readOnly: true
            securityContext:
              runAsUser: 0
        volumes:
          - name: blob-credentials
            secret:
              secretName: "{{ .Application.metadata.name }}-fdb-backup-creds"
              items:
                - key: blob_credentials.json
                  path: blob_credentials.json
EOF
  kubectl apply -f - <<EOF
apiVersion: backups.cozystack.io/v1alpha1
kind: BackupClass
metadata:
  name: ${BACKUPCLASS_NAME}
spec:
  strategies:
    - application:
        apiGroup: apps.cozystack.io
        kind: FoundationDB
      strategyRef:
        apiGroup: strategy.backups.cozystack.io
        kind: FoundationDB
        name: ${STRATEGY_NAME}
      parameters:
        accountName: "${ACCOUNT_NAME}"
        bucket: "${COSI_BUCKET}"
        secureConnection: "${SECURE_CONNECTION}"
EOF

  print_log "Step 3: ad-hoc BackupJob"
  apply_in_ns <<EOF
apiVersion: backups.cozystack.io/v1alpha1
kind: BackupJob
metadata:
  name: ${BACKUPJOB_NAME}
spec:
  applicationRef:
    apiGroup: apps.cozystack.io
    kind: FoundationDB
    name: ${SRC}
  backupClassName: ${BACKUPCLASS_NAME}
EOF
  # 45 minutes max; matches the driver-side foundationdbDefaultBackupDeadline.
  if ! kubectl -n "${NAMESPACE}" wait "backupjob.backups.cozystack.io/${BACKUPJOB_NAME}" \
       --for=jsonpath='{.status.phase}'=Succeeded --timeout=2700s; then
    echo "----- BackupJob status after timeout -----"
    kubectl -n "${NAMESPACE}" get "backupjob.backups.cozystack.io/${BACKUPJOB_NAME}" -o yaml || true
    echo "----- FoundationDBBackup objects -----"
    kubectl -n "${NAMESPACE}" get foundationdbbackups.apps.foundationdb.org -o wide || true
    echo "----- backupstrategy-controller log -----"
    kubectl -n cozy-backup-controller logs -l app.kubernetes.io/name=backupstrategy-controller --tail=120 || true
    return 1
  fi
  kubectl -n "${NAMESPACE}" get "backup.backups.cozystack.io/${BACKUPJOB_NAME}"

  print_log "Step 4: empty target FoundationDB for to-copy restore"
  apply_in_ns <<EOF
apiVersion: apps.cozystack.io/v1alpha1
kind: FoundationDB
metadata:
  name: ${DST}
spec:
  cluster:
    version: "7.3.63"
    processCounts:
      storage: 1
      stateless: -1
      cluster_controller: 1
    redundancyMode: "single"
    storageEngine: "ssd-2"
    faultDomain:
      key: "foundationdb.org/none"
      valueFrom: "\$FDB_ZONE_ID"
  storage:
    size: "1Gi"
    storageClass: ""
  resourcesPreset: "small"
  backup:
    enabled: false
  monitoring:
    enabled: true
  imageType: "unified"
  automaticReplacements: true
EOF
  kubectl -n "${NAMESPACE}" wait "hr/foundationdb-${DST}" --for=condition=ready --timeout=300s
  timeout 600 sh -ec "until kubectl -n ${NAMESPACE} get foundationdbclusters.apps.foundationdb.org foundationdb-${DST} -o jsonpath='{.status.health.available}' | grep -q true; do sleep 10; done"

  print_log "Step 5: to-copy RestoreJob (${SRC} -> ${DST})"
  apply_in_ns <<EOF
apiVersion: backups.cozystack.io/v1alpha1
kind: RestoreJob
metadata:
  name: ${RESTOREJOB_NAME}
spec:
  backupRef:
    name: ${BACKUPJOB_NAME}
  targetApplicationRef:
    apiGroup: apps.cozystack.io
    kind: FoundationDB
    name: ${DST}
EOF
  if ! kubectl -n "${NAMESPACE}" wait "restorejob.backups.cozystack.io/${RESTOREJOB_NAME}" \
       --for=jsonpath='{.status.phase}'=Succeeded --timeout=1800s; then
    echo "----- RestoreJob status after timeout -----"
    kubectl -n "${NAMESPACE}" get "restorejob.backups.cozystack.io/${RESTOREJOB_NAME}" -o yaml || true
    echo "----- FoundationDBRestore objects -----"
    kubectl -n "${NAMESPACE}" get foundationdbrestores.apps.foundationdb.org -o wide || true
    echo "----- backupstrategy-controller log -----"
    kubectl -n cozy-backup-controller logs -l app.kubernetes.io/name=backupstrategy-controller --tail=120 || true
    return 1
  fi

  print_log "Step 6: verify sentinel landed on '${DST}', source still works"
  ROW_DST=$(fdbcli_exec "${DST}" "get /backup-demo/sentinel")
  echo "${ROW_DST}" | grep -q "round-trip"

  # To-copy must not touch the source. If we ever regress and start
  # mutating fdb-src on a to-copy restore this catches it before users do.
  ROW_SRC=$(fdbcli_exec "${SRC}" "get /backup-demo/sentinel")
  echo "${ROW_SRC}" | grep -q "round-trip"
}
