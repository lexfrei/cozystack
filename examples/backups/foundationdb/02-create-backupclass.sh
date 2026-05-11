#!/bin/bash
# Step 02: Map the FoundationDB application kind to the FoundationDB
# strategy from step 01, parameterised with the blob-store routing fields
# the strategy templates against (.Parameters.accountName / bucket / region
# / secureConnection).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/00-helpers.sh"

print_header "Step 02: Create BackupClass '${BACKUPCLASS_NAME}'"

# The BackupClass parameters are filled by step 03 once the Bucket has
# materialised its BucketInfo Secret (seaweedfs-s3 endpoint, bucket name,
# credentials). Step 03 re-applies the BackupClass with the resolved
# values; this initial apply is a placeholder so an admin running 01..02
# without a Bucket yet still gets a valid (if not yet runnable) class.
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
        # Filled by step 03 from BucketInfo. Empty values cause the strategy
        # to render an invalid FoundationDBBackup (accountName is required),
        # which surfaces as a BackupJob Failed condition - intentional, so a
        # half-configured tenant fails fast.
        accountName: "REPLACE_ME"
        bucket: "REPLACE_ME"
        region: "us-east-1"
        secureConnection: "0"
EOF

log_success "BackupClass '${BACKUPCLASS_NAME}' created (parameters filled by step 03)."
echo -e "\n${GREEN}${BOLD}Next:${NC} ./03-create-bucket.sh"
