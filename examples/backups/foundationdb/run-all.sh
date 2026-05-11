#!/bin/bash
# Convenience runner that executes 01..07 in order.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

bash 01-create-strategy.sh
bash 02-create-backupclass.sh
bash 03-create-bucket.sh
bash 04-create-foundationdb-src.sh
bash 05-create-backupjob.sh
bash 06-restore-in-place.sh
bash 07-restore-to-copy.sh
