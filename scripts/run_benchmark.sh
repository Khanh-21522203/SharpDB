#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIGURATION="${CONFIGURATION:-Release}"

ROWS="${ROWS:-100000}"
WARMUP="${WARMUP:-3}"
ITERS="${ITERS:-15}"
SELECT_BATCH="${SELECT_BATCH:-5000}"
WRITE_BATCH="${WRITE_BATCH:-1000}"
WAL="${WAL:-false}"
DB_PATH="${DB_PATH:-}"

echo "[benchmark] build (${CONFIGURATION})"
dotnet build "${ROOT_DIR}/SharpDB.Benchmark/SharpDB.Benchmark.csproj" -c "${CONFIGURATION}" -v minimal

echo "[benchmark] run"
CMD=(
  dotnet run
  --project "${ROOT_DIR}/SharpDB.Benchmark/SharpDB.Benchmark.csproj"
  --configuration "${CONFIGURATION}"
  --
  --rows "${ROWS}"
  --warmup "${WARMUP}"
  --iters "${ITERS}"
  --select-batch "${SELECT_BATCH}"
  --write-batch "${WRITE_BATCH}"
  --wal "${WAL}"
)

if [[ -n "${DB_PATH}" ]]; then
  CMD+=(--db "${DB_PATH}")
fi

"${CMD[@]}"
