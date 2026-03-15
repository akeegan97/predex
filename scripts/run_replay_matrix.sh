#!/usr/bin/env bash
set -euo pipefail

BASE_CONFIG="${1:-docs/trader_config.example.json}"
PAYLOADS="${2:-docs/replay/top3_markets_fixed.jsonl}"

BUILD_PRESET="${BUILD_PRESET:-build-dev-vcpkg}"
REPLAY_APP_PATH="${REPLAY_APP_PATH:-./build/dev/cpp/replay_app}"
REPEAT="${REPEAT:-300}"
PUSH_BATCH="${PUSH_BATCH:-16}"
MAX_DRAIN="${MAX_DRAIN:-50000}"

profiles=(
  "baseline"
  "latency"
  "stress"
)

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required" >&2
  exit 1
fi

echo "building replay_app target..."
cmake --build --preset "${BUILD_PRESET}" --parallel --target replay_app

for profile in "${profiles[@]}"; do
  override="docs/profiles/paper_oms.${profile}.override.json"
  if [[ ! -f "${override}" ]]; then
    echo "error: missing override ${override}" >&2
    exit 1
  fi
  if [[ ! -f "${BASE_CONFIG}" ]]; then
    echo "error: missing base config ${BASE_CONFIG}" >&2
    exit 1
  fi
  if [[ ! -f "${PAYLOADS}" ]]; then
    echo "error: missing payload file ${PAYLOADS}" >&2
    exit 1
  fi

  tmp_config="$(mktemp)"
  jq -s '.[0] * .[1]' "${BASE_CONFIG}" "${override}" > "${tmp_config}"

  echo
  echo "==== replay profile: ${profile} ===="
  "${REPLAY_APP_PATH}" \
    --config "${tmp_config}" \
    --payloads "${PAYLOADS}" \
    --repeat "${REPEAT}" \
    --push-batch "${PUSH_BATCH}" \
    --max-drain "${MAX_DRAIN}"

  rm -f "${tmp_config}"
done
