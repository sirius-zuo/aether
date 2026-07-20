#!/usr/bin/env bash
# Launch the six llm-planner agents on the AgentVerse built-in server, then
# run the orchestrator driver. Requires a local OpenAI-compatible model at
# $MODEL_BASE_URL.
set -euo pipefail

MODEL_BASE_URL="${MODEL_BASE_URL:-http://localhost:9090/v1}"
MODEL_API_KEY="${MODEL_API_KEY:-}"
MODEL_NAME="${MODEL_NAME:-Qwen3.6-35B-A3B-GGUF}"
export MODEL_BASE_URL MODEL_API_KEY MODEL_NAME

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

declare -A ROLES=(
  [plan]=9101 [gather_context]=9102 [analyze_pros]=9103
  [analyze_cons]=9104 [assess_cost]=9105 [synthesize]=9106
)

for role in "${!ROLES[@]}"; do
  ROLE="$role" PORT="${ROLES[$role]}" \
    cargo run -q -p example-llm-planner --bin llm-planner-agent &
  pids+=($!)
done

# Wait for every agent's /health to answer.
for port in "${ROLES[@]}"; do
  for _ in $(seq 1 50); do
    curl -sf "http://127.0.0.1:${port}/health" >/dev/null && break
    sleep 0.2
  done
done

cargo run -q -p example-llm-planner --bin llm-planner -- "${1:-Should we migrate from REST to gRPC?}"
