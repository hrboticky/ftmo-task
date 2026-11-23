#!/usr/bin/env bash
# Part of the homework infrastructure. Don't change; see CONTRACT.md.
#
# Creates the topics declared in a topics file (default /infra/topics.txt).
# Runs as the one-shot `kafka-init` compose service before your services start.
set -euo pipefail

topics_file="${1:-/infra/topics.txt}"
bootstrap="${KAFKA_BOOTSTRAP:-kafka:9092}"
kafka_topics="/opt/kafka/bin/kafka-topics.sh"

line_number=0
created=0
while IFS= read -r line || [ -n "$line" ]; do
  line_number=$((line_number + 1))
  line="${line%%#*}"                       # strip comments
  read -r name partitions config extra <<<"$line" || true
  [ -z "${name:-}" ] && continue

  if [ -n "${extra:-}" ] || ! [[ "${partitions:-}" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: $topics_file:$line_number: expected 'name partitions [key=value,...]', got: $line" >&2
    exit 1
  fi

  args=(--bootstrap-server "$bootstrap" --create --if-not-exists --topic "$name" --partitions "$partitions" --replication-factor 1)
  if [ -n "${config:-}" ]; then
    IFS=',' read -ra settings <<<"$config"
    for setting in "${settings[@]}"; do
      if ! [[ "$setting" =~ ^[a-z0-9.]+=.+$ ]]; then
        echo "error: $topics_file:$line_number: invalid config '$setting', expected key=value" >&2
        exit 1
      fi
      args+=(--config "$setting")
    done
  fi

  "$kafka_topics" "${args[@]}"
  created=$((created + 1))
done <"$topics_file"

if [ "$created" -eq 0 ]; then
  echo "error: $topics_file declares no topics" >&2
  exit 1
fi

echo "Topics in the cluster:"
"$kafka_topics" --bootstrap-server "$bootstrap" --describe --exclude-internal | grep -E '^Topic:' || true
