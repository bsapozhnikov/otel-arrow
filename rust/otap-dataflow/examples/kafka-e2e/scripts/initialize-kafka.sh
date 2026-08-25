#!/usr/bin/env bash
set -euo pipefail

create_topic() {
  kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$1" \
    --partitions 1 \
    --replication-factor 1
}

case "${KAFKA_SCENARIO:-auth}" in
  auth)
    kafka-configs --bootstrap-server kafka:29092 --alter \
  --entity-type users \
  --entity-name scram256 \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=scram256-secret]'

    kafka-configs --bootstrap-server kafka:29092 --alter \
  --entity-type users \
  --entity-name scram512 \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=scram512-secret]'

    for topic in otlp-logs-plain otlp-logs-scram-256 otlp-logs-scram-512; do
      create_topic "${topic}"
    done

    echo "Created SCRAM users and OTLP log topics."
    ;;
  syslog)
    create_topic syslog-logs
    create_topic syslog-raw
    create_topic syslog-raw-logstash
    echo "Created syslog scenario topics."
    ;;
  *)
    echo "Unknown Kafka E2E scenario: ${KAFKA_SCENARIO}" >&2
    exit 1
    ;;
esac
