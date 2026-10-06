from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

ALERT_FIELDS = (
    "card_id",
    "account_id",
    "customer_id",
    "transaction_count",
    "first_transaction_at",
    "last_transaction_at",
)


def _timestamp(value: Any, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    else:
        raise ValueError(f"{field} must be an ISO-8601 timestamp")

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.isoformat(timespec="milliseconds")


def normalize_alert(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("Alert value must be an Avro record")

    alert: dict[str, Any] = {}
    for field in ALERT_FIELDS:
        if field not in value:
            raise ValueError(f"Alert is missing required field: {field}")

    for field in ("card_id", "account_id", "customer_id"):
        field_value = value[field]
        if not isinstance(field_value, str) or not field_value:
            raise ValueError(f"{field} must be a non-empty string")
        alert[field] = field_value

    count = value["transaction_count"]
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError("transaction_count must be a positive integer")
    alert["transaction_count"] = count
    alert["first_transaction_at"] = _timestamp(
        value["first_transaction_at"], "first_transaction_at"
    )
    alert["last_transaction_at"] = _timestamp(
        value["last_transaction_at"], "last_transaction_at"
    )
    return alert


def alert_id(alert: dict[str, Any]) -> str:
    canonical = json.dumps(alert, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def open_database(path: str) -> sqlite3.Connection:
    database_path = Path(path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = FULL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            event_id TEXT PRIMARY KEY,
            card_id TEXT NOT NULL,
            account_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            transaction_count INTEGER NOT NULL,
            first_transaction_at TEXT NOT NULL,
            last_transaction_at TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            offset_id INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def handle_alert(
    connection: sqlite3.Connection,
    value: Any,
    topic: str,
    partition: int,
    offset: int,
    commit_offset: Callable[[], Any],
) -> bool:
    if not topic or partition < 0 or offset < 0:
        raise ValueError("Kafka topic, partition, and offset must be valid")

    alert = normalize_alert(value)
    event_key = alert_id(alert)
    payload = json.dumps(alert, sort_keys=True, separators=(",", ":"))
    with connection:
        result = connection.execute(
            """
            INSERT OR IGNORE INTO fraud_alerts (
                event_id, card_id, account_id, customer_id, transaction_count,
                first_transaction_at, last_transaction_at, topic,
                partition_id, offset_id, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_key,
                alert["card_id"],
                alert["account_id"],
                alert["customer_id"],
                alert["transaction_count"],
                alert["first_transaction_at"],
                alert["last_transaction_at"],
                topic,
                partition,
                offset,
                payload,
            ),
        )

    # The durable idempotency record must exist before Kafka can advance.
    commit_offset()
    return result.rowcount == 1


def run(max_messages: int | None = None) -> None:
    try:
        from confluent_kafka import Consumer, KafkaError
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        from confluent_kafka.schema_registry.error import SchemaRegistryError
        from confluent_kafka.serialization import MessageField, SerializationContext
    except ImportError as exc:
        raise RuntimeError(
            "Install consumer dependencies with "
            "`python -m pip install -r consumer/requirements.txt`"
        ) from exc

    required = (
        "CONFLUENT_BOOTSTRAP_SERVER",
        "CONSUMER_KAFKA_API_KEY",
        "CONSUMER_KAFKA_API_SECRET",
        "SCHEMA_REGISTRY_URL",
        "SCHEMA_REGISTRY_API_KEY",
        "SCHEMA_REGISTRY_API_SECRET",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    topic = os.environ.get("FRAUD_ALERT_TOPIC", "desafio.fraud.detected")
    database_path = os.environ.get(
        "CONSUMER_SQLITE_PATH", "data/fraud-alerts.sqlite"
    )
    registry = SchemaRegistryClient(
        {
            "url": os.environ["SCHEMA_REGISTRY_URL"],
            "basic.auth.user.info": (
                f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:"
                f"{os.environ['SCHEMA_REGISTRY_API_SECRET']}"
            ),
        }
    )
    deserializer = AvroDeserializer(registry)
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVER"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.environ["CONSUMER_KAFKA_API_KEY"],
            "sasl.password": os.environ["CONSUMER_KAFKA_API_SECRET"],
            "group.id": os.environ.get(
                "CONSUMER_GROUP_ID", "desafio-fraud-alerts"
            ),
            "enable.auto.commit": False,
            "auto.offset.reset": os.environ.get("CONSUMER_OFFSET_RESET", "earliest"),
        }
    )
    connection = open_database(database_path)
    processed = 0
    inserted = 0
    try:
        consumer.subscribe([topic])
        while max_messages is None or processed < max_messages:
            message = consumer.poll(1.0)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka consumer error: {error}")

            try:
                value = deserializer(
                    message.value(),
                    SerializationContext(message.topic(), MessageField.VALUE),
                )
            except SchemaRegistryError as exc:
                raise RuntimeError(
                    f"Schema Registry denied or failed the schema read for {topic}: {exc}"
                ) from exc
            is_new = handle_alert(
                connection,
                value,
                message.topic(),
                message.partition(),
                message.offset(),
                lambda: consumer.commit(message=message, asynchronous=False),
            )
            processed += 1
            inserted += int(is_new)
            print(
                f"{'stored' if is_new else 'duplicate'} alert "
                f"at {message.topic()}[{message.partition()}]@{message.offset()}"
            )
    finally:
        connection.close()
        consumer.close()

    print(f"Processed {processed} message(s); stored {inserted} new alert(s).")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consume fraud alerts with SQLite-backed idempotency."
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="stop after handling this many messages (useful for a bounded smoke test)",
    )
    args = parser.parse_args()
    if args.max_messages is not None and args.max_messages < 1:
        parser.error("--max-messages must be greater than zero")

    try:
        run(args.max_messages)
    except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
        print(f"Consumer failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
