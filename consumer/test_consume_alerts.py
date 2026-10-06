import sqlite3
import unittest
from datetime import datetime, timezone

from consume_alerts import handle_alert, normalize_alert, open_database


def sample_alert() -> dict[str, object]:
    return {
        "card_id": "card-0001",
        "account_id": "account-0001",
        "customer_id": "customer-0001",
        "transaction_count": 3,
        "first_transaction_at": datetime(2026, 10, 6, 18, 56, 14, 449000, tzinfo=timezone.utc),
        "last_transaction_at": datetime(2026, 10, 6, 18, 56, 14, 450000, tzinfo=timezone.utc),
    }


class AlertConsumerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.connection.execute(
            """
            CREATE TABLE fraud_alerts (
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

    def tearDown(self) -> None:
        self.connection.close()

    def test_replayed_alert_is_stored_once_and_offset_is_committed(self) -> None:
        committed_offsets: list[int] = []

        stored = []
        for offset in (10, 10):
            stored.append(
                handle_alert(
                    self.connection,
                    sample_alert(),
                    "desafio.fraud.detected",
                    5,
                    offset,
                    lambda offset=offset: committed_offsets.append(offset),
                )
            )

        stored_count = self.connection.execute(
            "SELECT COUNT(*) FROM fraud_alerts"
        ).fetchone()[0]
        self.assertEqual(stored, [True, False])
        self.assertEqual(stored_count, 1)
        self.assertEqual(committed_offsets, [10, 10])

    def test_redelivery_after_offset_commit_failure_is_idempotent(self) -> None:
        def fail_commit() -> None:
            raise RuntimeError("simulated Kafka offset commit failure")

        with self.assertRaisesRegex(RuntimeError, "offset commit failure"):
            handle_alert(
                self.connection,
                sample_alert(),
                "desafio.fraud.detected",
                5,
                10,
                fail_commit,
            )

        commits: list[bool] = []
        is_new = handle_alert(
            self.connection,
            sample_alert(),
            "desafio.fraud.detected",
            5,
            10,
            lambda: commits.append(True),
        )

        self.assertFalse(is_new)
        self.assertEqual(commits, [True])
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM fraud_alerts").fetchone()[0],
            1,
        )

    def test_rejects_timestamp_without_timezone(self) -> None:
        alert = sample_alert()
        alert["first_transaction_at"] = "2026-10-06T18:56:14.449"
        with self.assertRaisesRegex(ValueError, "include a timezone"):
            normalize_alert(alert)

    def test_database_is_persistent_and_creates_parent_directory(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "nested" / "alerts.sqlite"
            connection = open_database(str(database_path))
            connection.close()
            self.assertTrue(database_path.is_file())


if __name__ == "__main__":
    unittest.main()
