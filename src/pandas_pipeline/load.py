import logging
import os
import sqlite3
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def write_outputs(valid_df, invalid_df, staging_dir="data/staging", quarantine_dir="data/quarantine"):
    os.makedirs(staging_dir, exist_ok=True)
    os.makedirs(quarantine_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    valid_path = f"{staging_dir}/flights_processed_{ts}.csv"
    invalid_path = f"{quarantine_dir}/flights_quarantine_{ts}.csv"

    valid_df.to_csv(valid_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)

    return valid_path, invalid_path


def load_sqlite(df, db_path="data/processed/aviation.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS flights (
            flight_id TEXT PRIMARY KEY,
            flight_number TEXT,
            flight_date TEXT,
            departure_airport TEXT,
            arrival_airport TEXT,
            scheduled_departure_time TEXT,
            scheduled_arrival_time TEXT,
            airline TEXT
        )
        """
    )

    existing_ids = {row[0] for row in conn.execute("SELECT flight_id FROM flights")}
    duplicate_ids = existing_ids & set(df["flight_id"])
    if duplicate_ids:
        logger.warning(
            "Skipping %s rows already present in flights: %s",
            len(duplicate_ids),
            sorted(duplicate_ids),
        )

    rows = [
        (
            row.flight_id,
            row.flight_number,
            row.flight_date,
            row.departure_airport,
            row.arrival_airport,
            row.scheduled_departure_time.isoformat() if pd.notnull(row.scheduled_departure_time) else None,
            row.scheduled_arrival_time.isoformat() if pd.notnull(row.scheduled_arrival_time) else None,
            row.airline,
        )
        for row in df.itertuples(index=False)
    ]

    cursor = conn.executemany(
        """
        INSERT OR IGNORE INTO flights (
            flight_id, flight_number, flight_date, departure_airport,
            arrival_airport, scheduled_departure_time, scheduled_arrival_time,
            airline
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()

    inserted_rows = cursor.rowcount if cursor.rowcount != -1 else 0
    skipped_rows = len(rows) - inserted_rows
    total_rows = conn.execute("SELECT COUNT(*) FROM flights").fetchone()[0]
    conn.close()

    return {
        "inserted_rows": inserted_rows,
        "skipped_rows": skipped_rows,
        "total_rows": total_rows,
    }
