"""
Azure Function — Event Hub trigger on the SurgeAlert consumer group.

Receives surge-alert records emitted by Stream Analytics' SlidingWindow query
and upserts them into Azure SQL Database (or Synapse) using a MERGE statement,
so re-deliveries are idempotent.

Required App Settings (set in Azure Portal or local.settings.json):
    SQL_CONNECTION_STRING  — ADO.NET connection string for the target DB
"""

import json
import logging
import os

import azure.functions as func
import pyodbc

app = func.FunctionApp()

_conn_str = os.environ.get("SQL_CONNECTION_STRING", "")


def _get_conn():
    return pyodbc.connect(_conn_str, autocommit=False)


@app.event_hub_message_trigger(
    arg_name="events",
    event_hub_name="%EVENT_HUB_NAME%",
    connection="EventHubConnection",
    consumer_group="surge-alerts",
    cardinality="many",
)
def process_surge_alerts(events: list[func.EventHubEvent]) -> None:
    if not events:
        return

    rows = []
    for event in events:
        try:
            data = json.loads(event.get_body().decode())
            rows.append(
                (
                    data["pickup_borough"],
                    float(data["avg_surge"]),
                    int(data["ride_count"]),
                    float(data["max_fare"]),
                    data["alert_time"],
                )
            )
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            logging.warning("Skipping malformed event: %s", exc)

    if not rows:
        return

    _upsert_alerts(rows)
    logging.info("Upserted %d surge alert rows", len(rows))


_MERGE_SQL = """
MERGE dbo.surge_alerts AS target
USING (VALUES (?, ?, ?, ?, ?)) AS source
    (pickup_borough, avg_surge, ride_count, max_fare, alert_time)
ON  target.pickup_borough = source.pickup_borough
AND target.alert_time     = source.alert_time
WHEN MATCHED THEN
    UPDATE SET
        avg_surge  = source.avg_surge,
        ride_count = source.ride_count,
        max_fare   = source.max_fare
WHEN NOT MATCHED THEN
    INSERT (pickup_borough, avg_surge, ride_count, max_fare, alert_time)
    VALUES (source.pickup_borough, source.avg_surge,
            source.ride_count, source.max_fare, source.alert_time);
"""


def _upsert_alerts(rows: list[tuple]) -> None:
    with _get_conn() as conn:
        cursor = conn.cursor()
        cursor.executemany(_MERGE_SQL, rows)
        conn.commit()
