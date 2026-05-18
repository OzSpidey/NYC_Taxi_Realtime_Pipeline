"""
Unit tests — no Azure credentials needed.
Run: pytest tests/ -v
"""

import importlib
import importlib.util
import json
import pathlib
import sys
import types
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch


# ── Helpers to stub Azure SDK imports so tests run without credentials ────────

def _stub_azure_modules():
    # Build a hierarchy of stub modules so nested imports resolve
    stub_names = [
        "azure",
        "azure.eventhub",
        "azure.eventhub.aio",
        "azure.identity",
        "azure.identity.aio",
        "azure.functions",
        "pyodbc",
    ]
    for mod in stub_names:
        if mod not in sys.modules:
            sys.modules[mod] = types.ModuleType(mod)

    # Wire parent → child so `from azure.eventhub.aio import X` resolves
    sys.modules["azure"].eventhub = sys.modules["azure.eventhub"]
    sys.modules["azure.eventhub"].aio = sys.modules["azure.eventhub.aio"]
    sys.modules["azure"].identity = sys.modules["azure.identity"]
    sys.modules["azure.identity"].aio = sys.modules["azure.identity.aio"]

    # azure.eventhub.aio — needs EventHubProducerClient
    aeh_aio = sys.modules["azure.eventhub.aio"]
    aeh_aio.EventHubProducerClient = MagicMock()

    # azure.identity.aio — needs DefaultAzureCredential
    aid_aio = sys.modules["azure.identity.aio"]
    aid_aio.DefaultAzureCredential = MagicMock()

    # azure.functions — needs EventHubEvent and FunctionApp
    af = sys.modules["azure.functions"]
    if not hasattr(af, "EventHubEvent"):
        class _FakeEvent:
            def __init__(self, body: bytes):
                self._body = body
            def get_body(self):
                return self._body
        af.EventHubEvent = _FakeEvent

        # Make all decorator methods passthrough so the real function is preserved
        class _PassthroughApp:
            def __getattr__(self, _name):
                def _decorator_factory(**_kwargs):
                    return lambda fn: fn  # return the original function unchanged
                return _decorator_factory

        af.FunctionApp = lambda: _PassthroughApp()

    # azure.eventhub — needs EventData
    aeh = sys.modules["azure.eventhub"]
    if not hasattr(aeh, "EventData"):
        aeh.EventData = lambda body: {"body": body}


_stub_azure_modules()


# ── Import modules under test ─────────────────────────────────────────────────

_BASE = pathlib.Path(__file__).parent.parent

def _load(path):
    spec = importlib.util.spec_from_file_location("_mod", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

with patch.dict("os.environ", {
    "EVENT_HUB_NAMESPACE": "test.servicebus.windows.net",
    "EVENT_HUB_NAME": "taxi-events",
    "SQL_CONNECTION_STRING": "Driver=test;Server=test",
}):
    producer_mod = _load(_BASE / "src" / "event_producer.py")
    func_mod     = _load(_BASE / "function_app" / "function_app.py")


# ── Tests: event producer ─────────────────────────────────────────────────────

class TestEventProducer(unittest.TestCase):

    def test_event_has_required_keys(self):
        event = producer_mod._make_event()
        required = {
            "ride_id", "pickup_borough", "dropoff_borough", "driver_id",
            "distance_miles", "fare_amount", "surge_multiplier",
            "payment_type", "passenger_count", "event_time",
        }
        self.assertTrue(required.issubset(event.keys()))

    def test_ride_id_is_uuid(self):
        import uuid
        event = producer_mod._make_event()
        uuid.UUID(event["ride_id"])  # raises ValueError if not valid UUID

    def test_fare_is_positive(self):
        for _ in range(20):
            self.assertGreater(producer_mod._make_event()["fare_amount"], 0)

    def test_surge_is_known_value(self):
        valid = {1.0, 1.5, 2.0, 2.5}
        for _ in range(30):
            self.assertIn(producer_mod._make_event()["surge_multiplier"], valid)

    def test_borough_is_valid(self):
        valid = {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"}
        for _ in range(20):
            e = producer_mod._make_event()
            self.assertIn(e["pickup_borough"], valid)
            self.assertIn(e["dropoff_borough"], valid)

    def test_event_time_is_iso_utc(self):
        event = producer_mod._make_event()
        dt = datetime.fromisoformat(event["event_time"])
        self.assertIsNotNone(dt.tzinfo)

    def test_passenger_count_range(self):
        for _ in range(20):
            count = producer_mod._make_event()["passenger_count"]
            self.assertGreaterEqual(count, 1)
            self.assertLessEqual(count, 4)

    def test_distance_range(self):
        for _ in range(20):
            d = producer_mod._make_event()["distance_miles"]
            self.assertGreaterEqual(d, 0.5)
            self.assertLessEqual(d, 25.0)

    def test_payment_type_is_valid(self):
        valid = {"credit_card", "cash", "no_charge", "dispute"}
        for _ in range(20):
            self.assertIn(producer_mod._make_event()["payment_type"], valid)

    def test_event_serialises_to_json(self):
        event = producer_mod._make_event()
        raw = json.dumps(event)
        parsed = json.loads(raw)
        self.assertEqual(parsed["ride_id"], event["ride_id"])


# ── Tests: function app ───────────────────────────────────────────────────────

class TestFunctionApp(unittest.TestCase):

    def _make_hub_event(self, data: dict):
        body = json.dumps(data).encode()
        return sys.modules["azure.functions"].EventHubEvent(body)

    def test_upsert_sql_has_merge(self):
        self.assertIn("MERGE", func_mod._MERGE_SQL)

    def test_upsert_sql_has_all_columns(self):
        for col in ("pickup_borough", "avg_surge", "ride_count", "max_fare", "alert_time"):
            self.assertIn(col, func_mod._MERGE_SQL)

    def test_malformed_event_skipped(self):
        events = [self._make_hub_event({"bad": "data"})]
        with patch.object(func_mod, "_upsert_alerts") as mock_upsert:
            func_mod.process_surge_alerts(events)
            mock_upsert.assert_not_called()

    def test_valid_events_upserted(self):
        payload = {
            "pickup_borough": "Manhattan",
            "avg_surge": 2.1,
            "ride_count": 15,
            "max_fare": 45.0,
            "alert_time": "2024-01-01T12:00:00Z",
        }
        events = [self._make_hub_event(payload)]
        with patch.object(func_mod, "_upsert_alerts") as mock_upsert:
            func_mod.process_surge_alerts(events)
            mock_upsert.assert_called_once()
            rows = mock_upsert.call_args[0][0]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], "Manhattan")

    def test_empty_event_list_no_op(self):
        with patch.object(func_mod, "_upsert_alerts") as mock_upsert:
            func_mod.process_surge_alerts([])
            mock_upsert.assert_not_called()


if __name__ == "__main__":
    unittest.main()
