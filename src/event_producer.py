"""
Simulates a stream of NYC taxi ride events and publishes them to Azure Event Hubs.

Run locally:
    pip install -r requirements.txt
    python src/event_producer.py

Environment variables (or .env file):
    EVENT_HUB_NAMESPACE  e.g. my-namespace.servicebus.windows.net
    EVENT_HUB_NAME       e.g. taxi-events
    EVENTS_PER_SECOND    (optional) default 10
"""

import asyncio
import json
import os
import random
import uuid
from datetime import datetime, timezone

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.identity.aio import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()

NAMESPACE = os.environ["EVENT_HUB_NAMESPACE"]
HUB_NAME = os.environ["EVENT_HUB_NAME"]
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "10"))

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
PAYMENT_TYPES = ["credit_card", "cash", "no_charge", "dispute"]


def _make_event() -> dict:
    distance = round(random.uniform(0.5, 25.0), 2)
    base_fare = round(2.50 + distance * 1.75, 2)
    surge = random.choice([1.0, 1.0, 1.0, 1.5, 2.0, 2.5])
    fare = round(base_fare * surge, 2)
    return {
        "ride_id": str(uuid.uuid4()),
        "pickup_borough": random.choice(BOROUGHS),
        "dropoff_borough": random.choice(BOROUGHS),
        "driver_id": f"D{random.randint(1000, 1999):04d}",
        "distance_miles": distance,
        "fare_amount": fare,
        "surge_multiplier": surge,
        "payment_type": random.choice(PAYMENT_TYPES),
        "passenger_count": random.randint(1, 4),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


async def produce(total_events: int = 0) -> None:
    """
    Publish events forever (total_events=0) or until total_events is reached.
    Used in tests with total_events > 0.
    """
    credential = DefaultAzureCredential()
    producer = EventHubProducerClient(
        fully_qualified_namespace=NAMESPACE,
        eventhub_name=HUB_NAME,
        credential=credential,
    )

    sent = 0
    delay = 1.0 / EVENTS_PER_SECOND

    async with producer:
        while True:
            batch = await producer.create_batch()
            for _ in range(EVENTS_PER_SECOND):
                payload = json.dumps(_make_event()).encode()
                batch.add(EventData(payload))
                sent += 1
                if total_events and sent >= total_events:
                    await producer.send_batch(batch)
                    print(f"Sent {sent} events — stopping.")
                    return
            await producer.send_batch(batch)
            print(f"[{datetime.now().isoformat()}] Sent {EVENTS_PER_SECOND} events (total={sent})")
            await asyncio.sleep(delay * EVENTS_PER_SECOND)


if __name__ == "__main__":
    asyncio.run(produce())
