import json
from getpass import getpass
from pathlib import Path

from confluent_kafka import Producer


# 1. Locate the saved batch and its expected results.
project_dir = Path(__file__).resolve().parent

events_path = project_dir / "payments_batch_001.jsonl"
manifest_path = project_dir / "payments_batch_001_manifest.json"

with manifest_path.open("r", encoding="utf-8") as file:
    manifest = json.load(file)

with events_path.open("r", encoding="utf-8") as file:
    events = [
        json.loads(line)
        for line in file
        if line.strip()
    ]

expected_count = manifest["messages_to_send"]

if len(events) != expected_count:
    raise ValueError(
        f"Expected {expected_count} messages, "
        f"but the file contains {len(events)}."
    )


# 2. Configure the producer.
bootstrap_servers = input(
    "Kafka bootstrap server (hostname:port): "
).strip()

api_key = getpass("Producer API key: ").strip()
api_secret = getpass("Producer API secret: ").strip()

producer = Producer({
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": api_key,
    "sasl.password": api_secret,
    "acks": "all",
    "delivery.timeout.ms": 30000,
})


# 3. Track Kafka acknowledgements.
delivery = {"succeeded": 0, "failed": 0}


def on_delivery(error, message):
    if error is not None:
        delivery["failed"] += 1
        print(f"Delivery failed: {error}")
    else:
        delivery["succeeded"] += 1


# 4. Send every saved message, including deliberate duplicates.
for event in events:
    producer.produce(
        topic="payments",
        key=json.dumps(event["payment_id"]).encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=on_delivery,
    )

    producer.poll(0)


# 5. Wait for delivery and verify the result.
remaining = producer.flush(timeout=35)

print("Batch:", manifest["batch_id"])
print("Expected:", expected_count)
print("Confirmed:", delivery["succeeded"])
print("Failed:", delivery["failed"])
print("Still queued:", remaining)

if (
    delivery["succeeded"] != expected_count
    or delivery["failed"] > 0
    or remaining > 0
):
    raise RuntimeError(
        "Some messages were not confirmed. "
        "Inspect the results before replaying the batch."
    )

print("Payment batch acknowledged by Kafka.")