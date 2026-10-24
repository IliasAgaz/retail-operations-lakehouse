import json
from getpass import getpass

from confluent_kafka import Producer


# Collect the same connection details used previously.
bootstrap_servers = input("Kafka bootstrap server (hostname:port): ").strip()
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


# Define one update to the existing order.
event = {
    "event_id": "evt-order-10001-v2",
    "event_type": "order_updated",
    "schema_version": 1,
    "order_id": "O10001",
    "order_version": 2,
    "customer_id": "C001",
    "event_time": "2026-10-06T12:00:00Z",
    "status": "paid",
    "currency": "GBP",
    "items": [
        {
            "line_id": 1,
            "product_id": "P001",
            "quantity": 2,
            "unit_price": 25.00,
        }
    ],
}


# Track acknowledgements.
delivery = {"succeeded": 0, "failed": 0}


def on_delivery(error, message):
    if error is not None:
        delivery["failed"] += 1
        print(f"Delivery failed: {error}")
    else:
        delivery["succeeded"] += 1
        print(
            f"Delivered: partition={message.partition()}, "
            f"offset={message.offset()}"
        )


# Serialize once so both deliveries contain exactly the same data.
message_key = json.dumps(event["order_id"]).encode("utf-8")
message_value = json.dumps(event).encode("utf-8")

# Send the same event twice.
for _ in range(2):
    producer.produce(
        topic="orders",
        key=message_key,
        value=message_value,
        on_delivery=on_delivery,
    )
    producer.poll(0)

remaining = producer.flush(timeout=35)

print("Confirmed:", delivery["succeeded"])
print("Failed:", delivery["failed"])
print("Still queued:", remaining)

if (
    delivery["succeeded"] != 2
    or delivery["failed"] > 0
    or remaining > 0
):
    raise RuntimeError("Both deliveries were not confirmed.")

print("Update and duplicate were acknowledged by Kafka.")