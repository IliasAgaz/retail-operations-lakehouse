import json
from getpass import getpass

from confluent_kafka import Producer


# Read connection details.
bootstrap_servers = input("Kafka bootstrap server: ").strip()
api_key = getpass("Producer API key: ").strip()
api_secret = getpass("Producer API secret: ").strip()

if not bootstrap_servers or not api_key or not api_secret:
    raise ValueError("All connection details are required.")


producer = Producer({
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": api_key,
    "sasl.password": api_secret,
    "acks": "all",
    "delivery.timeout.ms": 30000,
})


# Fixed identifiers and timestamp make retries send the same event.
payment_id = "PAY-21a85a68eca6"

event = {
    "event_id": "evt-PAY-21a85a68eca6-v1",
    "event_type": "payment_captured",
    "schema_version": 1,
    "payment_id": payment_id,
    "payment_version": 1,
    "order_id": "O-21a85a68eca6",
    "event_time": "2026-10-17T12:30:00+00:00",
    "status": "captured",
    "amount": "50.00",
    "currency": "GBP",
    "payment_method": "card",
}


delivery_result = {"confirmed": False, "error": None}


def on_delivery(error, message):
    if error is not None:
        delivery_result["error"] = str(error)
    else:
        delivery_result["confirmed"] = True
        print(
            f"Delivered to topic={message.topic()}, "
            f"partition={message.partition()}, "
            f"offset={message.offset()}"
        )


producer.produce(
    topic="payments",
    key=json.dumps(payment_id).encode("utf-8"),
    value=json.dumps(event).encode("utf-8"),
    on_delivery=on_delivery,
)

remaining = producer.flush(timeout=35)

if delivery_result["error"] is not None:
    raise RuntimeError(delivery_result["error"])

if remaining > 0 or not delivery_result["confirmed"]:
    raise RuntimeError("Payment delivery was not confirmed.")

print(json.dumps(event, indent=2))