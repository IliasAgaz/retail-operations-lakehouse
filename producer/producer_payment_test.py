import json
from getpass import getpass

from confluent_kafka import Producer


# 1. Read connection details.
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


# 2. Define a payment event.
event = {
    "event_id": "evt-payment-10001-v1",
    "event_type": "payment_captured",
    "schema_version": 1,
    "payment_id": "PAY10001",
    "payment_version": 1,
    "order_id": "O10001",
    "event_time": "2026-10-06T12:00:00Z",
    "status": "captured",
    "amount": "50.00",
    "currency": "GBP",
    "payment_method": "card",
}


# 3. Track the delivery result.
delivery = {"confirmed": False, "error": None}


def on_delivery(error, message):
    if error is not None:
        delivery["error"] = str(error)
    else:
        delivery["confirmed"] = True
        print(
            f"Delivered to topic={message.topic()}, "
            f"partition={message.partition()}, "
            f"offset={message.offset()}"
        )


# 4. Send the payment.
producer.produce(
    topic="payments",
    key=json.dumps(event["payment_id"]).encode("utf-8"),
    value=json.dumps(event).encode("utf-8"),
    on_delivery=on_delivery,
)

remaining = producer.flush(timeout=35)


# 5. Verify Kafka acknowledged it.
if delivery["error"] is not None:
    raise RuntimeError(
        f"Payment delivery failed: {delivery['error']}"
    )

if remaining > 0 or not delivery["confirmed"]:
    raise RuntimeError("Payment delivery was not confirmed.")

print(json.dumps(event, indent=2))
print("Synthetic payment acknowledged by Kafka.")