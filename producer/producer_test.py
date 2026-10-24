import json
import uuid
from datetime import datetime, timezone
from getpass import getpass

from confluent_kafka import Producer


# 1. Read connection details.
bootstrap_servers = input(
    "Kafka bootstrap server (hostname:port): "
).strip()

api_key = getpass("Producer API key: ").strip()
api_secret = getpass("Producer API secret: ").strip()

if not bootstrap_servers or not api_key or not api_secret:
    raise ValueError(
        "Bootstrap server, API key, and API secret are required."
    )


# 2. Configure the producer.
producer = Producer({
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": api_key,
    "sasl.password": api_secret,
    "acks": "all",
    "delivery.timeout.ms": 30000,
})


# 3. Build one new order event.
order_id = f"O-{uuid.uuid4().hex[:12]}"

event = {
    "event_id": str(uuid.uuid4()),
    "event_type": "order_created",
    "schema_version": 1,
    "order_id": order_id,
    "order_version": 1,
    "customer_id": "C001",
    "event_time": datetime.now(timezone.utc).isoformat(),
    "status": "placed",
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


# 4. Track the delivery outcome.
delivery_result = {
    "confirmed": False,
    "error": None,
}


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


# 5. Serialize and queue the event.
producer.produce(
    topic="orders",
    key=json.dumps(order_id).encode("utf-8"),
    value=json.dumps(event).encode("utf-8"),
    on_delivery=on_delivery,
)


# 6. Wait for delivery confirmation.
remaining = producer.flush(timeout=35)

if delivery_result["error"] is not None:
    raise RuntimeError(
        f"Kafka delivery failed: {delivery_result['error']}"
    )

if remaining > 0 or not delivery_result["confirmed"]:
    raise RuntimeError(
        "Delivery was not confirmed before the timeout."
    )


# 7. Show the successfully delivered event.
print(f"Order ID: {order_id}")
print(json.dumps(event, indent=2))