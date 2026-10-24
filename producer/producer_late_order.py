import json
from getpass import getpass

from confluent_kafka import Producer


# Connect using the existing producer credentials.
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


# This order occurred before the payment but is delivered later.
event = {
    "event_id": "evt-order-B001-late-v1",
    "event_type": "order_created",
    "schema_version": 1,
    "order_id": "O-B001-NOT-YET-ARRIVED",
    "order_version": 1,
    "customer_id": "C001",
    "event_time": "2026-10-10T11:59:00Z",
    "status": "placed",
    "currency": "GBP",
    "items": [
        {
            "line_id": 1,
            "product_id": "P005",
            "quantity": 2,
            "unit_price": 7.50,
        }
    ],
}


delivery = {"confirmed": False, "error": None}


def on_delivery(error, message):
    if error is not None:
        delivery["error"] = str(error)
    else:
        delivery["confirmed"] = True
        print(
            f"Delivered: topic={message.topic()}, "
            f"partition={message.partition()}, "
            f"offset={message.offset()}"
        )


producer.produce(
    topic="orders",
    key=json.dumps(event["order_id"]).encode("utf-8"),
    value=json.dumps(event).encode("utf-8"),
    on_delivery=on_delivery,
)

remaining = producer.flush(timeout=35)

if delivery["error"] is not None:
    raise RuntimeError(delivery["error"])

if remaining > 0 or not delivery["confirmed"]:
    raise RuntimeError("Order delivery was not confirmed.")

print("Late-arriving order acknowledged by Kafka.")