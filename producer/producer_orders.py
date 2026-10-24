import json
import random
import uuid
from datetime import datetime, timezone
from getpass import getpass

from confluent_kafka import Producer


# 1. Configure this test run.
ORDER_COUNT = 100
TOPIC = "orders"

rng = random.Random(42)
run_id = uuid.uuid4().hex[:12]

products = [
    {"product_id": "P001", "unit_price": 25.00},
    {"product_id": "P002", "unit_price": 49.50},
    {"product_id": "P003", "unit_price": 12.75},
    {"product_id": "P004", "unit_price": 89.00},
    {"product_id": "P005", "unit_price": 7.50},
]


# 2. Collect your Kafka connection details.
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


# 3. Track delivery confirmations.
delivery = {"succeeded": 0, "failed": 0}


def on_delivery(error, message):
    if error is not None:
        delivery["failed"] += 1
        print(f"Delivery failed: {error}")
    else:
        delivery["succeeded"] += 1


# 4. Generate and send the orders.
print(f"Run ID: {run_id}")

for order_number in range(1, ORDER_COUNT + 1):
    order_id = f"O-{run_id}-{order_number:04d}"

    selected_products = rng.sample(
        products,
        k=rng.randint(1, 3),
    )

    items = [
        {
            "line_id": line_number,
            "product_id": product["product_id"],
            "quantity": rng.randint(1, 4),
            "unit_price": product["unit_price"],
        }
        for line_number, product in enumerate(
            selected_products, start=1
        )
    ]

    event = {
        "event_id": f"evt-{order_id}-v1",
        "event_type": "order_created",
        "schema_version": 1,
        "order_id": order_id,
        "order_version": 1,
        "customer_id": f"C{rng.randint(1, 20):03d}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "status": "placed",
        "currency": "GBP",
        "items": items,
    }

    producer.produce(
        topic=TOPIC,
        key=json.dumps(order_id).encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        on_delivery=on_delivery,
    )

    producer.poll(0)


# 5. Wait for acknowledgements and check the result.
remaining = producer.flush(timeout=35)

print(f"Run ID: {run_id}")
print(f"Expected: {ORDER_COUNT}")
print(f"Confirmed: {delivery['succeeded']}")
print(f"Failed: {delivery['failed']}")
print(f"Still queued: {remaining}")

if (
    delivery["succeeded"] != ORDER_COUNT
    or delivery["failed"] > 0
    or remaining > 0
):
    raise RuntimeError(
        "Some deliveries were not confirmed. "
        "Check the output before sending another batch."
    )

print("All 100 orders were acknowledged by Kafka.")