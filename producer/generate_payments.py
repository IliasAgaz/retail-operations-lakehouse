import csv
import json
from decimal import Decimal
from pathlib import Path


# 1. Define input and output files beside this script.
project_dir = Path(__file__).resolve().parent

seed_path = project_dir / "payment_seed.csv"
events_path = project_dir / "payments_batch_001.jsonl"
manifest_path = project_dir / "payments_batch_001_manifest.json"

if events_path.exists() or manifest_path.exists():
    raise FileExistsError(
        "This batch already exists. Keep it for replay."
    )


# 2. Read and validate the order seed.
with seed_path.open(
    "r", encoding="utf-8-sig", newline=""
) as file:
    orders = list(csv.DictReader(file))

orders.sort(key=lambda row: row["order_id"])

if len(orders) < 25:
    raise ValueError("At least 25 orders are required.")

if len({row["order_id"] for row in orders}) != len(orders):
    raise ValueError("The seed contains duplicate order IDs.")

for order in orders:
    total = Decimal(order["order_total"])

    if order["currency"] != "GBP":
        raise ValueError("This batch supports GBP only.")

    if not total.is_finite() or total <= 0:
        raise ValueError("Order totals must be finite and positive.")

    if total != total.quantize(Decimal("0.01")):
        raise ValueError("Order totals must have at most two decimals.")


# 3. Build payment events with fixed identifiers and timestamps.
def make_payment(label, order_id, amount):
    payment_id = f"PAY-B001-{label}"

    return {
        "event_id": f"evt-{payment_id}-v1",
        "event_type": "payment_captured",
        "schema_version": 1,
        "payment_id": payment_id,
        "payment_version": 1,
        "order_id": order_id,
        "event_time": "2026-10-10T12:00:00Z",
        "status": "captured",
        "amount": format(amount, ".2f"),
        "currency": "GBP",
        "payment_method": "card",
    }


events = []
expected_order_payments = []


# 4. Create one full payment for each of the first 20 orders.
for number, order in enumerate(orders[:20], start=1):
    total = Decimal(order["order_total"])

    events.append(
        make_payment(
            f"FULL-{number:03d}",
            order["order_id"],
            total,
        )
    )

    expected_order_payments.append({
        "order_id": order["order_id"],
        "payment_count": 1,
        "expected_paid": format(total, ".2f"),
    })


# 5. Split each of the next five orders into two payments.
for number, order in enumerate(orders[20:25], start=1):
    total = Decimal(order["order_total"])
    total_pence = int(total * 100)

    if total_pence < 2:
        raise ValueError("Split payments require at least two pence.")

    first_amount = Decimal(total_pence // 2) / Decimal(100)
    second_amount = total - first_amount

    events.append(
        make_payment(
            f"SPLIT-{number:03d}-A",
            order["order_id"],
            first_amount,
        )
    )

    events.append(
        make_payment(
            f"SPLIT-{number:03d}-B",
            order["order_id"],
            second_amount,
        )
    )

    expected_order_payments.append({
        "order_id": order["order_id"],
        "payment_count": 2,
        "expected_paid": format(total, ".2f"),
    })


# 6. Add three identical duplicate deliveries.
events.extend([
    events[0].copy(),
    events[1].copy(),
    events[2].copy(),
])


# 7. Add two deliberately invalid payments.
events.append(
    make_payment(
        "INVALID-NEGATIVE",
        orders[0]["order_id"],
        Decimal("-10.00"),
    )
)

missing_order = make_payment(
    "INVALID-MISSING-ORDER",
    orders[1]["order_id"],
    Decimal("10.00"),
)
missing_order["order_id"] = None
events.append(missing_order)


# 8. Add a valid payment whose order has not arrived yet.
events.append(
    make_payment(
        "EARLY-ORDER",
        "O-B001-NOT-YET-ARRIVED",
        Decimal("15.00"),
    )
)


# 9. Save the messages and the expected results.
manifest = {
    "batch_id": "B001",
    "messages_to_send": len(events),
    "expected_valid_deliveries": 34,
    "expected_invalid_deliveries": 2,
    "expected_distinct_valid_events": 31,
    "expected_duplicate_extra_deliveries": 3,
    "expected_conflicting_event_ids": 0,
    "expected_unmatched_payment_id": "PAY-B001-EARLY-ORDER",
    "expected_order_payments": expected_order_payments,
}

with events_path.open("x", encoding="utf-8") as file:
    for event in events:
        file.write(json.dumps(event) + "\n")

with manifest_path.open("x", encoding="utf-8") as file:
    json.dump(manifest, file, indent=2)

print(f"Generated {len(events)} messages.")
print(f"Events: {events_path}")
print(f"Expected results: {manifest_path}")