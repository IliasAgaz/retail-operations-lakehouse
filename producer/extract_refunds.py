import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen


API_URL = "http://127.0.0.1:8000/refunds"

OUTPUT_DIRECTORY = (
    Path(__file__).resolve().parent
    / "exports"
    / "refunds"
)


def main():
    # Fetch the response from the local API.
    with urlopen(API_URL, timeout=30) as response:
        payload = json.loads(
            response.read().decode("utf-8")
        )

    # Validate the response structure.
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object from the API.")

    records = payload.get("data")

    if not isinstance(records, list):
        raise ValueError("The API's data field must be a list.")

    if not all(isinstance(record, dict) for record in records):
        raise ValueError("Each refund must be a JSON object.")

    if payload.get("record_count") != len(records):
        raise ValueError(
            "The API record_count does not match the records received."
        )

    if payload.get("next_cursor") is not None:
        raise ValueError(
            "The API has more pages. Pagination must be implemented "
            "before exporting this response."
        )

    if not records:
        print("The API returned no refunds. No file was created.")
        return

    # Identify this extraction.
    extracted_at = datetime.now(timezone.utc)
    extraction_id = uuid.uuid4().hex

    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

    filename = (
        f"refunds_{extracted_at.strftime('%Y%m%dT%H%M%S%fZ')}"
        f"_{extraction_id[:8]}.json"
    )

    output_path = OUTPUT_DIRECTORY / filename
    temporary_path = output_path.with_suffix(".json.tmp")

    # Preserve each source record with extraction metadata.
    with temporary_path.open(
        "x",
        encoding="utf-8",
        newline="\n"
    ) as output_file:

        for record in records:
            exported_record = {
                "refund_payload": record,
                "source_system": "simulated_returns_api",
                "extraction_id": extraction_id,
                "extracted_at": extracted_at.isoformat(),
            }

            output_file.write(
                json.dumps(exported_record, ensure_ascii=False)
                + "\n"
            )

    # Make the completed export available under its final name.
    temporary_path.rename(output_path)

    print(f"Extracted refunds: {len(records)}")
    print(f"Extraction ID: {extraction_id}")
    print(f"Saved file: {output_path}")


if __name__ == "__main__":
    main()