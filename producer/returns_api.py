import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse


# Synthetic records representing the current state of return requests.
RETURNS = [
    {
        "return_id": "RET-0001",
        "order_id": "O-21a85a68eca6",
        "line_id": 1,
        "quantity": 1,
        "reason": "damaged",
        "status": "approved",
        "created_at": "2026-10-18T10:00:00Z",
        "updated_at": "2026-10-20T10:00:00Z",
    }
]

REFUNDS = [
    {
        "refund_id": "REF-0001",
        "return_id": "RET-0001",
        "payment_id": "PAY-21a85a68eca6",
        "amount": "25.00",
        "currency": "GBP",
        "status": "succeeded",
        "created_at": "2026-10-20T12:00:00Z",
        "updated_at": "2026-10-21T10:00:00Z",
    }
]

class ReturnsAPIHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        # Extract the path from the requested URL.
        path = urlparse(self.path).path

        if path == "/health":
            self.send_json(
                200,
                {"status": "ok"}
            )

        elif path == "/returns":
            self.send_json(
                200,
                {
                    "data": RETURNS,
                    "record_count": len(RETURNS),
                    "next_cursor": None,
                }
            )
            
        elif path == "/refunds":
            self.send_json(
                200,
                {
                    "data": REFUNDS,
                    "record_count": len(REFUNDS),
                    "next_cursor": None,
                }
            )

        else:
            self.send_json(
                404,
                {"error": "Endpoint not found"}
            )

    def send_json(self, status_code, payload):
        # Convert Python objects into JSON bytes.
        body = json.dumps(payload).encode("utf-8")

        # Send the HTTP status and response headers.
        self.send_response(status_code)
        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8"
        )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

        # Send the actual response body.
        self.wfile.write(body)


if __name__ == "__main__":
    server = HTTPServer(
        ("127.0.0.1", 8000),
        ReturnsAPIHandler
    )

    print("Returns API running at http://127.0.0.1:8000")
    print("Returns endpoint: http://127.0.0.1:8000/returns")
    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping the returns API.")
    finally:
        server.server_close()