from getpass import getpass
from confluent_kafka.admin import AdminClient

bootstrap_servers = input("Bootstrap server: ").strip()
api_key = getpass("API key: ").strip()
api_secret = getpass("API secret: ").strip()

client = AdminClient({
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": api_key,
    "sasl.password": api_secret,
})

try:
    metadata = client.list_topics(timeout=15)

    print("Metadata request succeeded.")
    print("Broker count:", len(metadata.brokers))

except Exception as error:
    print("Metadata request failed:")
    print(error)