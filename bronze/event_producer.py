# Databricks notebook source
# property_producer.py

import json
import time
import random
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# ── Key Vault config ──────────────────────────────────────
VAULT_URL    = "https://property-pulse-kv.vault.azure.net/"
credential   = DefaultAzureCredential()
kv_client    = SecretClient(vault_url=VAULT_URL, credential=credential)

# ── Fetch connection string from Key Vault ────────────────
conn_str     = kv_client.get_secret("eventhub-connection-string").value
EVENTHUB     = "property-events"

# ── Sample data pools ─────────────────────────────────────
CITIES = {
    "Bengaluru" : ["Whitefield", "Koramangala", "Indiranagar", "HSR Layout"],
    "Mumbai"    : ["Bandra", "Andheri", "Powai", "Worli"],
    "Hyderabad" : ["Gachibowli", "Madhapur", "Kondapur", "Jubilee Hills"],
    "Pune"      : ["Hinjewadi", "Kharadi", "Baner", "Viman Nagar"],
    "Chennai"   : ["Anna Nagar", "Virugambakkam", "Ashok Nagar", "ICF"]
}
EVENT_TYPES  = ["NEW_LISTING", "PRICE_CHANGE", "REMOVED"]

# ── Event generator ───────────────────────────────────────
def generate_event():
    city        = random.choice(list(CITIES.keys()))
    locality    = random.choice(CITIES[city])
    event_type  = random.choice(EVENT_TYPES)
    price       = random.randint(3000000, 15000000) if event_type != "REMOVED" else None

    return {
        "listing_id"  : f"LST-{random.randint(10000, 99999)}",
        "event_type"  : event_type,
        "city"        : city,
        "locality"    : locality,
        "price"       : price,
        "bedrooms"    : random.choice([1, 2, 3, 4, 5]),
        "area_sqft"   : random.randint(500, 3000),
        "agent_id"    : f"AGT-{random.randint(100, 999)}",
        "timestamp"   : datetime.now(timezone.utc).isoformat(),
        "source"      : "property-pulse-producer"
    }

# ── Send events to Event Hubs ─────────────────────────────
def send_events(batch_size=5, interval_seconds=3):
    producer = EventHubProducerClient.from_connection_string(
        conn_str      = conn_str,
        eventhub_name = EVENTHUB
    )

    print("🚀 Producer started! Sending events...\n")

    with producer:
        while True:
            batch = producer.create_batch()
            for _ in range(batch_size):
                event = generate_event()
                batch.add(EventData(json.dumps(event)))
                print(f"  ✅ Sent: {event['event_type']} | "
                      f"{event['city']} - {event['locality']} | "
                      f"₹{event['price']}")

            producer.send_batch(batch)
            print(f"\n📦 Batch of {batch_size} sent! "
                  f"Waiting {interval_seconds}s...\n")
            time.sleep(interval_seconds)

# ── Entry point ───────────────────────────────────────────
if __name__ == "__main__":
    send_events(batch_size=5, interval_seconds=3)