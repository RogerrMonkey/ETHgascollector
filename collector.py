import os
import psycopg2
import requests
from datetime import datetime, timezone
from decimal import Decimal

# ==============================
# Database Config (Railway ENV)
# ==============================
DB_CONFIG = {
    "dbname": os.getenv("PGDATABASE", "postgres"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD"),
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", 5432)
}

# ==============================
# Infura Config
# ==============================
INFURA_URL = "https://sepolia.infura.io/v3/ab261d9c903d4bd1944bc549681c3d68"

# ==============================
# DB Setup
# ==============================
def init_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gas_fees (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            base_fee_gwei NUMERIC,
            gas_used NUMERIC,
            gas_limit NUMERIC,
            transactions_count INTEGER
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Database initialized or already exists.")

# ==============================
# Save Data to DB
# ==============================
def save_to_db(row):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gas_fees (timestamp, base_fee_gwei, gas_used, gas_limit, transactions_count)
        VALUES (%s, %s, %s, %s, %s)
    """, row)
    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 Data saved: {row}")

# ==============================
# Collect Ethereum Data
# ==============================
def collect_data():
    print("🚀 Starting Ethereum gas fee collector...")
    init_db()

    # Get latest block number
    response = requests.post(INFURA_URL, json={
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }).json()

    latest_block_number = response["result"]

    # Get latest block data
    block_data = requests.post(INFURA_URL, json={
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [latest_block_number, True],
        "id": 1
    }).json()["result"]

    # Extract fields
    timestamp = datetime.fromtimestamp(int(block_data["timestamp"], 16), tz=timezone.utc)
    base_fee_wei = int(block_data.get("baseFeePerGas", "0x0"), 16)
    base_fee_gwei = Decimal(base_fee_wei) / Decimal(1e9)
    gas_used = int(block_data["gasUsed"], 16)
    gas_limit = int(block_data["gasLimit"], 16)
    tx_count = len(block_data["transactions"])

    # Save to DB
    save_to_db((timestamp, base_fee_gwei, gas_used, gas_limit, tx_count))

if __name__ == "__main__":
    collect_data()