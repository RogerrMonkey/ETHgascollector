import os
import requests
import psycopg2
import time
from datetime import datetime, timezone

# Infura endpoint
INFURA_URL = "https://mainnet.infura.io/v3/ab261d9c903d4bd1944bc549681c3d68"

# PostgreSQL credentials from Railway environment variables
DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT", "5432"),
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'sslmode': os.getenv("DB_SSLMODE", "require")
}

# -------------------------
# DATABASE SETUP
# -------------------------
def init_db():
    """Create the gas_fees table if it doesn't exist."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gas_fees (
            id SERIAL PRIMARY KEY,
            block_number BIGINT,
            gas_price NUMERIC,
            gas_limit NUMERIC,
            timestamp TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Database initialized (table ready).")


# -------------------------
# SAVE TO DB
# -------------------------
def save_to_db(row):
    """Insert gas fee record into the database."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO gas_fees (block_number, gas_price, gas_limit, timestamp)
        VALUES (%s, %s, %s, %s)
    """, (row["block_number"], row["gas_price"], row["gas_limit"], row["timestamp"]))
    conn.commit()
    cur.close()
    conn.close()
    print(f"💾 Saved block {row['block_number']} to DB")


# -------------------------
# FETCH LATEST BLOCK DATA
# -------------------------
def get_latest_block():
    """Fetch latest block details from Ethereum blockchain via Infura."""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": ["latest", True],
        "id": 1
    }
    response = requests.post(INFURA_URL, json=payload)
    data = response.json()

    if "result" not in data or not data["result"]:
        raise Exception("❌ Could not fetch latest block data.")

    block = data["result"]

    block_number = int(block["number"], 16)
    gas_limit = int(block["gasLimit"], 16)

    # Calculate average gas price from transactions
    gas_prices = []
    for tx in block["transactions"]:
        if "gasPrice" in tx:
            gas_prices.append(int(tx["gasPrice"], 16))

    avg_gas_price = sum(gas_prices) / len(gas_prices) if gas_prices else 0

    timestamp = datetime.fromtimestamp(int(block["timestamp"], 16), tz=timezone.utc)

    return {
        "block_number": block_number,
        "gas_price": avg_gas_price,
        "gas_limit": gas_limit,
        "timestamp": timestamp
    }


# -------------------------
# MAIN COLLECTION LOOP
# -------------------------
def collect_data():
    """Continuously fetch and store Ethereum gas fee data every minute."""
    print("🚀 Starting Ethereum gas fee collector...")
    init_db()

    while True:
        try:
            latest_block_data = get_latest_block()
            save_to_db(latest_block_data)
        except Exception as e:
            print(f"❌ Error collecting data: {e}")

        print("⏳ Waiting 60 seconds before next fetch...\n")
        time.sleep(60)  # wait for 1 minute


if __name__ == "__main__":
    collect_data()