import os
import psycopg2
import json
import requests
from datetime import datetime, timezone
from decimal import Decimal
import time
from web3 import Web3

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
# Ethereum & Contract Config
# ==============================
INFURA_RPC_URL = "https://sepolia.infura.io/v3/ab261d9c903d4bd1944bc549681c3d68"
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS")
CONTRACT_ADDRESS = "0x3a77498d7BB9855c03ce60bDf7e7baae6A603bC5"

MIN_GAS_PRICE_WEI = int(0.4 * 1e9)  # 0.4 gwei
CHECK_INTERVAL = 60  # seconds

# ==============================
# Contract ABI (Hardcoded)
# ==============================
with open(os.path.join(os.path.dirname(__file__), "contract_abi.json")) as f:
    CONTRACT_ABI = json.load(f)

# ==============================
# Init Web3
# ==============================
w3 = Web3(Web3.HTTPProvider(INFURA_RPC_URL))
contract = w3.eth.contract(address=CONTRACT_ADDRESS, abi=CONTRACT_ABI)

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
# Save Gas Data
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
# Ethereum Data Collection
# ==============================
def collect_gas_data():
    # Get latest block number
    response = requests.post(INFURA_RPC_URL, json={
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }).json()
    latest_block_number = response["result"]

    # Get latest block data
    block_data = requests.post(INFURA_RPC_URL, json={
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

    save_to_db((timestamp, base_fee_gwei, gas_used, gas_limit, tx_count))

# ==============================
# Contract Interaction
# ==============================
def get_pending_deposits():
    pending_ids = []
    deposit_count = contract.functions.getDepositCount().call()
    for deposit_id in range(deposit_count):
        _, _, _, _, status = contract.functions.getDeposit(deposit_id).call()
        if status == 0:  # Pending
            pending_ids.append(deposit_id)
    return pending_ids

def send_transaction(deposit_id):
    nonce = w3.eth.get_transaction_count(WALLET_ADDRESS)
    txn = contract.functions.sendWhenGasLow(deposit_id).build_transaction({
        "from": WALLET_ADDRESS,
        "nonce": nonce,
        "gas": 200000,
        "gasPrice": w3.eth.gas_price
    })
    signed_txn = w3.eth.account.sign_transaction(txn, private_key=PRIVATE_KEY)
    tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
    print(f"✅ Sent tx for deposit {deposit_id}: {tx_hash.hex()}")

# ==============================
# Main Loop
# ==============================
if __name__ == "__main__":
    print("🚀 Collector + Sender started...")
    init_db()

    while True:
        try:
            # Step 1: Collect & save gas data
            collect_gas_data()

            # Step 2: Check gas price
            gas_price = w3.eth.gas_price
            print(f"⛽ Current Gas Price: {gas_price / 1e9:.2f} gwei")

            if gas_price <= MIN_GAS_PRICE_WEI:
                print("✅ Gas price low — sending pending deposits...")
                pending_ids = get_pending_deposits()
                for deposit_id in pending_ids:
                    send_transaction(deposit_id)
            else:
                print("❌ Gas price too high, waiting...")

        except Exception as e:
            print(f"⚠️ Error: {e}")

        time.sleep(CHECK_INTERVAL)