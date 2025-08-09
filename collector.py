import requests
import psycopg2
from datetime import datetime
import time

# Infura endpoint
INFURA_URL = "https://mainnet.infura.io/v3/ab261d9c903d4bd1944bc549681c3d68"

# Supabase DB connection details
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "Q-u3ABq6R9f.?ah",
    "host": "db.rtxjctlneutglbghqvby.supabase.co",
    "port": 5432,
    "sslmode": "require"
}

def get_block(block_tag="latest"):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getBlockByNumber",
        "params": [block_tag, True]
    }
    response = requests.post(INFURA_URL, json=payload)
    response.raise_for_status()
    return response.json()["result"]

def save_to_db(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gas_fee_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            blockNumber BIGINT,
            baseFee_Gwei FLOAT,
            avgPriorityFee_Gwei FLOAT,
            gasUsed BIGINT,
            gasLimit BIGINT,
            mempoolTxCount INT
        );
    """)
    
    cur.execute("""
        INSERT INTO gas_fee_data (timestamp, blockNumber, baseFee_Gwei, avgPriorityFee_Gwei, gasUsed, gasLimit, mempoolTxCount)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        data["timestamp"],
        data["blockNumber"],
        data["baseFee_Gwei"],
        data["avgPriorityFee_Gwei"],
        data["gasUsed"],
        data["gasLimit"],
        data["mempoolTxCount"]
    ))
    
    conn.commit()
    cur.close()
    conn.close()

def collect_data():
    latest_block = get_block("latest")
    pending_block = get_block("pending")  # For mempool size
    
    block_number = int(latest_block["number"], 16)
    timestamp = datetime.utcfromtimestamp(int(latest_block["timestamp"], 16))
    base_fee = int(latest_block.get("baseFeePerGas", "0x0"), 16) / 1e9  # in Gwei
    gas_used = int(latest_block["gasUsed"], 16)
    gas_limit = int(latest_block["gasLimit"], 16)
    
    tips = [
        int(tx.get("maxPriorityFeePerGas", "0x0"), 16) / 1e9
        for tx in latest_block["transactions"] if "maxPriorityFeePerGas" in tx
    ]
    avg_priority_fee = sum(tips) / len(tips) if tips else 0
    
    mempool_size = len(pending_block["transactions"]) if pending_block else None
    
    row = {
        "timestamp": timestamp,
        "blockNumber": block_number,
        "baseFee_Gwei": base_fee,
        "avgPriorityFee_Gwei": avg_priority_fee,
        "gasUsed": gas_used,
        "gasLimit": gas_limit,
        "mempoolTxCount": mempool_size
    }
    
    save_to_db(row)
    print(f"Block {block_number} | Base: {base_fee:.2f} Gwei | Tip: {avg_priority_fee:.2f} Gwei | Mempool: {mempool_size}")

if __name__ == "__main__":
    while True:
        collect_data()
        time.sleep(12)  # ~1 Ethereum block
