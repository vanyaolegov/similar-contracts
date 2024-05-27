import pandas as pd
from moralis import evm_api
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Set your Moralis API key
api_key = "Api-key"

def get_wallet_transaction_history(address, chain, order="ASC", limit=300, retries=3):
    params = {
        "address": address,
        "chain": chain,
        "order": order,
        "limit": limit
    }
    
    for attempt in range(retries):
        try:
            result = evm_api.transaction.get_wallet_transactions(api_key=api_key, params=params)
            return result
        except Exception as e:
            print(f"Error fetching data for {address} on {chain}: {e}")
            if attempt < retries - 1:
                print(f"Retrying... ({attempt + 1}/{retries})")
                time.sleep(5)  # Delay before retrying
            else:
                return None

def extract_contract_interactions(transactions, contracts):
    interactions = {contract: {"count": 0, "first_interaction": None, "last_interaction": None, "values": []} for contract in contracts}
    
    if transactions and 'result' in transactions and transactions['result']:
        for tx in transactions['result']:
            to_address = tx.get('to_address')
            if to_address:
                to_address = to_address.lower()
                value = tx.get('value', '0')
                if to_address in interactions:
                    interactions[to_address]["count"] += 1
                    timestamp = tx['block_timestamp']
                    
                    if interactions[to_address]["first_interaction"] is None:
                        interactions[to_address]["first_interaction"] = timestamp
                    interactions[to_address]["last_interaction"] = timestamp
                    interactions[to_address]["values"].append(value)

    for contract in contracts:
        if interactions[contract]["first_interaction"]:
            interactions[contract]["first_interaction"] = pd.to_datetime(interactions[contract]["first_interaction"]).tz_localize(None)
        if interactions[contract]["last_interaction"]:
            interactions[contract]["last_interaction"] = pd.to_datetime(interactions[contract]["last_interaction"]).tz_localize(None)
    
    return interactions

def process_wallet(address, contracts, chains):
    data = {"address": address}

    with ThreadPoolExecutor(max_workers=len(chains)) as executor:
        futures = {executor.submit(get_wallet_transaction_history, address, chain): chain for chain in chains}
        for future in as_completed(futures):
            chain = futures[future]
            transactions = future.result()
            if transactions:
                chain_data = extract_contract_interactions(transactions, contracts)
                for contract in contracts:
                    data[f"{chain}_{contract}_transaction_count"] = chain_data[contract]["count"]
                    data[f"{chain}_{contract}_first_interaction"] = chain_data[contract]["first_interaction"]
                    data[f"{chain}_{contract}_last_interaction"] = chain_data[contract]["last_interaction"]
                    for i, value in enumerate(chain_data[contract]["values"]):
                        data[f"{chain}_{contract}_value_{i+1}"] = value

    return data

# Read addresses from file
with open("addresses.txt", "r") as file:
    addresses = file.read().splitlines()

# Read contracts from file
with open("contracts.txt", "r") as file:
    contracts = [line.strip().lower() for line in file.readlines()]

# Read chains from file
with open("chains.txt", "r") as file:
    chains = [line.strip() for line in file.readlines()]

# Program start time
start_time = datetime.now()
print(f"Starting search at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

all_data = []

# Process each wallet in parallel, no more than 10 at the same time
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(process_wallet, address, contracts, chains): address for address in addresses}
    for future in as_completed(futures):
        address = futures[future]
        wallet_data = future.result()
        all_data.append(wallet_data)

# Create DataFrame from collected data
df = pd.DataFrame(all_data)

# Convert dates to timezone-unaware format
for chain in chains:
    for contract in contracts:
        if f"{chain}_{contract}_first_interaction" in df.columns:
            df[f"{chain}_{contract}_first_interaction"] = pd.to_datetime(df[f"{chain}_{contract}_first_interaction"]).dt.tz_localize(None)
        if f"{chain}_{contract}_last_interaction" in df.columns:
            df[f"{chain}_{contract}_last_interaction"] = pd.to_datetime(df[f"{chain}_{contract}_last_interaction"]).dt.tz_localize(None)

# Reorder columns
columns = ["address"]
for chain in chains:
    for contract in contracts:
        columns.extend([
            f"{chain}_{contract}_transaction_count",
            f"{chain}_{contract}_first_interaction",
            f"{chain}_{contract}_last_interaction"
        ])
        for i in range(df[[col for col in df.columns if col.startswith(f"{chain}_{contract}_value_")]].shape[1]):
            columns.append(f"{chain}_{contract}_value_{i+1}")

df = df[columns]

# Sort by index to maintain order
df['original_index'] = df.index
df = df.sort_values(by='original_index')
df = df.drop(columns=['original_index'])

# Save data to Excel
df.to_excel("contract_interactions.xlsx", index=False)

# Program end time
end_time = datetime.now()
print(f"Data successfully saved to contract_interactions.xlsx at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Execution time: {end_time - start_time}")
