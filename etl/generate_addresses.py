import os
import pandas as pd
from faker import Faker

fake = Faker()
PROC_DIR = "data/processed"

def generate_addresses():
    """
    For each customer, generate exactly one synthetic address.
    """
    os.makedirs(PROC_DIR, exist_ok=True)
    df_customers = pd.read_csv(os.path.join(PROC_DIR, "customers.csv"))

    records = []
    for uid in df_customers["id"]:
        addr = fake.address().replace("\n", ", ")[:255]
        records.append({"u_id": uid, "address": addr})

    df_addr = pd.DataFrame(records)
    out = os.path.join(PROC_DIR, "addresses.csv")
    df_addr.to_csv(out, index=False)
    print(f"[✅] Generated {len(df_addr)} addresses → {out}")
    return df_addr

if __name__ == "__main__":
    generate_addresses()
