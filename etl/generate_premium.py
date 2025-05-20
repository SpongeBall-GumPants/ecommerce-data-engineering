import os
import pandas as pd
from faker import Faker
import random

fake = Faker()
PROC_DIR = "data/processed"

def generate_premium(pct=0.2):
    os.makedirs(PROC_DIR, exist_ok=True)
    df_customers = pd.read_csv(os.path.join(PROC_DIR, "customers.csv"))
    uids = df_customers["id"].tolist()
    n = int(len(uids) * pct)

    recs = []
    for uid in random.sample(uids, n):
        start = fake.date_between(start_date="-1y", end_date="today")
        end = fake.date_between(start_date=start, end_date="+6M")
        recs.append({"u_id": uid, "s_date": start.isoformat(), "e_date": end.isoformat()})

    df_prem = pd.DataFrame(recs)
    out = os.path.join(PROC_DIR, "premium.csv")
    df_prem.to_csv(out, index=False)
    print(f"[✅] premium.csv → {len(df_prem)} rows")
    return df_prem

if __name__ == "__main__":
    generate_premium()
