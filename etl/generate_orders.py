import os
import pandas as pd
import random
from faker import Faker

fake = Faker()
PROC_DIR = "data/processed"

def generate_orders(n_orders=5000, refer_pct=0.2):
    os.makedirs(PROC_DIR, exist_ok=True)

    df_customers = pd.read_csv(os.path.join(PROC_DIR, "customers.csv"))
    df_addresses = pd.read_csv(os.path.join(PROC_DIR, "addresses.csv"))
    df_shop_prod = pd.read_csv(os.path.join(PROC_DIR, "shop_product.csv"))
    df_payments  = pd.read_csv(os.path.join(PROC_DIR, "payment.csv"))
    df_prem      = pd.read_csv(os.path.join(PROC_DIR, "premium.csv"))
    df_aff       = pd.read_csv(os.path.join(PROC_DIR, "affiliate.csv"))

    premium_users = set(df_prem["u_id"].tolist())
    aff_codes     = df_aff["aff_code"].tolist()

    # Build payment lookup
    pay_lookup = df_payments.groupby("p_u_id")["p_id"].apply(list).to_dict()

    # 1) LOGISTICS
    logistics = [{
        "id": f"LOGI{i:04d}",
        "name": fake.company(),
        "type": random.choice(["normal", "express"]),
        "premium": random.choice([True, False])
    } for i in range(5000)]
    df_log = pd.DataFrame(logistics)
    df_log.to_csv(os.path.join(PROC_DIR, "logistics.csv"), index=False)
    print(f"[✅] logistics.csv → {len(df_log)} rows")

    # Pre-split logistic carriers
    premium_carriers = df_log[df_log["premium"] == True].to_dict("records")
    normal_carriers  = df_log[df_log["premium"] == False].to_dict("records")
    if not premium_carriers:
        premium_carriers = normal_carriers  # fallback

    # 2) ORDERS
    orders = []
    for i in range(n_orders):
        cust_row = df_customers.sample(1).iloc[0]
        uid = cust_row["id"]

        # pick valid payment
        pay_ids = pay_lookup.get(uid, [])
        pay_id  = random.choice(pay_ids) if pay_ids else f"PAY{i:05d}"

        # pick address
        addr = df_addresses.loc[df_addresses["u_id"] == uid, "address"].iloc[0]

        # pick shop-product link
        link = df_shop_prod.sample(1).iloc[0]
        s_id = link["s_id"]
        p_id = link["p_id"]

        if uid in premium_users:
            carrier = random.choice(premium_carriers)
        else:
            carrier = random.choice(normal_carriers)

        # assign a refer_id ~20% of the time
        if aff_codes and random.random() < refer_pct:
            refer = random.choice(aff_codes)
        else:
            refer = None

        orders.append({
            "u_id":        uid,
            "log_id":      carrier["id"],
            "sp_id":       p_id,
            "sp_shop_id":  s_id,
            "order_id":    f"ORD{i:05d}",
            "status":      random.choice(["carted","cancelled","completed","refunded"]),
            "date":        fake.date_this_year().isoformat(),
            "address":     addr,
            "payer_id":    uid,
            "pay_id":      pay_id,
            "pay_amount":  random.randint(10, 500),
            "refer_id":    refer,
            "coupon":      random.choice([None, "DISC10", "WELCOME5"]),
            "amount":      random.randint(1,5)
        })

    df_ord = pd.DataFrame(orders)
    out = os.path.join(PROC_DIR, "orders.csv")
    df_ord.to_csv(out, index=False)
    print(f"[✅] orders.csv → {len(df_ord)} rows")

    return df_ord, df_log


if __name__ == "__main__":
    generate_orders()
