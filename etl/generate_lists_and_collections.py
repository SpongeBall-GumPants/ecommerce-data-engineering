import os
import pandas as pd
import random
from faker import Faker

fake = Faker()
PROC_DIR = "data/processed"

def generate_lists_and_collections(n=2000):
    os.makedirs(PROC_DIR, exist_ok=True)

    df_customers = pd.read_csv(os.path.join(PROC_DIR, "customers.csv"))
    df_shop_prod = pd.read_csv(os.path.join(PROC_DIR, "shop_product.csv"))
    sample_aff = df_customers["id"].sample(frac=0.3, random_state=42).tolist()
    df_aff = pd.DataFrame([
        {"u_id": uid, "aff_code": f"AFF{idx:05d}"}
        for idx, uid in enumerate(sample_aff)
    ])
    df_aff.to_csv(os.path.join(PROC_DIR, "affiliate.csv"), index=False)
    print(f"[✅] affiliate.csv → {len(df_aff)} rows")
    valid_sp = set(
        zip(df_shop_prod["s_id"], df_shop_prod["p_id"])
    )
    col_recs = []
    for i in range(n):
        shop_id, product_id = random.choice(list(valid_sp))
        collector = random.choice(df_aff["u_id"])
        col_recs.append({
            "shop_id":      shop_id,
            "product_id":   product_id,
            "collector_id": collector,
            "collection_id": f"COLL{i:05d}"
        })
    df_col = pd.DataFrame(col_recs).drop_duplicates()
    df_col.to_csv(os.path.join(PROC_DIR, "collection.csv"), index=False)
    print(f"[✅] collection.csv → {len(df_col)} rows")
    list_recs = []
    for i in range(n):
        shop_id, product_id = random.choice(list(valid_sp))
        buyer = random.choice(df_customers["id"].tolist())
        list_id = f"LIST{i:05d}"
        # use list_id as the name to guarantee uniqueness
        name = list_id
        list_recs.append({
            "shop_id":    shop_id,
            "product_id": product_id,
            "buyer_id":   buyer,
            "list_id":    list_id,
            "name":       name
        })
    df_ls = pd.DataFrame(list_recs).drop_duplicates(
        subset=["shop_id","product_id","buyer_id","list_id"]
    )
    df_ls.to_csv(os.path.join(PROC_DIR, "lists.csv"), index=False)
    print(f"[✅] lists.csv → {len(df_ls)} rows (all names unique via list_id)")
    sna_recs = []
    for i in range(n):
        shop_id, product_id = random.choice(list(valid_sp))
        buyer = random.choice(df_customers["id"].tolist())
        sna_recs.append({
            "shop_id":    shop_id,
            "product_id": product_id,
            "buyer_id":   buyer,
            "setdate":    fake.date_this_year().isoformat()
        })
    df_sna = pd.DataFrame(sna_recs).drop_duplicates(
        subset=["shop_id","product_id","buyer_id"]
    )
    df_sna.to_csv(os.path.join(PROC_DIR, "sna.csv"), index=False)
    print(f"[✅] sna.csv → {len(df_sna)} rows")

    return df_aff, df_col, df_ls, df_sna
