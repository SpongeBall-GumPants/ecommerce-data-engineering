import os
import pandas as pd
import random

def generate_shop_product(n_links=100000, proc_dir="data/processed"):
    os.makedirs(proc_dir, exist_ok=True)
    df_shops = pd.read_csv(os.path.join(proc_dir, "shops.csv"))
    df_prod  = pd.read_csv(os.path.join(proc_dir, "products.csv"))

    shop_ids = df_shops['id'].tolist()
    prod_ids = df_prod['id'].tolist()
    currencies = ['USD','EUR','GBP','TRY']

    links = []
    for _ in range(n_links):
        sid = random.choice(shop_ids)
        pid = random.choice(prod_ids)
        links.append({
            "s_id": sid,
            "p_id": pid,
            "value": round(random.uniform(5.0, 1000.0), 2),
            "currency": random.choice(currencies),
            "stock": random.randint(0, 500),
            "score": round(random.uniform(0.0, 5.0), 1)
        })
    df_links = pd.DataFrame(links).drop_duplicates(subset=["s_id","p_id"])
    df_links.to_csv(os.path.join(proc_dir, "shop_product.csv"), index=False)
    print(f"[✅] Generated and saved {len(df_links)} shop–product links")
    return df_links
