import os
import pandas as pd
import random
from faker import Faker
import time

PROC_DIR = "data/processed"
fake = Faker()


def transform_products_and_shops(df_products, n_shops=500, n_links=5000):
    os.makedirs(PROC_DIR, exist_ok=True)

    # — generate shops —
    start_shops = time.time()
    shops = []
    for i in range(n_shops):
        shops.append({
            "id": f"SHOP{i:04d}",
            "name": fake.company(),
            "email": fake.company_email(),
            "address": fake.address().replace("\n", ", "),
            "country": fake.country(),
            "woman_ent_flag": fake.boolean(),
            "score": round(random.uniform(0, 5), 1)
        })
    df_shops = pd.DataFrame(shops)

    df_shops["name"] = df_shops["name"].fillna(fake.company())
    df_shops["name"] = df_shops["name"].replace("", "unknown shop")

    df_shops["email"] = df_shops["email"].fillna(fake.company_email())
    df_shops["address"] = df_shops["address"].fillna(fake.address().replace("\n", ", "))
    df_shops["country"] = df_shops["country"].fillna(fake.country())
    df_shops["score"] = df_shops["score"].fillna(random.uniform(0, 5))

    df_shops["score"] = df_shops["score"].astype(float)

    shop_csv = os.path.join(PROC_DIR, "shops.csv")
    df_shops.to_csv(shop_csv, index=False)
    print(f"[✅] Generated {len(df_shops)} shops to {shop_csv}")
    print(f"[🕒] Transform products & generate {len(df_shops)} shops took {time.time() - start_shops:.2f}s")

    start_links = time.time()
    links = []
    for _ in range(n_links):
        shop = df_shops.sample(1).iloc[0]
        prod = df_products.sample(1).iloc[0]
        links.append({
            "s_id": shop["id"],
            "p_id": prod["id"],
            "value": random.randint(10, 1000),
            "currency": fake.currency_code(),
            "stock": random.randint(0, 200),
            "score": round(random.uniform(0, 5), 1)
        })
    df_links = pd.DataFrame(links)
    link_csv = os.path.join(PROC_DIR, "shop_product.csv")
    df_links.to_csv(link_csv, index=False)
    print(f"[✅] Generated and saved {len(df_links)} shop–product links")
    print(f"[🕒] Generate {len(df_links)} shop_product links took {time.time() - start_links:.2f}s")

    return df_shops, df_links
