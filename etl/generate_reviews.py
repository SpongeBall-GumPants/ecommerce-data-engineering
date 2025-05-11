# etl/generate_reviews.py

import os
import pandas as pd
import random
from faker import Faker

fake = Faker()
PROC_DIR = "data/processed"

def generate_reviews(n_reviews=2000):
    os.makedirs(PROC_DIR, exist_ok=True)

    # Load customers and shops/products
    df_customers  = pd.read_csv(os.path.join(PROC_DIR, "customers.csv"))
    df_shop_prod  = pd.read_csv(os.path.join(PROC_DIR, "shop_product.csv"))

    # Build list of valid (shop_id, product_id) combos
    combos = df_shop_prod[["s_id","p_id"]].drop_duplicates() \
                .rename(columns={"s_id":"shop_id","p_id":"product_id"}) \
                .to_dict("records")

    # Generate PRODUCT_REVIEW entries
    prod_reviews = []
    for _ in range(n_reviews):
        combo = random.choice(combos)
        prod_reviews.append({
            "shop_id":     combo["shop_id"],
            "product_id":  combo["product_id"],
            "reviewer_id": random.choice(df_customers["id"].tolist()),
            "review":      fake.sentence(nb_words=10),
            "score":       round(random.uniform(1.0,5.0),1)
        })

    df_pr = (
        pd.DataFrame(prod_reviews)
          .drop_duplicates(subset=["shop_id","product_id","reviewer_id"])
    )
    df_pr.to_csv(os.path.join(PROC_DIR, "product_review.csv"), index=False)
    print(f"[✅] product_review.csv → {len(df_pr)} rows")

    # Generate SHOP_REVIEW entries (shops exist independently)
    df_shops = pd.read_csv(os.path.join(PROC_DIR, "shops.csv"))
    shop_reviews = []
    for _ in range(n_reviews):
        shop_reviews.append({
            "shop_id":     random.choice(df_shops["id"].tolist()),
            "reviewer_id": random.choice(df_customers["id"].tolist()),
            "review":      fake.sentence(nb_words=8),
            "score":       round(random.uniform(1.0,5.0),1)
        })

    df_sr = (
        pd.DataFrame(shop_reviews)
          .drop_duplicates(subset=["shop_id","reviewer_id"])
    )
    df_sr.to_csv(os.path.join(PROC_DIR, "shop_review.csv"), index=False)
    print(f"[✅] shop_review.csv → {len(df_sr)} rows")

    return df_pr, df_sr

if __name__ == "__main__":
    generate_reviews()
