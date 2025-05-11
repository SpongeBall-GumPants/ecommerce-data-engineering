import os
import pandas as pd
from faker import Faker
import random

fake = Faker()

def transform_products_and_shops(
    df_products: pd.DataFrame,
    proc_dir: str = "data/processed",
    max_shops: int = 1000
):
    """
    1) Ensure every product has a brand
    2) Rewrite products.csv (with brand filled)
    3) Generate up to max_shops shops from unique brands
    4) Write shops.csv
    """
    os.makedirs(proc_dir, exist_ok=True)

    # 1) Fill missing brand (empty string -> 'Generic')
    df_products['brand'] = df_products['brand'].replace('', 'Generic')

    # 2) Save updated products
    products_out = os.path.join(proc_dir, "products.csv")
    df_products.to_csv(products_out, index=False)
    print(f"[✅] Saved {len(df_products)} products to {products_out}")

    # 3) Build shops from unique brands (limit to max_shops)
    unique_brands = df_products['brand'].unique().tolist()[:max_shops]
    shops = []
    for i, brand in enumerate(unique_brands):
        shops.append({
            "id": f"SHOP{i:04d}",
            "name": brand[:50],
            # properly escape the newline replacement in one line:
            "email": fake.company_email(),
            "address": fake.address().replace("\\n", ", ")[:255],
            "country": fake.country(),
            "woman_ent_flag": random.choice([True, False]),
            "score": round(random.uniform(1.0, 5.0), 1)
        })

    df_shops = pd.DataFrame(shops)
    shops_out = os.path.join(proc_dir, "shops.csv")
    df_shops.to_csv(shops_out, index=False)
    print(f"[✅] Generated {len(df_shops)} shops to {shops_out}")

    return df_shops