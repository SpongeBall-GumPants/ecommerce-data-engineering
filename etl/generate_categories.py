import os
import pandas as pd
import random

PROC_DIR = "data/processed"

CATS = [
    "Electronics", "Books", "Clothing", "Home", "Toys",
    "Sports", "Beauty", "Automotive", "Grocery", "Garden"
]

def generate_categories(max_per_product=3):
    os.makedirs(PROC_DIR, exist_ok=True)
    df_products = pd.read_csv(os.path.join(PROC_DIR, "products.csv"))

    recs = []
    for pid in df_products["id"]:
        n = random.randint(1, max_per_product)
        for cat in random.sample(CATS, n):
            recs.append({"p_id": pid, "category": cat})

    df_cat = pd.DataFrame(recs).drop_duplicates()
    out = os.path.join(PROC_DIR, "categories.csv")
    df_cat.to_csv(out, index=False)
    print(f"[✅] categories.csv → {len(df_cat)} rows")
    return df_cat

if __name__ == "__main__":
    generate_categories()
