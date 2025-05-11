# etl/extract_kaggle_products.py

import os, zipfile, subprocess
import pandas as pd
import random
from faker import Faker
from kaggle.api.kaggle_api_extended import KaggleApi

RAW_DIR = "data/raw"
PROC_DIR = "data/processed"
DATASET = "mkechinov/ecommerce-behavior-data-from-multi-category-store"
CSV_NAME = "2019-Nov.csv"


def extract_and_generate_products(n_products=1000):
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROC_DIR, exist_ok=True)

    zip_path = os.path.join(RAW_DIR, f"{CSV_NAME}.zip")
    csv_path = os.path.join(RAW_DIR, CSV_NAME)

    # Try to download & extract just the one CSV
    try:
        if not os.path.exists(zip_path):
            print(f"[ℹ] Downloading {CSV_NAME}.zip via Kaggle CLI…")
            subprocess.run([
                "kaggle", "datasets", "download",
                "-d", DATASET,
                "-f", CSV_NAME,
                "-p", RAW_DIR,
                "--force"
            ], check=True)
        if not os.path.exists(csv_path):
            print(f"[ℹ] Extracting {CSV_NAME} from {zip_path}…")
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extract(CSV_NAME, path=RAW_DIR)
            os.remove(zip_path)
        print(f"[✅] Ready to stream from {csv_path}")

        # Stream‐read and pick n_products unique views
        products = {}
        for chunk in pd.read_csv(csv_path, chunksize=25_000):
            df = chunk[chunk.event_type == "view"]
            for _, row in df.iterrows():
                pid = str(row.product_id)
                if pid not in products:
                    products[pid] = {
                        "id": pid,
                        "name": (row.product_name or "").strip()[:50],
                        "brand": (row.brand or "Generic").strip()[:255],
                        "tr_flag": False
                    }
                    if len(products) >= n_products:
                        break
            if len(products) >= n_products:
                break

        df_prod = pd.DataFrame(products.values())

    except Exception as e:
        # Fallback to synthetic if anything goes wrong
        print(f"[⚠️] Kaggle fetch failed ({e}); falling back to synthetic data.")
        fake = Faker()
        df_prod = pd.DataFrame([{
            "id": f"P{i:05d}",
            "name": fake.word().title(),
            "brand": fake.company()[:255],
            "tr_flag": random.choice([True, False])
        } for i in range(n_products)])

    # Save & return
    out = os.path.join(PROC_DIR, "products.csv")
    df_prod.to_csv(out, index=False)
    print(f"[✅] Generated {len(df_prod)} products → {out}")
    return df_prod


if __name__ == "__main__":
    extract_and_generate_products(1000)
