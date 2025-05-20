import os
import pandas as pd
from faker import Faker
import time

RAW_DIR   = "data/raw"
PROC_DIR  = "data/processed"
INPUT_CSV = "first_100k_rows.csv"

fake = Faker()

def extract_and_generate_products(n_products=1000):
    os.makedirs(PROC_DIR, exist_ok=True)
    out_csv = os.path.join(PROC_DIR, "products.csv")

    start = time.time()
    try:
        df = pd.read_csv(os.path.join(RAW_DIR, INPUT_CSV))

        for col in ['product_id', 'Product ID', 'ProductID', 'id']:
            if col in df.columns:
                df['product_id'] = df[col].astype(str)
                break
        else:
            df['product_id'] = df.iloc[:, 0].astype(str)

        df['product_brand'] = df.get('brand', pd.Series()).fillna('')
        df['product_brand'] = df['product_brand'].replace('', fake.company())

        df = df.drop_duplicates('product_id').head(n_products)

        df['price'] = df.get('price', pd.Series())
        df['product_name'] = df.apply(
            lambda r: f"Product of {r['product_brand']} at ${r['price']}" if pd.notna(r.get('price')) else f"Product of {r['product_brand']}",
            axis=1
        )


        products = pd.DataFrame({
            'id':    df['product_id'],
            'name':  df['product_name'],
            'brand': df['product_brand'],
            'tr_flag': False
        })
        products.to_csv(out_csv, index=False)
        print(f"[✅] Saved {len(products)} products to {out_csv}")


    except Exception as e:
        print(f"Failed to extract products: {e}")
        products = [
            {
                'id':      f"P{i:05d}",
                'name':    fake.word().capitalize(),
                'brand':   fake.company(),
                'tr_flag': fake.boolean()
            }
            for i in range(n_products)
        ]
        products = pd.DataFrame(products)
        products.to_csv(out_csv, index=False)
        print(f"[✅] Synthetic fallback → {len(products)} rows → {out_csv}")

    print(f"[🕒] Extract/generate {n_products:,} products took {time.time() - start:.2f}s")
    return pd.read_csv(out_csv, dtype=str)