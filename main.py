# main.py

import os
import time

from etl.generate_customers        import generate_customers
from etl.extract_kaggle_products  import extract_and_generate_products
from etl.transform_products_and_shops import transform_products_and_shops
from etl.generate_shop_product    import generate_shop_product
from etl.generate_addresses       import generate_addresses
from etl.generate_payments        import generate_payments
from etl.generate_orders          import generate_orders
from etl.generate_reviews         import generate_reviews
from etl.generate_lists_and_collections import generate_lists_and_collections
from etl.generate_categories      import generate_categories
from etl.generate_variants        import generate_variants
from etl.generate_premium         import generate_premium
from etl.load_data                import load_csv_to_db

RAW_DIR  = "data/raw"
PROC_DIR = "data/processed"

def safe_run(fn, *args, desc=""):
    """Run fn(*args), catch and log any exception, and return None on failure."""
    start = time.perf_counter()
    try:
        result = fn(*args)
        print(f"[🕒] {desc} took {time.perf_counter() - start:.2f}s")
        return result
    except Exception as e:
        print(f"[⚠️] {desc} FAILED after {time.perf_counter() - start:.2f}s: {e}")
        return None

def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROC_DIR, exist_ok=True)

    # 1) Customers
    df_cust = safe_run(generate_customers, 1000,
                       desc="Generate 1 000 customers")
    if df_cust is None:
        raise RuntimeError("Critical: customers generation failed")

    # 2) Products (chunked Kaggle download / fallback to synthetic)
    df_prod = None
    for chunk_size in (1000, 500, 200, 100):
        df = safe_run(extract_and_generate_products, chunk_size,
                      desc=f"Extract/generate {chunk_size} products")
        # Got a valid DataFrame?  Break out.
        if df is not None and not df.empty:
            df_prod = df
            break

    if df_prod is None:
        print("[⚠️] All chunk sizes failed. Falling back to 50 synthetic products.")
        df_prod = extract_and_generate_products(50)

    # 3) Shops from products
    df_shops = safe_run(transform_products_and_shops, df_prod, 500,
                        desc="Transform products & generate 500 shops")
    if df_shops is None:
        print("[⚠️] Shop transformation failed—skipping shop-product linking.")
    else:
        # 4) Shop-Product links
        safe_run(generate_shop_product, 5000,
                 desc="Generate 5 000 shop_product links")

    # 5) Addresses
    safe_run(generate_addresses, desc="Generate 1 000 addresses")

    # 6) Payments & Financials
    safe_run(generate_payments, desc="Generate payments, wallets, ccards & transactions")

    # 7) Orders & Logistics
    safe_run(generate_orders, 5000,
             desc="Generate 5 000 orders & 50 logistics")

    # 8) Reviews
    safe_run(generate_reviews, 2000,
             desc="Generate 2 000 product & shop reviews")

    # 9) Affiliates, Collections, Lists & SNA
    safe_run(generate_lists_and_collections, 2000,
             desc="Generate 2 000 affiliates, collections, lists & SNA entries")

    # 10) Categories, Variants, Premium
    safe_run(generate_categories, 3,
             desc="Generate 1–3 categories per product")
    safe_run(generate_variants, 2,
             desc="Generate up to 2 variants per product")
    safe_run(generate_premium, 0.2,
             desc="Generate premium subs for 20% of customers")

    # 11) Load into PostgreSQL
    print("\n[⏳] Loading all CSVs into PostgreSQL…")
    load_start = time.perf_counter()
    tables = [
      ("CUSTOMER",       "customers.csv"),
      ("PRODUCT",        "products.csv"),
      ("SHOP",           "shops.csv"),
      ("SHOP_PRODUCT",   "shop_product.csv"),
      ("ADDRESSES",      "addresses.csv"),
      ("PAYMENT",        "payment.csv"),
      ("WALLET",         "wallet.csv"),
      ("CCARD",          "ccard.csv"),
      ("TRANSACTION",    "transaction.csv"),
      ("LOGISTICS",      "logistics.csv"),
      ("PRODUCT_REVIEW", "product_review.csv"),
      ("SHOP_REVIEW",    "shop_review.csv"),
      ("AFFILIATE",      "affiliate.csv"),
      ("COLLECTION",     "collection.csv"),
      ("LISTS",          "lists.csv"),
      ("SNA",            "sna.csv"),
      ("CATEGORIES",     "categories.csv"),
      ("VARIANT",        "variant.csv"),
      ("PREMIUM",        "premium.csv"),
      ("customer_orders", "orders.csv")
    ]
    for tbl, fname in tables:
        path = os.path.join(PROC_DIR, fname)
        t0 = time.perf_counter()
        safe_run(load_csv_to_db, tbl, path, desc=f"Load {tbl}")
        print(f"  • {tbl} load took {time.perf_counter() - t0:.2f}s")

    print(f"\n[✅] All tables loaded in {time.perf_counter() - load_start:.2f}s")

if __name__ == "__main__":
    main()
