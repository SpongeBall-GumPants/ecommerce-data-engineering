import pandas as pd
from sqlalchemy import create_engine, text

DB_USER = "postgres"
DB_PASS = "efeser2004"
DB_NAME = "ecomm_db"
DB_HOST = "localhost"
DB_PORT = "5432"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
     pool_size=5,
     max_overflow=10,
     pool_timeout=30,
     pool_pre_ping=True
)

def get_optimal_dtypes(table_name: str) -> dict:
    dtype_maps = {
        "CUSTOMER": {
            "id":          "category",
            "name":        "string",
            "email":       "string",
            "age":         "Int8",
            "gender":      "category",
            "country":     "category",
            "signup_date": "datetime64[ns]",
            "is_premium":  "boolean",
        },
        "PRODUCT": {
            "id":      "category",
            "name":    "string",
            "brand":   "category",
            "tr_flag": "boolean",
        },
        "SHOP": {
            "id":             "category",
            "name":           "string",
            "email":          "string",
            "address":        "string",
            "country":        "category",
            "woman_ent_flag": "boolean",
            "score":          "float32",
        },
        "SHOP_PRODUCT": {
            "s_id":     "category",
            "p_id":     "category",
            "value":    "float32",    # ← was Int32, but can be fractional or overflow
            "currency": "category",
            "stock":    "Int32",
            "score":    "float32",
        },
        "ADDRESSES": {
            "u_id":   "category",
            "address":"string",
        },
        "PAYMENT": {
            "p_u_id":"category",
            "p_id":  "category",
        },
        "WALLET": {
            "u_id":     "category",
            "wallet_id":"category",
            "balance":  "float32",
        },
        "CCARD": {
            "u_id":     "category",
            "p_id":     "category",
            "name":     "string",
            "ccv":      "Int16",
            "ccnumber": "string",
            "exp_date": "string",
        },
        "TRANSACTION": {
            "w_u_id":    "category",
            "t_id":      "category",
            "amount":    "float32",
            "timestamp": "datetime64[ns]",
        },
        "LOGISTICS": {
            "id":      "category",
            "name":    "string",
            "type":    "category",
            "premium": "boolean",
        },
        "PRODUCT_REVIEW": {
            "shop_id":    "category",
            "product_id": "category",
            "reviewer_id":"category",
            "review":     "string",
            "score":      "float32",
        },
        "SHOP_REVIEW": {
            "shop_id":    "category",
            "reviewer_id":"category",
            "review":     "string",
            "score":      "float32",
        },
        "AFFILIATE": {
            "u_id":        "category",
            "affiliate_id":"category",
        },
        "COLLECTION": {
            "shop_id":     "category",
            "product_id":  "category",
            "collector_id":"category",
            "collection_id":"category",
        },
        "LISTS": {
            "shop_id":    "category",
            "product_id": "category",
            "buyer_id":   "category",
            "list_id":    "category",
            "name":       "category",
        },
        "SNA": {
            "shop_id":    "category",
            "product_id": "category",
            "buyer_id":   "category",
        },
        "CATEGORIES": {
            "p_id":     "category",
            "category":"category",
        },
        "VARIANT": {
            "p_id":       "category",
            "p_vart_id":  "category",
        },
        "PREMIUM": {
            "u_id":       "category",
            "start_date": "datetime64[ns]",
            "end_date":   "datetime64[ns]",
        },
        "CUSTOMER_ORDERS": {
            "u_id":         "category",
            "log_id":       "category",
            "sp_id":        "category",
            "sp_shop_id":   "category",
            "order_id":     "category",
            "status":       "category",
            # "date":      "datetime64[ns]"  ← remove this line
            "address":      "string",
            "payer_id":     "category",
            "pay_id":       "category",
            "pay_amount":   "Int32",
            "refer_id":     "category",
            "coupon":       "category",
            "amount":       "Int8",
        },
    }

    key = table_name.upper()
    if key == "ORDERS":
        key = "CUSTOMER_ORDERS"

    return dtype_maps.get(key, {})


def load_csv_to_db(table_name, csv_path):
    # Read CSV file with optimized data types
    df = pd.read_csv(
        csv_path,
        dtype=get_optimal_dtypes(table_name),  # <-- Added optimization
        engine='c'  # <-- Added for faster parsing
    )

    with engine.begin() as conn:
        # Disable indexes before process
        conn.execute(text(f'ALTER TABLE public."{table_name}" DISABLE TRIGGER ALL'))

        # Clear table
        conn.execute(text(f'TRUNCATE TABLE public."{table_name}" RESTART IDENTITY CASCADE'))

        # Load data
        df.to_sql(table_name, conn, if_exists="append", index=False)

        # Re-enable indexes
        conn.execute(text(f'ALTER TABLE public."{table_name}" ENABLE TRIGGER ALL'))

    print(f"[✅] Loaded {len(df)} records into '{table_name}'")