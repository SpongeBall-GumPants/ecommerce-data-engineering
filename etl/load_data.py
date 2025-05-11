import pandas as pd
from sqlalchemy import create_engine, text

# Adjust these as needed
DB_USER = "postgres"
DB_PASS = "efeser2004"
DB_NAME = "ecomm_db"
DB_HOST = "localhost"
DB_PORT = "5432"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def load_csv_to_db(table_name, csv_path):
    df = pd.read_csv(csv_path)

    # Use a transaction block; conn.begin() will commit on exit if no errors
    with engine.begin() as conn:
        # 1) Truncate existing data
        conn.execute(text(f'TRUNCATE TABLE public."{table_name}" RESTART IDENTITY CASCADE'))
        # 2) Load new data
        df.to_sql(table_name, conn, if_exists="append", index=False)

    print(f"[✅] Loaded {len(df)} records into '{table_name}'")
