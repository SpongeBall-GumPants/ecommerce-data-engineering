import os
import pandas as pd
import random

PROC_DIR = "data/processed"

def generate_variants(max_variants=2):
    os.makedirs(PROC_DIR, exist_ok=True)
    df_products = pd.read_csv(os.path.join(PROC_DIR, "products.csv"))
    pids = df_products["id"].tolist()

    recs = []
    for pid in pids:
        for i in range(random.randint(0, max_variants)):
            vart = random.choice(pids)
            if vart != pid:
                recs.append({"p_id": pid, "p_vart_id": vart})

    df_var = pd.DataFrame(recs).drop_duplicates()
    out = os.path.join(PROC_DIR, "variant.csv")
    df_var.to_csv(out, index=False)
    print(f"[✅] variant.csv → {len(df_var)} rows")
    return df_var

if __name__ == "__main__":
    generate_variants()
