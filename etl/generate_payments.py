import os
import pandas as pd
import random
from faker import Faker

fake = Faker()
PROC_DIR = "data/processed"

def generate_payments():
    os.makedirs(PROC_DIR, exist_ok=True)

    # 1) Read customers for user IDs
    df_customers = pd.read_csv(os.path.join(PROC_DIR, "customers.csv"))
    uids = df_customers["id"].tolist()

    # 2) PAYMENT table
    payments = [
        {"p_u_id": uid, "p_id": f"PAY{i:05d}"}
        for i, uid in enumerate(uids)
    ]
    df_pay = pd.DataFrame(payments)
    df_pay.to_csv(os.path.join(PROC_DIR, "payment.csv"), index=False)
    print(f"[✅] payment.csv → {len(df_pay)} rows")

    # 3) WALLET table
    wallets = [
        {
            "u_id": rec["p_u_id"],
            "p_id": rec["p_id"],
            "ammount": random.randint(0, 1000),
            "currency": random.choice(["USD","EUR","TRY"])
        }
        for rec in payments
    ]
    df_wallet = pd.DataFrame(wallets)
    df_wallet.to_csv(os.path.join(PROC_DIR, "wallet.csv"), index=False)
    print(f"[✅] wallet.csv → {len(df_wallet)} rows")

    # 4) CCARD table (ccv ≤3 chars, ccnumber ≤16 chars)
    ccards = []
    for rec in payments:
        # generate and truncate CCV to 3 digits
        raw_ccv = fake.credit_card_security_code()
        ccv = str(raw_ccv)[:3]

        # generate and keep only digits up to 16 chars
        raw_num = fake.credit_card_number()
        digits = "".join(filter(str.isdigit, raw_num))
        ccnum = digits[:16]

        # expiry as numeric already handled
        mm, yy = fake.credit_card_expire(end="+5y").split("/")
        exp_date = round(int(mm) + int(yy) / 100.0, 2)

        ccards.append({
            "u_id": rec["p_u_id"],
            "p_id": rec["p_id"],
            "name": fake.name(),
            "ccv": ccv,
            "ccnumber": ccnum,
            "exp_date": exp_date
        })

    df_ccard = pd.DataFrame(ccards)
    df_ccard.to_csv(os.path.join(PROC_DIR, "ccard.csv"), index=False)
    print(f"[✅] ccard.csv → {len(df_ccard)} rows")

    # 5) TRANSACTION table (2 per payment, deduped)
    transactions = []
    for rec in payments:
        for _ in range(2):
            transactions.append({
                "w_u_id": rec["p_u_id"],
                "w_p_id": rec["p_id"],
                "cc_u_id": rec["p_u_id"],
                "cc_p_id": rec["p_id"],
                "ammount": random.randint(1, 500),
                "date": fake.date_this_year().isoformat()
            })
    df_tr = pd.DataFrame(transactions).drop_duplicates(
        subset=["w_u_id","w_p_id","cc_u_id","cc_p_id"]
    )
    df_tr.to_csv(os.path.join(PROC_DIR, "transaction.csv"), index=False)
    print(f"[✅] transaction.csv → {len(df_tr)} rows")

    return df_pay, df_wallet, df_ccard, df_tr

if __name__ == "__main__":
    generate_payments()
