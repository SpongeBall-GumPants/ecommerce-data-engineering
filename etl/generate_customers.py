from faker import Faker
import pandas as pd
import random

fake = Faker()

def generate_customers(n=1000):
    data = []
    for i in range(n):
        uid = f"CUST{i:05d}"
        data.append({
            "id": uid,
            "name": fake.first_name(),
            "minit": fake.random_uppercase_letter(),
            "lname": fake.last_name(),
            "country": fake.country(),
            "points": random.randint(0, 10000),
            "phone": fake.msisdn()[0:11],
            "bdate": fake.date_of_birth(minimum_age=18, maximum_age=70),
            "sex": random.choice(["M", "F"])
        })
    df = pd.DataFrame(data)
    df.to_csv("data/processed/customers.csv", index=False)
    return df

if __name__ == "__main__":
    df = generate_customers()
    print(df.head())
