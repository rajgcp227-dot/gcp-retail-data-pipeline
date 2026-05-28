import os
import random
from datetime import datetime, timedelta

import pandas as pd
import yaml
from faker import Faker

fake = Faker("en_IN")
random.seed(42)
Faker.seed(42)


def load_config():
    with open("config/project_config.yaml", "r") as f:
        return yaml.safe_load(f)


def ensure_output_path(path):
    os.makedirs(path, exist_ok=True)


def generate_customers(count, load_date, load_type):
    rows = []
    for i in range(1, count + 1):
        rows.append(
            {
                "customer_id": (
                    f"CUST{i:06d}"
                    if load_type == "FULL"
                    else f"CUSTD{load_date}{i:05d}"
                ),
                "customer_name": fake.name(),
                "email": fake.email(),
                "city": fake.city(),
                "state": fake.state(),
                "created_date": datetime.strptime(str(load_date), "%Y%m%d").date(),
                "load_type": load_type,
            }
        )

    df = pd.DataFrame(rows)

    # bad records
    if count > 10:
        df.loc[0, "customer_id"] = None
        df.loc[1, "email"] = "invalid_email"
        df.loc[2, "customer_id"] = df.loc[3, "customer_id"]

    return df


def generate_products(count, load_date, load_type):
    categories = ["Electronics", "Fashion", "Grocery", "Home", "Beauty", "Sports"]
    rows = []

    for i in range(1, count + 1):
        rows.append(
            {
                "product_id": (
                    f"PROD{i:05d}"
                    if load_type == "FULL"
                    else f"PRODD{load_date}{i:04d}"
                ),
                "product_name": fake.word().title() + " Item",
                "category": random.choice(categories),
                "brand": fake.company(),
                "price": round(random.uniform(50, 5000), 2),
                "created_date": datetime.strptime(str(load_date), "%Y%m%d").date(),
                "load_type": load_type,
            }
        )

    df = pd.DataFrame(rows)

    # bad records
    if count > 10:
        df.loc[0, "product_id"] = None
        df.loc[1, "price"] = -100
        df.loc[2, "product_id"] = df.loc[3, "product_id"]

    return df


def generate_stores(count, load_date, load_type):
    rows = []

    for i in range(1, count + 1):
        rows.append(
            {
                "store_id": (
                    f"STORE{i:04d}"
                    if load_type == "FULL"
                    else f"STORED{load_date}{i:03d}"
                ),
                "store_name": fake.company(),
                "city": fake.city(),
                "state": fake.state(),
                "region": random.choice(["North", "South", "East", "West"]),
                "created_date": datetime.strptime(str(load_date), "%Y%m%d").date(),
                "load_type": load_type,
            }
        )

    df = pd.DataFrame(rows)

    # bad records
    if count > 10:
        df.loc[0, "store_id"] = None
        df.loc[1, "region"] = "UNKNOWN_REGION"

    return df


def generate_sales(count, load_date, load_type):
    rows = []
    sale_date = datetime.strptime(str(load_date), "%Y%m%d").date()

    for i in range(1, count + 1):
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(50, 5000), 2)
        discount = round(random.uniform(0, 200), 2)
        tax = round(unit_price * quantity * 0.05, 2)
        sale_amount = round((unit_price * quantity) - discount + tax, 2)

        rows.append(
            {
                "order_id": f"ORD{load_date}{i:07d}",
                "customer_id": f"CUST{random.randint(1, 5000):06d}",
                "product_id": f"PROD{random.randint(1, 1000):05d}",
                "store_id": f"STORE{random.randint(1, 100):04d}",
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_amount": discount,
                "tax_amount": tax,
                "sale_amount": sale_amount,
                "sale_date": sale_date,
                "payment_method": random.choice(["UPI", "CARD", "CASH", "NETBANKING"]),
                "load_type": load_type,
            }
        )

    df = pd.DataFrame(rows)

    # bad records
    if count > 10:
        df.loc[0, "customer_id"] = None
        df.loc[1, "sale_amount"] = -500
        df.loc[2, "quantity"] = 0
        df.loc[3, "sale_date"] = datetime.now().date() + timedelta(days=10)
        df.loc[4, "order_id"] = df.loc[5, "order_id"]
        df.loc[6, "product_id"] = "INVALID_PRODUCT"

    # Schema drift examples
    if load_type == "DELTA":
        if str(load_date) == "20260503":
            df["coupon_code"] = random.choice(["NEW10", "SAVE20", None])
        elif str(load_date) == "20260504":
            df = df[
                [
                    "sale_date",
                    "order_id",
                    "customer_id",
                    "product_id",
                    "store_id",
                    "quantity",
                    "unit_price",
                    "discount_amount",
                    "tax_amount",
                    "sale_amount",
                    "payment_method",
                    "load_type",
                ]
            ]
        elif str(load_date) == "20260505":
            df["sale_channel"] = random.choice(["ONLINE", "STORE", "APP"])
        elif str(load_date) == "20260506":
            # df.loc[7, "sale_amount"] = "ABC"
            df["sale_amount"] = df["sale_amount"].astype("object")
            df.loc[7, "sale_amount"] = "ABC"

    return df


def write_file(df, output_path, entity, load_type, load_date):
    file_name = f"{entity}_{load_type.lower()}_{load_date}.csv"
    file_path = os.path.join(output_path, file_name)
    df.to_csv(file_path, index=False)
    print(f"Created: {file_path} | rows: {len(df)}")


def main():
    config = load_config()
    output_path = config["source_data_path"]
    ensure_output_path(output_path)

    counts = config["record_counts"]
    full_date = config["full_load_date"]

    # Full load
    write_file(
        generate_customers(counts["customers_full"], full_date, "FULL"),
        output_path,
        "customers",
        "FULL",
        full_date,
    )
    write_file(
        generate_products(counts["products_full"], full_date, "FULL"),
        output_path,
        "products",
        "FULL",
        full_date,
    )
    write_file(
        generate_stores(counts["stores_full"], full_date, "FULL"),
        output_path,
        "stores",
        "FULL",
        full_date,
    )
    write_file(
        generate_sales(counts["sales_full"], full_date, "FULL"),
        output_path,
        "sales",
        "FULL",
        full_date,
    )

    # Delta loads
    for load_date in config["incremental_dates"]:
        write_file(
            generate_customers(counts["customers_delta"], load_date, "DELTA"),
            output_path,
            "customers",
            "DELTA",
            load_date,
        )
        write_file(
            generate_products(counts["products_delta"], load_date, "DELTA"),
            output_path,
            "products",
            "DELTA",
            load_date,
        )
        write_file(
            generate_stores(counts["stores_delta"], load_date, "DELTA"),
            output_path,
            "stores",
            "DELTA",
            load_date,
        )
        write_file(
            generate_sales(counts["sales_delta"], load_date, "DELTA"),
            output_path,
            "sales",
            "DELTA",
            load_date,
        )


if __name__ == "__main__":
    main()
