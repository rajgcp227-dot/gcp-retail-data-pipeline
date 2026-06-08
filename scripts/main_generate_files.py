from __future__ import annotations

import argparse
import hashlib
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import yaml
from faker import Faker

# ============================================================
# CONFIG AND COMMON HELPERS
# ============================================================


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def ensure_output_path(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_load_date(load_date: str | int):
    return datetime.strptime(str(load_date), "%Y%m%d").date()


def stable_seed(entity: str, load_type: str, load_date: str | int) -> int:
    seed_value = f"{entity}_{load_type}_{load_date}"
    return int(hashlib.md5(seed_value.encode("utf-8")).hexdigest()[:8], 16)


def get_fake(entity: str, load_type: str, load_date: str | int) -> Faker:
    fake = Faker("en_IN")
    fake.seed_instance(stable_seed(entity, load_type, load_date))
    return fake


def get_rng(entity: str, load_type: str, load_date: str | int) -> random.Random:
    return random.Random(stable_seed(entity, load_type, load_date))


def random_choices(rng: random.Random, values: list, count: int) -> list:
    return [rng.choice(values) for _ in range(count)]


# ============================================================
# DATE AND DYNAMIC VOLUME LOGIC
# ============================================================


def get_incremental_dates(
    config: dict,
    override_dates: Optional[list[str]] = None,
) -> list[str]:

    if override_dates:
        return [str(date) for date in override_dates]

    mode = str(config.get("incremental_date_mode", "FIXED_DATES")).upper()

    if mode == "CURRENT_DATE":
        days_back = int(config.get("incremental_days_back", 0))
        load_date = datetime.now() - timedelta(days=days_back)
        return [load_date.strftime("%Y%m%d")]

    if mode == "FIXED_DATES":
        return [str(date) for date in config.get("incremental_dates", [])]

    raise ValueError(f"Invalid incremental_date_mode: {mode}")


def get_volume_profile(load_date: str | int, config: dict) -> str:
    load_date_string = str(load_date)
    date_object = datetime.strptime(load_date_string, "%Y%m%d").date()

    festival_dates = {str(date) for date in config.get("festival_dates", [])}

    if load_date_string in festival_dates:
        return "festival"

    if date_object.day >= 28:
        return "month_end"

    if date_object.weekday() in (5, 6):
        return "weekend"

    return "weekday"


def get_dynamic_delta_counts(load_date: str | int, config: dict) -> dict:
    profile = get_volume_profile(load_date, config)

    dynamic_rules = config.get("dynamic_delta_record_counts", {})

    if profile in dynamic_rules:
        rules = dynamic_rules[profile]
        rng = random.Random(int(str(load_date)))

        counts = {
            "customers_delta": rng.randint(
                int(rules["customers_delta_min"]),
                int(rules["customers_delta_max"]),
            ),
            "products_delta": rng.randint(
                int(rules["products_delta_min"]),
                int(rules["products_delta_max"]),
            ),
            "stores_delta": rng.randint(
                int(rules["stores_delta_min"]),
                int(rules["stores_delta_max"]),
            ),
            "sales_delta": rng.randint(
                int(rules["sales_delta_min"]),
                int(rules["sales_delta_max"]),
            ),
        }

    else:
        static_counts = config.get("record_counts", {})

        counts = {
            "customers_delta": int(static_counts.get("customers_delta", 300)),
            "products_delta": int(static_counts.get("products_delta", 100)),
            "stores_delta": int(static_counts.get("stores_delta", 10)),
            "sales_delta": int(static_counts.get("sales_delta", 5000)),
        }

    print(
        f"Load date: {load_date} | "
        f"Volume profile: {profile} | "
        f"Base good-record counts: {counts}"
    )

    return counts


def should_apply_schema_drift(
    load_date: str | int,
    drift_days: set[int],
) -> bool:
    day_number = datetime.strptime(str(load_date), "%Y%m%d").day
    return day_number in drift_days


# ============================================================
# DQ SIMULATION HELPERS
# ============================================================


def dq_simulation_enabled(config: dict) -> bool:
    return bool(config.get("dq_simulation", {}).get("enabled", True))


def append_bad_rows(
    dataframe: pd.DataFrame,
    bad_rows: list[dict],
) -> pd.DataFrame:

    if not bad_rows:
        return dataframe

    return pd.concat(
        [dataframe, pd.DataFrame(bad_rows)],
        ignore_index=True,
        sort=False,
    )


def get_bad_sales_rules(config: dict) -> dict:
    default_rules = {
        "null_customer_fk": 1,
        "invalid_customer_fk": 1,
        "invalid_product_fk": 1,
        "invalid_store_fk": 1,
        "invalid_quantity": 1,
        "invalid_sale_amount": 1,
        "future_sale_date": 1,
        "invalid_payment_method": 1,
        "duplicate_order_id": 1,
    }

    dq_config = config.get("dq_simulation", {})

    configured_rules = (
        dq_config.get("bad_sales_records") or dq_config.get("sales_bad_records") or {}
    )

    return {
        rule_name: int(configured_rules.get(rule_name, default_count))
        for rule_name, default_count in default_rules.items()
    }


# ============================================================
# CUSTOMER GENERATOR
# ============================================================


def generate_customers(
    count: int,
    load_date: str | int,
    load_type: str,
    config: dict,
) -> pd.DataFrame:

    fake = get_fake("customers", load_type, load_date)
    rng = get_rng("customers", load_type, load_date)

    created_date = parse_load_date(load_date)

    rows = []

    for row_number in range(1, count + 1):

        customer_id = (
            f"CUST{row_number:06d}"
            if load_type == "FULL"
            else f"CUSTD{load_date}{row_number:05d}"
        )

        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": fake.name(),
                "email": fake.email(),
                "city": fake.city(),
                "state": fake.state(),
                "created_date": created_date,
                "load_type": load_type,
            }
        )

    dataframe = pd.DataFrame(rows)

    # Controlled schema drift
    if load_type == "DELTA":

        if should_apply_schema_drift(load_date, {5, 15, 25}):
            dataframe["loyalty_tier"] = random_choices(
                rng,
                ["SILVER", "GOLD", "PLATINUM", None],
                len(dataframe),
            )

        if should_apply_schema_drift(load_date, {6, 16, 26}):
            dataframe["customer_segment"] = random_choices(
                rng,
                ["REGULAR", "PREMIUM", "NEW", "CHURN_RISK"],
                len(dataframe),
            )

    # Append bad records without removing valid customer IDs
    if dq_simulation_enabled(config) and not dataframe.empty:

        sample = dataframe.iloc[0].to_dict()

        bad_rows = [
            {
                **sample,
                "customer_id": None,
                "customer_name": "Null Customer ID",
            },
            {
                **sample,
                "customer_id": f"CUST_BAD_EMAIL_{load_date}",
                "customer_name": "Invalid Email Customer",
                "email": "invalid_email",
            },
            {
                **sample,
                "customer_id": sample["customer_id"],
                "customer_name": "Duplicate Customer ID",
            },
            {
                **sample,
                "customer_id": f"CUST_BAD_NAME_{load_date}",
                "customer_name": "",
            },
            {
                **sample,
                "customer_id": f"CUST_FUTURE_{load_date}",
                "customer_name": "Future Date Customer",
                "created_date": created_date + timedelta(days=10),
            },
        ]

        dataframe = append_bad_rows(dataframe, bad_rows)

    return dataframe


# ============================================================
# PRODUCT GENERATOR
# ============================================================


def generate_products(
    count: int,
    load_date: str | int,
    load_type: str,
    config: dict,
) -> pd.DataFrame:

    fake = get_fake("products", load_type, load_date)
    rng = get_rng("products", load_type, load_date)

    created_date = parse_load_date(load_date)

    categories = [
        "Electronics",
        "Fashion",
        "Grocery",
        "Home",
        "Beauty",
        "Sports",
    ]

    rows = []

    for row_number in range(1, count + 1):

        product_id = (
            f"PROD{row_number:05d}"
            if load_type == "FULL"
            else f"PRODD{load_date}{row_number:04d}"
        )

        rows.append(
            {
                "product_id": product_id,
                "product_name": fake.word().title() + " Item",
                "category": rng.choice(categories),
                "brand": fake.company(),
                "price": round(rng.uniform(50, 5000), 2),
                "created_date": created_date,
                "load_type": load_type,
            }
        )

    dataframe = pd.DataFrame(rows)

    # Controlled schema drift
    if load_type == "DELTA":

        if should_apply_schema_drift(load_date, {5, 15, 25}):
            dataframe["supplier_id"] = [
                f"SUP{rng.randint(1, 50):04d}" for _ in range(len(dataframe))
            ]

        if should_apply_schema_drift(load_date, {6, 16, 26}):

            dataframe["product_status"] = random_choices(
                rng,
                ["ACTIVE", "INACTIVE", "DISCONTINUED"],
                len(dataframe),
            )

            if len(dataframe) > 8:
                dataframe["price"] = dataframe["price"].astype("object")
                dataframe.loc[7, "price"] = "ABC"

    # Append bad records without removing valid product IDs
    if dq_simulation_enabled(config) and not dataframe.empty:

        sample = dataframe.iloc[0].to_dict()

        bad_rows = [
            {
                **sample,
                "product_id": None,
                "product_name": "Null Product ID",
            },
            {
                **sample,
                "product_id": f"PROD_BAD_PRICE_{load_date}",
                "product_name": "Negative Price Product",
                "price": -100,
            },
            {
                **sample,
                "product_id": sample["product_id"],
                "product_name": "Duplicate Product ID",
            },
            {
                **sample,
                "product_id": f"PROD_BAD_CATEGORY_{load_date}",
                "product_name": "Blank Category Product",
                "category": "",
            },
            {
                **sample,
                "product_id": f"PROD_FUTURE_{load_date}",
                "product_name": "Future Date Product",
                "created_date": created_date + timedelta(days=10),
            },
        ]

        dataframe = append_bad_rows(dataframe, bad_rows)

    return dataframe


# ============================================================
# STORE GENERATOR
# ============================================================


def generate_stores(
    count: int,
    load_date: str | int,
    load_type: str,
    config: dict,
) -> pd.DataFrame:

    fake = get_fake("stores", load_type, load_date)
    rng = get_rng("stores", load_type, load_date)

    created_date = parse_load_date(load_date)

    valid_regions = ["North", "South", "East", "West"]

    rows = []

    for row_number in range(1, count + 1):

        store_id = (
            f"STORE{row_number:04d}"
            if load_type == "FULL"
            else f"STORED{load_date}{row_number:03d}"
        )

        rows.append(
            {
                "store_id": store_id,
                "store_name": fake.company(),
                "city": fake.city(),
                "state": fake.state(),
                "region": rng.choice(valid_regions),
                "created_date": created_date,
                "load_type": load_type,
            }
        )

    dataframe = pd.DataFrame(rows)

    # Controlled schema drift
    if load_type == "DELTA":

        if should_apply_schema_drift(load_date, {5, 15, 25}):
            dataframe["store_type"] = random_choices(
                rng,
                ["MALL", "STANDALONE", "WAREHOUSE", "FRANCHISE"],
                len(dataframe),
            )

        if should_apply_schema_drift(load_date, {6, 16, 26}):
            dataframe["manager_name"] = [fake.name() for _ in range(len(dataframe))]

    # Append bad records without removing valid store IDs
    if dq_simulation_enabled(config) and not dataframe.empty:

        sample = dataframe.iloc[0].to_dict()

        bad_rows = [
            {
                **sample,
                "store_id": None,
                "store_name": "Null Store ID",
            },
            {
                **sample,
                "store_id": f"STORE_BAD_REGION_{load_date}",
                "store_name": "Invalid Region Store",
                "region": "UNKNOWN_REGION",
            },
            {
                **sample,
                "store_id": sample["store_id"],
                "store_name": "Duplicate Store ID",
            },
            {
                **sample,
                "store_id": f"STORE_BAD_NAME_{load_date}",
                "store_name": "",
            },
            {
                **sample,
                "store_id": f"STORE_FUTURE_{load_date}",
                "store_name": "Future Date Store",
                "created_date": created_date + timedelta(days=10),
            },
        ]

        dataframe = append_bad_rows(dataframe, bad_rows)

    return dataframe


# ============================================================
# SALES GENERATOR
# ============================================================


def inject_controlled_sales_bad_records(
    dataframe: pd.DataFrame,
    load_date: str | int,
    config: dict,
) -> pd.DataFrame:

    if not dq_simulation_enabled(config) or dataframe.empty:
        return dataframe

    rules = get_bad_sales_rules(config)

    required_rows = sum(rules.values())

    if len(dataframe) < required_rows + 25:
        raise ValueError(
            "Not enough sales rows to inject controlled bad records. "
            f"Required at least {required_rows + 25}, "
            f"available {len(dataframe)}."
        )

    cursor = 20

    def next_index() -> int:
        nonlocal cursor

        selected_index = cursor
        cursor += 1

        return selected_index

    for _ in range(rules["null_customer_fk"]):
        dataframe.loc[next_index(), "customer_id"] = None

    for _ in range(rules["invalid_customer_fk"]):
        dataframe.loc[next_index(), "customer_id"] = "INVALID_CUSTOMER"

    for _ in range(rules["invalid_product_fk"]):
        dataframe.loc[next_index(), "product_id"] = "INVALID_PRODUCT"

    for _ in range(rules["invalid_store_fk"]):
        dataframe.loc[next_index(), "store_id"] = "INVALID_STORE"

    for _ in range(rules["invalid_quantity"]):
        dataframe.loc[next_index(), "quantity"] = 0

    for _ in range(rules["invalid_sale_amount"]):
        dataframe.loc[next_index(), "sale_amount"] = -500.0

    for _ in range(rules["future_sale_date"]):
        dataframe.loc[next_index(), "sale_date"] = parse_load_date(
            load_date
        ) + timedelta(days=10)

    for _ in range(rules["invalid_payment_method"]):
        dataframe.loc[next_index(), "payment_method"] = "CRYPTO"

    for duplicate_number in range(rules["duplicate_order_id"]):

        duplicate_row_index = next_index()
        original_row_index = duplicate_number

        dataframe.loc[duplicate_row_index, "order_id"] = dataframe.loc[
            original_row_index,
            "order_id",
        ]

    return dataframe


def generate_sales(
    count: int,
    load_date: str | int,
    load_type: str,
    config: dict,
) -> pd.DataFrame:

    rng = get_rng("sales", load_type, load_date)

    sale_date = parse_load_date(load_date)

    maximum_discount_percentage = float(
        config.get("dq_simulation", {}).get(
            "good_sales_max_discount_pct",
            0.20,
        )
    )

    rows = []

    for row_number in range(1, count + 1):

        quantity = rng.randint(1, 5)
        unit_price = round(rng.uniform(50, 5000), 2)

        gross_amount = unit_price * quantity

        maximum_discount = min(
            200.0,
            gross_amount * maximum_discount_percentage,
        )

        discount_amount = round(
            rng.uniform(0, maximum_discount),
            2,
        )

        tax_amount = round(gross_amount * 0.05, 2)

        sale_amount = round(
            gross_amount - discount_amount + tax_amount,
            2,
        )

        rows.append(
            {
                "order_id": f"ORD{load_date}{row_number:07d}",
                "customer_id": f"CUST{rng.randint(1, 5000):06d}",
                "product_id": f"PROD{rng.randint(1, 1000):05d}",
                "store_id": f"STORE{rng.randint(1, 100):04d}",
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_amount": discount_amount,
                "tax_amount": tax_amount,
                "sale_amount": sale_amount,
                "sale_date": sale_date,
                "payment_method": rng.choice(["UPI", "CARD", "CASH", "NETBANKING"]),
                "load_type": load_type,
            }
        )

    dataframe = pd.DataFrame(rows)

    # Controlled schema drift
    if load_type == "DELTA":

        if should_apply_schema_drift(load_date, {3, 13, 23}):
            dataframe["coupon_code"] = random_choices(
                rng,
                ["NEW10", "SAVE20", "FESTIVE15", None],
                len(dataframe),
            )

        elif should_apply_schema_drift(load_date, {4, 14, 24}):

            dataframe = dataframe[
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

        elif should_apply_schema_drift(load_date, {5, 15, 25}):

            dataframe["sale_channel"] = random_choices(
                rng,
                ["ONLINE", "STORE", "APP"],
                len(dataframe),
            )

        elif should_apply_schema_drift(load_date, {6, 16, 26}):

            if len(dataframe) > 8:
                dataframe["sale_amount"] = dataframe["sale_amount"].astype("object")
                dataframe.loc[7, "sale_amount"] = "ABC"

            dataframe["delivery_type"] = random_choices(
                rng,
                ["HOME_DELIVERY", "PICKUP", "EXPRESS"],
                len(dataframe),
            )

    dataframe = inject_controlled_sales_bad_records(
        dataframe=dataframe,
        load_date=load_date,
        config=config,
    )

    return dataframe


# ============================================================
# FILE WRITING
# ============================================================


def write_file(
    dataframe: pd.DataFrame,
    output_path: str,
    entity: str,
    load_type: str,
    load_date: str | int,
    overwrite: bool = False,
    dry_run: bool = False,
) -> Optional[str]:

    file_name = f"{entity}_{load_type.lower()}_{load_date}.csv"
    file_path = os.path.join(output_path, file_name)

    if os.path.exists(file_path) and not overwrite:
        print(f"SKIPPED existing file: {file_path}")
        return None

    if dry_run:
        print(
            f"DRY RUN: would create {file_path} | "
            f"rows: {len(dataframe)} | "
            f"columns: {list(dataframe.columns)}"
        )
        return file_path

    dataframe.to_csv(file_path, index=False)

    print(
        f"CREATED: {file_path} | "
        f"rows: {len(dataframe)} | "
        f"columns: {len(dataframe.columns)}"
    )

    return file_path


# ============================================================
# GENERATION RUNNERS
# ============================================================


def generate_full_load(
    config: dict,
    counts: dict,
    full_date: str | int,
    output_path: str,
    overwrite: bool,
    dry_run: bool,
) -> list[str]:

    created_files = []

    jobs = [
        ("customers", generate_customers, int(counts["customers_full"])),
        ("products", generate_products, int(counts["products_full"])),
        ("stores", generate_stores, int(counts["stores_full"])),
        ("sales", generate_sales, int(counts["sales_full"])),
    ]

    for entity, generator, count in jobs:

        file_path = write_file(
            generator(count, full_date, "FULL", config),
            output_path,
            entity,
            "FULL",
            full_date,
            overwrite=overwrite,
            dry_run=dry_run,
        )

        if file_path:
            created_files.append(file_path)

    return created_files


def generate_delta_loads(
    config: dict,
    incremental_dates: Iterable[str | int],
    output_path: str,
    overwrite: bool,
    dry_run: bool,
) -> list[str]:

    created_files = []

    for load_date in incremental_dates:

        delta_counts = get_dynamic_delta_counts(load_date, config)

        jobs = [
            (
                "customers",
                generate_customers,
                delta_counts["customers_delta"],
            ),
            (
                "products",
                generate_products,
                delta_counts["products_delta"],
            ),
            (
                "stores",
                generate_stores,
                delta_counts["stores_delta"],
            ),
            (
                "sales",
                generate_sales,
                delta_counts["sales_delta"],
            ),
        ]

        for entity, generator, count in jobs:

            file_path = write_file(
                generator(count, load_date, "DELTA", config),
                output_path,
                entity,
                "DELTA",
                load_date,
                overwrite=overwrite,
                dry_run=dry_run,
            )

            if file_path:
                created_files.append(file_path)

    return created_files


def print_generation_summary(files: list[str]) -> None:

    if not files:
        print("\nNo new files created.")
        return

    print("\nGeneration Summary")
    print("-" * 100)

    for file_path in files:

        path = Path(file_path)

        if path.exists():

            dataframe = pd.read_csv(path)

            print(
                f"{path.name:35s} | "
                f"rows: {len(dataframe):8d} | "
                f"columns: {len(dataframe.columns):3d}"
            )

        else:
            print(f"{path.name:35s} | dry-run file")


# ============================================================
# COMMAND-LINE ARGUMENTS
# ============================================================


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Generate retail source files with dynamic volume, "
            "controlled bad records, and schema drift."
        )
    )

    parser.add_argument(
        "--config",
        default="config/project_config.yaml",
        help="Path to project config YAML file.",
    )

    parser.add_argument(
        "--dates",
        nargs="*",
        help="Generate selected DELTA dates.",
    )

    generation_mode = parser.add_mutually_exclusive_group()

    generation_mode.add_argument(
        "--full-only",
        action="store_true",
        help="Generate only full-load files.",
    )

    generation_mode.add_argument(
        "--delta-only",
        action="store_true",
        help="Generate only delta-load files.",
    )

    parser.add_argument(
        "--include-full",
        action="store_true",
        help="Generate full load along with delta files.",
    )

    parser.add_argument(
        "--output-path",
        default=None,
        help="Override source_data_path from config.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show output without creating files.",
    )

    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================


def main() -> None:

    args = parse_args()

    config = load_config(args.config)

    output_path = args.output_path or config["source_data_path"]

    ensure_output_path(output_path)

    counts = config["record_counts"]
    full_date = config["full_load_date"]

    created_files = []

    print("=" * 100)
    print("Retail Source Data Generator")
    print("=" * 100)
    print(f"Config path      : {args.config}")
    print(f"Output path      : {output_path}")
    print(f"Overwrite        : {args.overwrite}")
    print(f"Dry run          : {args.dry_run}")
    print(f"Full only        : {args.full_only}")
    print(f"Delta only       : {args.delta_only}")
    print(f"Include full     : {args.include_full}")
    print(f"Override dates   : {args.dates}")
    print("=" * 100)

    if args.full_only:

        created_files.extend(
            generate_full_load(
                config=config,
                counts=counts,
                full_date=full_date,
                output_path=output_path,
                overwrite=args.overwrite,
                dry_run=args.dry_run,
            )
        )

    else:

        if args.include_full:

            created_files.extend(
                generate_full_load(
                    config=config,
                    counts=counts,
                    full_date=full_date,
                    output_path=output_path,
                    overwrite=args.overwrite,
                    dry_run=args.dry_run,
                )
            )

        incremental_dates = get_incremental_dates(
            config,
            args.dates,
        )

        created_files.extend(
            generate_delta_loads(
                config=config,
                incremental_dates=incremental_dates,
                output_path=output_path,
                overwrite=args.overwrite,
                dry_run=args.dry_run,
            )
        )

    print_generation_summary(created_files)

    print("=" * 100)
    print("Generation completed.")
    print("=" * 100)


if __name__ == "__main__":
    main()
