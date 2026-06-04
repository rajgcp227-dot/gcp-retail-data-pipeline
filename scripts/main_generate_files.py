# ============================================================
# main_generate_files.py
# Retail Data Pipeline - Source File Generator
#
# Purpose:
#   Generate simulated retail source CSV files with:
#   - good records
#   - bad records
#   - duplicate records
#   - schema drift records
#   - dynamic daily volume
#
# Supports:
#   - FULL load generation
#   - DELTA load generation
#   - CURRENT_DATE mode
#   - FIXED_DATES / backfill mode
#   - weekday/weekend/month-end/festival volume variation
# ============================================================

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
# CONFIG HELPERS
# ============================================================


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def ensure_output_path(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_load_date(load_date: str | int):
    return datetime.strptime(str(load_date), "%Y%m%d").date()


def stable_seed(entity: str, load_type: str, load_date: str | int) -> int:
    """
    Stable seed per entity + load type + date.
    Same date generates same data if re-run.
    """
    seed_text = f"{entity}_{load_type}_{load_date}"
    return int(hashlib.md5(seed_text.encode("utf-8")).hexdigest()[:8], 16)


def get_fake(entity: str, load_type: str, load_date: str | int) -> Faker:
    fake = Faker("en_IN")
    fake.seed_instance(stable_seed(entity, load_type, load_date))
    return fake


def get_rng(entity: str, load_type: str, load_date: str | int) -> random.Random:
    return random.Random(stable_seed(entity, load_type, load_date))


def random_choices(rng: random.Random, values: list, count: int) -> list:
    return [rng.choice(values) for _ in range(count)]


# ============================================================
# DATE + VOLUME LOGIC
# ============================================================


def get_incremental_dates(
    config: dict, override_dates: Optional[list[str]] = None
) -> list[str]:
    """
    Decide which incremental dates to generate.

    Priority:
      1. CLI --dates
      2. Config incremental_date_mode = CURRENT_DATE
      3. Config incremental_date_mode = FIXED_DATES
    """

    if override_dates:
        return [str(date) for date in override_dates]

    mode = config.get("incremental_date_mode", "FIXED_DATES")

    if mode == "CURRENT_DATE":
        days_back = int(config.get("incremental_days_back", 0))
        load_date = datetime.now() - timedelta(days=days_back)
        return [load_date.strftime("%Y%m%d")]

    if mode == "FIXED_DATES":
        return [str(date) for date in config.get("incremental_dates", [])]

    raise ValueError(f"Invalid incremental_date_mode: {mode}")


def get_volume_profile(load_date: str | int, config: dict) -> str:
    """
    Decide business volume profile.

    Priority:
      1. Festival date
      2. Month-end
      3. Weekend
      4. Weekday
    """

    load_date_str = str(load_date)
    date_obj = datetime.strptime(load_date_str, "%Y%m%d").date()

    festival_dates = [str(date) for date in config.get("festival_dates", [])]

    if load_date_str in festival_dates:
        return "festival"

    if date_obj.day >= 28:
        return "month_end"

    if date_obj.weekday() in [5, 6]:
        return "weekend"

    return "weekday"


def get_dynamic_delta_counts(load_date: str | int, config: dict) -> dict:
    """
    Generate dynamic record counts based on business date profile.

    Example:
      weekday   -> medium sales
      weekend   -> high sales
      month_end -> higher volume
      festival  -> peak volume
    """

    profile = get_volume_profile(load_date, config)

    dynamic_rules = config.get("dynamic_delta_record_counts")

    if not dynamic_rules:
        # Fallback to static old config if dynamic section is missing.
        static_counts = config["record_counts"]

        counts = {
            "customers_delta": static_counts["customers_delta"],
            "products_delta": static_counts["products_delta"],
            "stores_delta": static_counts["stores_delta"],
            "sales_delta": static_counts["sales_delta"],
        }

        print(
            f"Load date: {load_date} | Volume profile: static_config | Counts: {counts}"
        )
        return counts

    rules = dynamic_rules[profile]

    rng = random.Random(int(str(load_date)))

    counts = {
        "customers_delta": rng.randint(
            rules["customers_delta_min"],
            rules["customers_delta_max"],
        ),
        "products_delta": rng.randint(
            rules["products_delta_min"],
            rules["products_delta_max"],
        ),
        "stores_delta": rng.randint(
            rules["stores_delta_min"],
            rules["stores_delta_max"],
        ),
        "sales_delta": rng.randint(
            rules["sales_delta_min"],
            rules["sales_delta_max"],
        ),
    }

    print(f"Load date: {load_date} | Volume profile: {profile} | Counts: {counts}")

    return counts


def should_apply_schema_drift(load_date: str | int, drift_days: set[int]) -> bool:
    """
    Applies schema drift based on day of month.

    Example:
      5th, 15th, 25th can simulate new column drift.
    """
    day = datetime.strptime(str(load_date), "%Y%m%d").day
    return day in drift_days


# ============================================================
# CUSTOMER GENERATOR
# ============================================================


def generate_customers(
    count: int, load_date: str | int, load_type: str
) -> pd.DataFrame:
    fake = get_fake("customers", load_type, load_date)
    rng = get_rng("customers", load_type, load_date)
    created_date = parse_load_date(load_date)

    rows = []

    for i in range(1, count + 1):
        customer_id = (
            f"CUST{i:06d}" if load_type == "FULL" else f"CUSTD{load_date}{i:05d}"
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

    df = pd.DataFrame(rows)

    # Bad records
    if count >= 10:
        df.loc[0, "customer_id"] = None
        df.loc[1, "email"] = "invalid_email"
        df.loc[2, "customer_id"] = df.loc[3, "customer_id"]
        df.loc[4, "customer_name"] = ""
        df.loc[5, "created_date"] = datetime.now().date() + timedelta(days=10)

    # Schema drift examples for delta files
    if load_type == "DELTA":
        if should_apply_schema_drift(load_date, {5, 15, 25}):
            df["loyalty_tier"] = random_choices(
                rng,
                ["SILVER", "GOLD", "PLATINUM", None],
                len(df),
            )

        if should_apply_schema_drift(load_date, {6, 16, 26}):
            df["customer_segment"] = random_choices(
                rng,
                ["REGULAR", "PREMIUM", "NEW", "CHURN_RISK"],
                len(df),
            )

    return df


# ============================================================
# PRODUCT GENERATOR
# ============================================================


def generate_products(count: int, load_date: str | int, load_type: str) -> pd.DataFrame:
    fake = get_fake("products", load_type, load_date)
    rng = get_rng("products", load_type, load_date)
    created_date = parse_load_date(load_date)

    categories = ["Electronics", "Fashion", "Grocery", "Home", "Beauty", "Sports"]

    rows = []

    for i in range(1, count + 1):
        product_id = (
            f"PROD{i:05d}" if load_type == "FULL" else f"PRODD{load_date}{i:04d}"
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

    df = pd.DataFrame(rows)

    # Bad records
    if count >= 10:
        df.loc[0, "product_id"] = None
        df.loc[1, "price"] = -100
        df.loc[2, "product_id"] = df.loc[3, "product_id"]
        df.loc[4, "category"] = ""
        df.loc[5, "created_date"] = datetime.now().date() + timedelta(days=10)

    # Schema drift examples for delta files
    if load_type == "DELTA":
        if should_apply_schema_drift(load_date, {5, 15, 25}):
            df["supplier_id"] = [f"SUP{rng.randint(1, 50):04d}" for _ in range(len(df))]

        if should_apply_schema_drift(load_date, {6, 16, 26}):
            df["product_status"] = random_choices(
                rng,
                ["ACTIVE", "INACTIVE", "DISCONTINUED"],
                len(df),
            )

            # Type drift example
            if len(df) > 8:
                df["price"] = df["price"].astype("object")
                df.loc[7, "price"] = "ABC"

    return df


# ============================================================
# STORE GENERATOR
# ============================================================


def generate_stores(count: int, load_date: str | int, load_type: str) -> pd.DataFrame:
    fake = get_fake("stores", load_type, load_date)
    rng = get_rng("stores", load_type, load_date)
    created_date = parse_load_date(load_date)

    valid_regions = ["North", "South", "East", "West"]

    rows = []

    for i in range(1, count + 1):
        store_id = (
            f"STORE{i:04d}" if load_type == "FULL" else f"STORED{load_date}{i:03d}"
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

    df = pd.DataFrame(rows)

    # Bad records
    if count >= 10:
        df.loc[0, "store_id"] = None
        df.loc[1, "region"] = "UNKNOWN_REGION"
        df.loc[2, "store_name"] = ""
        df.loc[3, "created_date"] = datetime.now().date() + timedelta(days=10)

    # Schema drift examples for delta files
    if load_type == "DELTA":
        if should_apply_schema_drift(load_date, {5, 15, 25}):
            df["store_type"] = random_choices(
                rng,
                ["MALL", "STANDALONE", "WAREHOUSE", "FRANCHISE"],
                len(df),
            )

        if should_apply_schema_drift(load_date, {6, 16, 26}):
            df["manager_name"] = [fake.name() for _ in range(len(df))]

    return df


# ============================================================
# SALES GENERATOR
# ============================================================


def generate_sales(count: int, load_date: str | int, load_type: str) -> pd.DataFrame:
    rng = get_rng("sales", load_type, load_date)
    sale_date = parse_load_date(load_date)

    rows = []

    for i in range(1, count + 1):
        quantity = rng.randint(1, 5)
        unit_price = round(rng.uniform(50, 5000), 2)
        discount = round(rng.uniform(0, 200), 2)
        tax = round(unit_price * quantity * 0.05, 2)
        sale_amount = round((unit_price * quantity) - discount + tax, 2)

        rows.append(
            {
                "order_id": f"ORD{load_date}{i:07d}",
                "customer_id": f"CUST{rng.randint(1, 5000):06d}",
                "product_id": f"PROD{rng.randint(1, 1000):05d}",
                "store_id": f"STORE{rng.randint(1, 100):04d}",
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_amount": discount,
                "tax_amount": tax,
                "sale_amount": sale_amount,
                "sale_date": sale_date,
                "payment_method": rng.choice(["UPI", "CARD", "CASH", "NETBANKING"]),
                "load_type": load_type,
            }
        )

    df = pd.DataFrame(rows)

    # Bad records
    if count >= 10:
        df.loc[0, "customer_id"] = None
        df.loc[1, "sale_amount"] = -500
        df.loc[2, "quantity"] = 0
        df.loc[3, "sale_date"] = datetime.now().date() + timedelta(days=10)
        df.loc[4, "order_id"] = df.loc[5, "order_id"]
        df.loc[6, "product_id"] = "INVALID_PRODUCT"
        df.loc[7, "customer_id"] = "INVALID_CUSTOMER"
        df.loc[8, "store_id"] = "INVALID_STORE"
        df.loc[9, "payment_method"] = "CRYPTO"

    # Schema drift examples for delta files
    if load_type == "DELTA":
        if should_apply_schema_drift(load_date, {3, 13, 23}):
            df["coupon_code"] = random_choices(
                rng,
                ["NEW10", "SAVE20", "FESTIVE15", None],
                len(df),
            )

        elif should_apply_schema_drift(load_date, {4, 14, 24}):
            # Column order drift
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

        elif should_apply_schema_drift(load_date, {5, 15, 25}):
            df["sale_channel"] = random_choices(
                rng,
                ["ONLINE", "STORE", "APP"],
                len(df),
            )

        elif should_apply_schema_drift(load_date, {6, 16, 26}):
            # Type drift
            if len(df) > 8:
                df["sale_amount"] = df["sale_amount"].astype("object")
                df.loc[7, "sale_amount"] = "ABC"

            df["delivery_type"] = random_choices(
                rng,
                ["HOME_DELIVERY", "PICKUP", "EXPRESS"],
                len(df),
            )

    return df


# ============================================================
# FILE WRITER
# ============================================================


def write_file(
    df: pd.DataFrame,
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
            f"rows: {len(df)} | columns: {list(df.columns)}"
        )
        return file_path

    df.to_csv(file_path, index=False)
    print(f"CREATED: {file_path} | rows: {len(df)} | columns: {len(df.columns)}")
    return file_path


# ============================================================
# GENERATION RUNNERS
# ============================================================


def generate_full_load(
    counts: dict,
    full_date: str | int,
    output_path: str,
    overwrite: bool,
    dry_run: bool,
) -> list[str]:
    created_files = []

    jobs = [
        ("customers", generate_customers, counts["customers_full"]),
        ("products", generate_products, counts["products_full"]),
        ("stores", generate_stores, counts["stores_full"]),
        ("sales", generate_sales, counts["sales_full"]),
    ]

    for entity, generator, count in jobs:
        file_path = write_file(
            generator(count, full_date, "FULL"),
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
            ("customers", generate_customers, delta_counts["customers_delta"]),
            ("products", generate_products, delta_counts["products_delta"]),
            ("stores", generate_stores, delta_counts["stores_delta"]),
            ("sales", generate_sales, delta_counts["sales_delta"]),
        ]

        for entity, generator, count in jobs:
            file_path = write_file(
                generator(count, load_date, "DELTA"),
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


# ============================================================
# SUMMARY
# ============================================================


def print_generation_summary(files: list[str]) -> None:
    if not files:
        print("\nNo new files created.")
        return

    print("\nGeneration Summary")
    print("-" * 100)

    for file_path in files:
        path = Path(file_path)

        if path.exists():
            df = pd.read_csv(path)
            print(
                f"{path.name:35s} | rows: {len(df):8d} | columns: {len(df.columns):3d}"
            )
        else:
            print(f"{path.name:35s} | dry-run file")


# ============================================================
# ARGPARSE
# ============================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate retail source files with good, bad, schema drift, "
            "and dynamic business volume records."
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
        help="Generate selected DELTA dates. Example: --dates 20260604 20260605",
    )

    parser.add_argument(
        "--full-only",
        action="store_true",
        help="Generate only FULL load.",
    )

    parser.add_argument(
        "--delta-only",
        action="store_true",
        help="Generate only DELTA load.",
    )

    parser.add_argument(
        "--include-full",
        action="store_true",
        help="Generate FULL load also along with DELTA load.",
    )

    parser.add_argument(
        "--output-path",
        default=None,
        help="Override output path. If not passed, uses source_data_path from config.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be generated without writing files.",
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
                    counts=counts,
                    full_date=full_date,
                    output_path=output_path,
                    overwrite=args.overwrite,
                    dry_run=args.dry_run,
                )
            )

        incremental_dates = get_incremental_dates(config, args.dates)

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
