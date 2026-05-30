import csv
import uuid
import random
from datetime import datetime, timezone
from pathlib import Path

# Output file path inside your project
output_dir = Path("source_data")
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / "customers_good_10000.csv"


first_names = [
    "Aarav",
    "Vivaan",
    "Aditya",
    "Vihaan",
    "Arjun",
    "Sai",
    "Reyansh",
    "Ayaan",
    "Krishna",
    "Ishaan",
    "Ananya",
    "Diya",
    "Myra",
    "Aadhya",
    "Saanvi",
    "Kiara",
    "Ira",
    "Anika",
    "Meera",
    "Riya",
]

last_names = [
    "Sharma",
    "Reddy",
    "Kumar",
    "Nair",
    "Patel",
    "Gupta",
    "Verma",
    "Rao",
    "Yadav",
    "Mishra",
    "Iyer",
    "Menon",
    "Das",
    "Chopra",
    "Jain",
    "Shetty",
    "Pillai",
    "Bose",
    "Naidu",
    "Khan",
]

cities_states = [
    ("Hyderabad", "Telangana"),
    ("Bengaluru", "Karnataka"),
    ("Chennai", "Tamil Nadu"),
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal"),
    ("Ahmedabad", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Lucknow", "Uttar Pradesh"),
    ("Vijayawada", "Andhra Pradesh"),
    ("Visakhapatnam", "Andhra Pradesh"),
]


headers = [
    "customer_id",
    "customer_name",
    "email",
    "city",
    "state",
    "created_date",
    "load_type",
    "source_file_name",
    "batch_id",
    "load_timestamp",
]


batch_id = str(uuid.uuid4())
load_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")


with output_path.open("w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(headers)

    for i in range(1, 10001):
        first = random.choice(first_names)
        last = random.choice(last_names)
        city, state = random.choice(cities_states)

        customer_id = f"CUST{i:06d}"
        customer_name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        created_date = "2026-05-30"
        load_type = "FULL"
        source_file_name = "customers_good_10000.csv"

        writer.writerow(
            [
                customer_id,
                customer_name,
                email,
                city,
                state,
                created_date,
                load_type,
                source_file_name,
                batch_id,
                load_timestamp,
            ]
        )


print(f"CSV file created successfully: {output_path}")
print("Total records generated: 10000")
print("Expected schema columns:", len(headers))
print("No duplicate customer_id")
print("No null customer_id")
print("No null email")
