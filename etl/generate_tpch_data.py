from __future__ import annotations

import argparse
import csv
import json
import random
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Sequence


MARKET_SEGMENTS = [
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "MACHINERY",
    "HOUSEHOLD",
]

ORDER_PRIORITIES = [
    "1-URGENT",
    "2-HIGH",
    "3-MEDIUM",
    "4-NOT SPECIFIED",
    "5-LOW",
]

SHIP_MODES = [
    "TRUCK",
    "MAIL",
    "AIR",
    "RAIL",
    "SHIP",
    "FOB",
    "REG AIR",
]

SHIP_INSTRUCTS = [
    "DELIVER IN PERSON",
    "COLLECT COD",
    "NONE",
    "TAKE BACK RETURN",
]

PART_TYPES = [
    "SMALL BRUSHED STEEL",
    "LARGE POLISHED BRASS",
    "MEDIUM ANODIZED COPPER",
    "STANDARD BURNISHED TIN",
    "PROMO PLATED NICKEL",
]

CONTAINERS = [
    "SM BOX",
    "LG BOX",
    "MED BAG",
    "JUMBO CAN",
    "WRAP CASE",
]

BRANDS = [
    f"Brand#{manufacturer}{brand}"
    for manufacturer in range(1, 6)
    for brand in range(1, 6)
]


@dataclass(frozen=True)
class ScaleConfig:
    customers: int
    suppliers: int
    parts: int
    orders: int
    min_lineitems_per_order: int
    max_lineitems_per_order: int


SCALE_PROFILES = {
    "dev": ScaleConfig(
        customers=1_000,
        suppliers=100,
        parts=500,
        orders=3_000,
        min_lineitems_per_order=3,
        max_lineitems_per_order=3,
    ),
    "v2": ScaleConfig(
        customers=15_000,
        suppliers=1_000,
        parts=5_000,
        orders=60_000,
        min_lineitems_per_order=2,
        max_lineitems_per_order=4,
    ),
    "baseline": ScaleConfig(
        customers=15_000,
        suppliers=1_000,
        parts=5_000,
        orders=60_000,
        min_lineitems_per_order=2,
        max_lineitems_per_order=4,
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic TPC-H-style CSV data for LineageIQ."
    )

    parser.add_argument(
        "--scale",
        choices=sorted(SCALE_PROFILES),
        default="dev",
        help="Dataset scale profile. Default: dev.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed used for deterministic generation.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "tpch_data",
        help="Directory where generated CSV files will be written.",
    )

    return parser.parse_args()


def random_date(
    rng: random.Random,
    start_year: int = 1993,
    end_year: int = 1998,
) -> date:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    return start + timedelta(days=rng.randint(0, (end - start).days))


def random_phone(rng: random.Random) -> str:
    return (
        f"{rng.randint(10, 99)}-"
        f"{rng.randint(100, 999)}-"
        f"{rng.randint(100, 999)}-"
        f"{rng.randint(1000, 9999)}"
    )


def random_name(prefix: str, key: int) -> str:
    return f"{prefix}#{key:09d}"


def random_comment(rng: random.Random, max_length: int) -> str:
    words = [
        "special",
        "pending",
        "unusual",
        "express",
        "final",
        "regular",
        "silent",
        "blithely",
        "carefully",
        "slyly",
        "quickly",
        "furiously",
    ]

    comment = " ".join(
        rng.choices(words, k=rng.randint(3, 8))
    )

    return comment[:max_length]


def write_customers(
    output_dir: Path,
    count: int,
    rng: random.Random,
) -> list[int]:
    path = output_dir / "customer.csv"
    customer_keys: list[int] = []

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                "c_custkey",
                "c_name",
                "c_address",
                "c_nationkey",
                "c_phone",
                "c_acctbal",
                "c_mktsegment",
                "c_comment",
            ]
        )

        for customer_key in range(1, count + 1):
            writer.writerow(
                [
                    customer_key,
                    random_name("Customer", customer_key),
                    f"{rng.randint(1, 999)} Main St",
                    rng.randint(0, 24),
                    random_phone(rng),
                    round(rng.uniform(-999.99, 9999.99), 2),
                    rng.choice(MARKET_SEGMENTS),
                    random_comment(rng, 117),
                ]
            )

            customer_keys.append(customer_key)

    return customer_keys


def write_suppliers(
    output_dir: Path,
    count: int,
    rng: random.Random,
) -> list[int]:
    path = output_dir / "supplier.csv"
    supplier_keys: list[int] = []

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                "s_suppkey",
                "s_name",
                "s_address",
                "s_nationkey",
                "s_phone",
                "s_acctbal",
                "s_comment",
            ]
        )

        for supplier_key in range(1, count + 1):
            writer.writerow(
                [
                    supplier_key,
                    random_name("Supplier", supplier_key),
                    f"{rng.randint(1, 999)} Commerce Ave",
                    rng.randint(0, 24),
                    random_phone(rng),
                    round(rng.uniform(-999.99, 9999.99), 2),
                    random_comment(rng, 101),
                ]
            )

            supplier_keys.append(supplier_key)

    return supplier_keys


def write_parts(
    output_dir: Path,
    count: int,
    rng: random.Random,
) -> list[int]:
    path = output_dir / "part.csv"
    part_keys: list[int] = []

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                "p_partkey",
                "p_name",
                "p_mfgr",
                "p_brand",
                "p_type",
                "p_size",
                "p_container",
                "p_retailprice",
                "p_comment",
            ]
        )

        for part_key in range(1, count + 1):
            writer.writerow(
                [
                    part_key,
                    random_name("Part", part_key),
                    f"Manufacturer#{rng.randint(1, 5)}",
                    rng.choice(BRANDS),
                    rng.choice(PART_TYPES),
                    rng.randint(1, 50),
                    rng.choice(CONTAINERS),
                    round(rng.uniform(900, 2099), 2),
                    random_comment(rng, 23),
                ]
            )

            part_keys.append(part_key)

    return part_keys


def write_orders_and_lineitems(
    output_dir: Path,
    config: ScaleConfig,
    customer_keys: Sequence[int],
    supplier_keys: Sequence[int],
    part_keys: Sequence[int],
    rng: random.Random,
) -> tuple[int, int]:
    orders_path = output_dir / "orders.csv"
    lineitems_path = output_dir / "lineitem.csv"

    order_count = 0
    lineitem_count = 0

    with (
        orders_path.open("w", newline="", encoding="utf-8") as orders_file,
        lineitems_path.open("w", newline="", encoding="utf-8") as lineitems_file,
    ):
        order_writer = csv.writer(orders_file)
        lineitem_writer = csv.writer(lineitems_file)

        order_writer.writerow(
            [
                "o_orderkey",
                "o_custkey",
                "o_orderstatus",
                "o_totalprice",
                "o_orderdate",
                "o_orderpriority",
                "o_clerk",
                "o_shippriority",
                "o_comment",
            ]
        )

        lineitem_writer.writerow(
            [
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_linenumber",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_tax",
                "l_returnflag",
                "l_linestatus",
                "l_shipdate",
                "l_commitdate",
                "l_receiptdate",
                "l_shipinstruct",
                "l_shipmode",
                "l_comment",
            ]
        )

        for order_key in range(1, config.orders + 1):
            order_date = random_date(rng)
            order_status = rng.choice(["O", "F", "P"])
            lineitem_total = 0.0

            number_of_lineitems = rng.randint(
                config.min_lineitems_per_order,
                config.max_lineitems_per_order,
            )

            order_lineitems: list[list[object]] = []

            for line_number in range(1, number_of_lineitems + 1):
                part_key = rng.choice(part_keys)
                supplier_key = rng.choice(supplier_keys)

                quantity = round(rng.uniform(1, 50), 2)
                extended_price = round(rng.uniform(100, 5000), 2)
                discount = round(rng.uniform(0, 0.10), 2)
                tax = round(rng.uniform(0, 0.08), 2)

                ship_date = order_date + timedelta(
                    days=rng.randint(1, 60)
                )

                commit_date = order_date + timedelta(
                    days=rng.randint(30, 90)
                )

                receipt_date = ship_date + timedelta(
                    days=rng.randint(1, 30)
                )

                lineitem_total += extended_price

                order_lineitems.append(
                    [
                        order_key,
                        part_key,
                        supplier_key,
                        line_number,
                        quantity,
                        extended_price,
                        discount,
                        tax,
                        rng.choice(["R", "A", "N"]),
                        "F",
                        ship_date,
                        commit_date,
                        receipt_date,
                        rng.choice(SHIP_INSTRUCTS),
                        rng.choice(SHIP_MODES),
                        random_comment(rng, 44),
                    ]
                )

            order_writer.writerow(
                [
                    order_key,
                    rng.choice(customer_keys),
                    order_status,
                    round(lineitem_total, 2),
                    order_date,
                    rng.choice(ORDER_PRIORITIES),
                    f"Clerk#{rng.randint(1, 1000):09d}",
                    rng.randint(0, 1),
                    random_comment(rng, 79),
                ]
            )

            order_count += 1

            for lineitem in order_lineitems:
                lineitem_writer.writerow(lineitem)
                lineitem_count += 1

    return order_count, lineitem_count


def count_csv_rows(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8") as file:
        return max(sum(1 for _ in file) - 1, 0)


def validate_generated_data(
    output_dir: Path,
    expected: dict[str, int],
) -> dict[str, int]:
    actual = {
        "customer": count_csv_rows(output_dir / "customer.csv"),
        "supplier": count_csv_rows(output_dir / "supplier.csv"),
        "part": count_csv_rows(output_dir / "part.csv"),
        "orders": count_csv_rows(output_dir / "orders.csv"),
        "lineitem": count_csv_rows(output_dir / "lineitem.csv"),
    }

    for table_name in ["customer", "supplier", "part", "orders"]:
        if actual[table_name] != expected[table_name]:
            raise ValueError(
                f"{table_name} row count mismatch: "
                f"expected {expected[table_name]}, "
                f"generated {actual[table_name]}"
            )

    minimum_lineitems = expected["orders"] * 2
    maximum_lineitems = expected["orders"] * 4

    if not minimum_lineitems <= actual["lineitem"] <= maximum_lineitems:
        raise ValueError(
            "lineitem row count is outside the configured range: "
            f"{actual['lineitem']}"
        )

    return actual


def get_file_sizes(output_dir: Path) -> dict[str, int]:
    return {
        path.name: path.stat().st_size
        for path in output_dir.glob("*.csv")
    }


def main() -> None:
    args = parse_args()
    config = SCALE_PROFILES[args.scale]

    output_dir: Path = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(args.seed)

    started_at = datetime.utcnow()
    start_time = time.perf_counter()

    print("=" * 68)
    print("LineageIQ TPC-H Data Generator")
    print("=" * 68)
    print(f"Scale profile : {args.scale}")
    print(f"Seed          : {args.seed}")
    print(f"Output folder : {output_dir}")
    print()

    print("Generating customers...")
    customer_keys = write_customers(
        output_dir,
        config.customers,
        rng,
    )

    print("Generating suppliers...")
    supplier_keys = write_suppliers(
        output_dir,
        config.suppliers,
        rng,
    )

    print("Generating parts...")
    part_keys = write_parts(
        output_dir,
        config.parts,
        rng,
    )

    print("Generating orders and lineitems...")
    order_count, lineitem_count = write_orders_and_lineitems(
        output_dir=output_dir,
        config=config,
        customer_keys=customer_keys,
        supplier_keys=supplier_keys,
        part_keys=part_keys,
        rng=rng,
    )

    expected = {
        "customer": config.customers,
        "supplier": config.suppliers,
        "part": config.parts,
        "orders": order_count,
        "lineitem": lineitem_count,
    }

    actual = validate_generated_data(
        output_dir,
        expected,
    )

    duration_seconds = round(
        time.perf_counter() - start_time,
        3,
    )

    manifest = {
        "run_id": (
            f"generation_{started_at.strftime('%Y%m%dT%H%M%SZ')}"
            f"_seed_{args.seed}"
        ),
        "generated_at_utc": started_at.isoformat(),
        "scale_profile": args.scale,
        "seed": args.seed,
        "configuration": asdict(config),
        "row_counts": actual,
        "duration_seconds": duration_seconds,
        "file_sizes_bytes": get_file_sizes(output_dir),
    }

    manifest_path = output_dir / "generation_manifest.json"

    with manifest_path.open(
        "w",
        encoding="utf-8",
    ) as manifest_file:
        json.dump(
            manifest,
            manifest_file,
            indent=2,
        )

    print()
    print("Generation completed successfully.")
    print(f"Duration: {duration_seconds:.3f} seconds")
    print()
    print("Generated row counts:")

    for table_name, row_count in actual.items():
        print(f"  {table_name:<10} {row_count:>10,}")

    print()
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()