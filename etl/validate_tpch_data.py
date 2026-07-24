from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate generated LineageIQ TPC-H-style CSV data."
    )

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "tpch_data",
        help="Directory containing generated CSV files.",
    )

    return parser.parse_args()


def load_csv(data_dir: Path, filename: str) -> pd.DataFrame:
    path = data_dir / filename

    if not path.exists():
        raise FileNotFoundError(f"Required file does not exist: {path}")

    print(f"Loading {filename}...")
    return pd.read_csv(path)


def validate_unique_key(
    df: pd.DataFrame,
    columns: list[str],
    table_name: str,
    errors: list[str],
) -> None:
    duplicate_count = int(df.duplicated(subset=columns).sum())

    if duplicate_count > 0:
        errors.append(
            f"{table_name}: found {duplicate_count:,} duplicate rows "
            f"for key {columns}"
        )


def validate_not_null(
    df: pd.DataFrame,
    columns: list[str],
    table_name: str,
    errors: list[str],
) -> None:
    for column in columns:
        null_count = int(df[column].isna().sum())

        if null_count > 0:
            errors.append(
                f"{table_name}.{column}: found {null_count:,} null values"
            )


def validate_foreign_key(
    child_df: pd.DataFrame,
    child_column: str,
    parent_df: pd.DataFrame,
    parent_column: str,
    relationship_name: str,
    errors: list[str],
) -> None:
    parent_values = set(parent_df[parent_column].dropna())
    child_values = set(child_df[child_column].dropna())

    missing_values = child_values - parent_values

    if missing_values:
        sample = list(missing_values)[:10]

        errors.append(
            f"{relationship_name}: found {len(missing_values):,} "
            f"unmatched foreign-key values. Sample: {sample}"
        )


def validate_nonnegative(
    df: pd.DataFrame,
    columns: list[str],
    table_name: str,
    errors: list[str],
) -> None:
    for column in columns:
        negative_count = int((df[column] < 0).sum())

        if negative_count > 0:
            errors.append(
                f"{table_name}.{column}: found "
                f"{negative_count:,} negative values"
            )


def validate_lineitem_dates(
    lineitem: pd.DataFrame,
    errors: list[str],
) -> None:
    for column in [
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
    ]:
        lineitem[column] = pd.to_datetime(
            lineitem[column],
            errors="coerce",
        )

    invalid_ship_receipt = int(
        (
            lineitem["l_receiptdate"]
            < lineitem["l_shipdate"]
        ).sum()
    )

    if invalid_ship_receipt > 0:
        errors.append(
            "LINEITEM: found "
            f"{invalid_ship_receipt:,} rows where receipt date "
            "is earlier than ship date"
        )


def main() -> int:
    args = parse_args()
    data_dir = args.data_dir.resolve()

    print("=" * 72)
    print("LineageIQ TPC-H Data Validation")
    print("=" * 72)
    print(f"Data directory: {data_dir}")
    print()

    errors: list[str] = []

    customer = load_csv(data_dir, "customer.csv")
    supplier = load_csv(data_dir, "supplier.csv")
    part = load_csv(data_dir, "part.csv")
    orders = load_csv(data_dir, "orders.csv")
    lineitem = load_csv(data_dir, "lineitem.csv")

    print()
    print("Running validation checks...")

    validate_unique_key(
        customer,
        ["c_custkey"],
        "CUSTOMER",
        errors,
    )

    validate_unique_key(
        supplier,
        ["s_suppkey"],
        "SUPPLIER",
        errors,
    )

    validate_unique_key(
        part,
        ["p_partkey"],
        "PART",
        errors,
    )

    validate_unique_key(
        orders,
        ["o_orderkey"],
        "ORDERS",
        errors,
    )

    validate_unique_key(
        lineitem,
        ["l_orderkey", "l_linenumber"],
        "LINEITEM",
        errors,
    )

    validate_not_null(
        customer,
        ["c_custkey"],
        "CUSTOMER",
        errors,
    )

    validate_not_null(
        supplier,
        ["s_suppkey"],
        "SUPPLIER",
        errors,
    )

    validate_not_null(
        part,
        ["p_partkey"],
        "PART",
        errors,
    )

    validate_not_null(
        orders,
        ["o_orderkey", "o_custkey"],
        "ORDERS",
        errors,
    )

    validate_not_null(
        lineitem,
        [
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
        ],
        "LINEITEM",
        errors,
    )

    validate_foreign_key(
        orders,
        "o_custkey",
        customer,
        "c_custkey",
        "ORDERS.o_custkey -> CUSTOMER.c_custkey",
        errors,
    )

    validate_foreign_key(
        lineitem,
        "l_orderkey",
        orders,
        "o_orderkey",
        "LINEITEM.l_orderkey -> ORDERS.o_orderkey",
        errors,
    )

    validate_foreign_key(
        lineitem,
        "l_partkey",
        part,
        "p_partkey",
        "LINEITEM.l_partkey -> PART.p_partkey",
        errors,
    )

    validate_foreign_key(
        lineitem,
        "l_suppkey",
        supplier,
        "s_suppkey",
        "LINEITEM.l_suppkey -> SUPPLIER.s_suppkey",
        errors,
    )

    validate_nonnegative(
        orders,
        ["o_totalprice"],
        "ORDERS",
        errors,
    )

    validate_nonnegative(
        lineitem,
        [
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
        ],
        "LINEITEM",
        errors,
    )

    validate_lineitem_dates(
        lineitem,
        errors,
    )

    row_counts = {
        "customer": len(customer),
        "supplier": len(supplier),
        "part": len(part),
        "orders": len(orders),
        "lineitem": len(lineitem),
    }

    validation_report = {
        "status": "PASSED" if not errors else "FAILED",
        "row_counts": row_counts,
        "error_count": len(errors),
        "errors": errors,
    }

    report_path = data_dir / "validation_report.json"

    with report_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            validation_report,
            file,
            indent=2,
        )

    print()
    print("Row counts:")

    for table_name, row_count in row_counts.items():
        print(f"  {table_name:<10} {row_count:>10,}")

    print()

    if errors:
        print("VALIDATION FAILED")
        print("-" * 72)

        for error in errors:
            print(f"[FAIL] {error}")

        print()
        print(f"Report written to: {report_path}")
        return 1

    print("VALIDATION PASSED")
    print("All primary-key, null, foreign-key, numeric, and date checks passed.")
    print(f"Report written to: {report_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())