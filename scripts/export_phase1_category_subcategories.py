from __future__ import annotations

import csv
import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PHASE1_DB_PATH = DATA_DIR / "Census_Phase_1.duckdb"
OUTPUT_PATH = DATA_DIR / "phase1-category-subcategories.csv"


CATEGORY_DEFINITIONS = [
    {
        "category": "Baby food",
        "groups": [
            ("bbyfd_cate", {1: "formula", 2: "cereal", 3: "babydia"}),
            ("oth_baby", {}),
        ],
    },
    {
        "category": "Confectionery",
        "groups": [
            ("conf_cate", {1: "chewing", 2: "candies", 3: "choco"}),
        ],
    },
    {
        "category": "Food",
        "groups": [
            (
                "food_cate_hand",
                {
                    1: "instant",
                    2: "pasta",
                    3: "seasonings",
                    4: "tomatopaste",
                    5: "salt",
                    6: "edibleoil",
                    7: "powderedmilk",
                    8: "uht",
                    9: "evaporatedmilk",
                    10: "breakfastcereal",
                    11: "packagedrice",
                    12: "sugar",
                    13: "ballfoods",
                    14: "cocoabeverages",
                },
            ),
            ("oth_food", {}),
        ],
    },
    {
        "category": "Hair care",
        "groups": [
            ("haircare_cate", {1: "shampoo", 2: "conditioner", 3: "creams"}),
        ],
    },
    {
        "category": "Hair extensions",
        "groups": [
            ("hairex_cate", {1: "braids", 2: "locs", 3: "twist", 4: "curl", 5: "straight"}),
        ],
    },
    {
        "category": "Home care",
        "groups": [
            ("home_cate", {1: "toilat", 2: "bleaches"}),
            ("oth_home", {}),
        ],
    },
    {
        "category": "Non-alcoholic beverages",
        "groups": [
            (
                "nonal_cate",
                {
                    1: "csd",
                    2: "frtj",
                    3: "yogdrink",
                    4: "engdrink",
                    5: "flavomilk",
                    6: "water",
                    7: "maltdrink",
                },
            ),
        ],
    },
    {
        "category": "Nutraceuticals",
        "groups": [
            ("nutra_cate", {1: "multivitamin"}),
            ("oth_nutra", {}),
        ],
    },
    {
        "category": "Packaged snacks",
        "groups": [
            ("pckg_cate", {1: "biscuit", 2: "chinchin", 3: "cookie", 4: "chip", 5: "sausage"}),
        ],
    },
    {
        "category": "Personal care",
        "groups": [
            ("perso_cate", {1: "toothpa", 2: "toothbr"}),
            ("oth_personal", {}),
        ],
    },
    {
        "category": "Alcoholic beverages",
        "groups": [
            (
                "alcoholic_cate",
                {
                    1: "beer",
                    2: "rtd",
                    3: "alcobrand",
                    4: "alcogin",
                    5: "alcoliquer",
                    6: "rum",
                    7: "schnapps",
                    8: "vodka",
                    9: "whisky",
                },
            ),
        ],
    },
    {
        "category": "Tobacco",
        "groups": [
            ("tobacco_cate", {1: "cigarettes"}),
        ],
    },
]


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def resolve_phase1_table(connection: duckdb.DuckDBPyConnection) -> str:
    tables = [row[0] for row in connection.execute("show tables").fetchall()]
    phase1_tables = [name for name in tables if "Phase_1" in name]
    if len(phase1_tables) == 1:
        return phase1_tables[0]
    if len(tables) == 1:
        return tables[0]
    raise ValueError(f"Unable to resolve Phase 1 table from: {tables}")


def code_columns_for_group(table_columns: set[str], group_name: str) -> list[tuple[int, str]]:
    pattern = re.compile(rf"^{re.escape(group_name)}/(\d+)$")
    matches: list[tuple[int, str]] = []
    for column_name in table_columns:
        match = pattern.match(column_name)
        if not match:
            continue
        matches.append((int(match.group(1)), column_name))
    return sorted(matches, key=lambda item: item[0])


def placeholder_label(category_name: str, group_name: str, code: int) -> str:
    if group_name.startswith("oth_"):
        return f"Other {category_name.lower()} option {code}"
    return f"{category_name} option {code}"


def export_csv() -> Path:
    connection = duckdb.connect(str(PHASE1_DB_PATH), read_only=True)
    try:
        table_name = resolve_phase1_table(connection)
        table_columns = {
            row[0]
            for row in connection.execute(f"describe {quote_identifier(table_name)}").fetchall()
        }

        rows: list[dict[str, str | int]] = []
        for category_definition in CATEGORY_DEFINITIONS:
            category_name = category_definition["category"]
            for group_name, explicit_labels in category_definition["groups"]:
                for code, column_name in code_columns_for_group(table_columns, group_name):
                    subcategory_name = explicit_labels.get(code) or placeholder_label(
                        category_name, group_name, code
                    )
                    record_count = int(
                        connection.execute(
                            f"""
                            select count(*)
                            from {quote_identifier(table_name)}
                            where coalesce(try_cast({quote_identifier(column_name)} as double), 0) > 0
                            """
                        ).fetchone()[0]
                        or 0
                    )
                    rows.append(
                        {
                            "category": category_name,
                            "subcategory": subcategory_name,
                            "source_group": group_name,
                            "code": code,
                            "column_name": column_name,
                            "record_count": record_count,
                            "label_status": (
                                "explicit_from_schema" if code in explicit_labels else "coded_only_placeholder"
                            ),
                        }
                    )

        with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(
                output_file,
                fieldnames=[
                    "category",
                    "subcategory",
                    "source_group",
                    "code",
                    "column_name",
                    "record_count",
                    "label_status",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)

        return OUTPUT_PATH
    finally:
        connection.close()


if __name__ == "__main__":
    output_path = export_csv()
    print(output_path)
