from __future__ import annotations

import json
import math
import re
import shutil
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
from shapely.geometry import Point, shape
from shapely.ops import unary_union

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PUBLIC_DATA_DIR = ROOT / "frontend" / "public" / "data"

STATE_SOURCE_PATH = DATA_DIR / "NGA_State_Boundaries.geojson"
LGA_SOURCE_PATH = DATA_DIR / "NGA_LGA_Boundaries.geojson"
WARD_SOURCE_PATH = DATA_DIR / "Nigeria_-_Ward_Boundaries.geojson"

OUTPUT_FILENAMES = {
    "states": "state-boundaries.geojson",
    "lgas": "lga-boundaries.geojson",
    "wards": "ward-boundaries.geojson",
    "observations": "gps-observations.geojson",
    "summary": "dashboard-summary.json",
}
LEGACY_OUTPUT_PATHS = {
    name: PUBLIC_DATA_DIR / filename for name, filename in OUTPUT_FILENAMES.items()
}
DATASET_MANIFEST_PATH = PUBLIC_DATA_DIR / "datasets.json"

EARTH_RADIUS_METERS = 6_371_000
GRID_CELL_METERS = 10
DEFAULT_GPS_TOLERANCE_METERS = 10
MAX_GPS_TOLERANCE_METERS = 25
NEAR_TARGET_PERCENT = 80
WELL_COVERED_PERCENT = 85
MAX_FRONTEND_OBSERVATIONS = 50_000
RUNTIME_SCHEMA_VERSION = 7

PRODUCT_CATEGORY_LABELS = {
    "1": "Baby food",
    "2": "Confectionery",
    "3": "Food",
    "4": "Hair care",
    "5": "Hair extensions",
    "6": "Home care",
    "7": "Non-alcoholic beverages",
    "8": "Nutraceuticals",
    "9": "Packaged snacks",
    "10": "Personal care",
    "11": "Alcoholic beverages",
    "12": "Tobacco",
}

PHASE_PRODUCT_CATEGORY_COLUMNS = [
    ("1", '"Baby food"'),
    ("2", '"Confectionery"'),
    ("3", '"Food"'),
    ("4", '"Hair care"'),
    ("5", '"Hair extensions"'),
    ("6", '"Home care"'),
    ("7", '"nonalcoholic_beverages"'),
    ("8", '"nutraceuticals"'),
    ("9", '"packaged_snacks"'),
    ("10", '"personal_care"'),
    ("11", '"alcoholic_beverages"'),
    ("12", '"Tobacco"'),
]

OUTLET_SUBCATEGORY_GROUPS = [
    {
        "categoryCode": "1",
        "categoryLabel": "Baby food",
        "groupName": "bbyfd_cate",
        "fieldAlias": "bbyfd_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_bbyfd1-bbyfd_cate",
            "bbyfd_cate",
        ],
        "knownLabels": {
            1: "Formula",
            2: "Cereal",
            3: "Babydia",
        },
    },
    {
        "categoryCode": "2",
        "categoryLabel": "Confectionery",
        "groupName": "conf_cate",
        "fieldAlias": "conf_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_conf1-conf_cate",
            "conf_cate",
        ],
        "knownLabels": {
            1: "Chewing",
            2: "Candies",
            3: "Choco",
        },
    },
    {
        "categoryCode": "3",
        "categoryLabel": "Food",
        "groupName": "food_cate_hand",
        "fieldAlias": "food_cate_hand_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_food1-food_cate_hand",
            "food_cate_hand",
        ],
        "knownLabels": {
            1: "Instant noodles",
            2: "Pasta",
            3: "Seasonings",
            4: "Tomatopaste",
            5: "Salt",
            6: "Edibleoil",
            7: "Powderedmilk",
            8: "Uht",
            9: "Evaporatedmilk",
            10: "Breakfastcereal",
            11: "Packagedrice",
            12: "Sugar",
            13: "Ballfoods",
            14: "Cocoabeverages",
        },
    },
    {
        "categoryCode": "4",
        "categoryLabel": "Hair care",
        "groupName": "haircare_cate",
        "fieldAlias": "haircare_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_haircare1-haircare_cate",
            "haircare_cate",
        ],
        "knownLabels": {
            1: "Shampoo",
            2: "Conditioner",
            3: "Creams",
        },
    },
    {
        "categoryCode": "5",
        "categoryLabel": "Hair extensions",
        "groupName": "hairex_cate",
        "fieldAlias": "hairex_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_hairex1-hairex_cate",
            "hairex_cate",
        ],
        "knownLabels": {
            1: "Braids",
            2: "Locs",
            3: "Twist",
            4: "Curl",
            5: "Straight",
        },
    },
    {
        "categoryCode": "6",
        "categoryLabel": "Home care",
        "groupName": "home_cate",
        "fieldAlias": "home_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_home1-home_cate",
            "home_cate",
        ],
        "knownLabels": {
            1: "Toilat",
            2: "Bleaches",
        },
    },
    {
        "categoryCode": "7",
        "categoryLabel": "Non-alcoholic beverages",
        "groupName": "nonal_cate",
        "fieldAlias": "nonal_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_nonal1-nonal_cate",
            "nonal_cate",
        ],
        "knownLabels": {
            1: "Csd",
            2: "Frtj",
            3: "Yogdrink",
            4: "Engdrink",
            5: "Flavomilk",
            6: "Water",
            7: "Maltdrink",
        },
    },
    {
        "categoryCode": "8",
        "categoryLabel": "Nutraceuticals",
        "groupName": "nutra_cate",
        "fieldAlias": "nutra_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_nutra1-nutra_cate",
            "nutra_cate",
        ],
        "knownLabels": {
            1: "Multivitamin",
        },
    },
    {
        "categoryCode": "9",
        "categoryLabel": "Packaged snacks",
        "groupName": "pckg_cate",
        "fieldAlias": "pckg_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_pckg1-pckg_cate",
            "pckg_cate",
        ],
        "knownLabels": {
            1: "Biscuit",
            2: "Chinchin",
            3: "Cookie",
            4: "Chip",
            5: "Sausage",
        },
    },
    {
        "categoryCode": "10",
        "categoryLabel": "Personal care",
        "groupName": "perso_cate",
        "fieldAlias": "perso_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_perso1-perso_cate",
            "perso_cate",
        ],
        "knownLabels": {
            1: "Toothpa",
            2: "Toothbr",
        },
    },
    {
        "categoryCode": "11",
        "categoryLabel": "Alcoholic beverages",
        "groupName": "alcoholic_cate",
        "fieldAlias": "alcoholic_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_alcoholic1-alcoholic_cate",
            "alcoholic_cate",
        ],
        "knownLabels": {
            1: "Beer",
            2: "Rtd",
            3: "Alcobrand",
            4: "Alcogin",
            5: "Alcoliquer",
            6: "Rum",
            7: "Schnapps",
            8: "Vodka",
            9: "Whisky",
        },
    },
    {
        "categoryCode": "12",
        "categoryLabel": "Tobacco",
        "groupName": "tobacco_cate",
        "fieldAlias": "tobacco_cate_raw",
        "sourceColumns": [
            "g_termination_1-g_termination_3-g_termination_4-grp_cigaretts1-tobacco_cate",
            "tobacco_cate",
        ],
        "knownLabels": {
            1: "Cigarettes",
        },
    },
]


def normalize_admin_value(value: Any) -> str:
    if value is None:
        return ""

    normalized = str(value).strip().lower().replace("&", "and")
    normalized = re.sub(r"[^a-z0-9]+", "", normalized)

    aliases = {
        "fct": "federalcapitalterritory",
        "federalcapitalterritoryabuja": "federalcapitalterritory",
        "municipalareacouncil": "amac",
        "portharcourt": "portharcourtcity",
        "ifakoijaye": "ifakoijaiye",
    }

    return aliases.get(normalized, normalized)


def build_ward_key(state_name: Any, lga_name: Any, ward_name: Any) -> str:
    return "::".join(
        [
            normalize_admin_value(state_name),
            normalize_admin_value(lga_name),
            normalize_admin_value(ward_name),
        ]
    )


def build_lga_key(state_name: Any, lga_name: Any) -> str:
    return "::".join(
        [
            normalize_admin_value(state_name),
            normalize_admin_value(lga_name),
        ]
    )


def build_dataset_label(source_path: Path, index: int) -> str:
    label = source_path.stem.replace("_", " ").replace("-", " ").strip()
    return label or f"Dataset {index}"


def build_dataset_id(source_path: Path, used_ids: set[str]) -> str:
    base_id = re.sub(r"[^a-z0-9]+", "-", source_path.stem.lower()).strip("-") or "dataset"
    dataset_id = base_id
    suffix = 2

    while dataset_id in used_ids:
        dataset_id = f"{base_id}-{suffix}"
        suffix += 1

    used_ids.add(dataset_id)
    return dataset_id


def discover_duckdb_sources() -> list[Path]:
    return sorted(
        [path for path in DATA_DIR.glob("*.duckdb") if path.is_file()],
        key=lambda path: (path.name.lower() != "census.duckdb", path.name.lower()),
    )


def build_dataset_output_paths(dataset_id: str) -> dict[str, Path]:
    dataset_dir = PUBLIC_DATA_DIR / dataset_id
    return {name: dataset_dir / filename for name, filename in OUTPUT_FILENAMES.items()}


def build_dataset_public_paths(dataset_id: str) -> dict[str, str]:
    return {
        name: f"/data/{dataset_id}/{filename}"
        for name, filename in OUTPUT_FILENAMES.items()
    }


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def resolve_observation_source_table(connection: duckdb.DuckDBPyConnection) -> str:
    table_names = [row[0] for row in connection.execute("show tables").fetchall()]
    generated_tables = {
        "gps_events_clean",
        "gps_events_deduped",
        "ward_coverage_summary",
        "runtime_metadata",
    }
    candidate_tables = [name for name in table_names if name not in generated_tables]

    if "census_table" in candidate_tables:
        return "census_table"

    if len(candidate_tables) == 1:
        return candidate_tables[0]

    preferred_candidates = [
        name for name in candidate_tables if name.lower().endswith("_table")
    ]
    if len(preferred_candidates) == 1:
        return preferred_candidates[0]

    raise ValueError(
        "Unable to determine source observation table. "
        f"Available tables: {', '.join(table_names) or 'none'}"
    )


def get_table_columns(
    connection: duckdb.DuckDBPyConnection, table_name: str
) -> set[str]:
    return {
        row[0]
        for row in connection.execute(
            f"describe {quote_identifier(table_name)}"
        ).fetchall()
    }


def sql_trimmed_text(expression: str) -> str:
    return f"trim(cast({expression} as varchar))"


def sql_nullif_trimmed_text(expression: str) -> str:
    return f"nullif({sql_trimmed_text(expression)}, '')"


def sql_flag_is_true(expression: str) -> str:
    return (
        "coalesce("
        f"try_cast({expression} as double), "
        f"case when lower({sql_trimmed_text(expression)}) in ('1', 'true', 'yes', 'y') then 1.0 else 0.0 end, "
        "0.0"
        ") > 0"
    )


def first_available_column(table_columns: set[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in table_columns:
            return candidate
    return None


def build_source_rows_query(table_name: str, table_columns: set[str]) -> str:
    quoted_table_name = quote_identifier(table_name)
    def text_expr(column_name: str | None, default: str = "null") -> str:
        if column_name is None:
            return default
        return sql_nullif_trimmed_text(quote_identifier(column_name))

    def raw_expr(column_name: str | None, default: str = "null") -> str:
        if column_name is None:
            return default
        return quote_identifier(column_name)

    record_id_column = first_available_column(
        table_columns, "meta-instanceID", "meta_instanceID", "KEY", "Key", "id"
    )
    submission_column = first_available_column(table_columns, "SubmissionDate", "Today date", "todaydate")
    survey_date_column = first_available_column(table_columns, "todaydate", "Today date")
    state_column = first_available_column(table_columns, "g_select-statename", "statename", "State")
    lga_column = first_available_column(table_columns, "g_select-lganame", "lganame", "LGA")
    ward_name_column = first_available_column(table_columns, "g_select-wardname", "wardname", "Ward")
    ward_code_column = first_available_column(table_columns, "g_select-wardcode", "wardcode", "Ward Code")
    collector_column = first_available_column(
        table_columns, "g_info-enumarator", "enumarator", "SubmitterName", "Submitter Name"
    )
    submitter_name_column = first_available_column(
        table_columns, "SubmitterName", "Submitter Name"
    )
    device_id_column = first_available_column(table_columns, "mydeviceid", "DeviceID")
    business_name_primary_column = first_available_column(
        table_columns, "g_termination_1-grp_store2-outname", "outname"
    )
    outlet_type_label_column = first_available_column(
        table_columns, "g_termination_1-grp_outlet-value_outtype", "value_outtype", "Outlet Type"
    )
    outlet_type_value_column = first_available_column(
        table_columns, "g_termination_1-grp_outlet-outtype", "outtype"
    )
    product_codes_column = first_available_column(
        table_columns, "g_termination_1-g_termination_3-grp_product1-prdlist", "prdlist"
    )
    channel_column = first_available_column(
        table_columns, "g_termination_1-g_termination_3-g_termination_4-grp_storeser-sertype", "sertype"
    )
    interview_status_code_column = first_available_column(
        table_columns, "g_termination_1-grp_interview-interstatus", "interstatus"
    )
    interview_status_label_column = first_available_column(
        table_columns, "g_termination_1-grp_interview-value_interstatus", "value_interstatus", "Status"
    )
    review_state_column = first_available_column(table_columns, "review_state", "ReviewState")
    latitude_column = first_available_column(
        table_columns, "latitude", "g_gps-gps-Latitude", "g_gps_gps_Latitude", "Latitude"
    )
    longitude_column = first_available_column(
        table_columns, "longitude", "g_gps-gps-Longitude", "g_gps_gps_Longitude", "Longitude"
    )
    accuracy_column = first_available_column(
        table_columns, "accuracy", "g_gps-gps-Accuracy", "g_gps_gps_Accuracy"
    )

    phase_product_category_candidates = [
        ("1", ["g_termination_1-g_termination_3-grp_product1-baby_food", "baby_food", "Baby food"]),
        ("2", ["g_termination_1-g_termination_3-grp_product1-confectionery", "confectionery", "Confectionery"]),
        ("3", ["g_termination_1-g_termination_3-grp_product1-food", "food", "Food"]),
        ("4", ["g_termination_1-g_termination_3-grp_product1-hair_care", "hair_care", "Hair care"]),
        ("5", ["g_termination_1-g_termination_3-grp_product1-hair_extensions", "hair_extensions", "Hair extensions"]),
        ("6", ["g_termination_1-g_termination_3-grp_product1-home_care", "home_care", "Home care"]),
        ("7", ["g_termination_1-g_termination_3-grp_product1-nonalcoholic_beverages", "nonalcoholic_beverages", "nonalcoholic_beverages"]),
        ("8", ["g_termination_1-g_termination_3-grp_product1-nutraceuticals", "nutraceuticals", "nutraceuticals"]),
        ("9", ["g_termination_1-g_termination_3-grp_product1-packaged_snacks", "packaged_snacks", "packaged_snacks"]),
        ("10", ["g_termination_1-g_termination_3-grp_product1-personal_care", "personal_care", "personal_care"]),
        ("11", ["g_termination_1-g_termination_3-grp_product1-alcoholic_beverages", "alcoholic_beverages", "alcoholic_beverages"]),
        ("12", ["g_termination_1-g_termination_3-grp_product1-tobacco", "tobacco", "Tobacco"]),
    ]
    product_category_case_expressions = [
        f"case when {sql_flag_is_true(quote_identifier(column_name))} then '{code}' end"
        for code, candidates in phase_product_category_candidates
        for column_name in [first_available_column(table_columns, *candidates)]
        if column_name is not None
    ]
    subcategory_select_expressions = [
        (
            definition["fieldAlias"],
            text_expr(first_available_column(table_columns, *definition["sourceColumns"]), "''"),
        )
        for definition in OUTLET_SUBCATEGORY_GROUPS
    ]

    generic_columns = {
        submission_column,
        state_column,
        lga_column,
        ward_name_column,
        latitude_column,
        longitude_column,
    }
    submitter_filter_sql = (
        "where lower(coalesce(trim(cast("
        f"{quote_identifier(submitter_name_column)}"
        " as varchar)), '')) <> 'weblink'"
        if submitter_name_column is not None
        else ""
    )
    if all(column_name is not None for column_name in generic_columns):
        if product_codes_column is not None:
            product_category_expr = text_expr(product_codes_column, "''")
        elif product_category_case_expressions:
            product_category_expr = (
                "trim(\n"
                "              concat_ws(\n"
                "                ' ',\n"
                f"                {', '.join(product_category_case_expressions)}\n"
                "              )\n"
                "            )"
            )
        else:
            product_category_expr = "''"

        return f"""
          select
            coalesce(
              {text_expr(record_id_column)},
              'row-' || row_number() over ()
            ) as record_id,
            coalesce(
              {raw_expr(first_available_column(table_columns, 'endtime'))},
              {raw_expr(submission_column)},
              {raw_expr(first_available_column(table_columns, 'starttime'))}
            ) as event_ts,
            {raw_expr(submission_column)} as submission_ts,
            {raw_expr(first_available_column(table_columns, 'starttime'))} as starttime,
            {raw_expr(first_available_column(table_columns, 'endtime'))} as endtime,
            {raw_expr(survey_date_column, raw_expr(submission_column))} as survey_date,
            {text_expr(state_column)} as state_name,
            {text_expr(lga_column)} as lga_name,
            cast({raw_expr(ward_code_column)} as varchar) as ward_code,
            {text_expr(ward_name_column)} as ward_name,
            {text_expr(collector_column)} as collector_name,
            {text_expr(device_id_column, "''")} as device_id,
            coalesce(
              {text_expr(business_name_primary_column)},
              {text_expr(first_available_column(table_columns, 'meta-entity-label'))},
              {text_expr(first_available_column(table_columns, 'meta-instanceName', 'meta_instanceName'))},
              {text_expr(first_available_column(table_columns, 'mngname'))},
              {text_expr(record_id_column)},
              'Unnamed outlet'
            ) as business_name,
            coalesce(
              {text_expr(outlet_type_label_column)},
              {text_expr(outlet_type_value_column)},
              'Unknown'
            ) as outlet_type,
            {product_category_expr} as product_category_codes_raw,
            coalesce(
              {text_expr(channel_column)},
              'Unknown'
            ) as channel_type,
            {text_expr(interview_status_code_column)} as interview_status_code,
            coalesce(
              {text_expr(interview_status_label_column)},
              'Unknown'
            ) as interview_status_label,
            {text_expr(review_state_column, "''")} as review_state,
            cast({raw_expr(latitude_column)} as varchar) as latitude_raw,
            cast({raw_expr(longitude_column)} as varchar) as longitude_raw,
            cast({raw_expr(accuracy_column)} as varchar) as accuracy_raw,
            {',\n            '.join(f'{expression} as {alias}' for alias, expression in subcategory_select_expressions)}
          from {quoted_table_name}
          {submitter_filter_sql}
        """

    raise ValueError(
        "Unsupported observation schema. "
        f"Table '{table_name}' columns: {', '.join(sorted(table_columns))}"
    )


def geometry_bounds(geometry: dict[str, Any]) -> tuple[float, float, float, float]:
    longitudes: list[float] = []
    latitudes: list[float] = []

    geometry_type = geometry["type"]
    polygons = (
        geometry["coordinates"]
        if geometry_type == "MultiPolygon"
        else [geometry["coordinates"]]
    )

    for polygon in polygons:
        for ring in polygon:
            for longitude, latitude in ring:
                longitudes.append(longitude)
                latitudes.append(latitude)

    return min(longitudes), min(latitudes), max(longitudes), max(latitudes)


def project_coordinate(
    longitude: float, latitude: float, reference_latitude: float
) -> tuple[float, float]:
    x_value = (
        math.radians(longitude)
        * EARTH_RADIUS_METERS
        * math.cos(math.radians(reference_latitude))
    )
    y_value = math.radians(latitude) * EARTH_RADIUS_METERS
    return x_value, y_value


def project_coordinates(coordinates: Any, reference_latitude: float) -> Any:
    if isinstance(coordinates[0], (int, float)):
        longitude, latitude = coordinates
        return project_coordinate(float(longitude), float(latitude), reference_latitude)

    return [project_coordinates(child, reference_latitude) for child in coordinates]


def coverage_status_for_percent(percent: float) -> str:
    if percent >= WELL_COVERED_PERCENT:
        return "well_covered"

    if percent >= NEAR_TARGET_PERCENT:
        return "near_target"

    return "under_covered"


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    return value


def effective_tolerance_meters(accuracy_meters: Any) -> float:
    try:
        numeric_accuracy = float(accuracy_meters)
    except (TypeError, ValueError):
        numeric_accuracy = DEFAULT_GPS_TOLERANCE_METERS

    if math.isnan(numeric_accuracy) or numeric_accuracy <= 0:
        numeric_accuracy = DEFAULT_GPS_TOLERANCE_METERS

    return max(
        float(DEFAULT_GPS_TOLERANCE_METERS),
        min(float(MAX_GPS_TOLERANCE_METERS), numeric_accuracy),
    )


def parse_multi_select_codes(value: Any) -> list[str]:
    if value is None:
        return []

    raw_text = str(value).strip()
    if not raw_text:
        return []

    seen_codes: set[str] = set()
    parsed_codes: list[str] = []

    for code in re.split(r"\s+", raw_text):
        normalized_code = code.strip()
        if not normalized_code or normalized_code in seen_codes:
            continue

        seen_codes.add(normalized_code)
        parsed_codes.append(normalized_code)

    return parsed_codes


def broad_category_labels(value: Any) -> list[str]:
    return [PRODUCT_CATEGORY_LABELS.get(code, code) for code in parse_multi_select_codes(value)]


def sample_evenly(items: list[Any], max_items: int) -> list[Any]:
    if max_items <= 0:
        return []

    if len(items) <= max_items:
        return items

    if max_items == 1:
        return [items[0]]

    step = (len(items) - 1) / (max_items - 1)
    return [items[round(index * step)] for index in range(max_items)]


def assess_ward_coverage(
    geometry: dict[str, Any], scored_points: list[dict[str, float]]
) -> dict[str, Any]:
    if not scored_points:
        return {
            "coveragePercent": 0.0,
            "coveredCells": 0,
            "totalCells": 0,
            "coveredAreaM2": 0.0,
            "uncoveredAreaM2": 0.0,
            "averageAccuracyM": None,
            "coverageStatus": "no_gps",
        }

    west, south, east, north = geometry_bounds(geometry)
    reference_latitude = (south + north) / 2
    projected_geometry = {
        "type": geometry["type"],
        "coordinates": project_coordinates(geometry["coordinates"], reference_latitude),
    }
    ward_shape = shape(projected_geometry).buffer(0)

    if ward_shape.is_empty or ward_shape.area <= 0:
        return {
            "coveragePercent": 0.0,
            "coveredCells": 0,
            "totalCells": 0,
            "coveredAreaM2": 0.0,
            "uncoveredAreaM2": 0.0,
            "averageAccuracyM": None,
            "coverageStatus": "under_covered",
        }

    point_buffers = []

    for point in scored_points:
        latitude = float(point["latitude"])
        longitude = float(point["longitude"])
        tolerance = float(point["effectiveToleranceM"])
        projected_longitude, projected_latitude = project_coordinate(
            longitude,
            latitude,
            reference_latitude,
        )
        point_buffers.append(
            Point(projected_longitude, projected_latitude).buffer(
                tolerance,
                quad_segs=4,
            )
        )

    covered_geometry = ward_shape.intersection(unary_union(point_buffers))

    ward_area = ward_shape.area
    covered_area = max(0.0, covered_geometry.area)
    uncovered_area = max(0.0, ward_area - covered_area)
    coverage_percent = 0.0 if ward_area == 0 else (covered_area / ward_area) * 100
    cell_area = GRID_CELL_METERS * GRID_CELL_METERS
    total_cells = max(1, round(ward_area / cell_area))
    covered_cells = min(total_cells, round(covered_area / cell_area))
    average_accuracy = sum(point["gpsAccuracy"] for point in scored_points) / len(scored_points)

    return {
        "coveragePercent": round(coverage_percent, 2),
        "coveredCells": covered_cells,
        "totalCells": total_cells,
        "coveredAreaM2": round(covered_area, 2),
        "uncoveredAreaM2": round(uncovered_area, 2),
        "averageAccuracyM": round(average_accuracy, 2),
        "coverageStatus": coverage_status_for_percent(coverage_percent),
    }


def prepare_duckdb_observation_tables(connection: duckdb.DuckDBPyConnection) -> None:
    source_table_name = resolve_observation_source_table(connection)
    table_columns = get_table_columns(connection, source_table_name)
    source_rows_query = build_source_rows_query(source_table_name, table_columns)
    subcategory_columns_sql = ",\n          ".join(
        definition["fieldAlias"] for definition in OUTLET_SUBCATEGORY_GROUPS
    )

    connection.execute("drop table if exists gps_events_clean")
    connection.execute(
        f"""
        create table gps_events_clean as
        with source_rows as (
          {source_rows_query}
        )
        select
          record_id,
          coalesce(
            try_cast(event_ts as timestamp),
            cast(try_cast(event_ts as date) as timestamp)
          ) as event_ts,
          coalesce(
            try_cast(submission_ts as timestamp),
            cast(try_cast(submission_ts as date) as timestamp)
          ) as submission_ts,
          coalesce(
            try_cast(starttime as timestamp),
            cast(try_cast(starttime as date) as timestamp)
          ) as starttime,
          coalesce(
            try_cast(endtime as timestamp),
            cast(try_cast(endtime as date) as timestamp)
          ) as endtime,
          try_cast(survey_date as date) as survey_date,
          state_name,
          lga_name,
          ward_code,
          ward_name,
          collector_name,
          device_id,
          business_name,
          outlet_type,
          product_category_codes_raw,
          {subcategory_columns_sql},
          channel_type,
          coalesce(
            cast(cast(try_cast(interview_status_code as double) as integer) as varchar),
            interview_status_code
          ) as interview_status_code,
          interview_status_label,
          review_state,
          try_cast(latitude_raw as double) as latitude,
          try_cast(longitude_raw as double) as longitude,
          try_cast(accuracy_raw as double) as gps_accuracy,
          case
            when coalesce(
              cast(cast(try_cast(interview_status_code as double) as integer) as varchar),
              interview_status_code
            ) = '1' then 'Completed'
            when coalesce(
              cast(cast(try_cast(interview_status_code as double) as integer) as varchar),
              interview_status_code
            ) = '2' then 'Observation'
            when coalesce(
              cast(cast(try_cast(interview_status_code as double) as integer) as varchar),
              interview_status_code
            ) = '3' then 'Restricted'
            else interview_status_label
          end as visit_status,
          case
            when latitude_raw is null or longitude_raw is null then 'missing'
            when try_cast(latitude_raw as double) is null or try_cast(longitude_raw as double) is null then 'invalid'
            when try_cast(latitude_raw as double) not between 2 and 16 then 'outside_nigeria'
            when try_cast(longitude_raw as double) not between 2 and 16 then 'outside_nigeria'
            when try_cast(accuracy_raw as double) is null then 'unknown'
            when try_cast(accuracy_raw as double) <= 10 then 'good'
            when try_cast(accuracy_raw as double) <= 25 then 'fair'
            else 'poor'
          end as gps_quality_flag,
          greatest(
            10.0,
            least(
              coalesce(try_cast(accuracy_raw as double), 10.0),
              25.0
            )
          ) as effective_tolerance_m
        from source_rows
        where state_name is not null
          and lga_name is not null
          and ward_name is not null
        """
    )

    connection.execute("drop table if exists gps_events_deduped")
    connection.execute(
        """
        create table gps_events_deduped as
        with ranked_events as (
          select
            *,
            count(*) over (
              partition by
                lower(coalesce(collector_name, '')),
                lower(coalesce(state_name, '')),
                lower(coalesce(lga_name, '')),
                lower(coalesce(ward_name, '')),
                lower(coalesce(business_name, '')),
                round(latitude, 6),
                round(longitude, 6),
                date_trunc('day', event_ts)
            ) as duplicate_group_size,
            row_number() over (
              partition by
                lower(coalesce(collector_name, '')),
                lower(coalesce(state_name, '')),
                lower(coalesce(lga_name, '')),
                lower(coalesce(ward_name, '')),
                lower(coalesce(business_name, '')),
                round(latitude, 6),
                round(longitude, 6),
                date_trunc('day', event_ts)
              order by event_ts asc, record_id asc
            ) as duplicate_rank
          from gps_events_clean
          where latitude is not null
            and longitude is not null
            and gps_quality_flag not in ('missing', 'invalid', 'outside_nigeria')
        )
        select
          *
        from ranked_events
        where duplicate_rank = 1
        """
    )


def build_observation_feature(row: tuple[Any, ...]) -> dict[str, Any]:
    (
        record_id,
        event_ts,
        submission_ts,
        starttime,
        endtime,
        survey_date,
        state_name,
        lga_name,
        ward_code,
        ward_name,
        collector_name,
        device_id,
        business_name,
        outlet_type,
        product_category_codes_raw,
        channel_type,
        visit_status,
        interview_status_label,
        review_state,
        latitude,
        longitude,
        gps_accuracy,
        gps_quality_flag,
        effective_tolerance_m,
    ) = row

    return {
        "type": "Feature",
        "properties": {
            "id": record_id,
            "stateName": state_name,
            "lgaName": lga_name,
            "wardName": ward_name,
            "wardCode": ward_code,
            "wardKey": build_ward_key(state_name, lga_name, ward_name),
            "collectorName": collector_name or "Unknown collector",
            "deviceId": device_id or "",
            "status": visit_status or "Unknown",
            "statusDetail": interview_status_label or visit_status or "Unknown",
            "outletType": outlet_type or "Unknown",
            "productCategoryCodes": parse_multi_select_codes(product_category_codes_raw),
            "productCategories": broad_category_labels(product_category_codes_raw),
            "channelType": channel_type or "Unknown",
            "businessName": business_name or "Unnamed outlet",
            "preApproval": False,
            "gpsAccuracy": float(gps_accuracy or 0),
            "gpsQualityFlag": gps_quality_flag or "unknown",
            "effectiveToleranceM": float(
                effective_tolerance_m or DEFAULT_GPS_TOLERANCE_METERS
            ),
            "eventTs": json_safe(event_ts),
            "submissionTs": json_safe(submission_ts),
            "startTime": json_safe(starttime),
            "endTime": json_safe(endtime),
            "surveyDate": json_safe(survey_date),
            "reviewState": review_state or "",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [float(longitude), float(latitude)],
        },
    }


def load_frontend_observations(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    valid_filters = """
        latitude is not null
        and longitude is not null
        and gps_quality_flag not in ('missing', 'invalid', 'outside_nigeria')
    """
    ward_count = int(
        connection.execute(
            f"""
            select count(distinct concat_ws('::', coalesce(state_name, ''), coalesce(lga_name, ''), coalesce(ward_name, '')))
            from gps_events_clean
            where {valid_filters}
            """
        ).fetchone()[0]
        or 0
    )
    per_ward_cap = max(1, MAX_FRONTEND_OBSERVATIONS // max(1, ward_count))

    sampled_rows = connection.execute(
        f"""
        with ranked_rows as (
          select
            record_id,
            cast(event_ts as varchar) as event_ts,
            cast(submission_ts as varchar) as submission_ts,
            cast(starttime as varchar) as starttime,
            cast(endtime as varchar) as endtime,
            cast(survey_date as varchar) as survey_date,
            state_name,
            lga_name,
            ward_code,
            ward_name,
            collector_name,
            device_id,
            business_name,
            outlet_type,
            product_category_codes_raw,
            channel_type,
            visit_status,
            interview_status_label,
            review_state,
            latitude,
            longitude,
            gps_accuracy,
            gps_quality_flag,
            effective_tolerance_m,
            row_number() over (
              partition by state_name, lga_name, ward_name
              order by event_ts asc, record_id asc
            ) as ward_rank
          from gps_events_clean
          where {valid_filters}
        )
        select
          record_id,
          event_ts,
          submission_ts,
          starttime,
          endtime,
          survey_date,
          state_name,
          lga_name,
          ward_code,
          ward_name,
          collector_name,
          device_id,
          business_name,
          outlet_type,
          product_category_codes_raw,
          channel_type,
          visit_status,
          interview_status_label,
          review_state,
          latitude,
          longitude,
          gps_accuracy,
          gps_quality_flag,
          effective_tolerance_m
        from ranked_rows
        where ward_rank <= ?
        order by event_ts asc, record_id asc
        """,
        [per_ward_cap],
    ).fetchall()

    features = [build_observation_feature(row) for row in sampled_rows]
    if len(features) > MAX_FRONTEND_OBSERVATIONS:
        features = sample_evenly(features, MAX_FRONTEND_OBSERVATIONS)

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def load_observations(
    connection: duckdb.DuckDBPyConnection,
) -> tuple[
    dict[str, Any],
    Counter[str],
    Counter[str],
    dict[str, list[tuple[float, float]]],
    dict[str, list[dict[str, float]]],
    int,
]:
    raw_rows = connection.execute(
        """
        select
          record_id,
          cast(event_ts as varchar) as event_ts,
          cast(submission_ts as varchar) as submission_ts,
          cast(starttime as varchar) as starttime,
          cast(endtime as varchar) as endtime,
          cast(survey_date as varchar) as survey_date,
          state_name,
          lga_name,
          ward_code,
          ward_name,
          collector_name,
          device_id,
          business_name,
          outlet_type,
          product_category_codes_raw,
          channel_type,
          visit_status,
          interview_status_label,
          review_state,
          latitude,
          longitude,
          gps_accuracy,
          gps_quality_flag,
          effective_tolerance_m
        from gps_events_clean
        where latitude is not null
          and longitude is not null
          and gps_quality_flag not in ('missing', 'invalid', 'outside_nigeria')
        order by event_ts asc, record_id asc
        """
    ).fetchall()

    deduped_rows = connection.execute(
        """
        select
          record_id,
          state_name,
          lga_name,
          ward_name,
          latitude,
          longitude,
          gps_accuracy,
          effective_tolerance_m
        from gps_events_deduped
        order by event_ts asc, record_id asc
        """
    ).fetchall()

    features: list[dict[str, Any]] = []
    state_counts: Counter[str] = Counter()
    lga_counts: Counter[str] = Counter()
    raw_points_by_ward_key: dict[str, list[tuple[float, float]]] = defaultdict(list)
    scored_points_by_ward_key: dict[str, list[dict[str, float]]] = defaultdict(list)

    for row in raw_rows:
        (
            _record_id,
            _event_ts,
            _submission_ts,
            _starttime,
            _endtime,
            _survey_date,
            state_name,
            lga_name,
            _ward_code,
            ward_name,
            _collector_name,
            _device_id,
            _business_name,
            _outlet_type,
            _product_category_codes_raw,
            _channel_type,
            _visit_status,
            _interview_status_label,
            _review_state,
            latitude,
            longitude,
            _gps_accuracy,
            _gps_quality_flag,
            _effective_tolerance_m,
        ) = row

        ward_key = build_ward_key(state_name, lga_name, ward_name)
        lga_key = build_lga_key(state_name, lga_name)
        normalized_state = normalize_admin_value(state_name)

        features.append(build_observation_feature(row))

        state_counts[normalized_state] += 1
        lga_counts[lga_key] += 1
        raw_points_by_ward_key[ward_key].append((float(latitude), float(longitude)))

    for (
        _record_id,
        state_name,
        lga_name,
        ward_name,
        latitude,
        longitude,
        gps_accuracy,
        effective_tolerance_m,
    ) in deduped_rows:
        ward_key = build_ward_key(state_name, lga_name, ward_name)
        scored_points_by_ward_key[ward_key].append(
            {
                "latitude": float(latitude),
                "longitude": float(longitude),
                "gpsAccuracy": float(gps_accuracy or 0),
                "effectiveToleranceM": float(
                    effective_tolerance_m or DEFAULT_GPS_TOLERANCE_METERS
                ),
            }
        )

    frontend_features = features
    if len(features) > MAX_FRONTEND_OBSERVATIONS:
        features_by_ward_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for feature in features:
            features_by_ward_key[feature["properties"]["wardKey"]].append(feature)

        per_ward_cap = max(1, MAX_FRONTEND_OBSERVATIONS // max(1, len(features_by_ward_key)))
        frontend_features = []
        for ward_key in sorted(features_by_ward_key):
            frontend_features.extend(
                sample_evenly(features_by_ward_key[ward_key], per_ward_cap)
            )

    return (
        {
            "type": "FeatureCollection",
            "features": frontend_features,
        },
        state_counts,
        lga_counts,
        raw_points_by_ward_key,
        scored_points_by_ward_key,
        len(features),
    )


def transform_state_geojson(state_counts: Counter[str]) -> dict[str, Any]:
    with STATE_SOURCE_PATH.open("r", encoding="utf-8") as handle:
        geojson = json.load(handle)

    features: list[dict[str, Any]] = []
    observed_states = set(state_counts.keys())

    for feature in geojson["features"]:
        properties = feature["properties"]
        state_name = properties.get("statename")
        normalized_state = normalize_admin_value(state_name)

        features.append(
            {
                "type": "Feature",
                "properties": {
                    "stateName": state_name,
                    "stateCode": properties.get("statecode"),
                    "capitalCity": properties.get("capcity"),
                    "geoZone": properties.get("geozone"),
                    "hasObservations": normalized_state in observed_states,
                    "observationCount": state_counts.get(normalized_state, 0),
                },
                "geometry": feature["geometry"],
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def transform_lga_geojson(lga_counts: Counter[str]) -> dict[str, Any]:
    with LGA_SOURCE_PATH.open("r", encoding="utf-8") as handle:
        geojson = json.load(handle)

    features: list[dict[str, Any]] = []

    for index, feature in enumerate(geojson["features"], start=1):
        geometry = feature.get("geometry")
        if not geometry:
            continue

        properties = feature["properties"]
        lga_key = build_lga_key(properties.get("statename"), properties.get("lganame"))
        observation_count = lga_counts.get(lga_key, 0)

        features.append(
            {
                "type": "Feature",
                "properties": {
                    "id": properties.get("globalid")
                    or properties.get("uniq_id")
                    or f"lga-{index}",
                    "stateName": properties.get("statename"),
                    "stateCode": properties.get("statecode"),
                    "lgaName": properties.get("lganame"),
                    "lgaCode": properties.get("lgacode"),
                    "lgaKey": lga_key,
                    "hasObservations": observation_count > 0,
                    "observationCount": observation_count,
                },
                "geometry": geometry,
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def transform_ward_geojson(
    raw_points_by_ward_key: dict[str, list[tuple[float, float]]],
    scored_points_by_ward_key: dict[str, list[dict[str, float]]],
) -> tuple[dict[str, Any], list[tuple[Any, ...]]]:
    with WARD_SOURCE_PATH.open("r", encoding="utf-8") as handle:
        geojson = json.load(handle)

    features: list[dict[str, Any]] = []
    coverage_rows: list[tuple[Any, ...]] = []

    for index, feature in enumerate(geojson["features"], start=1):
        geometry = feature.get("geometry")
        if not geometry:
            continue

        properties = feature["properties"]
        ward_key = build_ward_key(
            properties.get("statename"),
            properties.get("lganame"),
            properties.get("wardname"),
        )
        raw_points = raw_points_by_ward_key.get(ward_key, [])
        scored_points = scored_points_by_ward_key.get(ward_key, [])
        coverage_summary = assess_ward_coverage(geometry, scored_points)

        ward_feature = {
            "type": "Feature",
            "properties": {
                "id": properties.get("globalid") or f"ward-{index}",
                "stateName": properties.get("statename"),
                "stateCode": properties.get("statecode"),
                "lgaName": properties.get("lganame"),
                "lgaCode": properties.get("lgacode"),
                "wardName": properties.get("wardname"),
                "wardCode": properties.get("wardcode"),
                "wardKey": ward_key,
                "urbanClass": properties.get("urban"),
                "hasObservations": len(raw_points) > 0,
                "observationCount": len(scored_points),
                "rawObservationCount": len(raw_points),
                **coverage_summary,
            },
            "geometry": geometry,
        }
        features.append(ward_feature)

        coverage_rows.append(
            (
                ward_key,
                properties.get("statename"),
                properties.get("lganame"),
                properties.get("wardname"),
                len(raw_points),
                len(scored_points),
                coverage_summary["coveragePercent"],
                coverage_summary["coveredCells"],
                coverage_summary["totalCells"],
                coverage_summary["coveredAreaM2"],
                coverage_summary["uncoveredAreaM2"],
                coverage_summary["averageAccuracyM"],
                coverage_summary["coverageStatus"],
            )
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }, coverage_rows


def load_ward_coverage_lookup(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
        """
        select
          ward_key,
          raw_observation_count,
          scored_observation_count,
          coverage_percent,
          covered_cells,
          total_cells,
          covered_area_m2,
          uncovered_area_m2,
          average_accuracy_m,
          coverage_status
        from ward_coverage_summary
        """
    ).fetchall()

    return {
        ward_key: {
            "rawObservationCount": int(raw_observation_count or 0),
            "observationCount": int(scored_observation_count or 0),
            "coveragePercent": float(coverage_percent or 0),
            "coveredCells": int(covered_cells or 0),
            "totalCells": int(total_cells or 0),
            "coveredAreaM2": float(covered_area_m2 or 0),
            "uncoveredAreaM2": float(uncovered_area_m2 or 0),
            "averageAccuracyM": (
                None
                if average_accuracy_m is None
                else float(average_accuracy_m)
            ),
            "coverageStatus": coverage_status or "no_gps",
        }
        for (
            ward_key,
            raw_observation_count,
            scored_observation_count,
            coverage_percent,
            covered_cells,
            total_cells,
            covered_area_m2,
            uncovered_area_m2,
            average_accuracy_m,
            coverage_status,
        ) in rows
    }


def load_observation_admin_counts(
    connection: duckdb.DuckDBPyConnection,
) -> tuple[Counter[str], Counter[str], int]:
    rows = connection.execute(
        """
        select
          state_name,
          lga_name,
          count(*) as observation_count
        from gps_events_clean
        where latitude is not null
          and longitude is not null
          and gps_quality_flag not in ('missing', 'invalid', 'outside_nigeria')
        group by 1, 2
        """
    ).fetchall()

    state_counts: Counter[str] = Counter()
    lga_counts: Counter[str] = Counter()
    valid_gps_count = 0

    for state_name, lga_name, observation_count in rows:
        count = int(observation_count or 0)
        if count <= 0:
            continue

        valid_gps_count += count
        normalized_state = normalize_admin_value(state_name)
        if normalized_state:
            state_counts[normalized_state] += count

        lga_key = build_lga_key(state_name, lga_name)
        if lga_key:
            lga_counts[lga_key] += count

    return state_counts, lga_counts, valid_gps_count


def load_visited_area_counts(connection: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    rows = connection.execute(
        """
        select ward_key, state_name, lga_name
        from ward_coverage_summary
        where raw_observation_count > 0
        """
    ).fetchall()

    visited_lga_keys = {
        build_lga_key(state_name, lga_name)
        for _, state_name, lga_name in rows
        if state_name and lga_name
    }
    visited_lga_keys.discard("")
    return len(rows), len(visited_lga_keys)


def transform_ward_geojson_from_lookup(
    coverage_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    with WARD_SOURCE_PATH.open("r", encoding="utf-8") as handle:
        geojson = json.load(handle)

    features: list[dict[str, Any]] = []

    for index, feature in enumerate(geojson["features"], start=1):
        geometry = feature.get("geometry")
        if not geometry:
            continue

        properties = feature["properties"]
        ward_key = build_ward_key(
            properties.get("statename"),
            properties.get("lganame"),
            properties.get("wardname"),
        )
        coverage_summary = coverage_lookup.get(
            ward_key,
            {
                "rawObservationCount": 0,
                "observationCount": 0,
                "coveragePercent": 0.0,
                "coveredCells": 0,
                "totalCells": 0,
                "coveredAreaM2": 0.0,
                "uncoveredAreaM2": 0.0,
                "averageAccuracyM": None,
                "coverageStatus": "no_gps",
            },
        )

        features.append(
            {
                "type": "Feature",
                "properties": {
                    "id": properties.get("globalid") or f"ward-{index}",
                    "stateName": properties.get("statename"),
                    "stateCode": properties.get("statecode"),
                    "lgaName": properties.get("lganame"),
                    "lgaCode": properties.get("lgacode"),
                    "wardName": properties.get("wardname"),
                    "wardCode": properties.get("wardcode"),
                    "wardKey": ward_key,
                    "urbanClass": properties.get("urban"),
                    "hasObservations": coverage_summary["rawObservationCount"] > 0,
                    **coverage_summary,
                },
                "geometry": geometry,
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }


def runtime_tables_are_current(connection: duckdb.DuckDBPyConnection) -> bool:
    table_names = {row[0] for row in connection.execute("show tables").fetchall()}
    required_tables = {
        "gps_events_clean",
        "gps_events_deduped",
        "ward_coverage_summary",
        "runtime_metadata",
    }

    if not required_tables.issubset(table_names):
        return False

    version_row = connection.execute(
        """
        select schema_version
        from runtime_metadata
        order by generated_at desc
        limit 1
        """
    ).fetchone()
    if int(version_row[0] or 0) != RUNTIME_SCHEMA_VERSION:
        return False

    stale_status_mapping = connection.execute(
        """
        select 1
        from gps_events_clean
        where (
            coalesce(
              cast(cast(try_cast(interview_status_code as double) as integer) as varchar),
              interview_status_code
            ) = '1' and visit_status <> 'Completed'
        ) or (
            coalesce(
              cast(cast(try_cast(interview_status_code as double) as integer) as varchar),
              interview_status_code
            ) = '2' and visit_status <> 'Observation'
        ) or (
            coalesce(
              cast(cast(try_cast(interview_status_code as double) as integer) as varchar),
              interview_status_code
            ) = '3' and visit_status <> 'Restricted'
        )
        limit 1
        """
    ).fetchone()
    return stale_status_mapping is None


def ensure_runtime_dataset_tables(
    connection: duckdb.DuckDBPyConnection, force_rebuild: bool = False
) -> None:
    if not force_rebuild and runtime_tables_are_current(connection):
        return

    prepare_duckdb_observation_tables(connection)
    (
        _observations_geojson,
        _state_counts,
        _lga_counts,
        raw_points_by_ward_key,
        scored_points_by_ward_key,
        _valid_gps_count,
    ) = load_observations(connection)
    _ward_geojson, coverage_rows = transform_ward_geojson(
        raw_points_by_ward_key,
        scored_points_by_ward_key,
    )
    persist_ward_coverage_summary(connection, coverage_rows)
    persist_runtime_metadata(connection)


def persist_ward_coverage_summary(
    connection: duckdb.DuckDBPyConnection, coverage_rows: list[tuple[Any, ...]]
) -> None:
    connection.execute("drop table if exists ward_coverage_summary")
    connection.execute(
        """
        create table ward_coverage_summary (
          ward_key varchar,
          state_name varchar,
          lga_name varchar,
          ward_name varchar,
          raw_observation_count integer,
          scored_observation_count integer,
          coverage_percent double,
          covered_cells integer,
          total_cells integer,
          covered_area_m2 double,
          uncovered_area_m2 double,
          average_accuracy_m double,
          coverage_status varchar
        )
        """
    )
    connection.executemany(
        """
        insert into ward_coverage_summary values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        coverage_rows,
    )


def persist_runtime_metadata(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("drop table if exists runtime_metadata")
    connection.execute(
        """
        create table runtime_metadata (
          schema_version integer,
          generated_at timestamp
        )
        """
    )
    connection.execute(
        """
        insert into runtime_metadata values (?, current_timestamp)
        """,
        [RUNTIME_SCHEMA_VERSION],
    )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"))


def build_dashboard_summary(
    connection: duckdb.DuckDBPyConnection,
    observations_geojson: dict[str, Any],
    valid_gps_count: int,
) -> dict[str, Any]:
    total_achieved, completed_count, observation_count = connection.execute(
        """
        select
          count(*) as total_achieved,
          sum(case when visit_status = 'Completed' then 1 else 0 end) as completed_count,
          sum(case when visit_status = 'Observation' then 1 else 0 end) as observation_count
        from gps_events_clean
        """
    ).fetchone()
    latest_data_timestamp = connection.execute(
        """
        select max(cast(event_ts as varchar))
        from gps_events_clean
        """
    ).fetchone()[0]

    visited_ward_keys = {
        feature["properties"]["wardKey"]
        for feature in observations_geojson["features"]
        if feature["properties"].get("wardKey")
    }
    visited_lga_keys = {
        build_lga_key(
            feature["properties"].get("stateName"),
            feature["properties"].get("lgaName"),
        )
        for feature in observations_geojson["features"]
        if feature["properties"].get("stateName") and feature["properties"].get("lgaName")
    }

    return {
        "totalAchieved": int(total_achieved or 0),
        "completedCount": int(completed_count or 0),
        "observationCount": int(observation_count or 0),
        "wardsVisitedCount": len(visited_ward_keys),
        "lgasVisitedCount": len(visited_lga_keys),
        "validGpsCount": int(valid_gps_count),
        "frontendObservationCount": len(observations_geojson["features"]),
        "observationsSampled": len(observations_geojson["features"]) != int(valid_gps_count),
        "generatedAt": latest_data_timestamp or "",
    }


def build_runtime_dashboard_summary(
    connection: duckdb.DuckDBPyConnection,
    valid_gps_count: int,
) -> dict[str, Any]:
    total_achieved, completed_count, observation_count = connection.execute(
        """
        select
          count(*) as total_achieved,
          sum(case when visit_status = 'Completed' then 1 else 0 end) as completed_count,
          sum(case when visit_status = 'Observation' then 1 else 0 end) as observation_count
        from gps_events_clean
        """
    ).fetchone()
    latest_data_timestamp = connection.execute(
        """
        select max(cast(event_ts as varchar))
        from gps_events_clean
        """
    ).fetchone()[0]
    wards_visited_count, lgas_visited_count = load_visited_area_counts(connection)

    return {
        "totalAchieved": int(total_achieved or 0),
        "completedCount": int(completed_count or 0),
        "observationCount": int(observation_count or 0),
        "wardsVisitedCount": int(wards_visited_count),
        "lgasVisitedCount": int(lgas_visited_count),
        "validGpsCount": int(valid_gps_count),
        "generatedAt": latest_data_timestamp or "",
    }


def copy_dataset_to_legacy_paths(output_paths: dict[str, Path]) -> None:
    for name, source_path in output_paths.items():
        legacy_path = LEGACY_OUTPUT_PATHS[name]
        legacy_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, legacy_path)


def prepare_dataset_bundle(
    source_path: Path,
    dataset_id: str,
    dataset_label: str,
    output_paths: dict[str, Path],
) -> dict[str, Any]:
    connection = duckdb.connect(str(source_path), read_only=False)
    try:
        prepare_duckdb_observation_tables(connection)
        (
            observations_geojson,
            state_counts,
            lga_counts,
            raw_points_by_ward_key,
            scored_points_by_ward_key,
            valid_gps_count,
        ) = load_observations(connection)
        state_geojson = transform_state_geojson(state_counts)
        lga_geojson = transform_lga_geojson(lga_counts)
        ward_geojson, coverage_rows = transform_ward_geojson(
            raw_points_by_ward_key,
            scored_points_by_ward_key,
        )
        persist_ward_coverage_summary(connection, coverage_rows)
        dashboard_summary = build_dashboard_summary(
            connection,
            observations_geojson,
            valid_gps_count,
        )

        write_json(output_paths["states"], state_geojson)
        write_json(output_paths["lgas"], lga_geojson)
        write_json(output_paths["wards"], ward_geojson)
        write_json(output_paths["observations"], observations_geojson)
        write_json(output_paths["summary"], dashboard_summary)

        clean_count = connection.execute(
            "select count(*) from gps_events_clean where latitude is not null and longitude is not null and gps_quality_flag not in ('missing', 'invalid', 'outside_nigeria')"
        ).fetchone()[0]
        deduped_count = connection.execute(
            "select count(*) from gps_events_deduped"
        ).fetchone()[0]
        coverage_status_rows = connection.execute(
            """
            select coverage_status, count(*)
            from ward_coverage_summary
            group by 1
            order by 2 desc
            """
        ).fetchall()

        print(f"Dataset: {dataset_label} ({source_path.name})")
        print(f"Wrote {output_paths['states'].relative_to(ROOT)}")
        print(f"Wrote {output_paths['lgas'].relative_to(ROOT)}")
        print(f"Wrote {output_paths['wards'].relative_to(ROOT)}")
        print(f"Wrote {output_paths['observations'].relative_to(ROOT)}")
        print(f"Wrote {output_paths['summary'].relative_to(ROOT)}")
        print("Updated DuckDB tables: gps_events_clean, gps_events_deduped, ward_coverage_summary")
        print(f"Clean GPS events: {clean_count}")
        print(f"Deduped scoring events: {deduped_count}")
        print(
            "Frontend observation features: "
            f"{len(observations_geojson['features'])} of {valid_gps_count}"
        )
        print(f"Observation states: {len(state_counts)}")
        print(f"Observation wards (raw): {len(raw_points_by_ward_key)}")
        print(f"Observation wards (scored): {len(scored_points_by_ward_key)}")
        print("Ward coverage status counts:")
        for coverage_status, count in coverage_status_rows:
            print(f"  {coverage_status}: {count}")
        print("")

        return {
            "id": dataset_id,
            "label": dataset_label,
            "sourceFile": source_path.name,
            "generatedAt": dashboard_summary["generatedAt"],
            "paths": build_dataset_public_paths(dataset_id),
        }
    finally:
        connection.close()


def main() -> None:
    source_paths = discover_duckdb_sources()
    if not source_paths:
        raise FileNotFoundError(f"No DuckDB sources found in {ROOT}")

    dataset_entries: list[dict[str, Any]] = []
    used_dataset_ids: set[str] = set()
    default_dataset_id = ""

    for index, source_path in enumerate(source_paths, start=1):
        dataset_id = build_dataset_id(source_path, used_dataset_ids)
        dataset_label = build_dataset_label(source_path, index)
        output_paths = build_dataset_output_paths(dataset_id)
        dataset_entry = prepare_dataset_bundle(
            source_path=source_path,
            dataset_id=dataset_id,
            dataset_label=dataset_label,
            output_paths=output_paths,
        )
        dataset_entries.append(dataset_entry)

        if index == 1:
            copy_dataset_to_legacy_paths(output_paths)
            default_dataset_id = dataset_id

    write_json(
        DATASET_MANIFEST_PATH,
        {
            "defaultDatasetId": default_dataset_id,
            "datasets": dataset_entries,
        },
    )

    print(f"Wrote {DATASET_MANIFEST_PATH.relative_to(ROOT)}")
    print(f"Default dataset: {default_dataset_id}")
    print(f"Discovered DuckDB sources: {len(dataset_entries)}")
    for dataset_entry in dataset_entries:
        print(f"  {dataset_entry['id']}: {dataset_entry['sourceFile']}")


if __name__ == "__main__":
    main()
