from __future__ import annotations

import csv
import gzip
import json
import math
import mimetypes
import os
import re
import sys
import traceback
from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "frontend" / "dist"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import build_dashboard_data as builder


@dataclass(frozen=True)
class DatasetDescriptor:
    id: str
    label: str
    source_file: str
    source_path: Path


class DashboardStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._cache: dict[str, dict[str, Any]] = {}

    def _prune_cache_locked(self, preferred_dataset_id: str) -> None:
        for dataset_id in list(self._cache.keys()):
            if dataset_id != preferred_dataset_id:
                del self._cache[dataset_id]

    def _discover_datasets(self) -> tuple[list[DatasetDescriptor], str]:
        source_paths = builder.discover_duckdb_sources()
        used_ids: set[str] = set()
        descriptors: list[DatasetDescriptor] = []

        for index, source_path in enumerate(source_paths, start=1):
            descriptors.append(
                DatasetDescriptor(
                    id=builder.build_dataset_id(source_path, used_ids),
                    label=builder.build_dataset_label(source_path, index),
                    source_file=source_path.name,
                    source_path=source_path,
                )
            )

        default_dataset_id = descriptors[0].id if descriptors else ""
        return descriptors, default_dataset_id

    def _dataset_manifest(self, descriptors: list[DatasetDescriptor], default_dataset_id: str) -> dict[str, Any]:
        datasets = []
        for descriptor in descriptors:
            cached = self._cache.get(descriptor.id)
            generated_at = ""
            if cached and cached.get("metrics"):
                generated_at = cached["metrics"]["summary"].get("generatedAt", "")

            datasets.append(
                {
                    "id": descriptor.id,
                    "label": descriptor.label,
                    "sourceFile": descriptor.source_file,
                    "generatedAt": generated_at,
                }
            )

        return {
            "defaultDatasetId": default_dataset_id,
            "datasets": datasets,
        }

    def get_manifest(self) -> dict[str, Any]:
        descriptors, default_dataset_id = self._discover_datasets()
        return self._dataset_manifest(descriptors, default_dataset_id)

    def get_dashboard(
        self,
        dataset_id: str | None,
        include_geometry: bool = True,
        include_observations: bool = True,
    ) -> dict[str, Any]:
        descriptors, default_dataset_id = self._discover_datasets()
        if not descriptors:
            raise FileNotFoundError(f"No DuckDB sources found in {ROOT}")

        descriptor_by_id = {descriptor.id: descriptor for descriptor in descriptors}
        requested_descriptor = descriptor_by_id.get(dataset_id or "")
        fallback_descriptor = descriptor_by_id.get(default_dataset_id) or descriptors[0]
        candidate_descriptors: list[DatasetDescriptor] = []

        if requested_descriptor is not None:
            candidate_descriptors.append(requested_descriptor)
        if fallback_descriptor not in candidate_descriptors:
            candidate_descriptors.append(fallback_descriptor)
        for descriptor in descriptors:
            if descriptor not in candidate_descriptors:
                candidate_descriptors.append(descriptor)

        active_descriptor = candidate_descriptors[0]
        payload: dict[str, Any] | None = None
        last_error: Exception | None = None
        for candidate in candidate_descriptors:
            try:
                payload = self._get_or_build_payload(candidate)
                active_descriptor = candidate
                break
            except duckdb.IOException as error:
                if requested_descriptor is not None and candidate.id == requested_descriptor.id:
                    raise
                last_error = error
                continue

        if payload is None:
            raise last_error or FileNotFoundError(f"No readable DuckDB sources found in {ROOT}")

        manifest = self._dataset_manifest(descriptors, default_dataset_id)
        metrics_payload = self._load_or_build_metrics_collections(active_descriptor)

        response = {
            "datasetOptions": manifest["datasets"],
            "activeDataset": {
                "id": active_descriptor.id,
                "label": active_descriptor.label,
                "sourceFile": active_descriptor.source_file,
                "generatedAt": payload["summary"].get("generatedAt", ""),
            },
            "states": metrics_payload["states"],
            "lgas": metrics_payload["lgas"],
            "wards": metrics_payload["wards"],
            "observations": empty_feature_collection(),
            "summary": payload["summary"],
        }

        if not include_geometry:
            response["states"] = strip_collection_geometry(response["states"])
            response["lgas"] = strip_collection_geometry(response["lgas"])
            response["wards"] = strip_collection_geometry(response["wards"])

        if include_observations:
            response["observations"] = self._get_or_build_observations(active_descriptor)

        return response

    def _get_or_build_payload(self, descriptor: DatasetDescriptor) -> dict[str, Any]:
        stat_result = descriptor.source_path.stat()
        source_token = (stat_result.st_mtime_ns, stat_result.st_size)

        with self._lock:
            self._prune_cache_locked(descriptor.id)
            cached = self._cache.get(descriptor.id)
            if cached and cached["token"] != source_token:
                cached = None

            if not cached:
                cached = {"token": source_token}
                self._cache[descriptor.id] = cached

            metrics_payload = cached.get("metrics")
            if metrics_payload is None:
                metrics_payload = self._build_metrics_payload(descriptor)
                cached["metrics"] = metrics_payload

            return metrics_payload

    def _get_or_build_observations(self, descriptor: DatasetDescriptor) -> dict[str, Any]:
        # Observation payloads are large enough to push Render over its RAM
        # limit when kept resident in memory alongside metrics/geometry caches.
        # Load them on demand instead of retaining them in STORE._cache.
        return self._build_observations_payload(descriptor)

    def _build_metrics_payload(self, descriptor: DatasetDescriptor) -> dict[str, Any]:
        prebuilt_summary = load_prebuilt_summary_payload(descriptor)
        if prebuilt_summary is not None:
            return {"summary": prebuilt_summary}

        connection = connect_with_runtime_tables(descriptor)

        try:
            summary = builder.build_runtime_dashboard_summary(
                connection,
                builder.count_valid_gps_events(connection),
            )

            return {"summary": summary}
        finally:
            connection.close()

    def _load_or_build_metrics_collections(self, descriptor: DatasetDescriptor) -> dict[str, Any]:
        prebuilt_geometry = load_prebuilt_geometry_payload(descriptor)
        if prebuilt_geometry is not None:
            return prebuilt_geometry

        connection = connect_with_runtime_tables(descriptor)

        try:
            state_counts, lga_counts, _valid_gps_count = builder.load_observation_admin_counts(
                connection
            )
            states_geojson = builder.transform_state_geojson(state_counts)
            lgas_geojson = builder.transform_lga_geojson(lga_counts)
            coverage_lookup = builder.load_ward_coverage_lookup(connection)
            wards_geojson = builder.transform_ward_geojson_from_lookup(coverage_lookup)
            return {
                "states": states_geojson,
                "lgas": lgas_geojson,
                "wards": wards_geojson,
            }
        finally:
            connection.close()

    def _build_observations_payload(self, descriptor: DatasetDescriptor) -> dict[str, Any]:
        prebuilt_payload = load_prebuilt_observations_payload(descriptor)
        if prebuilt_payload is not None:
            return prebuilt_payload

        connection = connect_with_runtime_tables(descriptor)

        try:
            return builder.load_frontend_observations(connection)
        finally:
            connection.close()


STORE = DashboardStore()
OUTLET_ANALYSIS_CACHE_LOCK = Lock()
OUTLET_ANALYSIS_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
_OUTLET_ANALYSIS_CACHE_MAX_ENTRIES = 1
RUNTIME_TABLE_CONNECTION_LOCK = Lock()
# Serializes concurrent fetch_outlet_analysis calls per dataset to prevent
# DuckDB write-write conflicts when multiple requests try to create/modify
# temporary tables simultaneously on the same database file.
_OUTLET_ANALYSIS_EXECUTION_LOCKS: dict[str, Lock] = {}
_OUTLET_ANALYSIS_EXECUTION_LOCKS_LOCK = Lock()


def _get_outlet_analysis_execution_lock(descriptor_id: str) -> Lock:
    with _OUTLET_ANALYSIS_EXECUTION_LOCKS_LOCK:
        if descriptor_id not in _OUTLET_ANALYSIS_EXECUTION_LOCKS:
            _OUTLET_ANALYSIS_EXECUTION_LOCKS[descriptor_id] = Lock()
        return _OUTLET_ANALYSIS_EXECUTION_LOCKS[descriptor_id]


PRODUCT_CATEGORY_ITEMS = sorted(
    builder.PRODUCT_CATEGORY_LABELS.items(),
    key=lambda item: int(item[0]),
)
CATEGORY_CODE_BY_LABEL = {
    label: code for code, label in builder.PRODUCT_CATEGORY_LABELS.items()
}
SUBCATEGORY_MAPPING_CSV_PATH = Path.home() / "Downloads" / "Categories_Subcat_PROPER.csv"
CLIENT_DISCONNECT_WINERRORS = {10053, 10054}
CLIENT_DISCONNECT_ERRNOS = {32, 104}


def load_prebuilt_metrics_payload(
    descriptor: DatasetDescriptor,
) -> dict[str, Any] | None:
    output_paths = builder.build_dataset_output_paths(descriptor.id)
    required_paths = {
        "states": output_paths["states"],
        "lgas": output_paths["lgas"],
        "wards": output_paths["wards"],
        "summary": output_paths["summary"],
    }

    if not all(path.exists() for path in required_paths.values()):
        return None

    try:
        return {
            "states": json.loads(required_paths["states"].read_text(encoding="utf-8")),
            "lgas": json.loads(required_paths["lgas"].read_text(encoding="utf-8")),
            "wards": json.loads(required_paths["wards"].read_text(encoding="utf-8")),
            "summary": json.loads(required_paths["summary"].read_text(encoding="utf-8")),
        }
    except (OSError, json.JSONDecodeError):
        return None


def load_prebuilt_summary_payload(
    descriptor: DatasetDescriptor,
) -> dict[str, Any] | None:
    output_paths = builder.build_dataset_output_paths(descriptor.id)
    summary_path = output_paths["summary"]
    if not summary_path.exists():
        return None

    try:
        return json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def load_prebuilt_geometry_payload(
    descriptor: DatasetDescriptor,
) -> dict[str, Any] | None:
    output_paths = builder.build_dataset_output_paths(descriptor.id)
    required_paths = {
        "states": output_paths["states"],
        "lgas": output_paths["lgas"],
        "wards": output_paths["wards"],
    }
    if not all(path.exists() for path in required_paths.values()):
        return None

    try:
        return {
            "states": json.loads(required_paths["states"].read_text(encoding="utf-8")),
            "lgas": json.loads(required_paths["lgas"].read_text(encoding="utf-8")),
            "wards": json.loads(required_paths["wards"].read_text(encoding="utf-8")),
        }
    except (OSError, json.JSONDecodeError):
        return None


def load_prebuilt_observations_payload(
    descriptor: DatasetDescriptor,
) -> dict[str, Any] | None:
    output_paths = builder.build_dataset_output_paths(descriptor.id)
    observations_path = output_paths["observations"]
    if not observations_path.exists():
        return None

    try:
        return json.loads(observations_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def load_prebuilt_outlet_analysis_payload(
    descriptor: DatasetDescriptor,
) -> dict[str, Any] | None:
    output_paths = builder.build_dataset_output_paths(descriptor.id)
    outlet_analysis_path = output_paths["outlet_analysis"]
    if not outlet_analysis_path.exists():
        return None

    try:
        payload = json.loads(outlet_analysis_path.read_text(encoding="utf-8"))
        # Reject prebuilt payloads that predate rawCategoryRows so the server
        # recomputes a fresh payload that includes the new fields.
        if "rawCategoryRows" not in payload:
            return None
        return payload
    except (OSError, json.JSONDecodeError):
        return None


def _normalize_subcategory_mapping_value(value: str) -> str:
    normalized = value.strip().lower()
    normalized = re.sub(r"\s+category$", "", normalized)
    normalized = re.sub(r"[^a-z0-9]+", "", normalized)
    return normalized


@lru_cache(maxsize=1)
def _load_subcategory_proper_labels() -> dict[tuple[str, str], str]:
    if not SUBCATEGORY_MAPPING_CSV_PATH.exists():
        return {}

    mapping: dict[tuple[str, str], str] = {}
    with SUBCATEGORY_MAPPING_CSV_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            category_name = (row.get("product_category") or "").strip()
            raw_subcategory_name = (row.get("product_sub_category") or "").strip()
            proper_subcategory_name = (row.get("product_sub_category_PROPER") or "").strip()
            if not category_name or not raw_subcategory_name or not proper_subcategory_name:
                continue

            mapping[
                (
                    _normalize_subcategory_mapping_value(category_name),
                    _normalize_subcategory_mapping_value(raw_subcategory_name),
                )
            ] = proper_subcategory_name

    return mapping


def _subcategory_placeholder_label(category_name: str, subcategory_code: str) -> str:
    return f"{category_name} option {subcategory_code}"


def _build_subcategory_mapping_rows() -> list[tuple[str, str, str]]:
    proper_labels = _load_subcategory_proper_labels()
    rows: list[tuple[str, str, str]] = []

    for definition in builder.OUTLET_SUBCATEGORY_GROUPS:
        category_name = str(definition["categoryLabel"])
        for code, raw_label in dict(definition["knownLabels"]).items():
            proper_label = proper_labels.get(
                (
                    _normalize_subcategory_mapping_value(category_name),
                    _normalize_subcategory_mapping_value(str(raw_label)),
                ),
                str(raw_label),
            )
            rows.append((category_name, str(code), proper_label))

    return rows


def strip_collection_geometry(collection: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": collection["type"],
        "features": [
            {
                "type": feature["type"],
                "id": feature.get("id"),
                "properties": feature["properties"],
            }
            for feature in collection["features"]
        ],
    }


def empty_feature_collection() -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": [],
    }


def _encode_varint(value: int) -> bytes:
    buffer = bytearray()
    unsigned = int(value)
    while True:
        next_value = unsigned & 0x7F
        unsigned >>= 7
        if unsigned:
            buffer.append(next_value | 0x80)
        else:
            buffer.append(next_value)
            break
    return bytes(buffer)


def _encode_length_delimited(field_number: int, payload: bytes) -> bytes:
    return _encode_varint((field_number << 3) | 2) + _encode_varint(len(payload)) + payload


def _encode_uint_field(field_number: int, value: int) -> bytes:
    return _encode_varint((field_number << 3) | 0) + _encode_varint(value)


def _encode_string_field(field_number: int, value: str) -> bytes:
    return _encode_length_delimited(field_number, value.encode("utf-8"))


def _zig_zag_encode(value: int) -> int:
    return (value << 1) ^ (value >> 31)


def _encode_packed_uints(field_number: int, values: list[int]) -> bytes:
    payload = b"".join(_encode_varint(value) for value in values)
    return _encode_length_delimited(field_number, payload)


def _encode_mvt_value(value: str) -> bytes:
    return _encode_string_field(1, value)


def _tile_bounds(z_value: int, x_value: int, y_value: int) -> tuple[float, float, float, float]:
    tiles_per_axis = 2**z_value
    west = (x_value / tiles_per_axis) * 360.0 - 180.0
    east = ((x_value + 1) / tiles_per_axis) * 360.0 - 180.0

    def tile_y_to_latitude(tile_y: float) -> float:
        radians = math.atan(math.sinh(math.pi * (1 - (2 * tile_y / tiles_per_axis))))
        return math.degrees(radians)

    north = tile_y_to_latitude(y_value)
    south = tile_y_to_latitude(y_value + 1)
    return west, south, east, north


def _tile_pixel_coordinates(
    longitude: float,
    latitude: float,
    z_value: int,
    x_value: int,
    y_value: int,
    extent: int,
) -> tuple[int, int]:
    clamped_latitude = max(min(latitude, 85.05112878), -85.05112878)
    world_scale = extent * (2**z_value)
    normalized_x = (longitude + 180.0) / 360.0
    latitude_radians = math.radians(clamped_latitude)
    normalized_y = (
        1.0
        - math.asinh(math.tan(latitude_radians)) / math.pi
    ) / 2.0

    pixel_x = normalized_x * world_scale - (x_value * extent)
    pixel_y = normalized_y * world_scale - (y_value * extent)
    return int(round(pixel_x)), int(round(pixel_y))


def _tile_bucket_size(z_value: int) -> int:
    if z_value <= 5:
        return 256
    if z_value <= 6:
        return 192
    if z_value <= 7:
        return 144
    if z_value <= 8:
        return 112
    if z_value <= 9:
        return 80
    if z_value <= 10:
        return 56
    return 0


def _build_point_tile_features(
    rows: list[tuple[Any, Any, Any, Any]],
    *,
    z_value: int,
    x_value: int,
    y_value: int,
    extent: int = 4096,
) -> list[dict[str, Any]]:
    bucket_size = _tile_bucket_size(z_value)

    if bucket_size <= 0:
        tile_features: list[dict[str, Any]] = []
        for record_id, visit_status, latitude, longitude in rows:
            if latitude is None or longitude is None:
                continue

            tile_x, tile_y = _tile_pixel_coordinates(
                float(longitude),
                float(latitude),
                z_value,
                x_value,
                y_value,
                extent,
            )
            tile_features.append(
                {
                    "id": len(tile_features) + 1,
                    "geometry": (tile_x, tile_y),
                    "properties": {
                        "id": record_id or "",
                        "status": visit_status or "Unknown",
                        "pointCount": 1,
                    },
                }
            )
        return tile_features

    buckets: dict[tuple[int, int], dict[str, Any]] = {}
    for record_id, visit_status, latitude, longitude in rows:
        if latitude is None or longitude is None:
            continue

        tile_x, tile_y = _tile_pixel_coordinates(
            float(longitude),
            float(latitude),
            z_value,
            x_value,
            y_value,
            extent,
        )
        bucket_key = (
            max(0, min(extent - 1, tile_x)) // bucket_size,
            max(0, min(extent - 1, tile_y)) // bucket_size,
        )
        status_key = visit_status or "Unknown"
        bucket = buckets.get(bucket_key)

        if bucket is None:
            bucket = {
                "sum_x": 0.0,
                "sum_y": 0.0,
                "count": 0,
                "status_counts": Counter(),
                "sample_id": record_id or "",
            }
            buckets[bucket_key] = bucket

        bucket["sum_x"] += tile_x
        bucket["sum_y"] += tile_y
        bucket["count"] += 1
        bucket["status_counts"][status_key] += 1

    tile_features: list[dict[str, Any]] = []
    for bucket in buckets.values():
        point_count = int(bucket["count"])
        dominant_status = max(
            bucket["status_counts"].items(),
            key=lambda item: (item[1], item[0] == "Completed", item[0]),
        )[0]
        tile_features.append(
            {
                "id": len(tile_features) + 1,
                "geometry": (
                    int(round(bucket["sum_x"] / point_count)),
                    int(round(bucket["sum_y"] / point_count)),
                ),
                "properties": {
                    "id": bucket["sample_id"],
                    "status": dominant_status,
                    "pointCount": point_count,
                },
            }
        )

    return tile_features


def _encode_mvt_layer(layer_name: str, features: list[dict[str, Any]], extent: int = 4096) -> bytes:
    key_index: dict[str, int] = {}
    keys: list[str] = []
    value_index: dict[str, int] = {}
    values: list[str] = []
    encoded_features: list[bytes] = []

    for feature in features:
        properties = feature["properties"]
        tags: list[int] = []
        for key, raw_value in properties.items():
            if raw_value is None:
                continue

            value = str(raw_value)
            if key not in key_index:
                key_index[key] = len(keys)
                keys.append(key)
            if value not in value_index:
                value_index[value] = len(values)
                values.append(value)

            tags.append(key_index[key])
            tags.append(value_index[value])

        geometry = feature["geometry"]
        geometry_commands = [
            (1 & 0x7) | (1 << 3),
            _zig_zag_encode(int(geometry[0])),
            _zig_zag_encode(int(geometry[1])),
        ]

        feature_payload = bytearray()
        if feature.get("id") is not None:
            feature_payload.extend(_encode_uint_field(1, int(feature["id"])))
        if tags:
            feature_payload.extend(_encode_packed_uints(2, tags))
        feature_payload.extend(_encode_uint_field(3, 1))
        feature_payload.extend(_encode_packed_uints(4, geometry_commands))
        encoded_features.append(_encode_length_delimited(2, bytes(feature_payload)))

    layer_payload = bytearray()
    layer_payload.extend(_encode_string_field(1, layer_name))
    for encoded_feature in encoded_features:
        layer_payload.extend(encoded_feature)
    for key in keys:
        layer_payload.extend(_encode_string_field(3, key))
    for value in values:
        layer_payload.extend(_encode_length_delimited(4, _encode_mvt_value(value)))
    layer_payload.extend(_encode_uint_field(5, extent))
    layer_payload.extend(_encode_uint_field(15, 2))
    return _encode_length_delimited(3, bytes(layer_payload))


def encode_point_tile(features: list[dict[str, Any]], layer_name: str = "points") -> bytes:
    return _encode_mvt_layer(layer_name, features)


def resolve_active_descriptor(dataset_id: str | None) -> DatasetDescriptor:
    descriptors, default_dataset_id = STORE._discover_datasets()
    if not descriptors:
        raise FileNotFoundError(f"No DuckDB sources found in {ROOT}")

    descriptor_by_id = {descriptor.id: descriptor for descriptor in descriptors}
    return (
        descriptor_by_id.get(dataset_id or "")
        or descriptor_by_id.get(default_dataset_id)
        or descriptors[0]
    )


def is_client_disconnect_error(error: BaseException) -> bool:
    if isinstance(error, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True

    if isinstance(error, OSError):
        if getattr(error, "winerror", None) in CLIENT_DISCONNECT_WINERRORS:
            return True
        if getattr(error, "errno", None) in CLIENT_DISCONNECT_ERRNOS:
            return True

    return False


def connect_with_runtime_tables(descriptor: DatasetDescriptor) -> duckdb.DuckDBPyConnection:
    with RUNTIME_TABLE_CONNECTION_LOCK:
        connection = duckdb.connect(str(descriptor.source_path), read_only=True)
        configure_duckdb_connection(connection, descriptor)
        if builder.runtime_tables_are_current(connection):
            return connection

        connection.close()
        connection = duckdb.connect(str(descriptor.source_path), read_only=False)
        configure_duckdb_connection(connection, descriptor)
        builder.ensure_runtime_dataset_tables(connection, force_rebuild=True)
        return connection


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _resolve_duckdb_temp_directory(descriptor: DatasetDescriptor) -> Path:
    raw_value = os.environ.get("DUCKDB_TEMP_DIRECTORY", "")
    cleaned_value = raw_value.strip()

    while (
        len(cleaned_value) >= 2
        and cleaned_value[0] == cleaned_value[-1]
        and cleaned_value[0] in {"'", '"'}
    ):
        cleaned_value = cleaned_value[1:-1].strip()

    cleaned_value = cleaned_value.replace('\\"', '"').replace("\\'", "'")
    cleaned_value = cleaned_value.replace('"', "").replace("'", "").strip()

    fallback_directory = descriptor.source_path.parent / ".duckdb_tmp"
    candidate_directory = Path(cleaned_value) if cleaned_value else fallback_directory
    if cleaned_value in {".", ".."}:
        candidate_directory = fallback_directory
    if os.name == "nt" and cleaned_value.startswith(("\\", "/")):
        candidate_directory = fallback_directory

    try:
        resolved_directory = candidate_directory.resolve()
        if os.name == "nt" and str(resolved_directory).startswith("\\\\"):
            raise OSError("Rejecting malformed DuckDB temp directory")
        resolved_directory.mkdir(parents=True, exist_ok=True)
        return resolved_directory
    except OSError:
        fallback_directory.mkdir(parents=True, exist_ok=True)
        return fallback_directory.resolve()


def configure_duckdb_connection(
    connection: duckdb.DuckDBPyConnection,
    descriptor: DatasetDescriptor,
) -> None:
    temp_directory_text = str(_resolve_duckdb_temp_directory(descriptor))
    running_on_render = bool(os.environ.get("RENDER"))
    memory_limit = os.environ.get(
        "DUCKDB_MEMORY_LIMIT",
        "128MB" if running_on_render else "256MB",
    )
    max_temp_directory_size = os.environ.get(
        "DUCKDB_MAX_TEMP_DIRECTORY_SIZE",
        "2GiB" if running_on_render else "10GiB",
    )
    threads = os.environ.get("DUCKDB_THREADS", "1")

    connection.execute(f"SET temp_directory={_sql_string_literal(temp_directory_text)}")
    connection.execute(
        f"SET max_temp_directory_size={_sql_string_literal(max_temp_directory_size)}"
    )
    connection.execute(f"SET memory_limit={_sql_string_literal(memory_limit)}")
    connection.execute(f"SET threads={threads}")
    connection.execute("SET preserve_insertion_order=false")


def _normalized_sql_text(column_name: str) -> str:
    return f"coalesce(nullif(trim(coalesce({column_name}, '')), ''), 'Unknown')"


def _normalized_admin_sql(column_name: str) -> str:
    base_expression = (
        "regexp_replace("
        f"replace(lower(trim(coalesce({column_name}, ''))), '&', 'and'), "
        "'[^a-z0-9]+', '', 'g'"
        ")"
    )
    return f"""
        case {base_expression}
          when 'fct' then 'federalcapitalterritory'
          when 'federalcapitalterritoryabuja' then 'federalcapitalterritory'
          when 'municipalareacouncil' then 'amac'
          when 'portharcourt' then 'portharcourtcity'
          when 'ifakoijaye' then 'ifakoijaiye'
          else {base_expression}
        end
    """


def _ward_key_sql() -> str:
    return (
        "concat_ws('::', "
        f"{_normalized_admin_sql('state_name')}, "
        f"{_normalized_admin_sql('lga_name')}, "
        f"{_normalized_admin_sql('ward_name')}"
        ")"
    )


def _product_category_match_sql(column_name: str, code: str) -> str:
    return f"regexp_matches(coalesce({column_name}, ''), '(^|\\s){code}(\\s|$)')"


def _build_outlet_scope_filters(
    *,
    state_name: str | None = None,
    lga_name: str | None = None,
    ward_key: str | None = None,
    category_name: str | None = None,
    outlet_type: str | None = None,
    outlet_types: list[str] | None = None,
    state_column: str = "state_name",
    lga_column: str = "lga_name",
    ward_key_expression: str | None = None,
    category_column: str | None = None,
    outlet_type_column: str = "outlet_type",
) -> tuple[list[str], list[Any]]:
    filters = ["1 = 1"]
    params: list[Any] = []
    ward_key_expression = ward_key_expression or _ward_key_sql()

    if state_name and state_name != "all":
        filters.append(f"{state_column} = ?")
        params.append(state_name)

    if lga_name and lga_name != "all":
        filters.append(f"{lga_column} = ?")
        params.append(lga_name)

    if ward_key:
        filters.append(f"{ward_key_expression} = ?")
        params.append(ward_key)

    if category_name and category_name != "all":
        if category_column is not None:
            filters.append(f"{category_column} = ?")
            params.append(category_name)
        else:
            category_code = CATEGORY_CODE_BY_LABEL.get(category_name)
            if category_code:
                filters.append(_product_category_match_sql("product_category_codes_raw", category_code))
            else:
                filters.append("1 = 0")

    normalized_outlet_types = [
        value.strip()
        for value in (outlet_types or ([] if outlet_type is None else [outlet_type]))
        if value and value.strip() and value.strip() != "all"
    ]
    if normalized_outlet_types:
        placeholders = ", ".join("?" for _ in normalized_outlet_types)
        filters.append(f"{_normalized_sql_text(outlet_type_column)} in ({placeholders})")
        params.extend(normalized_outlet_types)

    return filters, params


def _prepare_outlet_analysis_base_table(connection: duckdb.DuckDBPyConnection) -> None:
    source_table_name = builder.resolve_observation_source_table(connection)
    table_columns = builder.get_table_columns(connection, source_table_name)
    source_rows_query = builder.build_source_rows_query(source_table_name, table_columns)

    connection.execute("drop table if exists outlet_analysis_base")
    connection.execute(
        f"""
        create temporary table outlet_analysis_base as
        with source_rows as (
          {source_rows_query}
        )
        select
          state_name,
          lga_name,
          ward_name,
          concat_ws('::', coalesce(state_name, ''), coalesce(lga_name, ''), coalesce(ward_name, '')) as ward_key,
          coalesce(nullif(trim(coalesce(outlet_type, '')), ''), 'Unknown') as outlet_type,
          product_category_codes_raw,
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
            else coalesce(nullif(trim(coalesce(interview_status_label, '')), ''), 'Unknown')
          end as visit_status
        from source_rows
        where nullif(trim(coalesce(state_name, '')), '') is not null
          and nullif(trim(coalesce(lga_name, '')), '') is not null
          and nullif(trim(coalesce(ward_name, '')), '') is not null
        """
    )


def _build_outlet_scope_query(table_name: str, filters: list[str]) -> str:
    subcategory_columns_sql = ",\n          ".join(
        str(definition["fieldAlias"]) for definition in builder.OUTLET_SUBCATEGORY_GROUPS
    )
    return f"""
        select
          state_name,
          lga_name,
          ward_name,
          {_ward_key_sql()} as ward_key,
          coalesce(nullif(trim(coalesce(outlet_type, '')), ''), 'Unknown') as outlet_type,
          product_category_codes_raw,
          {subcategory_columns_sql},
          visit_status
        from {table_name}
        where {' and '.join(filters)}
    """


def _materialize_outlet_scope(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    source_table_name: str,
    filters: list[str],
    params: list[Any],
) -> None:
    connection.execute(f"drop table if exists {table_name}")
    connection.execute(
        f"""
        create temporary table {table_name} as
        {_build_outlet_scope_query(source_table_name, filters)}
        """,
        params,
    )


def _category_filter_definitions(*, include_uncategorized: bool) -> list[tuple[str, str]]:
    category_filters = [
        (label, _product_category_match_sql("product_category_codes_raw", code))
        for code, label in PRODUCT_CATEGORY_ITEMS
    ]

    if include_uncategorized:
        category_filters.append(
            (
                "No category recorded",
                "nullif(trim(coalesce(product_category_codes_raw, '')), '') is null",
            )
        )

    return category_filters


def _build_outlet_category_scope_query(table_name: str) -> str:
    category_union_parts = [
        """
        select
          state_name,
          lga_name,
          ward_name,
          ward_key,
          outlet_type,
          visit_status,
          category_code
        from {table_name},
        unnest(regexp_split_to_array(trim(coalesce(product_category_codes_raw, '')), '\\s+')) as split_codes(category_code)
        where nullif(trim(coalesce(product_category_codes_raw, '')), '') is not null
          and nullif(trim(category_code), '') is not null
        """.format(table_name=table_name),
        """
        select
          state_name,
          lga_name,
          ward_name,
          ward_key,
          outlet_type,
          visit_status,
          null as category_code
        from {table_name}
        where nullif(trim(coalesce(product_category_codes_raw, '')), '') is null
        """.format(table_name=table_name),
    ]

    category_mapping_rows_sql = ",\n              ".join(
        f"({_sql_string_literal(code)}, {_sql_string_literal(label)})"
        for code, label in PRODUCT_CATEGORY_ITEMS
    )

    return f"""
        with exploded_categories as (
          {' union all '.join(category_union_parts)}
        ),
        category_mapping(category_code, category_name) as (
          values
              {category_mapping_rows_sql}
        )
        select
          exploded_categories.state_name,
          exploded_categories.lga_name,
          exploded_categories.ward_name,
          exploded_categories.ward_key,
          exploded_categories.outlet_type,
          exploded_categories.visit_status,
          coalesce(
            category_mapping.category_name,
            case
              when exploded_categories.category_code is null then 'No category recorded'
              else exploded_categories.category_code
            end
          ) as category_name
        from exploded_categories
        left join category_mapping
          on exploded_categories.category_code = category_mapping.category_code
    """


def _materialize_outlet_category_scope(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    source_table_name: str,
) -> None:
    connection.execute(f"drop table if exists {table_name}")
    connection.execute(
        f"""
        create temporary table {table_name} as
        {_build_outlet_category_scope_query(source_table_name)}
        """
    )


def _build_outlet_subcategory_scope_query(table_name: str) -> str:
    union_parts: list[str] = []
    for definition in builder.OUTLET_SUBCATEGORY_GROUPS:
        field_alias = str(definition["fieldAlias"])
        category_name = _sql_string_literal(str(definition["categoryLabel"]))
        union_parts.append(
            f"""
            select
              state_name,
              lga_name,
              ward_name,
              ward_key,
              outlet_type,
              visit_status,
              {category_name} as category_name,
              subcategory_code
            from {table_name},
            unnest(regexp_split_to_array(trim(coalesce({field_alias}, '')), '\\s+')) as split_codes(subcategory_code)
            where nullif(trim(coalesce({field_alias}, '')), '') is not null
              and nullif(trim(subcategory_code), '') is not null
            """
        )

    return "\n            union all\n".join(union_parts)


def _materialize_outlet_subcategory_scope(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    source_table_name: str,
) -> None:
    connection.execute(f"drop table if exists {table_name}")
    mapping_rows = _build_subcategory_mapping_rows()

    connection.execute("drop table if exists outlet_subcategory_label_map")
    connection.execute(
        """
        create temporary table outlet_subcategory_label_map (
          category_name varchar,
          subcategory_code varchar,
          subcategory_name varchar
        )
        """
    )
    if mapping_rows:
        connection.executemany(
            "insert into outlet_subcategory_label_map values (?, ?, ?)",
            mapping_rows,
        )

    connection.execute(
        f"""
        create temporary table {table_name} as
        with exploded_codes as (
          {_build_outlet_subcategory_scope_query(source_table_name)}
        )
        select
          exploded_codes.state_name,
          exploded_codes.lga_name,
          exploded_codes.ward_name,
          exploded_codes.ward_key,
          exploded_codes.outlet_type,
          exploded_codes.visit_status,
          exploded_codes.category_name,
          exploded_codes.subcategory_code,
          coalesce(
            mapping.subcategory_name,
            {_sql_string_literal('')} || exploded_codes.category_name || ' option ' || exploded_codes.subcategory_code
          ) as subcategory_name
        from exploded_codes
        left join outlet_subcategory_label_map as mapping
          on exploded_codes.category_name = mapping.category_name
         and exploded_codes.subcategory_code = mapping.subcategory_code
        """
    )


def fetch_outlet_analysis(
    descriptor: DatasetDescriptor,
    *,
    state_name: str | None,
    lga_name: str | None,
    ward_key: str | None,
    category_name: str | None,
    outlet_type: str | None,
    outlet_types: list[str] | None = None,
) -> dict[str, Any]:
    normalized_outlet_types = tuple(
        sorted(
            {
                value.strip()
                for value in (outlet_types or ([] if outlet_type is None else [outlet_type]))
                if value and value.strip() and value.strip() != "all"
            }
        )
    )
    is_default_scope = (
        (not state_name or state_name == "all")
        and (not lga_name or lga_name == "all")
        and not ward_key
        and (not category_name or category_name == "all")
        and not normalized_outlet_types
        and (not outlet_type or outlet_type == "all")
    )
    if is_default_scope:
        prebuilt_payload = load_prebuilt_outlet_analysis_payload(descriptor)
        if prebuilt_payload is not None:
            return prebuilt_payload

    stat_result = descriptor.source_path.stat()
    cache_key = (
        descriptor.id,
        stat_result.st_mtime_ns,
        stat_result.st_size,
        state_name or "",
        lga_name or "",
        ward_key or "",
        category_name or "",
        normalized_outlet_types,
    )
    with OUTLET_ANALYSIS_CACHE_LOCK:
        cached_payload = OUTLET_ANALYSIS_CACHE.get(cache_key)
        # Discard stale cache entries that predate rawCategoryRows.
        if cached_payload is not None and "rawCategoryRows" not in cached_payload:
            cached_payload = None
            del OUTLET_ANALYSIS_CACHE[cache_key]
        if cached_payload is not None:
            return cached_payload

    # Serialize DB access per dataset. DuckDB does not support concurrent
    # writers on the same file — rapid multi-select clicks can fire several
    # requests simultaneously, all trying to open a read_only=False connection
    # to create temporary tables, which causes IOException / 502 errors.
    # Requests that arrive while one is running will wait here, then hit the
    # cache on the double-check below and return immediately without re-querying.
    execution_lock = _get_outlet_analysis_execution_lock(descriptor.id)
    with execution_lock:
        with OUTLET_ANALYSIS_CACHE_LOCK:
            cached_payload = OUTLET_ANALYSIS_CACHE.get(cache_key)
            if cached_payload is not None and "rawCategoryRows" not in cached_payload:
                cached_payload = None
                del OUTLET_ANALYSIS_CACHE[cache_key]
            if cached_payload is not None:
                return cached_payload

        connection = connect_with_runtime_tables(descriptor)

        try:
            option_filters, option_params = _build_outlet_scope_filters(
                state_name=state_name,
                lga_name=lga_name,
            )
            analysis_filters, analysis_params = _build_outlet_scope_filters(
                state_name=state_name,
                lga_name=lga_name,
                ward_key=ward_key,
                category_name=category_name,
                ward_key_expression="ward_key",
            )
            filtered_filters, filtered_params = _build_outlet_scope_filters(
                state_name=state_name,
                lga_name=lga_name,
                ward_key=ward_key,
                category_name=category_name,
                outlet_type=outlet_type,
                outlet_types=outlet_types,
                ward_key_expression="ward_key",
            )
            option_category_filters, option_category_params = _build_outlet_scope_filters(
                state_name=state_name,
                lga_name=lga_name,
                ward_key_expression="ward_key",
                category_column="category_name",
            )
            analysis_category_filters, analysis_category_params = _build_outlet_scope_filters(
                state_name=state_name,
                lga_name=lga_name,
                ward_key=ward_key,
                category_name=category_name,
                ward_key_expression="ward_key",
                category_column="category_name",
            )
            filtered_category_filters, filtered_category_params = _build_outlet_scope_filters(
                state_name=state_name,
                lga_name=lga_name,
                ward_key=ward_key,
                category_name=category_name,
                outlet_type=outlet_type,
                outlet_types=outlet_types,
                ward_key_expression="ward_key",
                category_column="category_name",
            )
            filtered_subcategory_filters, filtered_subcategory_params = _build_outlet_scope_filters(
                state_name=state_name,
                lga_name=lga_name,
                ward_key=ward_key,
                category_name=category_name,
                outlet_type=outlet_type,
                outlet_types=outlet_types,
                ward_key_expression="ward_key",
                category_column="category_name",
            )

            state_options = [
                row[0]
                for row in connection.execute(
                    f"""
                    select distinct state_name
                    from outlet_scope_summary
                    where nullif(trim(coalesce(state_name, '')), '') is not null
                    order by state_name asc
                    """
                ).fetchall()
            ]
            lga_options = [
                row[0]
                for row in connection.execute(
                    f"""
                    select distinct lga_name
                    from outlet_scope_summary
                    where {' and '.join(option_filters)}
                      and nullif(trim(coalesce(lga_name, '')), '') is not null
                    order by lga_name asc
                    """,
                    option_params,
                ).fetchall()
            ]
            category_options = [
                row[0]
                for row in connection.execute(
                    f"""
                    select distinct category_name
                    from outlet_category_scope_summary
                    where {' and '.join(option_category_filters)}
                      and category_name <> 'No category recorded'
                    order by category_name asc
                    """,
                    option_category_params,
                ).fetchall()
            ]

            scope_record_count = int(
                connection.execute(
                    f"""
                    select coalesce(sum(record_count), 0)
                    from outlet_scope_summary
                    where {' and '.join(analysis_filters)}
                    """,
                    analysis_params,
                ).fetchone()[0]
                or 0
            )
            filtered_record_count = int(
                connection.execute(
                    f"""
                    select coalesce(sum(record_count), 0)
                    from outlet_scope_summary
                    where {' and '.join(filtered_filters)}
                    """,
                    filtered_params,
                ).fetchone()[0]
                or 0
            )

            outlet_type_base_rows = connection.execute(
                f"""
                select
                  outlet_type,
                  sum(record_count) as record_count,
                  sum(completed_count) as completed_count,
                  sum(observation_count) as observation_count,
                  count(distinct state_name) as state_count,
                  count(distinct concat_ws('::', coalesce(state_name, ''), coalesce(lga_name, ''))) as lga_count,
                  count(distinct ward_key) as ward_count
                from outlet_scope_summary
                where {' and '.join(analysis_filters)}
                group by 1
                order by 2 desc, 1 asc
                """,
                analysis_params,
            ).fetchall()
            outlet_type_category_rows = [
                (str(outlet_type_name), str(category_name), int(record_count or 0))
                for outlet_type_name, category_name, record_count in connection.execute(
                    f"""
                    select
                      outlet_type,
                      category_name,
                      sum(record_count) as record_count
                    from outlet_category_scope_summary
                    where {' and '.join(analysis_category_filters)}
                    group by 1, 2
                    order by 1 asc, 3 desc, 2 asc
                    """,
                    analysis_category_params,
                ).fetchall()
            ]

            category_rows = [
                (
                    str(category_name),
                    int(record_count or 0),
                    int(completed_count or 0),
                    int(observation_count or 0),
                    int(state_count or 0),
                    int(lga_count or 0),
                    int(ward_count or 0),
                )
                for (
                    category_name,
                    record_count,
                    completed_count,
                    observation_count,
                    state_count,
                    lga_count,
                    ward_count,
                 ) in connection.execute(
                    f"""
                    select
                      category_name,
                      sum(record_count) as record_count,
                      sum(completed_count) as completed_count,
                      sum(observation_count) as observation_count,
                      count(distinct state_name) as state_count,
                      count(distinct concat_ws('::', coalesce(state_name, ''), coalesce(lga_name, ''))) as lga_count,
                      count(distinct ward_key) as ward_count
                    from outlet_category_scope_summary
                    where {' and '.join(filtered_category_filters)}
                    group by 1
                    order by 2 desc, 1 asc
                    """,
                    filtered_category_params,
                ).fetchall()
            ]
            total_category_count = sum(record_count for _, record_count, *_rest in category_rows)

            categories_by_outlet_type: dict[str, list[tuple[str, int]]] = {}
            for outlet_type_name, category_label, record_count in outlet_type_category_rows:
                categories_by_outlet_type.setdefault(str(outlet_type_name), []).append(
                    (str(category_label), int(record_count or 0))
                )

            outlet_type_rows = []
            for (
                outlet_type_name,
                record_count,
                completed_count,
                observation_count,
                state_count,
                lga_count,
                ward_count,
            ) in outlet_type_base_rows:
                category_counts = categories_by_outlet_type.get(str(outlet_type_name), [])
                categories_summary = ", ".join(
                    f"{category_label} ({count})"
                    for category_label, count in category_counts
                )
                outlet_type_rows.append(
                    {
                        "outletType": str(outlet_type_name),
                        "count": int(record_count or 0),
                        "completedCount": int(completed_count or 0),
                        "observationCount": int(observation_count or 0),
                        "sharePercent": (
                            0
                            if scope_record_count == 0
                            else (int(record_count or 0) / scope_record_count) * 100
                        ),
                        "stateCount": int(state_count or 0),
                        "lgaCount": int(lga_count or 0),
                        "wardCount": int(ward_count or 0),
                        "distinctCategoryCount": len(category_counts),
                        "categoriesSummary": categories_summary,
                    }
                )

            outlet_category_rows = [
                {
                    "categoryName": str(category_label),
                    "count": int(record_count or 0),
                    "completedCount": int(completed_count or 0),
                    "observationCount": int(observation_count or 0),
                    "sharePercent": (
                        0
                        if total_category_count == 0
                        else (int(record_count or 0) / total_category_count) * 100
                    ),
                    "stateCount": int(state_count or 0),
                    "lgaCount": int(lga_count or 0),
                    "wardCount": int(ward_count or 0),
                }
                for (
                    category_label,
                    record_count,
                    completed_count,
                    observation_count,
                    state_count,
                    lga_count,
                    ward_count,
                ) in category_rows
            ]
            subcategory_rows = connection.execute(
                f"""
                select
                  category_name,
                  subcategory_name,
                  sum(record_count) as record_count,
                  sum(completed_count) as completed_count,
                  sum(observation_count) as observation_count,
                  count(distinct state_name) as state_count,
                  count(distinct concat_ws('::', coalesce(state_name, ''), coalesce(lga_name, ''))) as lga_count,
                  count(distinct ward_key) as ward_count
                from outlet_subcategory_scope_summary
                where {' and '.join(filtered_subcategory_filters)}
                group by 1, 2
                order by 3 desc, 1 asc, 2 asc
                """,
                filtered_subcategory_params,
            ).fetchall()
            total_subcategory_count = sum(record_count or 0 for *_leading, record_count, _completed_count, _observation_count, _state_count, _lga_count, _ward_count in subcategory_rows)
            outlet_subcategory_rows = [
                {
                    "categoryName": str(category_name),
                    "subcategoryName": str(subcategory_name),
                    "count": int(record_count or 0),
                    "completedCount": int(completed_count or 0),
                    "observationCount": int(observation_count or 0),
                    "sharePercent": (
                        0
                        if total_subcategory_count == 0
                        else (int(record_count or 0) / total_subcategory_count) * 100
                    ),
                    "stateCount": int(state_count or 0),
                    "lgaCount": int(lga_count or 0),
                    "wardCount": int(ward_count or 0),
                }
                for (
                    category_name,
                    subcategory_name,
                    record_count,
                    completed_count,
                    observation_count,
                    state_count,
                    lga_count,
                    ward_count,
                ) in subcategory_rows
            ]

            # Raw per-outlet-type breakdowns used by the frontend for instant
            # client-side filtering — no extra server round-trip needed when
            # the user toggles outlet type buttons.
            raw_category_rows = [
                {
                    "outletType": str(outlet_type_val),
                    "categoryName": str(category_name),
                    "count": int(record_count or 0),
                    "completedCount": int(completed_count or 0),
                    "observationCount": int(observation_count or 0),
                    "stateCount": int(state_count or 0),
                    "lgaCount": int(lga_count or 0),
                    "wardCount": int(ward_count or 0),
                }
                for (
                    outlet_type_val,
                    category_name,
                    record_count,
                    completed_count,
                    observation_count,
                    state_count,
                    lga_count,
                    ward_count,
                ) in connection.execute(
                    f"""
                    select
                      outlet_type,
                      category_name,
                      sum(record_count) as record_count,
                      sum(completed_count) as completed_count,
                      sum(observation_count) as observation_count,
                      count(distinct state_name) as state_count,
                      count(distinct concat_ws('::', coalesce(state_name, ''), coalesce(lga_name, ''))) as lga_count,
                      count(distinct ward_key) as ward_count
                    from outlet_category_scope_summary
                    where {' and '.join(analysis_category_filters)}
                    group by 1, 2
                    order by 1 asc, 3 desc, 2 asc
                    """,
                    analysis_category_params,
                ).fetchall()
            ]

            raw_subcategory_rows = [
                {
                    "outletType": str(outlet_type_val),
                    "categoryName": str(category_name),
                    "subcategoryName": str(subcategory_name),
                    "count": int(record_count or 0),
                    "completedCount": int(completed_count or 0),
                    "observationCount": int(observation_count or 0),
                    "stateCount": int(state_count or 0),
                    "lgaCount": int(lga_count or 0),
                    "wardCount": int(ward_count or 0),
                }
                for (
                    outlet_type_val,
                    category_name,
                    subcategory_name,
                    record_count,
                    completed_count,
                    observation_count,
                    state_count,
                    lga_count,
                    ward_count,
                ) in connection.execute(
                    f"""
                    select
                      outlet_type,
                      category_name,
                      subcategory_name,
                      sum(record_count) as record_count,
                      sum(completed_count) as completed_count,
                      sum(observation_count) as observation_count,
                      count(distinct state_name) as state_count,
                      count(distinct concat_ws('::', coalesce(state_name, ''), coalesce(lga_name, ''))) as lga_count,
                      count(distinct ward_key) as ward_count
                    from outlet_subcategory_scope_summary
                    where {' and '.join(analysis_category_filters)}
                    group by 1, 2, 3
                    order by 1 asc, 4 desc, 2 asc, 3 asc
                    """,
                    analysis_category_params,
                ).fetchall()
            ]

            payload = {
                "stateOptions": state_options,
                "lgaOptions": lga_options,
                "categoryOptions": category_options,
                "scopeRecordCount": scope_record_count,
                "filteredRecordCount": filtered_record_count,
                "outletTypeRows": outlet_type_rows,
                "outletCategoryRows": outlet_category_rows,
                "outletSubcategoryRows": outlet_subcategory_rows,
                "rawCategoryRows": raw_category_rows,
                "rawSubcategoryRows": raw_subcategory_rows,
            }
            with OUTLET_ANALYSIS_CACHE_LOCK:
                while len(OUTLET_ANALYSIS_CACHE) >= _OUTLET_ANALYSIS_CACHE_MAX_ENTRIES:
                    OUTLET_ANALYSIS_CACHE.pop(next(iter(OUTLET_ANALYSIS_CACHE)))
                OUTLET_ANALYSIS_CACHE[cache_key] = payload
            return payload
        finally:
            connection.close()


def point_feature_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
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
            "wardKey": builder.build_ward_key(state_name, lga_name, ward_name),
            "collectorName": collector_name or "Unknown collector",
            "deviceId": device_id or "",
            "status": visit_status or "Unknown",
            "statusDetail": interview_status_label or visit_status or "Unknown",
            "outletType": outlet_type or "Unknown",
            "productCategoryCodes": builder.parse_multi_select_codes(product_category_codes_raw),
            "productCategories": builder.broad_category_labels(product_category_codes_raw),
            "channelType": channel_type or "Unknown",
            "businessName": business_name or "Unnamed outlet",
            "preApproval": False,
            "gpsAccuracy": float(gps_accuracy or 0),
            "gpsQualityFlag": gps_quality_flag or "unknown",
            "effectiveToleranceM": float(
                effective_tolerance_m or builder.DEFAULT_GPS_TOLERANCE_METERS
            ),
            "eventTs": builder.json_safe(event_ts),
            "submissionTs": builder.json_safe(submission_ts),
            "startTime": builder.json_safe(starttime),
            "endTime": builder.json_safe(endtime),
            "surveyDate": builder.json_safe(survey_date),
            "reviewState": review_state or "",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [float(longitude), float(latitude)],
        },
    }


def fetch_map_points(
    descriptor: DatasetDescriptor,
    bbox: tuple[float, float, float, float],
    zoom: float,
    state_name: str | None,
    lga_name: str | None,
) -> dict[str, Any]:
    west, south, east, north = bbox
    connection = connect_with_runtime_tables(descriptor)

    try:
        params: list[Any] = [west, east, south, north]
        filters = [
            "longitude between ? and ?",
            "latitude between ? and ?",
            "gps_quality_flag not in ('missing', 'invalid', 'outside_nigeria')",
        ]

        if state_name and state_name != "all":
            filters.append("state_name = ?")
            params.append(state_name)

        if lga_name and lga_name != "all":
            filters.append("lga_name = ?")
            params.append(lga_name)

        where_clause = " and ".join(filters)
        rows = connection.execute(
            f"""
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
            where {where_clause}
            order by event_ts asc, record_id asc
            """,
            params,
        ).fetchall()

        return {
            "type": "FeatureCollection",
            "features": [point_feature_from_row(row) for row in rows],
        }
    finally:
        connection.close()


def fetch_analysis_observations(
    descriptor: DatasetDescriptor,
    *,
    state_name: str | None,
    lga_name: str | None,
    ward_key: str | None,
) -> dict[str, Any]:
    connection = connect_with_runtime_tables(descriptor)

    try:
        params: list[Any] = []
        filters = [
            "latitude is not null",
            "longitude is not null",
            "gps_quality_flag not in ('missing', 'invalid', 'outside_nigeria')",
        ]

        if state_name and state_name != "all":
            filters.append("state_name = ?")
            params.append(state_name)

        if lga_name and lga_name != "all":
            filters.append("lga_name = ?")
            params.append(lga_name)

        if ward_key:
            filters.append(
                "concat_ws('::', coalesce(state_name, ''), coalesce(lga_name, ''), coalesce(ward_name, '')) = ?"
            )
            params.append(ward_key)

        where_clause = " and ".join(filters)
        rows = connection.execute(
            f"""
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
            where {where_clause}
            order by event_ts asc, record_id asc
            """,
            params,
        ).fetchall()

        return {
            "type": "FeatureCollection",
            "features": [point_feature_from_row(row) for row in rows],
        }
    finally:
        connection.close()


def fetch_map_tile(
    descriptor: DatasetDescriptor,
    z_value: int,
    x_value: int,
    y_value: int,
    state_name: str | None,
    lga_name: str | None,
    coverage_status: str | None,
) -> bytes:
    west, south, east, north = _tile_bounds(z_value, x_value, y_value)
    connection = connect_with_runtime_tables(descriptor)

    try:
        params: list[Any] = [west, east, south, north]
        point_table_name = "gps_events_deduped" if z_value <= 8 else "gps_events_clean"
        filters = [
            "points.longitude between ? and ?",
            "points.latitude between ? and ?",
            "points.gps_quality_flag not in ('missing', 'invalid', 'outside_nigeria')",
        ]
        from_clause = f"from {point_table_name} as points"

        if state_name and state_name != "all":
            filters.append("points.state_name = ?")
            params.append(state_name)

        if lga_name and lga_name != "all":
            filters.append("points.lga_name = ?")
            params.append(lga_name)

        if coverage_status and coverage_status != "all":
            from_clause += """
            left join ward_coverage_summary as coverage
              on points.state_name = coverage.state_name
             and points.lga_name = coverage.lga_name
             and points.ward_name = coverage.ward_name
            """
            filters.append("coverage.coverage_status = ?")
            params.append(coverage_status)

        where_clause = " and ".join(filters)
        rows = connection.execute(
            f"""
            select
              points.record_id,
              points.visit_status,
              points.latitude,
              points.longitude
            {from_clause}
            where {where_clause}
            """,
            params,
        ).fetchall()

        tile_features = _build_point_tile_features(
            rows,
            z_value=z_value,
            x_value=x_value,
            y_value=y_value,
        )

        return encode_point_tile(tile_features)
    finally:
        connection.close()


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "DuckDbDashboardAPI/1.0"

    def do_OPTIONS(self) -> None:
        try:
            self.send_response(HTTPStatus.NO_CONTENT)
            self._send_default_headers()
            self.end_headers()
        except OSError as error:
            if not is_client_disconnect_error(error):
                raise

    def do_GET(self) -> None:
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/") or "/"

        try:
            if path == "/api/health":
                self._send_json({"ok": True})
                return

            if path == "/api/datasets":
                self._send_json(STORE.get_manifest())
                return

            if path == "/api/dashboard":
                query = parse_qs(parsed_url.query)
                dataset_id = query.get("dataset", [None])[0]
                include_geometry = query.get("includeGeometry", ["1"])[0] != "0"
                include_observations = query.get("includeObservations", ["1"])[0] != "0"
                self._send_json(
                    STORE.get_dashboard(
                        dataset_id,
                        include_geometry=include_geometry,
                        include_observations=include_observations,
                    )
                )
                return

            if path == "/api/outlet-analysis":
                query = parse_qs(parsed_url.query)
                dataset_id = query.get("dataset", [None])[0]
                state_name = query.get("state", [None])[0]
                lga_name = query.get("lga", [None])[0]
                ward_key = query.get("wardKey", [None])[0]
                category_name = query.get("category", [None])[0]
                outlet_type = query.get("outletType", [None])[0]
                outlet_types_values = query.get("outletTypes", [])
                normalized_outlet_types: list[str] = []
                for raw_value in outlet_types_values:
                    for split_value in raw_value.split(","):
                        cleaned_value = split_value.strip()
                        if cleaned_value:
                            normalized_outlet_types.append(cleaned_value)
                outlet_types = normalized_outlet_types or None
                active_descriptor = resolve_active_descriptor(dataset_id)
                self._send_json(
                    fetch_outlet_analysis(
                        active_descriptor,
                        state_name=state_name,
                        lga_name=lga_name,
                        ward_key=ward_key,
                        category_name=category_name,
                        outlet_type=outlet_type,
                        outlet_types=outlet_types,
                    )
                )
                return

            if path == "/api/map-points":
                query = parse_qs(parsed_url.query)
                dataset_id = query.get("dataset", [None])[0]
                bbox_text = query.get("bbox", [None])[0]
                zoom_text = query.get("zoom", ["6"])[0]
                state_name = query.get("state", [None])[0]
                lga_name = query.get("lga", [None])[0]

                if not bbox_text:
                    self._send_json(
                        {"error": "Missing bbox query parameter"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                try:
                    bbox = tuple(float(value) for value in bbox_text.split(","))
                    if len(bbox) != 4:
                        raise ValueError
                except ValueError:
                    self._send_json(
                        {"error": "bbox must be west,south,east,north"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                try:
                    zoom = float(zoom_text)
                except ValueError:
                    zoom = 6.0

                active_descriptor = resolve_active_descriptor(dataset_id)
                self._send_json(
                    fetch_map_points(
                        active_descriptor,
                        bbox=bbox,
                        zoom=zoom,
                        state_name=state_name,
                        lga_name=lga_name,
                    )
                )
                return

            if path == "/api/analysis-observations":
                query = parse_qs(parsed_url.query)
                dataset_id = query.get("dataset", [None])[0]
                state_name = query.get("state", [None])[0]
                lga_name = query.get("lga", [None])[0]
                ward_key = query.get("wardKey", [None])[0]
                active_descriptor = resolve_active_descriptor(dataset_id)
                self._send_json(
                    fetch_analysis_observations(
                        active_descriptor,
                        state_name=state_name,
                        lga_name=lga_name,
                        ward_key=ward_key,
                    )
                )
                return

            if path.startswith("/api/tiles/") and path.endswith(".mvt"):
                query = parse_qs(parsed_url.query)
                dataset_id = query.get("dataset", [None])[0]
                state_name = query.get("state", [None])[0]
                lga_name = query.get("lga", [None])[0]
                coverage_status = query.get("coverageStatus", [None])[0]
                tile_parts = path.split("/")
                if len(tile_parts) != 6:
                    self._send_json(
                        {"error": "Tile path must be /api/tiles/{z}/{x}/{y}.mvt"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                try:
                    z_value = int(tile_parts[3])
                    x_value = int(tile_parts[4])
                    y_value = int(tile_parts[5].removesuffix(".mvt"))
                except ValueError:
                    self._send_json(
                        {"error": "Invalid tile coordinates"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return

                active_descriptor = resolve_active_descriptor(dataset_id)
                try:
                    tile_payload = fetch_map_tile(
                        active_descriptor,
                        z_value=z_value,
                        x_value=x_value,
                        y_value=y_value,
                        state_name=state_name,
                        lga_name=lga_name,
                        coverage_status=coverage_status,
                    )
                except Exception as error:
                    print(
                        "Tile generation failed:",
                        {
                            "dataset": active_descriptor.id,
                            "z": z_value,
                            "x": x_value,
                            "y": y_value,
                            "state": state_name,
                            "lga": lga_name,
                            "coverageStatus": coverage_status,
                            "error": str(error),
                        },
                        file=sys.stderr,
                    )
                    traceback.print_exc()
                    tile_payload = encode_point_tile([])

                self._send_bytes(
                    tile_payload,
                    content_type="application/vnd.mapbox-vector-tile",
                )
                return

            if path.startswith("/api/"):
                self._send_json(
                    {"error": f"Unknown endpoint: {path}"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return

            self._send_frontend(path)
        except FileNotFoundError as error:
            if is_client_disconnect_error(error):
                return
            self._send_json({"error": str(error)}, status=HTTPStatus.NOT_FOUND)
        except Exception as error:  # pragma: no cover - defensive handler
            if is_client_disconnect_error(error):
                return
            self._send_json(
                {"error": str(error)},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_default_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Connection", "close")

    def _send_json(
        self,
        payload: dict[str, Any],
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        try:
            self.send_response(status)
            self._send_default_headers()
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError as error:
            if not is_client_disconnect_error(error):
                raise

    def _send_bytes(
        self,
        payload: bytes,
        *,
        content_type: str,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        body = payload
        accepts_gzip = "gzip" in (self.headers.get("Accept-Encoding", "").lower())
        if accepts_gzip and payload:
            body = gzip.compress(payload, compresslevel=6)

        try:
            self.send_response(status)
            self._send_default_headers()
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Type", content_type)
            if body is not payload:
                self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError as error:
            if not is_client_disconnect_error(error):
                raise

    def _send_file(
        self,
        file_path: Path,
        *,
        content_type: str,
        status: HTTPStatus = HTTPStatus.OK,
        cache_control: str = "no-store",
    ) -> None:
        body = file_path.read_bytes()
        try:
            self.send_response(status)
            self._send_default_headers()
            self.send_header("Cache-Control", cache_control)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError as error:
            if not is_client_disconnect_error(error):
                raise

    def _send_frontend(self, path: str) -> None:
        if not DIST_DIR.exists():
            self._send_json(
                {"error": "Frontend build not found. Run `npm run build` before starting the server."},
                status=HTTPStatus.NOT_FOUND,
            )
            return

        requested_path = path.lstrip("/")
        candidate = (DIST_DIR / requested_path).resolve()
        dist_root = DIST_DIR.resolve()

        if (
            requested_path
            and candidate.exists()
            and candidate.is_file()
            and (candidate == dist_root or dist_root in candidate.parents)
        ):
            content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
            cache_control = (
                "public, max-age=31536000, immutable"
                if "/assets/" in candidate.as_posix() or candidate.parent.name == "assets"
                else "no-store"
            )
            self._send_file(
                candidate,
                content_type=content_type,
                cache_control=cache_control,
            )
            return

        index_path = DIST_DIR / "index.html"
        if not index_path.exists():
            self._send_json(
                {"error": "Frontend entrypoint not found. Run `npm run build` before starting the server."},
                status=HTTPStatus.NOT_FOUND,
            )
            return

        self._send_file(
            index_path,
            content_type="text/html; charset=utf-8",
            cache_control="no-store",
        )


def warm_default_api_caches() -> None:
    descriptors, _ = STORE._discover_datasets()
    for descriptor in descriptors:
        try:
            STORE.get_dashboard(
                dataset_id=descriptor.id,
                include_geometry=True,
                include_observations=False,
            )
            fetch_outlet_analysis(
                descriptor,
                state_name=None,
                lga_name=None,
                ward_key=None,
                category_name=None,
                outlet_type=None,
                outlet_types=None,
            )
        except Exception:
            continue


def warm_primary_api_cache() -> None:
    descriptors, _ = STORE._discover_datasets()
    if not descriptors:
        return

    descriptor = descriptors[0]
    try:
        STORE.get_dashboard(
            dataset_id=descriptor.id,
            include_geometry=True,
            include_observations=False,
        )
    except Exception:
        return


def main() -> None:
    host = os.environ.get("DUCKDB_DASHBOARD_HOST", "0.0.0.0")
    port = int(
        os.environ.get("DUCKDB_DASHBOARD_PORT")
        or os.environ.get("PORT")
        or "8000"
    )
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print(f"DuckDB dashboard API listening on http://{host}:{port}")
    enable_warmup_env = os.environ.get("DUCKDB_ENABLE_CACHE_WARMUP")
    running_on_render = bool(os.environ.get("RENDER"))
    enable_warmup = (
        enable_warmup_env == "1"
        if enable_warmup_env is not None
        else not running_on_render
    )
    if enable_warmup:
        Thread(target=warm_primary_api_cache, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("DuckDB dashboard API stopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
