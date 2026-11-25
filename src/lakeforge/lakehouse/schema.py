"""Spark schemas for the Bronze tables."""

from pyspark.sql.types import (
    DataType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakeforge.lakehouse.catalog import LANDING_RUN_COLUMN, TABLES, ColumnSpec

_TYPES: dict[str, DataType] = {
    "string": StringType(),
    "double": DoubleType(),
    "timestamp": TimestampType(),
    "date": DateType(),
}


def bronze_schema(dataset: str) -> StructType:
    """Return the Delta schema for one operational dataset, including the run id."""
    return StructType(
        [
            *(_field(spec) for spec in TABLES[dataset]),
            StructField(LANDING_RUN_COLUMN, StringType(), False),
        ]
    )


def landing_string_schema(dataset: str) -> StructType:
    """Return an all-string schema used to read one JSONL file before parsing."""
    return StructType([StructField(spec.name, StringType(), True) for spec in TABLES[dataset]])


def _field(spec: ColumnSpec) -> StructField:
    return StructField(spec.name, _TYPES[spec.kind], spec.nullable)
