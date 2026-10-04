import re

import psycopg2
from pyspark.sql import DataFrame


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(identifier: str) -> str:
    parts = identifier.split(".")
    if not parts or any(not _IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"Invalid SQL identifier: {identifier}")
    return ".".join(f'"{part}"' for part in parts)


def _truncate_staging_table(
    staging_table: str,
    pg_dsn: str,
) -> None:
    connection = psycopg2.connect(pg_dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"TRUNCATE TABLE {_quote_identifier(staging_table)}"
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def stage_dataframe(
    df: DataFrame,
    staging_table: str,
    jdbc_options: dict[str, str],
) -> None:
    pg_dsn = jdbc_options.get("pg_dsn")
    if not pg_dsn:
        raise ValueError("jdbc_options must include pg_dsn")

    spark_options = {
        key: value
        for key, value in jdbc_options.items()
        if key != "pg_dsn"
    }
    spark_options["dbtable"] = staging_table

    _truncate_staging_table(staging_table, pg_dsn)

    (
        df.write
        .format("jdbc")
        .options(**spark_options)
        .mode("append")
        .save()
    )


def replace_from_staging(
    target_table: str,
    staging_table: str,
    ordered_columns: list[str],
    pg_dsn: str,
) -> None:
    if not ordered_columns:
        raise ValueError("ordered_columns must not be empty")

    target_sql = _quote_identifier(target_table)
    staging_sql = _quote_identifier(staging_table)
    columns_sql = ", ".join(
        _quote_identifier(column)
        for column in ordered_columns
    )

    connection = psycopg2.connect(pg_dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {target_sql}")
            cursor.execute(
                f"INSERT INTO {target_sql} ({columns_sql}) "
                f"SELECT {columns_sql} FROM {staging_sql}"
            )
            cursor.execute(f"TRUNCATE TABLE {staging_sql}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def merge_from_staging(
    target_table: str,
    staging_table: str,
    ordered_columns: list[str],
    conflict_columns: list[str],
    pg_dsn: str,
) -> None:
    if not ordered_columns:
        raise ValueError("ordered_columns must not be empty")
    if not conflict_columns:
        raise ValueError("conflict_columns must not be empty")

    missing_conflicts = [
        column for column in conflict_columns
        if column not in ordered_columns
    ]
    if missing_conflicts:
        raise ValueError("conflict columns must be present in ordered_columns")

    update_columns = [
        column for column in ordered_columns
        if column not in conflict_columns
    ]
    if not update_columns:
        raise ValueError("at least one non-conflict column is required")

    target_sql = _quote_identifier(target_table)
    staging_sql = _quote_identifier(staging_table)
    columns_sql = ", ".join(
        _quote_identifier(column)
        for column in ordered_columns
    )
    conflict_sql = ", ".join(
        _quote_identifier(column)
        for column in conflict_columns
    )
    updates_sql = ", ".join(
        f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
        for column in update_columns
    )

    merge_sql = (
        f"INSERT INTO {target_sql} ({columns_sql}) "
        f"SELECT {columns_sql} FROM {staging_sql} "
        f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {updates_sql}"
    )

    connection = psycopg2.connect(pg_dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute(merge_sql)
            cursor.execute(f"TRUNCATE TABLE {staging_sql}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
