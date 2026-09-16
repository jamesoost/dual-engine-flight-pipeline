import logging
import json
import os
import shutil
from glob import glob
from datetime import datetime

import pandas as pd

from config import load_config
from src.extract import fetch_flights, save_raw_payload
from src.pandas_pipeline.load import load_sqlite, write_outputs
from src.pandas_pipeline.transform import transform_records as transform_pandas_records
from src.pyspark_pipeline.transform import transform_records as transform_spark_records
from src.schema import DATETIME_FIELDS, normalize_flight_payload


def setup_logging(logs_dir: str):
    os.makedirs(logs_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.basicConfig(
        filename=f"{logs_dir}/pipeline_{ts}.log",
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _latest_file(pattern: str) -> str:
    matches = sorted(glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files found for pattern: {pattern}")
    return matches[-1]


def _validate_spark_runtime() -> None:
    if shutil.which("java") is None:
        raise RuntimeError(
            "Java is not installed. Install a JRE/JDK (for example OpenJDK 17) before running ETL_ENGINE=spark."
        )

    if not os.getenv("JAVA_HOME"):
        raise RuntimeError(
            "JAVA_HOME is not set. Export JAVA_HOME to your JDK path before running ETL_ENGINE=spark."
        )


def _transform_with_engine(selected_engine: str, records: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if selected_engine == "pandas":
        return transform_pandas_records(records)

    if selected_engine == "spark":
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise RuntimeError(
                "PySpark is not installed. Install with: pip install -e '.[spark]'"
            ) from exc

        _validate_spark_runtime()

        spark = SparkSession.builder.appName("dual-engine-flight-pipeline").master("local[*]").getOrCreate()
        try:
            valid_sdf, invalid_sdf = transform_spark_records(spark, records)
            valid_df = pd.DataFrame([row.asDict(recursive=True) for row in valid_sdf.collect()])
            invalid_df = pd.DataFrame([row.asDict(recursive=True) for row in invalid_sdf.collect()])
        finally:
            spark.stop()

        return valid_df, invalid_df

    raise ValueError(f"Unsupported engine: {selected_engine}")


def _do_extract(cfg) -> tuple[dict, str, list[dict]]:
    payload = fetch_flights(limit=cfg.api_limit, offset=cfg.api_offset, timeout=cfg.api_timeout)
    raw_path = save_raw_payload(payload, raw_dir=cfg.raw_dir)
    records = normalize_flight_payload(payload)
    return payload, raw_path, records


def _do_transform(cfg, selected_engine: str, records: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame, str, str]:
    valid_df, invalid_df = _transform_with_engine(selected_engine, records)
    valid_path, invalid_path = write_outputs(
        valid_df,
        invalid_df,
        staging_dir=cfg.staging_dir,
        quarantine_dir=cfg.quarantine_dir,
    )
    return valid_df, invalid_df, valid_path, invalid_path


def _do_load(cfg, valid_df: pd.DataFrame) -> dict:
    return load_sqlite(valid_df, db_path=cfg.db_path)


def run_extract_step() -> dict:
    cfg = load_config()
    setup_logging(cfg.logs_dir)

    payload, raw_path, records = _do_extract(cfg)

    logging.info("Extract step complete. raw_path=%s records=%s", raw_path, len(records))

    return {
        "step": "extract",
        "raw_output": raw_path,
        "records": len(records),
    }


def run_transform_step(engine=None, raw_path=None) -> dict:
    cfg = load_config()
    selected_engine = engine or cfg.engine
    setup_logging(cfg.logs_dir)

    source_raw_path = raw_path or _latest_file(f"{cfg.raw_dir}/flights_*.json")

    with open(source_raw_path, "r", encoding="utf-8") as raw_file:
        payload_dict = json.load(raw_file)

    records = normalize_flight_payload(payload_dict)
    valid_df, invalid_df, valid_path, invalid_path = _do_transform(cfg, selected_engine, records)

    logging.info(
        "Transform step complete. engine=%s raw_input=%s valid=%s invalid=%s",
        selected_engine,
        source_raw_path,
        len(valid_df),
        len(invalid_df),
    )

    return {
        "step": "transform",
        "engine": selected_engine,
        "raw_input": source_raw_path,
        "valid_rows": len(valid_df),
        "invalid_rows": len(invalid_df),
        "valid_output": valid_path,
        "invalid_output": invalid_path,
    }


def run_load_step(valid_csv_path=None) -> dict:
    cfg = load_config()
    setup_logging(cfg.logs_dir)

    source_valid_csv = valid_csv_path or _latest_file(f"{cfg.staging_dir}/flights_processed_*.csv")
    valid_df = pd.read_csv(source_valid_csv)

    for field in DATETIME_FIELDS:
        if field in valid_df.columns:
            valid_df[field] = pd.to_datetime(valid_df[field], errors="coerce", utc=True)

    load_stats = _do_load(cfg, valid_df)

    logging.info("Load step complete. valid_input=%s stats=%s", source_valid_csv, load_stats)

    return {
        "step": "load",
        "valid_input": source_valid_csv,
        "sqlite_stats": load_stats,
    }


def run_pipeline(engine=None):
    cfg = load_config()
    selected_engine = engine or cfg.engine
    setup_logging(cfg.logs_dir)

    _, raw_path, records = _do_extract(cfg)
    logging.info("Fetched %s records and saved raw file to %s", len(records), raw_path)

    valid_df, invalid_df, valid_path, invalid_path = _do_transform(cfg, selected_engine, records)
    load_stats = _do_load(cfg, valid_df)

    logging.info(
        "Pipeline done. valid_rows=%s invalid_rows=%s valid_output=%s invalid_output=%s",
        len(valid_df),
        len(invalid_df),
        valid_path,
        invalid_path,
    )

    return {
        "engine": selected_engine,
        "valid_rows": len(valid_df),
        "invalid_rows": len(invalid_df),
        "valid_output": valid_path,
        "invalid_output": invalid_path,
        "sqlite_stats": load_stats,
    }


if __name__ == "__main__":
    result = run_pipeline()
    print(result)
