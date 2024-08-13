#!/usr/bin/env python 

import json
import os
from pathlib import Path
from importlib.resources import files
from loguru import logger

import hydra
import polars as pl
from omegaconf import DictConfig, OmegaConf
from aces import config, predicates, query
from tqdm import tqdm

from MEDS_tabular_automl.describe_codes import get_feature_columns, filter_parquet


def get_events_df(shard_df: pl.DataFrame, feature_columns) -> pl.DataFrame:
    """Extracts Events DataFrame with one row per observation (timestamps can be duplicated)"""
    # Filter out feature_columns that were not present in the training set
    raw_feature_columns = ["/".join(c.split("/")[:-1]) for c in feature_columns]
    shard_df = shard_df.filter(pl.col("code").is_in(raw_feature_columns))
    # Drop rows with missing timestamp or code to get events
    ts_shard_df = shard_df.drop_nulls(subset=["timestamp", "code"])
    return ts_shard_df


def get_unique_time_events_df(events_df: pl.DataFrame):
    """Updates Events DataFrame to have unique timestamps and sorted by patient_id and timestamp."""
    assert events_df.select(pl.col("timestamp")).null_count().collect().item() == 0
    # Check events_df is sorted - so it aligns with the ts_matrix we generate later in the pipeline
    events_df = (
        events_df.drop_nulls("timestamp")
        .select(pl.col(["patient_id", "timestamp"]))
        .unique(maintain_order=True)
    )
    assert events_df.sort(by=["patient_id", "timestamp"]).collect().equals(events_df.collect())
    return events_df

config_path = files("MEDS_tabular_automl").joinpath("configs")

@hydra.main(version_base=None, config_path=str(config_path.resolve()), config_name="task_specific_caching")
def main(cfg):

    logger.info(f"Running task extraction with config:\n{OmegaConf.to_yaml(cfg)}")

    tabularized_data_dir = Path(cfg.input_dir)
    MEDS_path = Path(cfg.MEDS_cohort_dir)

    task_cfg_fp = Path(os.environ["MEDS_TAB_MIMIC_IV_DIR"]) / "tasks" / f"{cfg.task_name}.yaml"

    logger.info(f"Loading task config from {str(task_cfg_fp.resolve())}")

    # create task configuration object
    task_cfg = config.TaskExtractorConfig.load(config_path=task_cfg_fp)

    # location of MEDS format Data
    cohort_dir = MEDS_path / "final_cohort"
    # output directory for tables with event_ids and labels
    output_dir = MEDS_path / cfg.task_name / "labels"

    shard_fps = list(cohort_dir.glob("**/*.parquet"))

    shard_fps_str = "\n".join(f"  * {str(fp.resolve())}" for fp in shard_fps)
    logger.info(f"Processing files:\n{shard_fps_str}")

    for in_fp in tqdm(shard_fps):
        shard_pfx = str(in_fp.relative_to(MEDS_path))
        out_fp = output_dir / shard_pfx
        out_fp.parent.mkdir(parents=True, exist_ok=True)
        # one of the following
        data_cfg = DictConfig({"path": str(in_fp.resolve()), "standard": "meds", "ts_format": None})
        predicates_df = predicates.get_predicates_df(task_cfg, data_cfg)

        # execute query
        df_result = query.query(task_cfg, predicates_df)
        label_df = (
            df_result.select(pl.col(["subject_id", "trigger", "label"]))
            .rename({"trigger": "timestamp", "subject_id": "patient_id"})
            .sort(by=["patient_id", "timestamp"])
        )
        feature_columns = get_feature_columns(cfg.tabularization.filtered_code_metadata_fp)

        data_df = filter_parquet(in_fp, cfg.tabularization._resolved_codes)
        data_df = get_unique_time_events_df(get_events_df(data_df, feature_columns))
        data_df = data_df.drop(["code", "numerical_value"])
        data_df = data_df.with_row_index("event_id")

        output_df = label_df.lazy().join_asof(other=data_df, by="patient_id", on="timestamp")

        # store it
        output_df.collect().write_parquet(out_fp, use_pyarrow=True)
        logger.info(f"Done with {shard_pfx}!")


if __name__ == "__main__":
    main()
