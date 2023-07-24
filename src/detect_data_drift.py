import warnings

warnings.simplefilter("ignore")

import io
import zipfile

import hydra
import pandas as pd
import requests
from evidently.metric_preset import DataDriftPreset
from evidently.pipeline.column_mapping import ColumnMapping
from evidently.report import Report
from omegaconf import DictConfig, ListConfig


def load_data(data_url: str):
    content = requests.get(data_url).content
    with zipfile.ZipFile(io.BytesIO(content)) as arc:
        raw_data = pd.read_csv(
            arc.open("day.csv"), header=0, sep=",", parse_dates=["dteday"]
        )
    return raw_data


def get_column_mapping(columns: DictConfig):
    column_mapping = ColumnMapping()
    column_mapping.datetime = columns.datetime
    column_mapping.numerical_features = columns.numerical_features
    return column_mapping


def get_batch_of_data(raw_data: pd.DataFrame, date_interval: ListConfig):
    start_date = date_interval[0]
    end_date = date_interval[1]
    return raw_data.loc[raw_data.dteday.between(start_date, end_date)]


def detect_dataset_drift(
    reference: pd.DataFrame, production: pd.DataFrame, column_mapping: ColumnMapping
):
    """
    Returns True if Data Drift is detected, else returns False.
    If get_ratio is True, returns the share of drifted features.
    """
    data_drift_report = Report(metrics=[DataDriftPreset()])
    data_drift_report.run(
        reference_data=reference, current_data=production, column_mapping=column_mapping
    )
    report = data_drift_report.as_dict()
    return report["metrics"][0]["result"]["dataset_drift"]


@hydra.main(config_path="..", config_name="config", version_base=None)
def main(config: DictConfig):
    raw_data = load_data(config.data.url)
    columns_mapping = get_column_mapping(config.columns)
    reference_data = get_batch_of_data(raw_data, config.dates.reference)

    for dates in config.dates.current:
        current_data = get_batch_of_data(raw_data, dates)
        if detect_dataset_drift(reference_data, current_data, columns_mapping):
            print(f"Detect dataset drift between {dates[0]} and {dates[1]}")
        else:
            print(f"Detect no dataset drift between {dates[0]} and {dates[1]}")


if __name__ == "__main__":
    main()
