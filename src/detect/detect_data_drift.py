import warnings

warnings.simplefilter("ignore")


import hydra
import pandas as pd
from evidently.metric_preset import DataDriftPreset
from evidently.pipeline.column_mapping import ColumnMapping
from evidently.report import Report
from kestra import Kestra
from omegaconf import DictConfig


def load_data(filename: str, date_column: str):
    return pd.read_csv(filename, parse_dates=[date_column])


def get_column_mapping(columns: DictConfig):
    column_mapping = ColumnMapping()
    column_mapping.datetime = columns.date
    column_mapping.numerical_features = columns.numerical_features
    return column_mapping


def get_dataset_drift_report(
    reference: pd.DataFrame, current: pd.DataFrame, column_mapping: ColumnMapping
):
    """
    Returns True if Data Drift is detected, else returns False.
    If get_ratio is True, returns the share of drifted features.
    """
    data_drift_report = Report(metrics=[DataDriftPreset()])
    data_drift_report.run(
        reference_data=reference, current_data=current, column_mapping=column_mapping
    )
    return data_drift_report


def detect_dataset_drift(report: Report):
    return report.as_dict()["metrics"][0]["result"]["dataset_drift"]


@hydra.main(config_path="../../config", config_name="detect", version_base=None)
def main(config: DictConfig):
    current_dates = config.dates

    reference_data = load_data(config.data.reference, config.columns.date)
    current_data = load_data(config.data.current, config.columns.date)

    columns_mapping = get_column_mapping(config.columns)

    report = get_dataset_drift_report(reference_data, current_data, columns_mapping)

    drift_detected = detect_dataset_drift(report)
    if drift_detected:
        print(
            f"Detect dataset drift between {current_dates.start} and {current_dates.end}"
        )
    else:
        print(
            f"Detect no dataset drift between {current_dates.start} and {current_dates.end}"
        )
    Kestra.outputs({"drift_detected": drift_detected})


if __name__ == "__main__":
    main()
