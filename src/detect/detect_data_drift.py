import warnings

warnings.simplefilter("ignore")


import hydra
import pandas as pd
from evidently.metric_preset import DataDriftPreset
from evidently.pipeline.column_mapping import ColumnMapping
from evidently.report import Report
from kestra import Kestra
from omegaconf import DictConfig, ListConfig


def load_reference_data(filename: str):
    return pd.read_csv(filename, header=0, sep=",", parse_dates=["dteday"])


def load_current_data(data_url: str, date_interval: ListConfig = None):
    df = pd.read_csv(data_url, header=0, sep=",", parse_dates=["dteday"])

    print(f"Getting data from {date_interval.start} to {date_interval.end}")
    return df.loc[df.dteday.between(date_interval.start, date_interval.end)]


def get_column_mapping(columns: DictConfig):
    column_mapping = ColumnMapping()
    column_mapping.datetime = columns.datetime
    column_mapping.numerical_features = columns.numerical_features
    return column_mapping


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


def save_data(data: pd.DataFrame, file_name: str):
    print(f"Save data to {file_name}")
    data.to_csv(file_name, sep=",", index=False)


@hydra.main(config_path="../../config", config_name="detect", version_base=None)
def main(config: DictConfig):
    current_dates = config.dates

    reference_data = load_reference_data(config.data.reference)
    current_data = load_current_data(config.data.url, current_dates)

    columns_mapping = get_column_mapping(config.columns)
    save_data(current_data, config.data.current)

    drift_detected = detect_dataset_drift(reference_data, current_data, columns_mapping)
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
