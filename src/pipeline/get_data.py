import warnings

warnings.simplefilter("ignore")


import hydra
import pandas as pd
from omegaconf import DictConfig, ListConfig


def load_data(data_url: str):
    return pd.read_csv(data_url, header=0, sep=",", parse_dates=["dteday"])


def get_batch_of_data(raw_data: pd.DataFrame, date_interval: ListConfig):
    print(f"Getting data from {date_interval.start} to {date_interval.end}")
    return raw_data.loc[raw_data.dteday.between(date_interval.start, date_interval.end)]


def save_data(data: pd.DataFrame, file_name: str):
    print(f"Save data to {file_name}")
    data.to_csv(file_name, sep=",", index=False)


@hydra.main(config_path="../..", config_name="config", version_base=None)
def get_data(config: DictConfig):
    raw_data = load_data(config.data.url)
    current_dates = config.dates.current
    current_data = get_batch_of_data(raw_data, current_dates)
    save_data(current_data, config.data.current)


if __name__ == "__main__":
    get_data()
