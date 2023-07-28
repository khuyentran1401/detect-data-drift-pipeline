import hydra
import pandas as pd
from omegaconf import DictConfig, ListConfig


def load_reference_data(data_url: str, date_interval: ListConfig):
    df = pd.read_csv(data_url, header=0, sep=",", parse_dates=["dteday"])
    return df.loc[df.dteday.between(date_interval.start, date_interval.end)]


def save_reference_data(filename: str, data: pd.DataFrame):
    data.to_csv(filename, index=False)


@hydra.main(config_path="../..", config_name="config", version_base=None)
def get_reference_data(config: DictConfig):
    df = load_reference_data(config.data.url, config.dates.reference)
    save_reference_data(config.data.reference, df)


if __name__ == "__main__":
    get_reference_data()
