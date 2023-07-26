import hydra
import pandas as pd
from omegaconf import DictConfig, ListConfig
from sqlalchemy import create_engine


def load_data(data_url: str):
    return pd.read_csv(data_url, header=0, sep=",", parse_dates=["dteday"])


def get_empty_current_df(df: pd.DataFrame):
    return pd.DataFrame([], columns=df.columns)


def get_reference_df(df: pd.DataFrame, date_interval: ListConfig):
    return df.loc[df.dteday.between(date_interval.start, date_interval.end)]


def save_to_db(df: pd.DataFrame, db: DictConfig, table_name: str):
    engine = create_engine(
        f"postgresql://{db.username}:{db.password}@{db.host}:{db.port}/{db.database}"
    )
    df.to_sql(table_name, engine, if_exists="replace", index=False)


@hydra.main(config_path="../..", config_name="config", version_base=None)
def create_table(config: DictConfig):
    df = load_data(config.data.url)
    current_df = get_empty_current_df(df)
    reference_df = get_reference_df(df, config.dates.reference)
    save_to_db(current_df, config.db, table_name="current")
    save_to_db(reference_df, config.db, table_name="reference")


if __name__ == "__main__":
    create_table()
