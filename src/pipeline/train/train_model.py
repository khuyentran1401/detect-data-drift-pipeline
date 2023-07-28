import hydra
import joblib
import numpy as np
import pandas as pd
from omegaconf import DictConfig
from sklearn import metrics
from sklearn.linear_model import Ridge
from sklearn.model_selection import GridSearchCV


def load_current_data(filename: str, date_column: str):
    return pd.read_csv(filename, header=0, sep=",", parse_dates=[date_column])


def coerce_to_categorical(df: pd.DataFrame, columns: list):
    for column in columns:
        df[column] = df[column].astype("category")
    return df


def split_train_test(df: pd.DataFrame, test_size: float):
    split_index = int(df.shape[0] * (1 - test_size))
    train_df = df.iloc[:split_index]
    test_df = df.iloc[split_index:]
    return train_df, test_df


def split_X_y(train_df: pd.DataFrame, test_df: pd.DataFrame, columns: DictConfig):
    X_train, y_train = train_df.drop(columns.drop, axis=1), train_df[columns.target]
    X_test, y_test = test_df.drop(columns.drop, axis=1), test_df[columns.target]
    return X_train, X_test, y_train, y_test


def rmsle(y, y_):
    log1 = np.nan_to_num(np.array([np.log(v + 1) for v in y]))
    log2 = np.nan_to_num(np.array([np.log(v + 1) for v in y_]))
    calc = (log1 - log2) ** 2
    return np.sqrt(np.mean(calc))


def train_model(X_train: pd.DataFrame, y_train: pd.Series, model_params: DictConfig):
    y_train_log = np.log1p(y_train)
    model = Ridge()
    scorer = metrics.make_scorer(rmsle, greater_is_better=True)
    params = dict(model_params)
    grid = GridSearchCV(model, params, scoring=scorer, cv=3, verbose=3)
    grid.fit(X_train, y_train_log)
    print(f"Best params: {grid.best_params_}")
    return grid


def evaluate_model(model: Ridge, X_test: pd.DataFrame, y_test: pd.Series):
    y_pred_log = model.predict(X_test)
    y_pred = np.expm1(y_pred_log)
    print("Actual vs Predicted")
    score = rmsle(y_test, y_pred)
    print(f"RMSLE: {score}")


def save_model(model: Ridge, filename: str):
    joblib.dump(model, filename)


@hydra.main(config_path="../../../config", config_name="train", version_base=None)
def train(config: DictConfig):
    df = load_current_data(config.data.current, config.columns.date)
    df = coerce_to_categorical(df, config.columns.categorical)
    train_df, test_df = split_train_test(df, config.process.test_size)
    X_train, X_test, y_train, y_test = split_X_y(train_df, test_df, config.columns)
    model = train_model(X_train, y_train, config.model.params)
    evaluate_model(model, X_test, y_test)
    save_model(model, config.model.path)


if __name__ == "__main__":
    train()
