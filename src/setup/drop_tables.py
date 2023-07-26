import hydra
import psycopg2


@hydra.main(config_path="../..", config_name="config", version_base=None)
def drop_table(config):
    db = config.db
    conn = psycopg2.connect(
        host=db.host,
        port=db.port,
        database=db.database,
        user=db.username,
        password=db.password,
    )
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {db.table}")
    conn.commit()
    cursor.close()


if __name__ == "__main__":
    drop_table()
