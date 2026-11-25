from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

from airflow.sdk import Connection


KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"


def load_olist_csv(table_name: str, create_table_sql: str) -> None:
    """Полностью обновить одну исходную таблицу из одноимённого CSV Olist."""
    import clickhouse_connect

    # Сначала получаем CSV в память. Ошибка скачивания не затронет таблицу.
    with urlopen(KAGGLE_URL, timeout=120) as response:
        zip_bytes = response.read()
    with ZipFile(BytesIO(zip_bytes)) as archive:
        csv_data = archive.read(f"olist_{table_name}_dataset.csv")

    conn = Connection.get("Clickhouse")
    with clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database="s01_extract_data",
    ) as client:
        # Имена таблиц и их структура заданы в коде DAG.
        client.command(create_table_sql)
        client.command(f"TRUNCATE TABLE {table_name}")
        client.raw_insert(table=table_name, insert_block=csv_data, fmt="CSVWithNames")
