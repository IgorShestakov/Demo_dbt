from datetime import timedelta
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import pendulum
from airflow.sdk import Connection, dag, task, Asset

orders = Asset("extract_data.orders")

KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"
ORDERS_FILE = "olist_orders_dataset.csv"


def read_orders_csv() -> bytes:
    # Читаем архив в память и берём из него только CSV с заказами.
    with urlopen(KAGGLE_URL, timeout=120) as response:
        zip_bytes = response.read()
    with ZipFile(BytesIO(zip_bytes)) as archive:
        return archive.read(ORDERS_FILE)


@dag(
    start_date=pendulum.datetime(2026, 11, 22, tz="Europe/Moscow"),
    schedule="0 12 * * *",  # Ежедневно в 12:00 по Москве.
    catchup=False,          # Пропускаем накопившиеся запуски за прошлые дни.
    max_active_runs=1,      # Одновременно обновлять таблицу может один запуск.
)
def olist_orders():
    # После ошибки — до двух повторов с паузой в пять минут.
    @task(retries=2, retry_delay=timedelta(minutes=5), outlets=[orders])
    def load_orders():
        import clickhouse_connect

        # Сначала получаем CSV, затем подключаемся к базе.
        orders_csv = read_orders_csv()
        conn = Connection.get("Clickhouse")

        with clickhouse_connect.get_client(
            host=conn.host,
            port=conn.port,
            username=conn.login,
            password=conn.password,
            database="extract_data",
        ) as client:
            # Заменяем предыдущую выгрузку полным набором заказов.
            client.command("TRUNCATE TABLE extract_data.orders")
            client.raw_insert(
                table="extract_data.orders",
                insert_block=orders_csv,
                fmt="CSVWithNames",  # Имена колонок находятся в первой строке CSV.
            )

    load_orders()  # Добавляем задачу в DAG.


olist_orders()  # Регистрируем DAG в Airflow.
