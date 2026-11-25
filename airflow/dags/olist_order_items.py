from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    start_date=pendulum.datetime(2026, 11, 22, tz="Europe/Moscow"),
    schedule="0 12 * * *",  # Ежедневно в 12:00 по Москве.
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
)
def olist_order_items():
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def load_order_items():
        from olist_ingest_csv import load_olist_csv

        # Сохраняем исходные поля CSV. Типы для аналитики задаются в dbt.
        load_olist_csv(
            table_name="order_items",
            create_table_sql="""
                CREATE TABLE IF NOT EXISTS s01_extract_data.order_items
                (
                    order_id String,
                    order_item_id String,
                    product_id String,
                    seller_id String,
                    shipping_limit_date String,
                    price String,
                    freight_value String
                )
                ENGINE = MergeTree
                ORDER BY (order_id, order_item_id)
            """,
        )

    load_order_items()


olist_order_items()
