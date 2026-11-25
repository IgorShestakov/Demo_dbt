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
def olist_order_reviews():
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def load_order_reviews():
        from olist_ingest_csv import load_olist_csv

        # Сохраняем исходные поля CSV. Типы для аналитики задаются в dbt.
        load_olist_csv(
            table_name="order_reviews",
            create_table_sql="""
                CREATE TABLE IF NOT EXISTS s01_extract_data.order_reviews
                (
                    review_id String,
                    order_id String,
                    review_score String,
                    review_comment_title String,
                    review_comment_message String,
                    review_creation_date String,
                    review_answer_timestamp String
                )
                ENGINE = MergeTree
                ORDER BY (order_id, review_id)
            """,
        )

    load_order_reviews()


olist_order_reviews()
