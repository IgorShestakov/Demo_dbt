from datetime import timedelta
import os
import subprocess

from airflow.sdk import dag, task,Asset

orders = Asset("extract_data.orders")
# Пока запускаем вручную. Два запуска одновременно не допускаем.
@dag(
    schedule=[orders],
    catchup=False,
    max_active_runs=1,
)
def transformation():

    # Один повтор через пять минут при ошибке.
    @task(retries=1, retry_delay=timedelta(minutes=5))
    def build_models():

        # Передаём dbt адрес ClickHouse, доступный из контейнера.
        env = os.environ.copy()
        env["DBT_CLICKHOUSE_HOST"] = "host.docker.internal"

        # Собираем модели проекта и выполняем заданные в dbt проверки.
        subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir", "/opt/dbt",
                "--profiles-dir", "/opt/dbt",
            ],
            env=env,
            check=True,  # Ошибка dbt завершит задачу Airflow с ошибкой.
        )

    build_models()


transformation()  # Создаём DAG для обнаружения Airflow.
