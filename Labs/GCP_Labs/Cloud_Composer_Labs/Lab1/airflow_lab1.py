from datetime import datetime
from pathlib import Path
from airflow.decorators import dag, task
from google.cloud import bigquery



PROJECT = "gcp-lab1-514817"
FILE = Path(__file__).parent / "top_names.csv"
TABLE = f"{PROJECT}.lab1.top_names"

default_args = {
    'owner': 'Abhishek Pramanik', 
    'start_date': datetime(2024, 6, 13),
    'retries': 1,
    'retry_delay': 120,
}

@dag(
    default_args=default_args,
    schedule="@once",
    catchup=False,
    is_paused_upon_creation=False,
)
def cloud_composer_lab1():
    @task
    def query_bigquery():
        sql = """
            SELECT name, SUM(number) AS total
            FROM `bigquery-public-data.usa_names.usa_1910_current`
            WHERE year = 2000
            GROUP BY name ORDER BY total DESC LIMIT 10
        """
        return [tuple(row) for row in bigquery.Client(project=PROJECT).query(sql).result()]

    @task
    def save_csv(rows):
        FILE.write_text("name,total\n" + "\n".join(f"{name},{total}" for name, total in rows))
        print(f"Saved {FILE}")

    @task
    def read_csv():
        print(FILE.read_text())

    @task
    def save_to_bigquery(rows):
        client = bigquery.Client(project=PROJECT)
        dataset = bigquery.Dataset(f"{PROJECT}.lab1")
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("total", "INTEGER"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )
        data = [{"name": name, "total": total} for name, total in rows]
        client.load_table_from_json(data, TABLE, job_config=job_config).result()
        print(f"Saved {len(data)} rows to {TABLE}")

    rows = query_bigquery()
    save_csv(rows) >> read_csv()
    save_to_bigquery(rows)


cloud_composer_lab1()