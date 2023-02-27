from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
import json
from dataclasses import dataclass
import logging

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='postgres_calculation',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)


@dataclass
class Item:
    value: int
    score: float
    ocr_score: float
    bounding_box: [int]


@dataclass
class Model:
    business_id: int
    total: [Item]
    line_items: [Item]


def raw_to_model(raw: str) -> Model:
    try:
        js = json.loads(raw)
        business_id = js.get('business_id', -1)
        total = js.get('total', [])
        line_items = js.get('line_items', [])
        [logging.info(i) for i in total if i != {}]
        total_model = [Item(item.get('value'), item.get('score'), item.get('ocr_score'), item.get('bounding_box')) for item in total if item != {}]
        line_items_model = [Item(item.get('value'), item.get('score'), item.get('ocr_score'), item.get('bounding_box')) for item in line_items if item != {}]

        return Model(business_id, total_model, line_items_model)
    except Exception as ex:
        logging.error(f'Parse error: {raw}')
        raise ex


def aggregate(values) -> str:
    cnt = {}
    cnt_total = {}
    cnt_line_items = {}
    for i in values:
        cnt[i.business_id] = cnt.get(i.business_id, 0) + 1
        cnt_total[i.business_id] = cnt_total.get(i.business_id, 0) + len(i.total)
        cnt_line_items[i.business_id] = cnt_line_items.get(i.business_id, 0) + len(i.line_items)
    return json.dumps({'cnt': cnt, 'cnt_total': cnt_total, 'cnt_line_items': cnt_line_items})


def calculate_and_save() -> None:
    postgres_hook = PostgresHook(postgres_conn_id='pg')
    result = postgres_hook.get_records("SELECT ml_response FROM documents")
    res: list[Model] = [raw_to_model(i[0]) for i in result]
    agg = aggregate(res)
    logging.info(agg)
    postgres_hook.run("INSERT INTO parsed_total (aggregation) VALUES (%s)", parameters=(agg,))


t1 = PythonOperator(
    task_id='calculate_and_save',
    python_callable=calculate_and_save,
    dag=dag,
)
