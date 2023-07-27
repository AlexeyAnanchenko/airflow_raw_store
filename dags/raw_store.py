from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.operators.postgres import PostgresOperator

import os
import csv
import requests
from psycopg2 import connect
from urllib.parse import urlencode

# создадим все пути
DIR_PATH = f'{Variable.get("RAW_DATA")}'
SUPERMARKET_PATH = f'{DIR_PATH}/supermarket_1'
RAW_DATA_PATH = f'{SUPERMARKET_PATH}/Sample_Superstore.csv'


def download_csv():
    """Скачивает файл с API Яндекс диска"""

    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    public_key = Variable.get("URL_FILE")

    # Получаем загрузочную ссылку
    final_url = base_url + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    download_url = response.json()['href']

    # Загружаем файл и сохраняем его
    download_response = requests.get(download_url)

    if download_response.status_code == 200:
        with open(RAW_DATA_PATH, 'w') as f:
            f.write(download_response.text)
            print('Файл успешно скачан!')
    else:
        raise ValueError(f'failed to download file: {public_key}')


def check_file_at_path():
    """Проверяет наличие файла и создаёт ветвление"""

    if os.path.exists(RAW_DATA_PATH):
        return 'sensor_file'
    return 'download_file'


def read_csv(file_path):
    """выделим чтение csv в отдельную функцию"""

    data = []
    with open(file_path) as csvfile:
        reader = csv.reader(csvfile)
        headers = [f'"{i}"' for i in next(reader)]
        for row in reader:
            data.append(tuple(row))
    return headers, data


def load_data_psql(**kwargs):
    """Загружает cырые данные в PostgreSQL"""

    # соединяемся с БД
    hook = BaseHook.get_connection('raw_store')
    conn = connect(
        host=hook.host, port=hook.port,
        database=hook.schema, user=hook.login,
        password=hook.password
    )
    cursor = conn.cursor()

    try:
        # удалим сначала всё содержимое таблицы
        table_name = Variable.get('TABLE_NAME')
        cursor.execute(f"DELETE FROM {table_name};")

        # считаем и загрузим данные
        headers, data = read_csv(RAW_DATA_PATH)
        headers_for_query = ', '.join(headers)
        query = f'INSERT INTO {table_name} ({headers_for_query}) VALUES ('
        cursor.executemany(
            query + ', '.join(['%s'] * len(headers)) + ');',
            data
        )
        conn.commit()

        # получим количество строк записанных в таблицу и отправим в xcom
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count_rows = cursor.fetchone()[0]
        kwargs['task_instance'].xcom_push(key='count_rows', value=count_rows)
        cursor.close()
        conn.close()
        print("Данные успешно загружены в таблицу!")
    except Exception as error:
        conn.rollback()
        raise Exception(f'Загрузить обновлённые данные не получилось: {error}!')


default_args = {
    'owner': 'alexey',
    'retries': 1,  # сколько перезапусков можно после первой ошибки при запуске
    'retry_delay': 60,  # через сколько секунд перезапускать даг
    'start_date': datetime(2022, 7, 26),
    'depends_on_past': False  # зависимость от предыдущих запусков дага
}

dag = DAG(
    'raw_store',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# пустой таск, в качестве старта
start = DummyOperator(task_id='start', dag=dag)

# создадим папку куда будем скачивать исходники для БД
mkdir = BashOperator(
    task_id='mkdir',
    bash_command=f'mkdir -p {SUPERMARKET_PATH}',
    dag=dag
)

# проверяем есть ли файл по пути
check_file_branch = BranchPythonOperator(
    task_id='check_file_at_path',
    python_callable=check_file_at_path,
    dag=dag
)

# Скачивает файл, если нет по пути
download_file = PythonOperator(
    task_id='download_file',
    python_callable=download_csv,
    dag=dag
)

# проверяем появление (наличие) файла
sensor_file = FileSensor(
    task_id='sensor_file',
    filepath=RAW_DATA_PATH,
    # По умолчанию для запуска данного оператора должны быть выполнены оба
    # предыдущих оператора: download_file и check_file_branch но нам
    # достаточно одного из них
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

# загружаем в postgresql сырые данные
load_postgres = PythonOperator(
    task_id='load_data_to_psql',
    python_callable=load_data_psql,
    dag=dag
)

# выводим кол-во загруженных строк в БД с помощью xcom
echo_count_rows = BashOperator(
    task_id='echo_count_rows',
    bash_command="echo {{ task_instance.xcom_pull(task_ids=\'load_data_to_"
                 "psql\', key=\'count_rows\') }}",
    dag=dag
)

# создадим витрину
create_mart_store = PostgresOperator(
    task_id='create_mart_store',
    dag=dag,
    postgres_conn_id='raw_store',
    sql="""
        CREATE MATERIALIZED VIEW IF NOT EXISTS public.mart_store AS

        WITH rs_extract_year AS (
            SELECT *, EXTRACT(YEAR FROM "Order Date") AS "year_order"
            FROM public.raw_store
        )

        SELECT rs."year_order", rs."Category", SUM(rs."Sales") AS "sum_of_sale"
        FROM rs_extract_year rs
        WHERE rs."Segment" = 'Corporate'
        GROUP BY rs."year_order", rs."Category"
        ORDER BY rs."year_order", rs."Category";
    """
)

# пустой таск, в качестве завершения
end = DummyOperator(task_id='end', dag=dag)


start >> mkdir >> check_file_branch >> [download_file, sensor_file]
download_file >> sensor_file >> load_postgres >> echo_count_rows
echo_count_rows >> create_mart_store >> end
