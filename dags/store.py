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
QUERY_DM_TEMPLATE = lambda category: f"""
        SELECT sc.sub_category_title, SUM(sum_of_sale) AS total_sales_in_2015
        FROM sales_store AS ss
            JOIN product AS p ON ss.product_id = p.product_id
            JOIN sub_category AS sc ON p.sub_category_id = sc.sub_category_id
            JOIN category AS c ON sc.category_id = c.category_id
            JOIN customer AS cs ON cs.customer_id = ss.customer_id
            JOIN segment AS s ON s.segment_id = cs.segment_id
            JOIN order_store AS os ON os.order_id = ss.order_id
            JOIN date_conversion AS dc ON dc.date_id = os.date_id
        WHERE s.segment_title = 'Corporate' AND dc."year" = 2015
                                            AND c.category_name = '{category}'
        GROUP BY sc.sub_category_title
        ORDER BY sc.sub_category_title;
"""

table_quries = {
    'date_conversion': """
        SELECT d.date_origin,
                EXTRACT(DAY FROM d.date_origin) AS "day",
                EXTRACT(MONTH FROM d.date_origin) AS "month",
                EXTRACT(YEAR FROM d.date_origin) AS "year",
                EXTRACT(QUARTER FROM d.date_origin) AS "quarter",
                EXTRACT(ISODOW FROM d.date_origin) AS day_of_week,
                EXTRACT(WEEK FROM d.date_origin) AS "week"
        FROM (
            SELECT DISTINCT "Order Date" AS date_origin FROM raw_store
            UNION
            SELECT DISTINCT "Ship Date" AS date_origin FROM raw_store
        ) AS d
        ORDER BY d.date_origin;
    """,
    'order_store': """
        SELECT DISTINCT rs."Order ID" AS order_id_from_raw,
                        dc.date_id
        FROM raw_store AS rs
        JOIN date_conversion AS dc ON rs."Order Date" = dc.date_origin
        ORDER BY order_id_from_raw;
    """,
    'ship_mode': """
        SELECT DISTINCT "Ship Mode" AS mode_title FROM raw_store;
    """,
    'shipment': """
        SELECT DISTINCT dc.date_id, sm.mode_id
        FROM raw_store AS rs
        JOIN date_conversion AS dc ON rs."Ship Date" = dc.date_origin
        JOIN ship_mode AS sm ON rs."Ship Mode" = sm.mode_title
        ORDER BY dc.date_id DESC;
    """,
    'segment': """
        SELECT DISTINCT rs."Segment" AS segment_title
        FROM raw_store AS rs;
    """,
    'customer': """
        SELECT DISTINCT rs."Customer ID" AS customer_id_from_raw,
                        rs."Customer Name" AS customer_name,
                        s.segment_id
        FROM raw_store AS rs
        JOIN segment AS s ON s.segment_title = rs."Segment"
        ORDER BY customer_id_from_raw;
    """,
    'category': """
        SELECT DISTINCT rs."Category" AS category_name
        FROM raw_store AS rs;
    """,
    'sub_category': """
        SELECT DISTINCT rs."Sub-Category" AS sub_category_title, c.category_id
        FROM raw_store AS rs
        JOIN category AS c ON rs."Category" = c.category_name;
    """,
    'product': """
        WITH
        price AS (
            SELECT rs."Product ID",
                rs."Product Name",
                ROUND(AVG(rs.price), 2) AS price
            FROM (
                SELECT rs."Product ID",
                    rs."Product Name",
                    ROUND(
                        (rs."Sales" / (1.00 - rs."Discount")) / rs."Quantity",
                        2
                    ) AS price
                FROM raw_store AS rs
            ) AS rs
            GROUP BY rs."Product ID", rs."Product Name"
        )

        SELECT DISTINCT rs."Product ID" AS product_id_from_raw,
                        rs."Product Name" AS product_name,
                        sc.sub_category_id,
                        pr.price
        FROM raw_store AS rs
        JOIN sub_category AS sc ON rs."Sub-Category" = sc.sub_category_title
        JOIN price AS pr ON rs."Product ID" = pr."Product ID"
                            AND rs."Product Name" = pr."Product Name";
    """,
    'country': """
        SELECT DISTINCT rs."Country" AS country_title FROM raw_store rs;
    """,
    'region': """
        SELECT DISTINCT rs."Region" AS region_title, c.country_id
        FROM raw_store AS rs
        JOIN country AS c ON rs."Country" = c.country_title;
    """,
    'state': """
        SELECT DISTINCT rs."State" AS state_title, r.region_id
        FROM raw_store AS rs
        JOIN region AS r ON rs."Region" = r.region_title
        ORDER BY 1;
    """,
    'city': """
        SELECT DISTINCT rs."City" AS city_title, s.state_id
        FROM raw_store AS rs
        JOIN state AS s ON rs."State" = s.state_title
        ORDER BY 1;
    """,
    'postal_code': """
        SELECT DISTINCT rs."Postal Code" AS postal_code, c.city_id
        FROM raw_store AS rs
        JOIN (
            SELECT c.city_id, c.city_title, c.state_id, s.state_title
            FROM city AS c
            JOIN state AS s ON c.state_id = s.state_id
        ) AS c ON rs."City" = c.city_title AND c.state_title = rs."State"
        ORDER BY 1;
    """,
    'sales_store': """
        WITH
        sales AS (
            SELECT os.order_id, sh.shipment_id, cu.customer_id,
                   ps.postal_code_id, pr.product_id, rs."Sales" AS sum_of_sale,
                   rs."Quantity" AS quantity, rs."Discount" AS discount,
                   rs."Profit" AS profit,
                   pr.price * rs."Quantity" AS sum_by_price
            FROM raw_store AS rs
            JOIN order_store AS os ON rs."Order ID" = os.order_id_from_raw
            JOIN (
                SELECT s.shipment_id, s.date_id, dc.date_origin, sm.mode_title
                FROM shipment AS s
                JOIN date_conversion AS dc ON s.date_id = dc.date_id
                JOIN ship_mode AS sm ON s.mode_id = sm.mode_id
            ) AS sh ON rs."Ship Date" = sh.date_origin
                       AND rs."Ship Mode" = sh.mode_title
            JOIN customer AS cu ON rs."Customer ID" = cu.customer_id_from_raw
            JOIN postal_code AS ps ON rs."Postal Code" = ps.postal_code
            JOIN product AS pr ON rs."Product ID" = pr.product_id_from_raw
                                  AND rs."Product Name" = pr.product_name
        )

        SELECT order_id, shipment_id, customer_id, postal_code_id, product_id,
            SUM(sum_of_sale) AS sum_of_sale,
            SUM(quantity) AS quantity,
            ROUND(
                (SUM(sum_by_price) - SUM(sum_of_sale)) / SUM(sum_by_price),
                2
            ) AS discount,
            SUM(profit) AS profit
        FROM sales
        GROUP BY order_id, shipment_id, customer_id, postal_code_id, product_id
        ORDER BY order_id;
    """
}


def download_csv():
    """Скачивает файл с API Яндекс диска"""

    base_url = ('https://cloud-api.yandex.net/v1/disk/public/'
                'resources/download?')
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


def get_connect(connection_title):
    """Возвращает объект подключения к БД PostgreSQL"""

    hook = BaseHook.get_connection(connection_title)
    conn = connect(
        host=hook.host, port=hook.port,
        database=hook.schema, user=hook.login,
        password=hook.password
    )
    return conn


def read_csv(file_path):
    """выделим чтение csv в отдельную функцию"""

    data = []
    with open(file_path) as csvfile:
        reader = csv.reader(csvfile)
        headers = [f'"{i}"' for i in next(reader)]
        for row in reader:
            data.append(tuple(row))
    return headers, data


def clean_table(connection, table_list):
    """Очищает данные из таблиц"""

    cursor = connection.cursor()

    for table in table_list:
        cursor.execute(f"DELETE FROM {table};")

    connection.commit()
    cursor.close()
    return connection


def select_data(conn, query):
    """Шаблон выбирки данных из таблицы по запросу"""

    cursor = conn.cursor()
    cursor.execute(query)
    headers = [f'"{col[0]}"' for col in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return headers, data


def load_data_by_psycopg2(table_name, connection, headers, data):
    """функция для перезагрузки данных в PSQL c переменными вход. данными"""

    try:
        cursor = connection.cursor()

        # удалим сначала всё содержимое таблицы
        if table_name == Variable.get('TABLE_NAME'):
            cursor.execute(f"DELETE FROM {table_name};")

        # далее загружаем в базу
        headers_for_query = ', '.join(headers)
        query = f'INSERT INTO {table_name} ({headers_for_query}) VALUES ('
        cursor.executemany(
            query + ', '.join(['%s'] * len(headers)) + ');',
            data
        )

        # закрываем соединение
        connection.commit()
        cursor.close()
        print(f"Данные успешно загружены в таблицу {table_name}!")
        return connection
    except Exception as error:
        connection.rollback()
        raise Exception(
                f'Загрузить обновлённые данные не получилось: {error}!'
        )


def load_raw_data():
    """Загружает cырые данные в PostgreSQL"""

    # подключаемся к БД raw_store
    conn = get_connect('raw_store')

    # считывааем и загружаем данные
    headers, data = read_csv(RAW_DATA_PATH)
    table_name = Variable.get('TABLE_NAME')
    # сначала очистим таблицу
    conn = clean_table(conn, [table_name])
    # потом загрузим свежие данные
    conn = load_data_by_psycopg2(table_name, conn, headers, data)
    conn.close()


def migrate_data():
    """Перенос таблицы из raw_store в core_store"""

    conn = get_connect('raw_store')
    table_name = Variable.get('TABLE_NAME')
    query = (f"SELECT * FROM {table_name} AS rs WHERE "
             "rs.\"Segment\" = 'Corporate';")
    headers, data = select_data(conn, query)

    # перезальём данные в core-слое
    conn = clean_table(get_connect('core_store'), [table_name])
    conn = load_data_by_psycopg2(table_name, conn, headers, data)
    conn.close()


def load_core_data():
    """Наполнение обновлёнными данными core-слоя"""

    # получим соединие и очистим таблицы
    conn = clean_table(
        get_connect('core_store'),
        list(table_quries.keys())[::-1]
    )

    # для каждой таблицы последовательно получим выборку и зальём в core-слой
    for table, query in table_quries.items():
        headers, data = select_data(conn, query)
        conn = load_data_by_psycopg2(table, conn, headers, data)

    conn.close()


def create_data_mart(query, table_name):
    """
    шаблон создания витрины данных (выгрузка из core-store,
    загрузка в mart-store)
    """

    conn = get_connect('core_store')
    headers, data = select_data(conn, query)

    # перезальём данные в mart-слое
    conn = clean_table(get_connect('mart_store'), [table_name])
    conn = load_data_by_psycopg2(table_name, conn, headers, data)
    conn.close()


def create_data_mart_by_year():
    """Витрина данных по продажам в разрезе категорий и лет"""

    query = """
        SELECT dc."year", c.category_name, SUM(ss.sum_of_sale) AS sum_of_sale
        FROM sales_store AS ss
        JOIN order_store AS os ON ss.order_id = os.order_id
        JOIN date_conversion AS dc ON os.date_id = dc.date_id
        JOIN product AS p ON ss.product_id = p.product_id
        JOIN sub_category AS sc ON p.sub_category_id = sc.sub_category_id
        JOIN category AS c ON c.category_id = sc.category_id
        GROUP BY dc."year", c.category_name
        ORDER BY dc."year", c.category_name;
    """
    create_data_mart(query, 'sales_by_year')


def get_random_category(**kwargs):
    """выбирает случайное название категории и передаёт в следующий task"""

    conn = get_connect('core_store')
    query = """
        SELECT category_name FROM category ORDER BY random() LIMIT 1;
    """
    _, data = select_data(conn, query)
    kwargs['ti'].xcom_push(key='category', value=data[0][0])


def category_definition_func(**kwargs):
    """узнаем продажи суб-категорий какой категории мы будем смотреть"""

    category = kwargs['ti'].xcom_pull(
        key='category', task_ids='get_random_category'
    )

    if category == 'Furniture':
        return 'get_furniture_sales'
    elif category == 'Office Supplies':
        return 'get_office_supp_sales'
    elif category == 'Technology':
        return 'get_technology_sales'


def query_dm_template(category):
    """шаблон запроса для суб-категорий"""

    return f"""
        SELECT sc.sub_category_title, SUM(sum_of_sale) AS total_sales_in_2015
        FROM sales_store AS ss
            JOIN product AS p ON ss.product_id = p.product_id
            JOIN sub_category AS sc ON p.sub_category_id = sc.sub_category_id
            JOIN category AS c ON sc.category_id = c.category_id
            JOIN customer AS cs ON cs.customer_id = ss.customer_id
            JOIN segment AS s ON s.segment_id = cs.segment_id
            JOIN order_store AS os ON os.order_id = ss.order_id
            JOIN date_conversion AS dc ON dc.date_id = os.date_id
        WHERE s.segment_title = 'Corporate' AND dc."year" = 2015
                                            AND c.category_name = '{category}'
        GROUP BY sc.sub_category_title
        ORDER BY sc.sub_category_title;
    """


def get_furniture_sales(**kwargs):
    """витрина продажи суб-категорий по категории 'Furniture' за 2015 год"""

    query = query_dm_template('Furniture')
    create_data_mart(query, 'sub_category_sales')


def get_office_supp_sales(**kwargs):
    """
    витрина продажи суб-категорий по категории 'Office Supplies' за 2015 год
    """

    query = query_dm_template('Office Supplies')
    create_data_mart(query, 'sub_category_sales')


def get_technology_sales(**kwargs):
    """витрина продажи суб-категорий по категории 'Technology' за 2015 год"""

    query = query_dm_template('Technology')
    create_data_mart(query, 'sub_category_sales')


# DAG


default_args = {
    'owner': 'alexey',
    'retries': 1,  # сколько перезапусков можно после первой ошибки при запуске
    'retry_delay': 60,  # через сколько секунд перезапускать даг
    'start_date': datetime(2022, 7, 26),
    'depends_on_past': False  # зависимость от предыдущих запусков дага
}

dag = DAG(
    'STORE',
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

# загружаем в raw_store сырые данные
load_raw_data_to_psql = PythonOperator(
    task_id='load_raw_data',
    python_callable=load_raw_data,
    dag=dag
)

# преобразуем даты raw_store в корректный формат
change_date_format = PostgresOperator(
    task_id='change_date_format',
    dag=dag,
    postgres_conn_id='raw_store',
    sql="""
        UPDATE raw_store
        SET "Order Date" = TO_DATE("Order Date", 'MM/DD/YYYY'),
            "Ship Date" = TO_DATE("Ship Date", 'MM/DD/YYYY');
    """
)

# проведём фильтрацию и миграцию данных из raw-слоя в core-слой
migrate_raw_to_core = PythonOperator(
    task_id='migrate_data',
    python_callable=migrate_data,
    dag=dag
)

# заполняем нормализованные таблицы core-слоя
load_core_data_to_psql = PythonOperator(
    task_id='load_core_data',
    python_callable=load_core_data,
    dag=dag
)

# создаём витрину данных в отдельной таблице
sum_of_sale_by_year = PythonOperator(
    task_id='create_mart_by_year',
    python_callable=create_data_mart_by_year,
    dag=dag
)

# выбирает случайное значение из category и передаём следующей task-е
random_category = PythonOperator(
    task_id='get_random_category',
    python_callable=get_random_category,
    dag=dag
)

# создадим ветвление в зависимости от названия категории
category_definition = BranchPythonOperator(
    task_id='category_definition',
    python_callable=category_definition_func,
    dag=dag
)

# получаем витрину данных по категории "Furniture"
furniture_sales = PythonOperator(
    task_id='get_furniture_sales',
    python_callable=get_furniture_sales,
    dag=dag
)

# получаем витрину данных по категории "Office Supplies"
office_supp_sales = PythonOperator(
    task_id='get_office_supp_sales',
    python_callable=get_office_supp_sales,
    dag=dag
)

# получаем витрину данных по категории "Technology"
technology_sales = PythonOperator(
    task_id='get_technology_sales',
    python_callable=get_technology_sales,
    dag=dag
)

# пустой таск, в качестве завершения
end = DummyOperator(
    task_id='end', dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS
)


start >> mkdir >> check_file_branch >> [download_file, sensor_file]
download_file >> sensor_file >> load_raw_data_to_psql >> change_date_format
change_date_format >> migrate_raw_to_core >> load_core_data_to_psql
load_core_data_to_psql >> sum_of_sale_by_year >> random_category
random_category >> category_definition >> [
    furniture_sales, office_supp_sales, technology_sales
] >> end
