from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

import json


TABLE_QUERY_CORE = {
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

QUERY_DM_1 = """
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


def set_var_func():
    """добавляем переменные в airflow"""

    # Преобразуем словарь в строку JSON,
    queries_json_core = json.dumps(TABLE_QUERY_CORE)
    Variable.set("TABLE_QUERY_CORE", queries_json_core)

    Variable.set("QUERY_DM_1", QUERY_DM_1)
    Variable.set("RAW_DATA", "/opt/airflow/raw_data")
    Variable.set("RAW_TABLE_NAME", 'raw_store')
    Variable.set("URL_FILE", 'https://disk.yandex.ru/d/ku1tss7PtOEQcw')


default_args = {
    'start_date': datetime.today(),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 60
}

dag = DAG(
    'SET_VAR_AND_CONN',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

set_var = PythonOperator(
    task_id='set_var',
    python_callable=set_var_func,
    dag=dag
)

set_conn_raw = BashOperator(
    task_id='add_pg_db_raw',
    bash_command='airflow connections add "raw_store" '
                 '--conn-uri "postgresql://postgres:postgres@host.docker.internal:5434/raw_store"',
    dag=dag,
)

set_conn_core = BashOperator(
    task_id='add_pg_db_core',
    bash_command='airflow connections add "core_store" '
                 '--conn-uri "postgresql://postgres:postgres@host.docker.internal:5430/core_store"',
    dag=dag,
)

set_conn_mart = BashOperator(
    task_id='add_pg_db_mart',
    bash_command='airflow connections add "mart_store" '
                 '--conn-uri "postgresql://postgres:postgres@host.docker.internal:5436/mart_store"',
    dag=dag,
)

set_fs_default = BashOperator(
    task_id='add_conn_fs_default',
    bash_command='airflow connections add "fs_default" --conn-type File',
    dag=dag,
)
