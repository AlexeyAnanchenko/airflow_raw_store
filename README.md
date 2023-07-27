# airflow_raw_store
Задание на построение паплайна в Airflow в рамках курса "Инженер Данных"

## Из чего состоит проект?

- Скрипт DDL для создания таблицы: <code>[./postgres_data/DDL/init.sql](https://github.com/AlexeyAnanchenko/airflow_raw_store/blob/main/postgres_data/DDL/01_init.sql)</code>.
- DAG для Airflow: <code>[./dags/raw_store.py](https://github.com/AlexeyAnanchenko/airflow_raw_store/blob/main/dags/raw_store.py)</code>.
- Скриншоты DAG-а и полученной витрины данных: <code>[./screenshots/](https://github.com/AlexeyAnanchenko/airflow_raw_store/blob/main/screenshots/)</code>.
-  docker-compose.yml для разворачивания проекта (добавлен дополнительный том в Airflow "raw_data" и БД PostgreSQL): <code>[./docker-compose.yml](https://github.com/AlexeyAnanchenko/airflow_raw_store/blob/main/docker-compose.yaml)</code>.

## Как развернуть проект?

Для работоспособности проекта необходимо добавить переменные в Airflow:
- RAW_DATA: /opt/airflow/raw_data
- TABLE_NAME: raw_store
- URL_FILE (публичная ссылка на файл в Яндекс Диск): https://disk.yandex.ru/d/ku1tss7PtOEQcw

Так же нужно добавить Connection к БД PostgreSQL (postgres-data), согласно настройкам из docker-compose

Для просмотра проекта достаточно склонировать его себе на локальный компьютер, иметь установленным docker-compose и ввести команду:

```sh
docker-compose up -d
```

Далее база данных доступна в контейнере, либо по порту 5432 из любого сервиса для подключения к БД (например, DBeaver)
