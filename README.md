# airflow_store
Задание на построение паплайна в Airflow в рамках курса "Инженер Данных"

## Задачи реализованные в рамках пайплайна:

1. Создать папку (если нет) на сервере Airflow, куда будем складывать в дальнейшем данные с API.
2. Проверяем наличие файла в папке.
    - Если файла нет, скачиваем с API ЯндексДиск
    - Если файл есть, проходим сразу к следующей задаче
3. С помощью FileSensor убеждаемся, что файл лежит в папке
4. Загружаем сырые данные из файла в БД raw_data
5. В уже загруженных данных меняем формат даты на корректный
6. Далее в целях удобства заполнения слоя Core, делаем миграцию данных из БД raw_store в БД core_store
7. Загружаем нормализованные по схеме "Снежинка" данные из raw в core
8. Создаём первую витрину на основе БД core_store и загружаем в БД mart_store. Витрина отображает суммарные продажи магазина в разрезе лет и категорий
9. Выгржаем случайное наименование категории и передаём в следующий task средствами работы с контекстом DAG-а - xcom
10. В следующем task-е добавляем ветвление, в котором в зависимости от полученной в xcom категории определяем последующий task
11. Создаём вторую витрину по суммарным продажам суб-категорий за 2015 год. Данные загружаем в БД mart_store

Отдельно так же создан вспомогательный DAG, который прогружает необходмые для работы основного pipeline-а переменные и подключения в Airflow.

## Из чего состоит проект?

- Скрипты DDL для создания таблиц в 3-х БД - raw_store, core_store, mart_store: <code>[./postgres_data/DDL/](https://github.com/AlexeyAnanchenko/airflow_raw_store/blob/main/postgres_data/DDL/)</code>.
- Вспомогательный DAG для создания переменны и подключений: <code>[./dags/set_var_and_conn.py](https://github.com/AlexeyAnanchenko/airflow_raw_store/blob/main/dags/set_var_and_conn.py)</code>.
- Основной DAG для Airflow: <code>[./dags/store.py](https://github.com/AlexeyAnanchenko/airflow_raw_store/blob/main/dags/store.py)</code>.
- Скриншоты DAG-а и полученных витрин данных: <code>[./screenshots/](https://github.com/AlexeyAnanchenko/airflow_raw_store/blob/main/screenshots/)</code>.
-  docker-compose.yml для разворачивания проекта (добавлен дополнительный том в Airflow "raw_data" и 3 БД PostgreSQL): <code>[./docker-compose.yml](https://github.com/AlexeyAnanchenko/airflow_store/blob/main/docker-compose.yaml)</code>.
-  ER-диаграмма со структурой DWH: <code>[./ER-diagram.png](https://github.com/AlexeyAnanchenko/airflow_raw_store/blob/main/ER-diagram.png)</code>.

## Как развернуть проект?

Для просмотра проекта достаточно склонировать его себе на локальный компьютер, иметь установленным docker-compose и ввести команду:

```sh
docker-compose up -d
```

- Сначала необходимо, чтобы отработал Dag "SET_VAR_AND_CONN", который установит необходимые переменные и подключения в Airflow.
- После чего прогрузиться и можно будет запускать основной DAG - "STORE"
