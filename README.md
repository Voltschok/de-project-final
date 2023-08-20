# Итоговый проект

### Описание
Реализован пайплан загрузки данных из БД Postgresql в базу данных Vertica и формирование витрины в соответствии с бизнес-требованиями.
ETL-процесс загрузки сырых данных, а также построения витрины реализован с помощью Apache Airflow.  Для визуализации витрины используется Metabase.

### Структура репозитория
Внутри `src` расположены папки:
- `/src/dags` -   содержит код DAG, который создает таблицы в Vertica - `0_create_table.py` (отрабатывает один раз), код  DAG, который поставляет данные из источника в хранилище - `1_data_import.py` (отрабатывае ежедевно), а также код DAG, который обновляет витрины данных - `2_datamart_update.py` (отрабатывае также ежедевно).
- `/src/sql` - содержит SQL-запросы формирования таблиц в `STAGING`- и `DWH`-слоях, а также скрипт подготовки данных для итоговой витрины.
- `/src/img` - содержит скриншот реализованного над витриной дашборда.

- ### Как работать с репозиторием
Необходимо задать параметры подключения к БД Postgresql и БД Vertica в файле `config.ini` в папке `dags/`.
