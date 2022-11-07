import json
import os

import psycopg2
import pytest

from ..parsing import db_connection, JsonPars
from decimal import *

TEST_DIR_CONFIG = 'test_data'
TEST_DIR_JSON = 'test_data/test_telemetry'


@pytest.fixture()
def clear_db():
    """Очистка таблицы sensor_value"""
    with db_connection() as db_con:
        cursor = db_con.cursor()
        cursor.execute("""
                    TRUNCATE sensor_value
                """)


@pytest.fixture()
def delete_files_fixture():
    """Удаление тестовых данных"""
    for file in os.listdir(TEST_DIR_JSON):
        os.remove(os.path.join(TEST_DIR_JSON, file))
    os.remove(os.path.join(TEST_DIR_CONFIG, 'config.json'))


@pytest.fixture()
def config_fixture():
    """Создание тестового файла config.json"""
    print("start config_fixture")
    data = {
        "loading_sensors":
            ["sensor1", "sensor5"]
    }
    with open(f"{TEST_DIR_CONFIG}/config.json", "w", encoding="UTF-8") as file:
        json.dump(data, file)


@pytest.fixture
def telemetry_fixture():
    """
    Создание тестовых файлов телеметрии sensors_<data>.json с содержимым
    {"timestamp": "2021-07-15 10:36:54",
    "sensor_values": [
        {"sensor_id": "sensor1", "value": 1.074},
        {"sensor_id": "sensor2", "value": 54.295}]}
    """
    items = [
        {"ts_str": "2021-07-15_10_53_05", "ts": "2021-07-15 10:53:03", "sensor_id": "sensor1", "value": 1.94305821185},
        {"ts_str": "2021-07-15_10_50_05", "ts": "2021-07-15 10:50:03", "sensor_id": "sensor2", "value": -0.1169083147},
        {"ts_str": "2021-07-14_09_53_05", "ts": "2021-07-14 09:53:03", "sensor_id": "sensor2", "value": 54.0502380317},
        {"ts_str": "2021-07-16_09_53_05", "ts": "2021-07-16 09:53:03", "sensor_id": "sensor1", "value": -62.437009544},
    ]
    for item in items:
        data = {
            "timestamp": item["ts"],
            "sensors": [{"sensor_id": item["sensor_id"], "value": item["value"]}]
        }
        with open(f"{TEST_DIR_JSON}/sensors_{item['ts_str']}.json", "w", encoding="UTF-8") as file:
            json.dump(data, file)


@pytest.fixture
def script_execute():
    """Запуск основного скрипта парсинга json"""
    instance = JsonPars(dir_config=TEST_DIR_CONFIG, dir_telemetry=TEST_DIR_JSON)
    instance.process_telemetry()


def test_fixture_create(delete_files_fixture):
    """Отладочная функция для работы с фикстурами"""
    pass


def test_db_connect():
    """Тест проверки подключения к БД Postgres"""
    connection_status = True
    try:
        with db_connection() as db_con:
            db_close = db_con.closed
            cursor = db_con.cursor()
            cursor.execute("""
                        SELECT *
                        FROM sensor_value
                    """)
    except psycopg2.OperationalError:
        connection_status = False
    assert connection_status
    assert db_close == 0


def query_execute(sql_query):
    """Выполнение SQL запроса для тестовой проверки"""
    with db_connection() as db_con:
        cursor = db_con.cursor()
        cursor.execute(sql_query)
        entry_exist = cursor.fetchone()[0]
    return entry_exist


def test_smoke():
    """Значение целевого датчика попадает в БД"""
    sql_query = """
            SELECT EXISTS (
                SELECT *
                FROM sensor_value
                WHERE sensor1 = 1.943);
    """
    assert query_execute(sql_query)


def test_unreq_sensor():
    """В БД попадают только данные датчиков из confing.json"""
    sql_query = """
                SELECT NOT EXISTS (
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name='sensor_value' and column_name='sensor2'
                        );
    """
    assert query_execute(sql_query)


def test_datetime_outofrange():
    """Файл с датой вне диапазона не обрабатывается"""
    sql_query = """
                SELECT NOT EXISTS (
                    SELECT *
                    FROM sensor_value
                    WHERE sensor1 = -62.437);
    """
    assert query_execute(sql_query)


@pytest.fixture
def boundary_telemetry_fixture():
    """
    Создание тестовых файлов телеметрии sensors_<data>.json с пограничными datetime
    """
    items = [
        {"ts_str": "2021-07-15_10_00_00", "ts": "2021-07-15 10:00:00", "sensor_id": "sensor1", "value": -7.07251617389},
        {"ts_str": "2021-07-15_11_00_00", "ts": "2021-07-15 11:00:00", "sensor_id": "sensor1", "value": -54.8477083328},
    ]
    for item in items:
        data = {
            "timestamp": item["ts"],
            "sensors": [{"sensor_id": item["sensor_id"], "value": item["value"]}]
        }
        with open(f"{TEST_DIR_JSON}/sensors_{item['ts_str']}.json", "w", encoding="UTF-8") as file:
            json.dump(data, file)
    # удаление тестовых файлов после завершения теста
    yield
    for file in os.listdir(TEST_DIR_JSON):
        os.remove(os.path.join(TEST_DIR_JSON, file))


def test_boundary_datetime(config_fixture, boundary_telemetry_fixture, script_execute):
    """Обработка json, которые лежат на границах промежутка [dt; dt+delta)"""
    sql_query_1 = """
                SELECT EXISTS (
                    SELECT *
                    FROM sensor_value
                    WHERE sensor1 = -7.073);
    """
    sql_query_2 = """
                SELECT NOT EXISTS (
                    SELECT *
                    FROM sensor_value
                    WHERE sensor1 = -54.847);
    """
    assert query_execute(sql_query_1), query_execute(sql_query_2)


SUMMARD1 = -61.2105297502
SUMMARD2 = -90.9436886104


@pytest.fixture
def avg_telemetry_fixture():
    """
    Создание тестовых файлов телеметрии sensors_<data>.json с данными для вычисления среднего значения одного датчика
    """
    items = [
        {"ts_str": "2021-07-15_10_00_00", "ts": "2021-07-15 10:00:00", "sensor_id": "sensor1", "value": -61.2105297502},
        {"ts_str": "2021-07-15_10_01_00", "ts": "2021-07-15 10:01:00", "sensor_id": "sensor1", "value": -90.9436886104},
    ]
    for item in items:
        data = {
            "timestamp": item["ts"],
            "sensors": [{"sensor_id": item["sensor_id"], "value": item["value"]}]
        }
        with open(f"{TEST_DIR_JSON}/sensors_{item['ts_str']}.json", "w", encoding="UTF-8") as file:
            json.dump(data, file)
    # удаление тестовых файлов после завершения теста
    yield
    for file in os.listdir(TEST_DIR_JSON):
        os.remove(os.path.join(TEST_DIR_JSON, file))


@pytest.fixture
def avg_telemetry_fixture():
    """
    Создание тестовых файлов телеметрии sensors_<data>.json с данными для вычисления среднего значения одного датчика
    """
    items = [
        {"ts_str": "2021-07-15_10_00_00", "ts": "2021-07-15 10:00:00", "sensor_id": "sensor1", "value": -61.2105297502},
        {"ts_str": "2021-07-15_10_01_00", "ts": "2021-07-15 10:01:00", "sensor_id": "sensor1", "value": -90.9436886104},
    ]
    for item in items:
        data = {
            "timestamp": item["ts"],
            "sensors": [{"sensor_id": item["sensor_id"], "value": item["value"]}]
        }
        with open(f"{TEST_DIR_JSON}/sensors_{item['ts_str']}.json", "w", encoding="UTF-8") as file:
            json.dump(data, file)
    # удаление тестовых файлов после завершения теста
    yield
    for file in os.listdir(TEST_DIR_JSON):
        os.remove(os.path.join(TEST_DIR_JSON, file))


def test_agv_value(clear_db, avg_telemetry_fixture, script_execute):
    """Среднее значение одного датчика правильно вычисляется и попадает в БД"""
    avg_value = Decimal((-61.2105297502 + -90.9436886104) / 2).quantize(Decimal('1.000'))
    sql_query = f"""
                SELECT EXISTS (
                    SELECT *
                    FROM sensor_value
                    WHERE sensor1 = {avg_value});
    """
    assert query_execute(sql_query)


