import json
import os

import psycopg2
import pytest

from ..parsing import db_connection, JsonPars

TEST_DIR_CONFIG = 'test_data'
TEST_DIR_JSON = 'test_data/test_telemetry'


@pytest.fixture
def config_fixture():
    """Создание тестового файла config.json"""
    data = {
        "loading_sensors":
        ["sensor1", "sensor5"]
    }
    with open(f"{TEST_DIR_CONFIG}/config.json", "w", encoding="UTF-8") as file:
        json.dump(data, file)


@pytest.fixture
def telemetry_fixture():
    """
    Создание тестового файлов телеметрии sensors_<data>.json с содержимым
    {"timestamp": "2021-07-15 10:36:54",
    "sensor_values": [
        {"sensor_id": "sensor1", "value": 1.074},
        {"sensor_id": "sensor2", "value": 54.295}]}
    """
    items = [
        {"ts_str": "2021-07-15_10_53_05", "ts": "2021-07-15 10:36:54", "sensor_id":  "sensor1", "value": 1.074},
        {"ts_str": "2021-07-14_09_53_05", "ts": "2021-07-14 09:36:54", "sensor_id":  "sensor1", "value": 54.295},
    ]
    for item in items:
        data = {
            "timestamp": item["ts"],
            "sensors": [{"sensor_id": item["sensor_id"], "value": item["value"]}]
        }
        with open(f"{TEST_DIR_JSON}/sensors_{item['ts_str']}.json", "w", encoding="UTF-8") as file:
            json.dump(data, file)


@pytest.fixture()
def delete_files_fixture():
    """Удаление тестовых данных"""
    for file in os.listdir(TEST_DIR_JSON):
        os.remove(os.path.join(TEST_DIR_JSON, file))
    os.remove(os.path.join(TEST_DIR_CONFIG, 'config.json'))


def pytest_sessionfinish(session, exitstatus):
    """Функция, которая должна выполниться после запуска всех тестов"""
    # Не работает, нужно добавить в отдельный файл conftest.py?
    for file in os.listdir(TEST_DIR_JSON):
        os.remove(os.path.join(TEST_DIR_JSON, file))
    os.remove(os.path.join(TEST_DIR_CONFIG, 'config.json'))


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


def test_smoke(config_fixture):
    """Значение целевого датчика попадает в БД"""
    instance = JsonPars(dir_config=TEST_DIR_CONFIG, dir_telemetry=TEST_DIR_JSON)
    instance.process_telemetry()
    with db_connection() as db_con:
        db_close = db_con.closed
        cursor = db_con.cursor()
        cursor.execute("""
                    SELECT *
                    FROM sensor_value
                """)
    assert True


def test_datetime_outofrange():
    """Файл с датой вне диапазона не обрабатывается"""
    pass


def test_unreq_sensor(config_fixture, telemetry_fixture):
    """В БД попадают только данные датчиков из confing.json"""
    pass



