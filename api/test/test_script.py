import pytest

from ..parsing import db_connection, JsonPars
import json
from datetime import datetime
import psycopg2
import subprocess


TEST_DIR_CONFIG = 'test_data'
TEST_DIR_JSON = 'test_data/test_telemetry'


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
    except (psycopg2.OperationalError, psycopg2.DatabaseError):
        connection_status = False
    assert connection_status and db_close == 0


@pytest.fixture
def config_fixture():
    """Создание тестового файла config.json"""
    data = {
        "loading_sensors":
        ["sensor1", "sensor5", "sensor6", "sensor8", "sensor10", "sensor24"]
    }
    with open(f"{TEST_DIR_CONFIG}/config.json", "w", encoding="UTF-8") as file:
        json.dump(data, file)


@pytest.fixture
def telemetry_fixture():
    """
    Создание тестового файла sensors_.json с содержимым
    {
    "timestamp": "2021-07-15 10:36:54",
    "sensor_values": [
        {"sensor_id": "sensor1", "value": 1.074},
        {"sensor_id": "sensor2", "value": 54.295}
    ]
    }
    """
    data = {
        "timestamp": "timestamp_variable",
        "sensor_values": []
    }
    items = [
        {"timestamp_str": "2021-07-15_10_53_05", "timestamp": datetime("2021-07-15 10:36:54"), },
        {}
    ]
    for item in items:
        with open(f"{TEST_DIR_JSON}/sensors_2021-07-15_10_53_05.json", "w", encoding="UTF-8") as file:
            json.dump(data, file)


def test_smoke(config_fixture):
    """Скрипт парсинга работает с корректными данными"""
    instance = JsonPars(dir_config=TEST_DIR_CONFIG)
    lst_sensors = instance.create_collections()[0]
    assert lst_sensors == ["sensor1", "sensor5", "sensor6", "sensor8", "sensor10", "sensor24"]


def test_datetime_outofrange():
    """Файл с датой вне диапазона не обрабатывается"""
    pass


def test_unreq_sensor():
    """В БД попадают только данные датчиков из confing.json"""
    pass



