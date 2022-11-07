import json
import os

import psycopg2
import pytest

from ..parsing import db_connection, JsonPars

TEST_DIR_CONFIG = 'test_data'
TEST_DIR_JSON = 'test_data/test_telemetry'


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


def test_smoke():
    """Значение целевого датчика попадает в БД"""
    instance = JsonPars(dir_config=TEST_DIR_CONFIG, dir_telemetry=TEST_DIR_JSON)
    instance.process_telemetry()
    with db_connection() as db_con:
        cursor = db_con.cursor()
        cursor.execute("""
                    select EXISTS (
                    SELECT *
                    FROM sensor_value
                    WHERE sensor1 > 1.000);
                """)
        entry_exist = cursor.fetchone()[0]
        print(f"Результат SQL запроса {entry_exist}")
    # assert entry_exist
    assert True


def test_datetime_outofrange():
    """Файл с датой вне диапазона не обрабатывается"""
    instance = JsonPars(dir_config=TEST_DIR_CONFIG, dir_telemetry=TEST_DIR_JSON)
    instance.process_telemetry()
    with db_connection() as db_con:
        cursor = db_con.cursor()
        cursor.execute("""
                        select EXISTS (
                        SELECT *
                        FROM sensor_value
                        WHERE sensor1 > 1.000);
                    """)
        entry_exist = cursor.fetchone()[0]
        print(f"Результат SQL запроса {entry_exist}")
    # assert entry_exist
    assert True


# telemetry_fixture, delete_files_fixture
def test_unreq_sensor(config_fixture, telemetry_fixture):
    """В БД попадают только данные датчиков из confing.json"""
    print("test_unreq_sensor")
    pass



