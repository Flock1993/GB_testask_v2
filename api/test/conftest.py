import json
import os

import psycopg2
import pytest

from ..parsing import db_connection, JsonPars

TEST_DIR_CONFIG = 'test/test_data'
TEST_DIR_JSON = 'test/test_data/test_telemetry'


@pytest.fixture()
def delete_files_fixture():
    """Удаление тестовых данных"""
    for file in os.listdir(TEST_DIR_JSON):
        os.remove(os.path.join(TEST_DIR_JSON, file))
    os.remove(os.path.join(TEST_DIR_CONFIG, 'config.json'))


# @pytest.fixture(scope="session")
# def config_fixture():
#     """Создание тестового файла config.json"""
#     print("start config_fixture")
#     data = {
#         "loading_sensors":
#         ["sensor1", "sensor5"]
#     }
#     with open(f"{TEST_DIR_CONFIG}/config.json", "w", encoding="UTF-8") as file:
#         json.dump(data, file)
#     yield
#     os.remove(os.path.join(TEST_DIR_CONFIG, 'config.json'))
#     print("end config_fixture")


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


