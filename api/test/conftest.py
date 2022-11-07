import pytest
import json
import os
from ..parsing import db_connection, JsonPars

TEST_DIR_CONFIG = 'test_data'
TEST_DIR_JSON = 'test_data/test_telemetry'


@pytest.fixture
def script_execute():
    """Запуск основного скрипта парсинга json"""
    instance = JsonPars(dir_config=TEST_DIR_CONFIG, dir_telemetry=TEST_DIR_JSON)
    instance.process_telemetry()


@pytest.fixture()
def clear_db():
    """Очистка таблицы sensor_value"""
    with db_connection() as db_con:
        cursor = db_con.cursor()
        cursor.execute("""
                    TRUNCATE sensor_value
                """)


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
    # удаление тестовых файлов после завершения теста
    yield
    for file in os.listdir(TEST_DIR_JSON):
        os.remove(os.path.join(TEST_DIR_JSON, file))
