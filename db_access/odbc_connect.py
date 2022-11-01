import os

import pyodbc
from dotenv import load_dotenv

load_dotenv()

server = 'localhost'
database = os.getenv('DB_NAME', default='DB')
username = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')

cnxn = pyodbc.connect(
    'DRIVER={PostgreSQL Unicode};'
    f'SERVER={server};'
    f'DATABASE={database};'
    'ENCRYPT=yes;'
    f'UID={username};'
    f'PWD={password};')
with cnxn:
    cursor = cnxn.cursor()
    count = cursor.execute("""
        CREATE TABLE IF NOT EXISTS t (
            id INT PRIMARY KEY,
            name VARCHAR NOT NULL
        )""")

"""
Поиск доступных драйверов в системе
driver_names = [x for x in pyodbc.drivers() if 'Postgre' in x]
print(driver_names)
"""
