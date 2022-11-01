import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

server = 'localhost'
db = os.getenv('DB_NAME', default='DB')
username = os.getenv('POSTGRES_USER')
psswrd = os.getenv('POSTGRES_PASSWORD')

cnxn = psycopg2.connect(
    host=server,
    database=db,
    user=username,
    password=psswrd,
)
with cnxn:
    cursor = cnxn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor_value(
                id SERIAL PRIMARY KEY,
                sensor CHAR(15) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                value REAL NOT NULL
                );
        """)
