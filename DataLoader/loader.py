import pandas as pd
import mysql.connector
import os
import logging
from dotenv import load_dotenv

load_dotenv()
os.makedirs('logs', exist_ok=True)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s | %(levelname)s | %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='/app/logs/log_file.log', filemode='a')

logger = logging.getLogger()


def main():
    try:
        conn = None
        cursor = None

        data = pd.read_csv('trucks_reference.csv')
        conn = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'mysql'),
            port='3306',
            user=os.getenv('MYSQL_USERNAME', 'root'),
            password=os.getenv('MYSQL_PASSWORD', 'root'),
            database=os.getenv('MYSQL_DATABASE', 'truck_db'),
        )

        cursor = conn.cursor()

        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS Trucks
                       (
                           truck_id VARCHAR(50) PRIMARY KEY,
                           model    VARCHAR(50),
                           region   VARCHAR(50),
                           status   VARCHAR(50)
                       )
                       """)

        cursor.close()
        cursor = conn.cursor()

        sql = "INSERT INTO Trucks (truck_id, model, region, status) VALUES (%s, %s, %s, %s)"
        data_to_insert = data.values.tolist()
        cursor.executemany(sql, data_to_insert)

        conn.commit()

    except Exception as ex:
        logger.error("while loading truck data error occurred: %s", ex, exc_info=True)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
