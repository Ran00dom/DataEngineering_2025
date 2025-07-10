

import psycopg2
from TableDataCSV import TableDataCSV

import logging

class ConnectorPostgresSQL:
    TABLE_NAME = 'analytics_data'
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        try:
            self.connect = psycopg2.connect(
                host='localhost',
                user='admin',
                port=5432,
                password='password',
                database='analytics_db'
            )
            self.cursor = self.connect.cursor()
            with open('init.sql', 'r') as sql_file:
                self.cursor.execute(sql_file.read())

        except psycopg2.OperationalError as e:
            logging.error(e)
        except FileNotFoundError as e:
            logging.error(e)

    def insert_data(self, table):
        if isinstance(table, TableDataCSV):
            for index,row in table.result_data.iterrows():
                self.insert_sql( index, row)


    def insert_sql(self, index, row):
        try:
            self.cursor.execute(
                f'INSERT INTO {self.TABLE_NAME} (value_name, value_array) VALUES (%s, %s)',
                (index, row[0].tolist())
            )
            self.connect.commit()
        except psycopg2.DatabaseError as error:
            print(f'ConnectorPostgresSQL insert_sql row/{index}: {error}')

    def __del__(self):
        self.connect.close()