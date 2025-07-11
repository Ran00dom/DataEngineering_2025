

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
                self.cursor.execute(sql_file.read().split(';')[0])
                self.connect.commit()
        except psycopg2.OperationalError as e:
            logging.error(e)
        except FileNotFoundError as e:
            logging.error(e)

    def insert_data(self, table):
        if isinstance(table, TableDataCSV):
            self.logger.info('Inserting data from PostgreSQL database')
            for index,row in table.result_data.iterrows():
                self.insert_sql( index, row)

    def insert_sql(self, index, row):
        try:
            self.connect.rollback()
            if not (self.cursor and not self.cursor.closed):
                raise psycopg2.DatabaseError("No database connection")
            with open('insert.sql', 'r') as sql_file:
                sql = sql_file.read().split(';')[0] # выбрать первый запрос
                # заменить аргументы
                sql = sql.replace('@array', '%s')
                sql = sql.replace('@name', '%s')
                # запустить запрос
                self.cursor.execute(f'{sql}',(index, row[0].tolist(), row[0].tolist()))

            self.connect.commit()
        except (psycopg2.DatabaseError, AttributeError) as error:
            print(f'ConnectorPostgresSQL insert_sql row/{index}: {error}')

    def create_table(self):
        try:
            self.connect.rollback()
            with open('init.sql', 'r') as sql_file:
                self.cursor.execute(sql_file.read().split(';')[0])
                self.connect.commit()
        except psycopg2.OperationalError as e:
            logging.error(e)
        except FileNotFoundError as e:
            logging.error(e)

    def drop_table(self):
        try:
            self.connect.rollback()
            with open('init.sql', 'r') as sql_file:
                self.cursor.execute(sql_file.read().split(';')[1])
                self.connect.commit()
        except psycopg2.OperationalError as e:
            logging.error(e)
        except FileNotFoundError as e:
            logging.error(e)

    def __del__(self):
        self.connect.close()