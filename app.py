import logging
import time
import threading

from TableDataCSV import TableDataCSV
from RequestsController import RequestsController
from connector_database import ConnectorPostgresSQL

class App:
    MIN_DELTA_TIME = 1
    auto_update_flag = False

    def __init__(self, api_url):
        self.delta_time = 0
        self.requests = RequestsController(api_url)
        self.connect = ConnectorPostgresSQL()

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.auto_update_process = threading.Thread(target=self.auto_update, args=())
        self.console_process = threading.Thread(target=self.run_console, args=())
        self.console_process.start()
        self.data = None

    def run_console(self):
        while True:
            command = input('Введите команду: ')
            if command == 'exit':
                self.auto_update_flag = False
                self.auto_update_process.join(10000)
                break
            if command == 'update':
                json = self.requests.getRequestJSON()
                if json is not None:
                    self.data = TableDataCSV(json)
                    self.data.save_to_cvs('table.csv')
                    self.connect.insert_data(self.data)
                else:
                    self.logger.error(f'json is null')
            if command.find("set time") != -1 :
                split = command.split('=')
                if len(split) == 2 :
                    self.delta_time = int(command.split('=')[1])
                    self.logger.info(f' time set {self.delta_time}')
                    if self.delta_time <= 0 and self.auto_update_process.is_alive():
                        self.logger.info(f' stop auto_update')
                        self.auto_update_flag = False
                        self.auto_update_process.join(10000)
                    else:
                        if self.delta_time > 0 :
                            self.auto_update_flag = True
                            if not self.auto_update_process.is_alive():
                                self.logger.info(f' start auto_update')
                                self.auto_update_process.start()

    def auto_update(self):
        while self.auto_update_flag:
            time.sleep(self.delta_time)
            self.logger.info('AUTO UPDATE')
            json = self.requests.getRequestJSON()
            if json is not None:
                self.data = TableDataCSV(json)
                self.data.save_to_cvs('table.csv')
                self.connect.insert_data(self.data)
            else:
                self.logger.error(f'json is null')

