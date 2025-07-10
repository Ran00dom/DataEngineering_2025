
import pandas as pd
import numpy as np

class TableDataCSV:

    # список единиц измерения для которых требуется посчитать среднее (avg)
    units_avg = ['°F', '%', 'kn', 'ft']

    # список с единицами измерения для которых требуется посчитать общую сумму (total)
    units_total = ['inch']

    # словарь для единиц измерения которые требуется переводить и функции для их преобразования
    units_result = {'°F' : ['celsius', lambda x: (x - 32) * 5/9],
                    'kn' : ["m_per_s", lambda x: x * 0.514],
                    'ft' : ['m', lambda x: x * 0.3048],
                    'inch' : ['mm', lambda x: x * 25.4],
                    'unixtime' : ['iso', lambda x: pd.to_datetime(x, unit='s').strftime('%Y-%m-%d %H:%M:%S')],
                    's' : ['hours', lambda x: pd.to_datetime(x, unit='s').hour]
                    }


    VALUE_COL = 'values' #имя главной колонки в новом датасете
    # список необходимых ключей в json
    DICTIONARY = ['latitude', 'longitude', 'generationtime_ms', 'timezone_abbreviation', 'elevation', 'hourly_units','hourly', 'daily', 'daily_units']

    def __init__(self, json):
        self.data = pd.DataFrame(json)
        self.result_data = pd.DataFrame() # итоговый DataFrame
        # проверка на наличие требуемых ключей json
        assert all(item in self.data.columns for item in self.DICTIONARY), f"Expecting json with correct dictionary columns"
        # соединяем столбцы с почасовыми и дневными данными
        self.time_zone = self.data['utc_offset_seconds'].unique()
        self.data['hourly'] = self.data['hourly'].combine_first(self.data['daily'])
        self.data['hourly_units'] = self.data['hourly_units'].combine_first(self.data['daily_units'])
        self.data.drop(columns=['daily_units', 'daily'], inplace=True)
        # запускаем заполнение result_data
        self.convert_data()

    # печать начального dataFrame
    def print_json_data(self):
        pd.set_option('display.max_columns', None)
        print(self.data)

    def convert_data(self):
        #разбиваем время на массивы по 24 элемента
        time = np.array(self.data.loc['time', 'hourly']).reshape(-1, 24, copy=False)
        # создаем масску для отсева почасовых данных которые не вошли в световой день
        index = 0
        mask_sunrise = []
        mask_sunset = []
        for item in time:
            mask_sunrise.append(item > self.data.loc['sunrise', 'hourly'][index])
            mask_sunset.append(item < self.data.loc['sunset', 'hourly'][index])
            index += 1
        mask =  np.array(mask_sunset) & np.array(mask_sunrise) # обьединяем маски для восхода и заката
        # преобразуем время восхода и заката для указанного часового пояса
        self.data.loc[['sunset', 'sunrise'],'hourly'] = self.data.loc[['sunset', 'sunrise'],'hourly'].apply(lambda x: x + self.time_zone)

        # добавляем данные в result_data указывая нужные параметры
        self.add_data(self.units_avg, np.mean, 24, 'avg_', lambda x: "_24h")
        self.add_data(self.units_total, np.sum, 24, 'total_', lambda x: "_24h")
        self.add_data(self.units_avg, np.nanmean, 24, 'avg_', lambda x: "_daylight", mask)
        self.add_data(self.units_total, np.nansum, 24, 'total_', lambda x: "_daylight", mask)
        self.add_data(self.units_result, lambda x: x, 0, "", lambda x: f"_{self.units_result[self.data.loc[x,'hourly_units']][0]}")

    # добавляет данные (units - список метрик для работы) (func - главная функция обработки) (part - размер групп на которые разбиваются циклические данные)
    # (postfix - добавляется к началу названия строки) (suffix - добавляется в конец названия строки) (mask фильтрует данные в массиве)
    def add_data(self, units, func, part, postfix, suffix, mask = None):
        # возвращает массив флагов для каждой строки (True если в этой строке метрика из units)
        data_filter = self.data['hourly_units'].isin(units)
        # если остались строки
        if not data_filter.empty:
            # новый датафрейм
            new_data = self.data[['hourly', 'hourly_units']][data_filter]
            # переименовываем столбцы
            new_data = new_data.rename(
                columns={'hourly' : self.VALUE_COL, 'hourly_units' : 'type'}
            )
            # если разбивка данных требуется
            if part > 1:
                # разбивка данных
                new_data[self.VALUE_COL] = new_data[self.VALUE_COL].apply(
                    lambda x:
                    # если требуется фильтрация применяем маску
                    np.where(mask,np.array(x).reshape(-1, part, copy=False), np.nan)
                    if type(mask) is np.ndarray # если не требуется просто разбиваем последовательность
                    else np.array(x).reshape(-1, part, copy=False)
                )
            # применяем функцию обработки данных к массиву
            new_data[self.VALUE_COL] = new_data[self.VALUE_COL].apply(
                lambda x:
                np.apply_along_axis(
                    func,
                    axis=1,
                    arr=x
                ) if part > 1 else func(np.array(x)) # если нет shape
            )
            # меняем метрики получившихся результатов
            new_data = new_data.apply(
                lambda x: self.change_metrics(x), axis=1)
            # переименовываем троки
            new_data = new_data.rename(
                lambda x: f"{postfix}{x}{suffix(x)}")
            # убираем вспомогательный столбец
            new_data = new_data.drop(columns=['type'])
            # добавляем строки из нового датасета
            if self.result_data is None:
                self.result_data = new_data
                print(f"Result data is None")
            else:
                self.result_data = pd.concat([self.result_data, new_data])
        else:
            print(f"No data available for {units}")

    def change_metrics(self, row):
        if self.units_result.__contains__(row['type']): # проверяем содержится ли метрика в словаре на преобразование
            return np.array(self.units_result[row['type']][1](row[self.VALUE_COL])) # если содержится, то обрабатываем массив функцией из словаря
        else:
            return np.array(row[self.VALUE_COL]) # если нет возвращаем без изменений

    def print(self):
        print(f'Result transforms >')
        print(self.result_data)

    def save_to_cvs(self, path):
        print(f'TableDataCSV save to {path}')
        self.result_data.to_csv(path, index_label=['name', self.VALUE_COL], header=True)