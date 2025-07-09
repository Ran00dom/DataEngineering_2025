
import pandas as pd
import numpy as np


class TableDataCSV:

    units_avg = ['°F', '%', 'kn', 'ft']
    units_total = ['inch']
    units_result = {'°F' : ['celsius', lambda x: (x - 32) * 5/9],
                    'kn' : ["m_per_s", lambda x: x * 0.514],
                    'ft' : ['m', lambda x: x * 0.3048],
                    'inch' : ['mm', lambda x: x * 25.4]
                    }

    NAME_COL = 'values'
    DICTIONARY = ['latitude', 'longitude', 'generationtime_ms', 'timezone_abbreviation', 'elevation', 'hourly_units','hourly', 'daily', 'daily_units']
    row_type = {""}

    def __init__(self, json):
        self.data = pd.DataFrame(json)
        self.result_data = None

        assert all(item in self.data.columns for item in self.DICTIONARY), f"Expecting json with correct dictionary columns"

        self.data['hourly'] = self.data['hourly'].combine_first(self.data['daily'])
        self.data['hourly_units'] = self.data['hourly_units'].combine_first(self.data['daily_units'])
        self.data.drop(columns=['daily_units', 'daily'], inplace=True)
        self.convert_data()

    def print_json_data(self):
        pd.set_option('display.max_columns', None)
        print(self.data)

    def convert_data(self):
        print(len(self.data.loc['time', 'hourly']))
        self.time = np.array(self.data.loc['time', 'hourly']).reshape(-1, 24, copy=False)

        index = 0
        mask = []
        for item in self.time:
            print(self.data.loc['sunrise', 'hourly'][index])
            print(item)
            print(self.data.loc['sunset', 'hourly'][index])
            mask.append(item < self.data.loc['sunrise', 'hourly'][index])
            index += 1
        print(mask)

        self.average(self.units_avg, np.mean, 24, 'avg_', "_24h")
        self.average(self.units_total, np.sum, 24, 'total_', "_24h")
        self.average(self.units_result, np.mean, 24, 'avg_', "_day", mask)

        print(self.result_data)
        self.print_json_data()

    def average(self, units, func, part, postfix, suffix, mask = True):
        data_filter = self.data['hourly_units'].isin(units)

        if not data_filter.empty:
            new_data = self.data[['hourly', 'hourly_units']][data_filter]

            new_data = new_data.rename(
                columns={'hourly' : self.NAME_COL, 'hourly_units' : 'type'}
            )

            if part > 1:

                new_data['values'] = new_data['values'].apply(
                    lambda x:
                    np.apply_along_axis(
                        func,
                        axis=0,
                        arr=np.where(mask, np.array(x).reshape(-1, part, copy=False), 0)
                    )
                )

            new_data = new_data.apply(
                lambda x: self.change_metrics(x), axis=1)
            new_data = new_data.rename(
                lambda x: f"{postfix}{x}{suffix}")

            if self.result_data is None:
                self.result_data = new_data
                print(f"Result data is None")
            else:
                self.result_data = pd.concat([self.result_data, new_data])
        else:
            print(f"No data available for {units}")

    def change_metrics(self, row):
        if self.units_result.__contains__(row['type']):
            return self.units_result[row['type']][1](row['values'])
        else:
            return row['values']

