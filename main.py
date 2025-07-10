
#  requests address
api_url = """https://api.open-\
meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&daily=sunrise,sunset,daylight_duration&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,\
apparent_temperature,temperature_80m,temperature_120m,wind_speed_10m,wind_sp\
eed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,\
weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall\
&timezone=auto&timeformat=unixtime&wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2025-05-16&end_date=2025-05-30"""

from RequestsController import RequestsController
from TableDataCSV import TableDataCSV

requests = RequestsController(api_url)
table = TableDataCSV(requests.getRequestJSON())
table.print()
table.save_to_cvs("table.csv")


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
