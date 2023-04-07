from dash import Dash, html, dash_table, dcc, callback, Output, Input
import dash
import plotly.express as px
import gzip
from urllib import request, error
import http
import csv
import pandas as pd
import win32file
import win32con
import pywintypes
import datetime
import geojson

WHO_LATEST_OUTPUT_FILE = 'latest.csv'
WHO_DAILY_REPORT_TABLE_OUTPUT_FILE = 'dailyReport.csv'
WHO_VACCINATION = 'vaccination.csv'
# local path
PATH_ = 'E:/PY/'
RES_FILE_PATH = f"{PATH_}program/testforDV/data"

# geojson
GEOJSON_FILE_PATH = f"{RES_FILE_PATH}/json/world.geojson"

# covid-19 data
WHO_LATEST_LOCAL_PATH = f"{PATH_}{WHO_LATEST_OUTPUT_FILE}"
WHO_DAILY_REPORT_TABLE_PATH = f"{PATH_}{WHO_DAILY_REPORT_TABLE_OUTPUT_FILE}"
WHO_VACCINATION_PATH = f"{PATH_}{WHO_VACCINATION}"
# url
URL_WHO_LATEST = 'https://covid19.who.int/WHO-COVID-19-global-table-data.csv'
URL_WHO_DAILY_REPORT = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'
URL_WHO_VACCINATION = 'https://covid19.who.int/who-data/vaccination-data.csv'


def get_geoJson_data(jsonFile_path):
    with open(jsonFile_path, 'r') as f:
        data = geojson.load(f)
    return data


data_geojson = get_geoJson_data(GEOJSON_FILE_PATH)

# time detection
handle_file_latest = win32file.CreateFile(WHO_LATEST_LOCAL_PATH, win32con.GENERIC_READ,
                                          win32con.FILE_SHARE_READ, None, win32con.OPEN_EXISTING, 0, None)
handle_file_daily_report = win32file.CreateFile(WHO_DAILY_REPORT_TABLE_PATH, win32con.GENERIC_READ,
                                                win32con.FILE_SHARE_READ, None, win32con.OPEN_EXISTING, 0, None)
handle_file_vaccination = win32file.CreateFile(WHO_VACCINATION_PATH, win32con.GENERIC_READ,
                                               win32con.FILE_SHARE_READ, None, win32con.OPEN_EXISTING, 0, None)

'''
for file in os.listdir(path):
    file_path = os.path.join(path, file)
    created_time = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
    print(f"{file}: {created_time}")
'''
# last modified time
handle_file_time_latest = win32file.GetFileTime(handle_file_latest)[2]
handle_file_time_daily_report = win32file.GetFileTime(
    handle_file_daily_report)[2]
handle_file_time_vaccination = win32file.GetFileTime(
    handle_file_vaccination)[2]

win32file.CloseHandle(handle_file_latest)
win32file.CloseHandle(handle_file_daily_report)
win32file.CloseHandle(handle_file_vaccination)

handle_file_time_latest = datetime.datetime.fromtimestamp(
    pywintypes.Time(handle_file_time_latest).timestamp())
handle_file_time_daily_report = datetime.datetime.fromtimestamp(
    pywintypes.Time(handle_file_time_daily_report).timestamp())
handle_file_time_vaccination = datetime.datetime.fromtimestamp(
    pywintypes.Time(handle_file_time_vaccination).timestamp())
now_time = datetime.datetime.fromtimestamp(
    pywintypes.Time(datetime.datetime.now()).timestamp())
diff_latest = now_time - handle_file_time_latest
diff_vaccination = now_time - handle_file_time_vaccination
diff_daily_report = now_time - handle_file_time_daily_report


def get_WHO_data(url):
    try:
        if url == URL_WHO_VACCINATION:
            with request.urlopen(url) as response:
                csv_data = response.read()
                decompressed_data = gzip.decompress(csv_data)
                csv_data = decompressed_data.decode('utf-8-sig').splitlines()
                csv_data = csv.reader(
                    csv_data, skipinitialspace=True, delimiter=',')
                return csv_data
        else:
            with request.urlopen(url) as response:
                csv_data = response.read().decode('utf-8-sig').splitlines()
                csv_data = csv.reader(
                    csv_data, skipinitialspace=True, delimiter=',')
                return csv_data
    except error.URLError as err:
        print(f"Error opening {url} {err}")
    except http.client.HTTPException as err:
        print(f"HTTP error {url} {err}")


def save_csv_file(output_file, func, url):
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        for row in func(url):
            writer.writerow(row)


cols = ['Country_Or_Region', 'WHO_Region', 'Cases', 'CasesPer01Million', 'NewlyCasesInLast7days', 'NewlyCasesInLast7daysPer01Million', 'NewlyCasesInLast1days',
        'Deaths', 'NewlyDeathsPer01Million', 'NewlyDeathsInLast7days', 'NewlyDeathsInLast7daysPer01Million', 'NewlyDeathsInLast1days']

# print(diff_daily_report, diff_latest, diff_vaccination)

if diff_latest > datetime.timedelta(hours=12):
    print('latest:', diff_latest)
    data_daily_report = pd.read_csv(
        WHO_DAILY_REPORT_TABLE_PATH, index_col=False)
    data_vaccination = pd.read_csv(WHO_VACCINATION_PATH, index_col=False)
    save_csv_file(WHO_LATEST_OUTPUT_FILE, get_WHO_data, url=URL_WHO_LATEST)
    data_latest = pd.read_csv(WHO_LATEST_LOCAL_PATH, index_col=False)

elif diff_vaccination > datetime.timedelta(hours=12):
    print('vaccination:', diff_vaccination)
    data_latest = pd.read_csv(WHO_LATEST_LOCAL_PATH, index_col=False)
    data_daily_report = pd.read_csv(
        WHO_DAILY_REPORT_TABLE_PATH, index_col=False)
    save_csv_file(WHO_VACCINATION, get_WHO_data, url=URL_WHO_VACCINATION)
    data_vaccination = pd.read_csv(WHO_VACCINATION_PATH, index_col=False)

elif diff_daily_report > datetime.timedelta(days=1):
    print('daily report:', diff_daily_report)
    data_latest = pd.read_csv(WHO_LATEST_LOCAL_PATH, index_col=False)
    data_vaccination = pd.read_csv(WHO_VACCINATION_PATH, index_col=False)
    save_csv_file(WHO_DAILY_REPORT_TABLE_OUTPUT_FILE, get_WHO_data,
                  url=URL_WHO_DAILY_REPORT)
    data_daily_report = pd.read_csv(
        WHO_DAILY_REPORT_TABLE_PATH, index_col=False)
else:
    data_latest = pd.read_csv(WHO_LATEST_LOCAL_PATH, index_col=False)
    data_daily_report = pd.read_csv(
        WHO_DAILY_REPORT_TABLE_PATH, index_col=False)
    data_vaccination = pd.read_csv(WHO_VACCINATION_PATH, index_col=False)

# save_csv_file(WHO_LATEST_OUTPUT_FILE, get_WHO_data, url=URL_WHO_LATEST)
# data_latest = pd.read_csv(WHO_LATEST_LOCAL_PATH, index_col=False)
# save_csv_file(WHO_VACCINATION, get_WHO_data, url=URL_WHO_VACCINATION)
# data_vaccination = pd.read_csv(WHO_VACCINATION_PATH, index_col=False)
# save_csv_file(WHO_DAILY_REPORT_TABLE_OUTPUT_FILE, get_WHO_data,
#               url=URL_WHO_DAILY_REPORT)
# data_daily_report = pd.read_csv(
#     WHO_DAILY_REPORT_TABLE_PATH, index_col=False)


print(data_latest)
print(data_daily_report)
print(data_vaccination)


# xfig = px.choropleth(data_vaccination, locations='COUNTRY', locationmode='country names', color='TOTAL_VACCINATIONS', range_color=[
#                      0, 1000000], color_continuous_scale=px.colors.sequential.Plasma, title='COVID-19 Cases by Country')
# # 创建Dash应用程序
# app = dash.Dash(__name__)

# # 定义应用程序布局
# app.layout = html.Div(children=[
#     html.H1(children='Global COVID-19 Cases by Country'),
#     dcc.Graph(id='covid-map', figure=xfig)
# ])

# if __name__ == '__main__':
#     app.run_server(debug=True)
print(__file__)
