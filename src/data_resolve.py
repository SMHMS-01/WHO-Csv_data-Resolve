import plotly.express as px
import gzip
from urllib import request, error
import http
import csv
import pandas as pd
import geopandas as gpd
import win32file
import win32con
import os
import pywintypes
import datetime
import matplotlib.pyplot as plt
WHO_LATEST_OUTPUT_FILE = 'latest.csv'
WHO_DAILY_REPORT_TABLE_OUTPUT_FILE = 'dailyReport.csv'
WHO_VACCINATION_OUTPUT_FILE = 'vaccination.csv'
# local path
PATH_ = 'E:/PY/'
RES_FILE_PATH = f"{PATH_}program/testforDV/data"

# geojson
GEOJSON_FILE_PATH = f"{RES_FILE_PATH}/json/WB_countries_Admin0_lowres.geojson"

# covid-19 data
WHO_LATEST_LOCAL_PATH = f"{PATH_}{WHO_LATEST_OUTPUT_FILE}"
WHO_DAILY_REPORT_TABLE_PATH = f"{PATH_}{WHO_DAILY_REPORT_TABLE_OUTPUT_FILE}"
WHO_VACCINATION_PATH = f"{PATH_}{WHO_VACCINATION_OUTPUT_FILE}"
# url
URL_WHO_LATEST = 'https://covid19.who.int/WHO-COVID-19-global-table-data.csv'
URL_WHO_DAILY_REPORT = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'
URL_WHO_VACCINATION = 'https://covid19.who.int/who-data/vaccination-data.csv'

WHO_LATEST_DATA = (URL_WHO_LATEST, WHO_LATEST_LOCAL_PATH,
                   WHO_LATEST_OUTPUT_FILE)
WHO_DAILY_REPORT_DATA = (
    URL_WHO_DAILY_REPORT, WHO_DAILY_REPORT_TABLE_PATH, WHO_DAILY_REPORT_TABLE_OUTPUT_FILE)
WHO_VACCINATION_DATA = (URL_WHO_VACCINATION,
                        WHO_VACCINATION_PATH, WHO_VACCINATION_OUTPUT_FILE)
# cols names
cols = ['Country_Or_Region', 'WHO_Region', 'Cases', 'CasesPer01Million', 'NewlyCasesInLast7days', 'NewlyCasesInLast7daysPer01Million', 'NewlyCasesInLast1days',
        'Deaths', 'NewlyDeathsPer01Million', 'NewlyDeathsInLast7days', 'NewlyDeathsInLast7daysPer01Million', 'NewlyDeathsInLast1days']


def geoJson_to_geodataframe(jsonFile_path=GEOJSON_FILE_PATH):
    gdf = gpd.read_file(jsonFile_path)
    # 将 GeoDataFrame 中的几何对象（geometry）转换成 Well-Known Text（WKT）格式，并将其存储在一个新的列 'wkt_geometry' 中
    gdf = gdf[['ISO_A3', 'geometry']]
    # gdf.plot()
    # plt.show()
    return gdf


def data_update_time_setting(latestFile=WHO_LATEST_DATA, dailyReportFile=WHO_DAILY_REPORT_DATA, vaccinationFile=WHO_VACCINATION_DATA):
    # time detection
    handle_file_latest = win32file.CreateFile(latestFile[1], win32con.GENERIC_READ,
                                              win32con.FILE_SHARE_READ, None, win32con.OPEN_EXISTING, 0, None)
    handle_file_daily_report = win32file.CreateFile(dailyReportFile[1], win32con.GENERIC_READ,
                                                    win32con.FILE_SHARE_READ, None, win32con.OPEN_EXISTING, 0, None)
    handle_file_vaccination = win32file.CreateFile(vaccinationFile[1], win32con.GENERIC_READ,
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
    diff_daily_report = now_time - handle_file_time_daily_report
    diff_vaccination = now_time - handle_file_time_vaccination
    return diff_latest, diff_daily_report, diff_vaccination


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


def data_file_updated(latestFile=WHO_LATEST_DATA, dailyReportFile=WHO_DAILY_REPORT_DATA, vaccinationFile=WHO_VACCINATION_DATA):
    latest_file_path = os.path.join(latestFile[1])
    daly_report_file_path = os.path.join(dailyReportFile[1])
    vaccination_file_path = os.path.join(vaccinationFile[1])
    if os.path.isfile(latest_file_path) and os.path.isfile(daly_report_file_path) and os.path.isfile(vaccination_file_path):
        diff_latest, diff_daily_report, diff_vaccination = data_update_time_setting()
        if diff_latest > datetime.timedelta(hours=18):
            print('latest:', diff_latest)
            data_daily_report = pd.read_csv(
                dailyReportFile[1], index_col=False)
            data_vaccination = pd.read_csv(vaccinationFile[1], index_col=False)
            save_csv_file(latestFile[2], get_WHO_data, url=latestFile[0])
            data_latest = pd.read_csv(latestFile[1], index_col=False)
            return data_latest, data_daily_report, data_vaccination

        elif diff_vaccination > datetime.timedelta(hours=18):
            print('vaccination:', diff_vaccination)
            data_latest = pd.read_csv(latestFile[1], index_col=False)
            data_daily_report = pd.read_csv(
                dailyReportFile[1], index_col=False)
            save_csv_file(vaccinationFile[2],
                          get_WHO_data, url=vaccinationFile[0])
            data_vaccination = pd.read_csv(vaccinationFile[1], index_col=False)
            return data_latest, data_daily_report, data_vaccination

        elif diff_daily_report > datetime.timedelta(days=4):
            print('daily report:', diff_daily_report)
            data_latest = pd.read_csv(latestFile[1], index_col=False)
            data_vaccination = pd.read_csv(vaccinationFile[1], index_col=False)
            save_csv_file(dailyReportFile[2], get_WHO_data,
                          url=dailyReportFile[0])
            data_daily_report = pd.read_csv(
                dailyReportFile[1], index_col=False)
            return data_latest, data_daily_report, data_vaccination
        else:
            print('latest:', diff_latest)
            print('daily report:', diff_daily_report)
            print('vaccination:', diff_vaccination)
            data_latest = pd.read_csv(latestFile[1], index_col=False)
            data_daily_report = pd.read_csv(
                dailyReportFile[1], index_col=False)
            data_vaccination = pd.read_csv(vaccinationFile[1], index_col=False)
            return data_latest, data_daily_report, data_vaccination
    else:
        save_csv_file(latestFile[2], get_WHO_data, url=latestFile[0])
        data_latest = pd.read_csv(latestFile[1], index_col=False)
        save_csv_file(dailyReportFile[2], get_WHO_data,
                      url=dailyReportFile[0])
        data_daily_report = pd.read_csv(
            dailyReportFile[1], index_col=False)
        save_csv_file(vaccinationFile[2],
                      get_WHO_data, url=vaccinationFile[0])
        data_vaccination = pd.read_csv(vaccinationFile[1], index_col=False)
        return data_latest, data_daily_report, data_vaccination


def df_gdf_merged_data(data_latest, data_vaccination, data_geojson):
    merged_data = pd.merge(data_latest, data_vaccination,
                           left_on='Country_Or_Region', right_on='COUNTRY')
    merged_data = pd.merge(merged_data, data_geojson,
                           left_on='ISO3', right_on='ISO_A3')
    merged_data = gpd.GeoDataFrame(merged_data)
    return merged_data


if __name__ == '__main__':
    # latestFile=WHO_LATEST_DATA, dailyReportFile=WHO_DAILY_REPORT_DATA, vaccinationFile=WHO_VACCINATION_DATA)
    global_data_latest, global_data_daily_report, global_data_vaccination = data_file_updated()

    global_data_geojson = geoJson_to_geodataframe()
    # print(global_data_geojson)

    # Use custom cols
    global_data_latest.columns = cols

    global_data_latest_vaccination_geojson = df_gdf_merged_data(global_data_latest, global_data_vaccination,
                                                                global_data_geojson)
    # global_data_latest_vaccination_geojson.plot(
    #     column='Cases', cmap='Spectral')
    # plt.show()
    # global_data_latest_vaccination_geojson.to_csv('Merged_data.csv')


# global_data_latest, global_data_daily_report, global_data_vaccination = data_file_updated()

# global_data_geojson = geoJson_to_geodataframe()

# # Use custom cols
# global_data_latest.columns = cols

# global_data_latest_vaccination_geojson = df_gdf_merged_data(global_data_latest, global_data_vaccination,
#                                                             global_data_geojson)
