from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect
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
import mysql.connector
from datetime import datetime
from sqlalchemy import create_engine, Integer, DATE, DECIMAL, VARCHAR, CHAR

# used database name
db_name = 'WHO_COVID19_DATASET'
# connect to mysql database

# cnx = mysql.connector.connect(user='root', password='HM&&S.914-1',
#                               host='localhost')
engine = create_engine(
    f"mysql+mysqlconnector://root:HM&&S.914-1@localhost/{db_name}")

# Connect to the database using sqlalchemy
engine = create_engine(
    f"mysql+mysqlconnector://root:HM&&S.914-1@localhost/{db_name}")

engine.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")

# res files
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

# mysql tables
WHO_LATEST_DATA_MYSQL_TABLE = 'who_covid_19_latest_data_table'
WHO_DAILY_REPORT_DATA_MYSQL_TABLE = 'who_covid_19_daily_report_data_table'
WHO_VACCINATION_DATA_MYSQL_TABLE = 'who_covid_19_vaccination_data_table'

# data field name and field types
WHO_LATEST_DATA_FIELD_NAME = ['Name', 'WHO_region', 'Cases - cumulative total', 'Cases - cumulative total per 100000 population', 'Cases - newly reported in last 7 days',
                              'Cases - newly reported in last 7 days per 100000 population', 'Cases - newly reported in last 24 hours', 'Deaths - cumulative total',
                              'Deaths - cumulative total', 'Deaths - cumulative total', 'Deaths - newly reported in last 7 days per 100000 population',
                              'Deaths - newly reported in last 7 days per 100000 population']
WHO_LATEST_DATA_FIELD_NAME_REPLACE = ['Country_Or_Region', 'WHO_Region', 'Cases', 'CasesPer01Million', 'NewlyCasesInLast7days', 'NewlyCasesInLast7daysPer01Million', 'NewlyCasesInLast1days',
                                      'Deaths', 'NewlyDeathsPer01Million', 'NewlyDeathsInLast7days', 'NewlyDeathsInLast7daysPer01Million', 'NewlyDeathsInLast1days']
WHO_LATEST_DATA_FIELD_TYPE = [VARCHAR(64), VARCHAR(32), Integer(), DECIMAL(), Integer(),
                              DECIMAL(), Integer(), Integer(), DECIMAL, Integer(), DECIMAL(), Integer()]

WHO_DAILY_REPORT_DATA_FIELD_NAME = ['Date_reported', 'Country_code', 'Country',
                                    'WHO_region', 'New_cases', 'Cumulative_cases', 'New_deaths', 'Cumulative_deaths']
WHO_DAILY_REPORT_DATA_FIELD_TYPE = [
    DATE(), CHAR(2), VARCHAR(64), CHAR(4), Integer(), Integer(), Integer(), Integer()]

WHO_VACCINATION_DATA_FIELD_NAME = ['COUNTRY', 'ISO3', 'WHO_REGION', 'DATA_SOURCE', 'DATE_UPDATED', 'TOTAL_VACCINATIONS', 'PERSONS_VACCINATED_1PLUS_DOSE', 'TOTAL_VACCINATIONS_PER100',
                                   'PERSONS_VACCINATED_1PLUS_DOSE_PER100', 'PERSONS_FULLY_VACCINATED', 'PERSONS_FULLY_VACCINATED_PER100', 'PERSONS_FULLY_VACCINATED_PER100',
                                   'FIRST_VACCINE_DATE', 'NUMBER_VACCINES_TYPES_USED', 'PERSONS_BOOSTER_ADD_DOSE', 'PERSONS_BOOSTER_ADD_DOSE_PER100']
WHO_VACCINATION_DATA_FIELD_TYPE = [VARCHAR(64), CHAR(3), CHAR(4), VARCHAR(16), DATE(), Integer(), DECIMAL(), Integer(), DECIMAL(), Integer(), DECIMAL(), VARCHAR(64),
                                   DATE(), Integer(), Integer(), DECIMAL()]

# data tuple (url, local_path, output_file, mysql_table, field_name, field_mysql_type)
WHO_LATEST_DATA = (URL_WHO_LATEST, WHO_LATEST_LOCAL_PATH,
                   WHO_LATEST_OUTPUT_FILE, WHO_LATEST_DATA_MYSQL_TABLE, WHO_LATEST_DATA_FIELD_NAME_REPLACE, WHO_LATEST_DATA_FIELD_TYPE)
WHO_DAILY_REPORT_DATA = (
    URL_WHO_DAILY_REPORT, WHO_DAILY_REPORT_TABLE_PATH, WHO_DAILY_REPORT_TABLE_OUTPUT_FILE, WHO_DAILY_REPORT_DATA_MYSQL_TABLE, WHO_DAILY_REPORT_DATA_FIELD_NAME, WHO_DAILY_REPORT_DATA_FIELD_TYPE)
WHO_VACCINATION_DATA = (URL_WHO_VACCINATION,
                        WHO_VACCINATION_PATH, WHO_VACCINATION_OUTPUT_FILE, WHO_VACCINATION_DATA_MYSQL_TABLE, WHO_VACCINATION_DATA_FIELD_NAME, WHO_VACCINATION_DATA_FIELD_TYPE)


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


# def update_mysql_table(dataFile):
#     global cnx
#     try:
#         cursor = cnx.cursor()
#         cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
#         cursor.execute(f"USE {db_name}")
#         table_schema = ','.join(['{} {}'.format(col_name, col_data_type)
#                                  for col_name, col_data_type in zip(dataFile[4], dataFile[5])])
#         cursor.execute(
#             f"CREATE TABLE IF NOT EXISTS {dataFile[3]} ({table_schema})")
#         data_reader = pd.read_csv(
#             dataFile[1], quotechar='"', delimiter=',', header=0)
#         data_reader.fillna(value='NULL', inplace=True)
#         data_reader.columns = map(str.lower, data_reader.columns)
#         field_types = dict(zip(dataFile[4], dataFile[5]))
#         with engine.begin() as connection:
#             data_reader.to_sql(
#                 name=dataFile[3], con=connection, if_exists='replace', dtype=field_types)
#         cnx.commit()
#         print("Data inserted successfully.")
#         cursor.close()
#     except mysql.connector.Error as err:
#         print(f"Error: {err}")
#         cursor.close()
#     except Exception as e:
#         print(e)
#         print(data_reader.loc[data_reader.isnull().any(axis=1)])


def update_mysql_table(dataFile):
    # Check if the table exists and has data
    insp = inspect(engine)
    table_exists = dataFile[3] in insp.get_table_names()
    table_has_data = False
    if table_exists:
        table_has_data = bool(engine.execute(
            f"SELECT EXISTS(SELECT 1 FROM {dataFile[3]} LIMIT 1)").scalar())

    # Create table if it doesn't exist or has no data
    if not table_exists or not table_has_data:
         # Read CSV data
        data_reader = pd.read_csv(
            dataFile[1], quotechar='"', delimiter=',', header=0)
        data_reader.fillna(value='NULL', inplace=True)
        data_reader.columns = map(str.lower, data_reader.columns)

        field_types = dict(zip(dataFile[4], dataFile[5]))
        table_schema = ','.join(['{} {}'.format(col_name, col_data_type)
                                 for col_name, col_data_type in field_types])
        with engine.begin() as connection:
            connection.execute(f"DROP TABLE IF EXISTS {dataFile[3]}")
            connection.execute(f"CREATE TABLE {dataFile[3]} ({table_schema})")
            data_reader.to_sql(
                name=dataFile[3], con=connection, if_exists='replace', index=False)
        print("Table created successfully.")
        return

    # Get latest date from the table
    with engine.begin() as connection:
        latest_date_str = connection.execute(
            f"SELECT MAX({dataFile[4][0]}) FROM {dataFile[3]}").scalar()
    latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d')

    # Compare latest date with current date
    days_diff = (datetime.today() - latest_date).days

    # Update table if latest data is older than 7 days
    if days_diff >= 7:
        field_types = dict(zip(dataFile[4], dataFile[5]))
        with engine.begin() as connection:
            data_reader.to_sql(
                name=dataFile[3], con=connection, if_exists='append', index=False)
        print("Data updated successfully.")
    else:
        print("Data is up to date. Don't need to do anything.")


def update_data_file(latestFile=WHO_LATEST_DATA, dailyReportFile=WHO_DAILY_REPORT_DATA, vaccinationFile=WHO_VACCINATION_DATA):
    latest_file_path = os.path.join(latestFile[1])
    daly_report_file_path = os.path.join(dailyReportFile[1])
    vaccination_file_path = os.path.join(vaccinationFile[1])
    if os.path.isfile(latest_file_path) and os.path.isfile(daly_report_file_path) and os.path.isfile(vaccination_file_path):
        diff_latest, diff_daily_report, diff_vaccination = data_update_time_setting()
        if diff_latest > datetime.timedelta(hours=18):
            print('latest:', diff_latest)
            save_csv_file(latestFile[2], get_WHO_data, url=latestFile[0])

        elif diff_vaccination > datetime.timedelta(hours=18):
            print('vaccination:', diff_vaccination)
            save_csv_file(vaccinationFile[2],
                          get_WHO_data, url=vaccinationFile[0])

        elif diff_daily_report > datetime.timedelta(days=4):
            print('daily report:', diff_daily_report)
            save_csv_file(dailyReportFile[2], get_WHO_data,
                          url=dailyReportFile[0])

        else:
            print('latest:', diff_latest)
            print('daily report:', diff_daily_report)
            print('vaccination:', diff_vaccination)

    else:
        save_csv_file(latestFile[2], get_WHO_data, url=latestFile[0])
        save_csv_file(dailyReportFile[2], get_WHO_data,
                      url=dailyReportFile[0])
        save_csv_file(vaccinationFile[2],
                      get_WHO_data, url=vaccinationFile[0])


def df_gdf_merged_data(data_latest, data_vaccination, data_geojson):
    merged_data = pd.merge(data_latest, data_vaccination,
                           left_on='Country_Or_Region', right_on='COUNTRY')
    merged_data = pd.merge(merged_data, data_geojson,
                           left_on='ISO3', right_on='ISO_A3')
    merged_data = gpd.GeoDataFrame(merged_data)
    return merged_data


if __name__ == '__main__':
    # latestFile=WHO_LATEST_DATA, dailyReportFile=WHO_DAILY_REPORT_DATA, vaccinationFile=WHO_VACCINATION_DATA)
    update_mysql_table(WHO_DAILY_REPORT_DATA)

# close connection
# cnx.close()

engine.dispose()
