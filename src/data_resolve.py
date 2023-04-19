import re
from datetime import datetime, timedelta
import plotly.express as px
import gzip
from urllib import request, error
import http
import csv
import pandas as pd
import geopandas as gpd
from shapely import wkt
import os
from win32 import win32file
from win32.lib import win32con
import pywintypes
import matplotlib.pyplot as plt
import numpy as np
from sqlalchemy import create_engine, MetaData, inspect, text, Integer, DATE, DECIMAL, VARCHAR, CHAR, MetaData, Table, Column
# used database name
db_name = ['WHO_COVID19_DATASET',
           'WORLD_BANK_COUNTRIES_ADMIN0_lOWERS_GEOJSON_DATASET']

ENGINE = 'InnoDB'
CHARSET = 'utf8mb4'
# Connect to the database using sqlalchemy
engine = create_engine(
    f"mysql+mysqlconnector://root:ROOT@localhost/{db_name[0]}", echo=False)
geo_engine = create_engine(
    f"mysql+mysqlconnector://root:ROOT@localhost/{db_name[1]}", echo=False)
# test the database connection
conn = engine.connect()
insp = inspect(conn)

# for database_name in db_name:
#     if database_name.lower() not in insp.get_schema_names():
#         conn.execute(text(
#             f"CREATE DATABASE {database_name} DEFAULT CHARACTER SET utf8mb4"))

# res files
WHO_LATEST_OUTPUT_FILE = 'latest.csv'
WHO_DAILY_REPORT_TABLE_OUTPUT_FILE = 'dailyReport.csv'
WHO_VACCINATION_OUTPUT_FILE = 'vaccination.csv'

WB_COUNTRIES_GEOJSON_FILE = 'WB_countries_Admin0_lowres'
WB_COUNTRIES_SHP_FILE = 'WB_countries_Admin0_10m'
# local path
PATH_ = 'E:/PY/'
RES_FILE_PATH = f"{PATH_}program/testforDV/data"


# geo-data
JSON_FILE_PATH = f"{RES_FILE_PATH}/json/"
SHP_FILE_PATH = f"{RES_FILE_PATH}/shp/"
GEOJSON_FILE_PATH = f"{JSON_FILE_PATH}{WB_COUNTRIES_GEOJSON_FILE}.geojson"
SHP_FILE_PATH = f"{SHP_FILE_PATH}{WB_COUNTRIES_SHP_FILE}/{WB_COUNTRIES_SHP_FILE}.shp"

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

WB_GEOJSON_DATA_TABLE = 'wb_countries_geojson_data_table'
WB_SHP_DATA_TABLE = 'wb_countries_shp_data_table'

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
                                   'PERSONS_VACCINATED_1PLUS_DOSE_PER100', 'PERSONS_FULLY_VACCINATED', 'PERSONS_FULLY_VACCINATED_PER100', 'VACCINES_USED',
                                   'FIRST_VACCINE_DATE', 'NUMBER_VACCINES_TYPES_USED', 'PERSONS_BOOSTER_ADD_DOSE', 'PERSONS_BOOSTER_ADD_DOSE_PER100']

WHO_VACCINATION_DATA_FIELD_TYPE = [VARCHAR(64), CHAR(3), CHAR(4), VARCHAR(16), DATE(), Integer(), DECIMAL(), Integer(), DECIMAL(), Integer(), DECIMAL(), VARCHAR(64),
                                   DATE(), Integer(), Integer(), DECIMAL()]


WB_GEOJSON_DATA_FIELD_NAME = ['FID', 'OBJECTID', 'FORMAL_EN', 'ISO_A2', 'ISO_A3_EH', 'WB_A2', 'WB_A3', 'CONTINENT', 'REGION_UN', 'SUBREGION', 'REGION_WB', 'NAME_DE', 'NAME_EN', 'WB_NAME',
                              'WB_REGION', 'Shape_Leng', 'Shape_Area', 'geometry']

WB_GEOJSON_DATA_FIELD_TYPE = ['INT', 'INT', 'VARCHAR(64)', 'CHAR(3)', 'CHAR(4)', 'CHAR(3)', 'CHAR(4)', 'VARCHAR(64)', 'VARCHAR(64)',
                              'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(16)', 'DOUBLE', 'DOUBLE', 'GEOMETRY']


WB_SHP_DATA_FIELD_NAME = ['OBJECTID', 'featurecla', 'LEVEL', 'TYPE', 'FORMAL_EN', 'FORMAL_FR', 'POP_EST', 'POP_RANK', 'GDP_MD_EST', 'POP_YEAR', 'LASTCENSUS', 'GDP_YEAR', 'ECONOMY', 'INCOME_GRP', 'FIPS_10_',
                          'ISO_A2', 'ISO_A3', 'ISO_A3_EH', 'ISO_N3', 'UN_A3', 'WB_A2', 'WB_A3', 'CONTINENT', 'REGION_UN', 'SUBREGION', 'REGION_WB', 'NAME_AR', 'NAME_BN', 'NAME_DE', 'NAME_EN', 'NAME_ES', 'NAME_FR',
                          'NAME_EL', 'NAME_HI', 'NAME_HU', 'NAME_ID', 'NAME_IT', 'NAME_JA', 'NAME_KO', 'NAME_NL', 'NAME_PL', 'NAME_PT', 'NAME_RU', 'NAME_SV', 'NAME_TR', 'NAME_VI', 'NAME_ZH', 'WB_NAME', 'WB_RULES',
                          'WB_REGION', 'Shape_Leng', 'Shape_Area', 'geometry']

WB_SHP_DATA_FIELD_TYPE = ['INT', 'VARCHAR(16)', 'INT', 'VARCHAR(32)', 'VARCHAR(64)', 'VARCHAR(64)', 'INT', 'INT', 'INT', 'INT', 'INT', 'INT', 'VARCHAR(128)', 'VARCHAR(128)', 'VARCHAR(64)', 'VARCHAR(4)', 'CHAR(4)', 'CHAR(4)', 'CHAR(4)', 'CHAR(4)',
                          'CHAR(4)', 'VARCHAR(32)', 'VARCHAR(32)', 'VARCHAR(32)', 'VARCHAR(32)', 'VARCHAR(32)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)',
                          'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)', 'VARCHAR(64)',
                          'VARCHAR(64)', 'VARCHAR(128)', 'VARCHAR(32)', 'INT', 'INT', 'GEOMETRY']
# data tuple (url, local_path, output_file, mysql_table, field_name, field_mysql_type)
WHO_LATEST_DATA = (URL_WHO_LATEST, WHO_LATEST_LOCAL_PATH,
                   WHO_LATEST_OUTPUT_FILE, WHO_LATEST_DATA_MYSQL_TABLE, WHO_LATEST_DATA_FIELD_NAME_REPLACE, WHO_LATEST_DATA_FIELD_TYPE)
WHO_DAILY_REPORT_DATA = (
    URL_WHO_DAILY_REPORT, WHO_DAILY_REPORT_TABLE_PATH, WHO_DAILY_REPORT_TABLE_OUTPUT_FILE, WHO_DAILY_REPORT_DATA_MYSQL_TABLE, WHO_DAILY_REPORT_DATA_FIELD_NAME, WHO_DAILY_REPORT_DATA_FIELD_TYPE)
WHO_VACCINATION_DATA = (URL_WHO_VACCINATION,
                        WHO_VACCINATION_PATH, WHO_VACCINATION_OUTPUT_FILE, WHO_VACCINATION_DATA_MYSQL_TABLE, WHO_VACCINATION_DATA_FIELD_NAME, WHO_VACCINATION_DATA_FIELD_TYPE)

WB_GEOJSON_DATA = (GEOJSON_FILE_PATH, WB_GEOJSON_DATA_TABLE,
                   WB_GEOJSON_DATA_FIELD_NAME, WB_GEOJSON_DATA_FIELD_TYPE)

WB_SHP_DATA = (SHP_FILE_PATH, WB_SHP_DATA_TABLE,
               WB_SHP_DATA_FIELD_NAME, WB_SHP_DATA_FIELD_TYPE)


def create_GeoJson_mysql_table(jsonFile_path):
    field_type = zip(jsonFile_path[2],
                     jsonFile_path[3])
    table_schema = ','.join(['{} {} not null'.format(col_name, col_data_type)
                             for col_name, col_data_type in field_type])
    create_table_sql_text = text(
        f"CREATE TABLE {jsonFile_path[1]} ({table_schema}) ENGINE={ENGINE} DEFAULT CHARSET={CHARSET};")
    conn.execute(create_table_sql_text)
    print(f"create table {jsonFile_path[1]} finished.")


def escape_quotes(match):
    return match.group(0).replace('"', '\\"')


def clean_wb_rules(wb_rule):
    pattern = re.compile(r'(?<!\\)"(.*?)(?<!\\)"')
    wb_rule = pattern.sub(escape_quotes, wb_rule)
    return wb_rule


def import_GeoJson_data_to_mysql_data(jsonFile_path):
    conn.execute(text(f"USE {db_name[1]}"))
    gdf = gpd.read_file(jsonFile_path[0])
    gdf.fillna(value='NULL', inplace=True)
    gdf['ISO_A3_EH'] = np.where(
        gdf['ISO_A3_EH'] == '-99', gdf['WB_A3'], gdf['ISO_A3_EH'])

    gdf['WB_RULES'] = gdf['WB_RULES'].apply(clean_wb_rules)

    gdf['geometry'] = gdf['geometry'].apply(
        lambda x: x.wkt)

    field_names = ','.join(jsonFile_path[2])

    # print(field_names)
    for i, r in gdf.iterrows():
        values = []
        for value in r.values[0:-1]:
            if isinstance(value, str):
                values.append(f"\"{value}\"")
            else:
                values.append(str(value))
        values.append(f"ST_GeomFromText(\"{r.values[-1]}\")")
        values_str = ','.join(values)
        sql = text(
            f"INSERT INTO {jsonFile_path[1]} ({field_names}) VALUES ({values_str})")
        conn.execute(sql)
        conn.commit()

    print(f"Data in {jsonFile_path[1]} created successfully.")


def geoJson_data_is_enough(jsonFile_path):
    data_is_enough = conn.execute(
        text(f"SELECT COUNT(*) FROM {jsonFile_path[1]};")).fetchone()[0]
    return data_is_enough


def update_mysql_geoJson(jsonFile_path):
    conn.execute(text(f"USE {db_name[1]}"))

    if not insp.has_table(jsonFile_path[1], db_name[1]):
        create_GeoJson_mysql_table(jsonFile_path)
        import_GeoJson_data_to_mysql_data(jsonFile_path)

    data_is_enough = geoJson_data_is_enough(jsonFile_path)
    if not bool(data_is_enough):
        import_GeoJson_data_to_mysql_data(jsonFile_path)
    elif data_is_enough < 238:
        conn.execute(
            text(f"TRUNCATE TABLE {jsonFile_path[1]}"))
        import_GeoJson_data_to_mysql_data(jsonFile_path)
    else:
        print(
            f"Data in {jsonFile_path[1]} is already OK. Don't need to do anything.")


def get_geoDataFrame(jsonFile_path):
    update_mysql_geoJson(jsonFile_path)
    field_name = jsonFile_path[2][0:-1]
    field_name.append(f"ST_AsTEXT({jsonFile_path[2][-1]})")
    field_names = ','.join(field_name)

    tmp_field_names = f"FORMAL_EN, ISO_A2, ISO_A3_EH, WB_A3, ST_AsTEXT(geometry)"

    sql = text(f"SELECT {tmp_field_names} FROM {jsonFile_path[1]}")

    # gdf = conn.execute(text(sql)).fetchall()
    # for chunk_dataframe in pd.read_sql(sql, geo_engine, chunksize=25):
    #     print(f"Got dataframe w/{len(chunk_dataframe)} rows")
    # for df in df_iter:
    #     print(df)

    gdf = pd.read_sql(sql=sql, con=geo_engine.connect())
    gdf['ST_AsTEXT(geometry)'] = gdf['ST_AsTEXT(geometry)'].apply(wkt.loads)
    gdf.rename(columns={'ST_AsTEXT(geometry)': 'geometry'}, inplace=True)
    gdf = gpd.GeoDataFrame(gdf, geometry='geometry', crs=4326)
    return gdf


def data_update_time_setting(who_covid19_file):
    handle_file = win32file.CreateFile(who_covid19_file[1], win32con.GENERIC_READ,
                                       win32con.FILE_SHARE_READ, None, win32con.OPEN_EXISTING, 0, None)
    '''
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        created_time = datetime.datetime.fromtimestamp(
            os.path.getctime(file_path))
        print(f"{file}: {created_time}")
    '''
    # last modified time

    handle_file_time = win32file.GetFileTime(handle_file)[2]
    win32file.CloseHandle(handle_file)
    handle_file_time_latest = datetime.fromtimestamp(
        pywintypes.Time(handle_file_time).timestamp())
    now_time = datetime.fromtimestamp(
        pywintypes.Time(datetime.now()).timestamp())
    diff_handle_file_time = now_time - handle_file_time_latest
    return diff_handle_file_time


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
                csv_data = response.read()
                csv_data = csv_data.decode('utf-8-sig').splitlines()
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


def create_mysql_table(dataFile):
    field_types = zip(dataFile[4], dataFile[5])

    # according to position you want insert to, you need to change position of ','
    # primary_key = ', ID INT AUTO_INCREMENT PRIMARY KEY'

    mysql_metadata = MetaData()
    table = Table(dataFile[3],
                  mysql_metadata,
                  *[Column(col_name, col_data_type, nullable=False)
                    for col_name, col_data_type in field_types],
                  mysql_engine=ENGINE,
                  mysql_charset=CHARSET
                  )
    table.create(bind=conn)
    # mysql_metadata.create_all(bind=engine)
    print(f"{dataFile[3]} created successfully.")


def import_data_to_table(dataFile):
    # Read CSV data
    data_reader = pd.read_csv(
        dataFile[1], quotechar='"', delimiter=',', header=0, index_col=False, encoding='ISO-8859-1')
    data_reader.fillna(value='NULL', inplace=True)
    data_reader.columns = map(str.lower, dataFile[4])
    if dataFile[3] != WHO_DAILY_REPORT_DATA_MYSQL_TABLE:
        data_reader.columns = dataFile[4]
        conn.execute(text(f"TRUNCATE TABLE {dataFile[3]}"))
    data_reader.to_sql(
        name=dataFile[3], con=engine, if_exists='replace', index=False, schema=db_name[0])

    # engine.execute(
    #     f"ALTER TABLE {dataFile[3]} ADD {primary_key} FIRST;")
    print(f"Data in {dataFile[3]} created successfully.")


def table_has_enough_data(dataFile):
    table_has_data = conn.execute(
        text(f"SELECT COUNT(*) FROM {dataFile[3]};")).fetchone()[0]
    return table_has_data


def init_mysql_table(dataFile):
    # Check if the table exists and has data
    conn.execute(text(f"USE {db_name[0]}"))
    table_exists = bool(conn.execute(text('SHOW TABLES')).fetchall())

    if not table_exists or not insp.has_table(dataFile[3], db_name[0]):
        create_mysql_table(dataFile)
        import_data_to_table(dataFile)
        return True
    # Create table if it doesn't exist or has no data
    elif table_has_enough_data(dataFile) < 251:
        import_data_to_table(dataFile)
        return True
    else:
        print("Initializated database, check whether this table need to update.")
        return False


def update_mysql_table(dataFile):
    print(f"Current table is {dataFile[3]}")
    if not init_mysql_table(dataFile):
        # Update table if latest data is older than 7 days
        if dataFile[3] == WHO_DAILY_REPORT_DATA_MYSQL_TABLE:
            # Get latest date from the table
            with engine.begin() as connection:
                latest_date_str = connection.execute(text(
                    f"SELECT MAX(date_reported) FROM {dataFile[3]}")).fetchone()[0]
                print(latest_date_str)
            latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d')
            # Compare latest date with current date
            days_diff = (datetime.today() - latest_date).days
            if days_diff >= 7:
                data_reader = pd.read_csv(
                    dataFile[1], quotechar='"', delimiter=',', header=0)
                data_reader.fillna(value='NULL', inplace=True)
                data_reader.columns = map(str.lower, data_reader.columns)

                data_reader['date_reported'] = pd.to_datetime(
                    data_reader['date_reported'])
                updated_data = data_reader[
                    data_reader['date_reported'] > latest_date]

                if updated_data.empty:
                    print(
                        f"Data in {dataFile[3]} is correspond to source data. Don't need to do anything.")
                    return
                with engine.begin() as connection:
                    updated_data.to_sql(
                        name=dataFile[3], con=connection, if_exists='append', index=True)
                print(f"Data in {dataFile[3]} updated successfully.")
            else:
                print(
                    f"Data in {dataFile[3]} is up to date. Don't need to do anything.")
        else:
            import_data_to_table(dataFile)
            print(f"Data in {dataFile[3]} updated successfully.")

    else:
        print(
            f"Data in {dataFile[3]} is created and already up to date. Don't need to do anything.\n***if table doesn't exists, it will created table***")


def get_who_covid19_dataframe(who_covid19_file):

    # !!!
    # To debug unknown data problems (including the API itself)
    # update_mysql_table(who_covid19_file)

    field_names = ','.join(who_covid19_file[4])
    sql = text(f"SELECT {field_names} from {who_covid19_file[3]}")
    df = pd.read_sql(sql, engine.connect())
    return df


def init_all_default_who_covid19_data(latestFile=WHO_LATEST_DATA, dailyReportFile=WHO_DAILY_REPORT_DATA, vaccinationFile=WHO_VACCINATION_DATA):
    save_csv_file(latestFile[2], get_WHO_data, url=latestFile[0])
    update_mysql_table(latestFile)
    save_csv_file(dailyReportFile[2], get_WHO_data,
                  url=dailyReportFile[0])
    update_mysql_table(dailyReportFile)
    save_csv_file(vaccinationFile[2],
                  get_WHO_data, url=vaccinationFile[0])
    update_mysql_table(vaccinationFile)


def update_who_data_file(who_covid19_file):
    who_covid19_file_path = os.path.join(who_covid19_file[1])
    if os.path.isfile(who_covid19_file_path):
        diff_file_time = data_update_time_setting(who_covid19_file)
        if who_covid19_file == WHO_LATEST_DATA or who_covid19_file == WHO_VACCINATION_DATA:
            if diff_file_time > timedelta(hours=16):
                print(f"{who_covid19_file[2]}: ", diff_file_time)
                save_csv_file(
                    who_covid19_file[2], get_WHO_data, url=who_covid19_file[0])
                update_mysql_table(who_covid19_file)
            else:
                print(
                    f"Curr diff timing not enough to update-----> {who_covid19_file[2]}: ", diff_file_time)
        else:
            if diff_file_time > timedelta(days=4):
                print(f"{who_covid19_file[2]}: ", diff_file_time)
                save_csv_file(
                    who_covid19_file[2], get_WHO_data, url=who_covid19_file[0])
                update_mysql_table(who_covid19_file)
            else:
                print(
                    f"Curr diff timing not enough to update-----> {who_covid19_file[2]}: ", diff_file_time)
    else:
        init_all_default_who_covid19_data()
        return
    print("Database is up to date.")


if __name__ == '__main__':
    latestFile = WHO_LATEST_DATA
    dailyReportFile = WHO_DAILY_REPORT_DATA
    vaccinationFile = WHO_VACCINATION_DATA
    update_who_data_file(latestFile)
    update_who_data_file(dailyReportFile)
    update_who_data_file(vaccinationFile)
    gdf = get_geoDataFrame(WB_GEOJSON_DATA)
    gdf = get_geoDataFrame(WB_SHP_DATA)
    df_latest = get_who_covid19_dataframe(latestFile)
    df_daily_report = get_who_covid19_dataframe(dailyReportFile)
    df_vaccination = get_who_covid19_dataframe(vaccinationFile)

    # Abnormal data processing
    # mysql decoding problem: 'Türkiye' : 'TÃ¼rkiye'
    df_latest['Country_Or_Region'] = df_latest['Country_Or_Region'].replace(
        'TÃ¼rkiye', 'Turkey')

    df_lat_vacc = pd.merge(df_latest, df_vaccination,
                           left_on='Country_Or_Region', right_on='COUNTRY')

    CURR_GDF = gpd.GeoDataFrame(pd.merge(gdf, df_lat_vacc, left_on='ISO_A3_EH',
                                         right_on='ISO3'), geometry='geometry')

    CURR_GDF.plot(column='Cases', legend=True, cmap='YlOrRd')

    # save geodataframe as shpfile
    CURR_GDF.to_file('output', encoding='utf-8')

    plt.show()


engine.dispose()
geo_engine.dispose()
