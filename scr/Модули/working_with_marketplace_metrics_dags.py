import pandas as pd
import sys
import glob
import xlrd
link_to_table = 'https://docs.google.com/spreadsheets/d/1A1TyZPeq9r2HiDi-CHfmTfRpuAQYToib1vqyTx4ZyRE/edit#gid=0'
df = pd.read_excel('/'.join(link_to_table.split('/')[:-1])+'/export?format=xlsx')
PATH = [dict(df.loc[x]) for x in list(df.index)]
df = None
i = 0
while len([py for py in glob.glob(f"{PATH[i]['path_downloads']}*.xlsx")]) == 0:
    i +=1
path_downloads = PATH[i]['path_downloads']
path_algoritm = PATH[i]['path_algoritm']
sys.path.append(f'{path_algoritm}Модули')
import constants_for_marketplace_metrics_dags as const
import maslow_working_module as mwm
from typing import Any, Dict, List
import pymysql
import pandas as pd
from sys import exit
from sqlalchemy import create_engine
import numpy as np
import re
import datetime
from tqdm import tqdm
import gspread
import gspread_dataframe as gd


maslow_json = path_downloads + 'my-test-project-396610-962484cc39dd.json'


# Проверка на уникальные артикулы
def unique_items(data):
    if 'Артикул' in data.columns:
        data['Артикул'] = data['Артикул'].astype(str)
        if len(data) - len(list(set(data['Артикул']))) != 0:
            print('в датафрейме неуникальные артикулы')
            exit()
        else:
            None
    else:
        print('в датафрейме нет столбца "Артикул"')
        
# Проверка на уникальные баркоды
def unique_barcode(data):
    if 'Баркод' in data.columns:
        if len(data) - len(list(set(data['Баркод']))) != 0:
            print('в датафрейме неуникальные баркоды')
            exit()
        else:
            None
    else:
        print('в датафрейме нет столбца "Баркод"')



# Получение Реестра из гугл таблицы
def Reestr_from_google_table(maslow_json=maslow_json, flag=1):
    gc = gspread.service_account(filename=maslow_json)
    sh = gc.open("Реестр")
    worksheet = sh.worksheet("Реестр")
    reestr = pd.DataFrame(worksheet.get_all_records())
    reestr['Арт'] = reestr['Арт'].astype(str)
    reestr['Название'] = reestr['Название'].astype(str)
    if flag == 1:
        reestr = reestr.rename(columns={'Арт': 'Артикул'})
        reestr = reestr[['Артикул', 'Название']]
        unique_items(reestr)
    else:
        None
    return reestr


# Получение Реестра
def Reestr(table_name='Реестр', schema=mwm.ir_schema, flag=1):
    req = f"""SELECT * FROM {schema}.{table_name};"""
    connection, cursor = mwm.conn()
    cursor.execute(req)
    rows = cursor.fetchall()
    reestr = pd.DataFrame(rows)
    req_columns = f"""SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position;"""
    cursor.execute(req_columns)
    rows_columns = cursor.fetchall()
    df_columns = pd.DataFrame(rows_columns)
    reestr.columns = list(df_columns[0])
    if flag == 1:
        reestr = reestr[['art', 'nazvanie']]
        reestr.columns = ['Артикул', 'Название']
        unique_items(reestr)
    else:
        None
    return reestr


reestr = Reestr()



bd_param = const.bd_param
engine_param = const.engine_param

dt_now = const.dt_now
end_date =const.end_date
start_date = const.start_date


    #paths
#wb
path_wb_orders_metrics = path_algoritm + 'FULL_DF/wb_orders_metrics.xlsx'
path_wb_fbs_stocks_metrics = path_algoritm + 'FULL_DF/wb_fbs_stocks_metrics.xlsx'
path_wb_fbo_stocks_metrics = path_algoritm + 'FULL_DF/wb_fbo_stocks_metrics.xlsx'
path_wb_price_metrics = path_algoritm + 'FULL_DF/wb_price_metrics.xlsx'

path_full_WB_df = path_algoritm + 'Все файлы для ежедневной презентации/full_WB_df.xlsx'

#ozon
path_ozon_orders_metrics = path_algoritm + 'FULL_DF/ozon_orders_metrics.xlsx'
path_ozon_fbs_stocks_metrics = path_algoritm + 'FULL_DF/ozon_fbs_stocks_metrics.xlsx'
path_ozon_fbo_stocks_metrics = path_algoritm + 'FULL_DF/ozon_fbo_stocks_metrics.xlsx'
path_ozon_price_metrics = path_algoritm + 'FULL_DF/ozon_price_metrics.xlsx'

path_full_OZON_df = path_algoritm + 'Все файлы для ежедневной презентации/full_OZON_df.xlsx'

#yandex
path_ya_orders_metrics = path_algoritm + 'FULL_DF/ya_orders_metrics.xlsx'
path_ya_fbs_stocks_metrics = path_algoritm + 'FULL_DF/ya_fbs_stocks_metrics.xlsx'
path_ya_fbo_stocks_metrics = path_algoritm + 'FULL_DF/ya_fbo_stocks_metrics.xlsx'
path_ya_price_metrics = path_algoritm + 'FULL_DF/ya_price_metrics.xlsx'

path_full_YA_df = path_algoritm + 'Все файлы для ежедневной презентации/full_YA_df.xlsx'

#sber
path_sb_orders_metrics = path_algoritm + 'FULL_DF/sb_orders_metrics.xlsx'
path_sb_fbo_stocks_metrics = path_algoritm + 'FULL_DF/sb_fbo_stocks_metrics.xlsx'
path_sb_price_metrics = path_algoritm + 'FULL_DF/sb_price_metrics.xlsx'

path_full_SBER_df = path_algoritm + 'Все файлы для ежедневной презентации/full_SBER_df.xlsx'

#letual
path_le_orders_metrics = path_algoritm + 'FULL_DF/le_orders_metrics.xlsx'
path_le_fbo_stocks_metrics = path_algoritm + 'FULL_DF/le_fbo_stocks_metrics.xlsx'
path_le_price_metrics = path_algoritm + 'FULL_DF/le_price_metrics.xlsx'

path_full_LE_df = path_algoritm + 'Все файлы для ежедневной презентации/full_LE_df.xlsx'


    #Наценки
#Вб
path_wb_markup_metrics = path_algoritm + 'WB_ORDERS/WB_ORDERS_NEW_ALGORITM/Наценка/wb_markup_metrics.xlsx'
#Озон
path_oz_markup_metrics = path_algoritm + 'OZON_ORDERS/OZON_ORDERS_NEW_ALGORITM/Наценка/oz_markup_metrics.xlsx'
#Яндекс
path_ya_markup_metrics = path_algoritm + 'YANDEX_ORDERS/Наценка/ya_markup_metrics.xlsx'
#Сбер
path_sb_markup_metrics = path_algoritm + 'SBER_ORDER/Наценка/sb_markup_metrics.xlsx'
#Летуаль
path_le_markup_metrics = path_algoritm + 'LETUAL_ORDERS/Наценка/le_markup_metrics.xlsx'


#РЦ
path_rc = path_algoritm + 'ОБЩИЕ ПРИЗНАКИ ДЛЯ МЕТРИК/РЦ/РЦ_обработанные.xlsx'


# Статусы
path_st = path_algoritm + 'ОБЩИЕ ПРИЗНАКИ ДЛЯ МЕТРИК/Статусы/Статусы.xlsx'



# Функция подключения
def conn(bd_param=bd_param):
    return pymysql.connect(host=bd_param['host'], 
                           port=bd_param['port'], 
                           user=bd_param['user'], 
                           password=bd_param['password'], 
                           database=bd_param['database'], 
                           cursorclass=bd_param['cursorclass'])

# Функция "очистки" артикула
def clear_article(data, name_columns):
    data[name_columns] = data[name_columns].astype(str)
    data = data.reset_index(drop=True)
    letter_dict = {'е': 'e', 
                   'Е': 'E', 
                   'Т': 'T', 
                   'у': 'y', 
                   'о': 'o', 
                   'О': 'O', 
                   'р': 'p', 
                   'Р': 'P', 
                   'а': 'a', 
                   'А': 'A', 
                   'Н': 'H',
                   'К': 'K',
                   'х': 'x',
                   'Х': 'X',
                   'с': 'c',
                   'С': 'C',
                   'В': 'B',
                   'М': 'M'}
    for i in range(len(data)):
        if len(set(data.loc[i][name_columns]) & set(letter_dict.keys())) > 0:
            art = data.loc[i][name_columns]
            for row in art:
                if row in letter_dict.keys():
                    art = art.replace(row, letter_dict[row])
            data.loc[i, name_columns] = art
    data[name_columns] = [re.sub(r'[^a-zA-Z0-9]', '', x) for x in list(data[name_columns])]
    data = data[data[name_columns].isin([x for x in list(data[name_columns]) if len(x) > 6])==False]
    return data

# Преобразование типа данных датафрейма (для корректного помещения данных в БД)
def astype_df(data): 
    types_dict = {}
    for key in data.loc[0].to_dict():
        if f'{type(data.loc[0].to_dict()[key])}' in  ["<class 'NoneType'>", "<class 'list'>", "<class 'dict'>"]:
            types_dict[key] = str
        else:
            types_dict[key] = type(data.loc[0].to_dict()[key])
    data = data.astype(types_dict)
    return data

# Создание SQL-запроса на создание таблицы в БД по датафрейму
def create_table_from_dict_template(table: str, dict_template: Dict[str, str], index_field: str = 'id'):
    fields_text = ''
    for key in dict_template:
        fields_text += f'`{key}` {dict_template[key]},'
    sql_request = f"CREATE TABLE IF NOT EXISTS `{table}` ({fields_text} PRIMARY KEY (`{index_field}`))"
    return  sql_request

# Определение типа данных
def recoginze_value_type(value: Any) -> str:
    default = 'VARCHAR(255)'
    if isinstance(value, int):
        return 'BIGINT'
    elif isinstance(value, float):
        return 'FLOAT'
    elif isinstance(value, bool):
        return 'BOOL'
    elif isinstance(value, str):
        try:
            dt_obj = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except:
            pass
        else:
            return 'DATETIME'
        return default
    else:
        return default
    
# Создание блока в запросе sql с определением типов данных 
def make_dict_template(fields_dict: Dict[str, Any]) -> Dict[str, str]:
    res = dict()
    for key in fields_dict:
        res[key] = recoginze_value_type(fields_dict[key])
    return res

# Функция создания запроса на SQL для формирования таблицы в БД
def create_table_unique_cols_sql_request(table, dict_template, unique_cols):
    return f"""CREATE TABLE IF NOT EXISTS {table} 
            ({', '.join([f"{x} {dict_template[x]}" for x in dict_template])},
            CONSTRAINT {'_'.join(unique_cols)+'_unique'} UNIQUE ({', '.join(unique_cols)}))"""

# Создание SQL-запроса на заполнение таблицы в БД данными датафрейма
def insert_values(data, data_name):
    connection = conn()
    cursor = connection.cursor()
    cols = str(tuple(data.columns)).replace("'",'')
    chunks = np.array_split(list(range(len(data))), len(data) // 100)
    for chunk in chunks:
        val = str([tuple(x) for x in (data.loc[chunk].to_numpy())])[1:-1]
        req = f"INSERT INTO {data_name} {cols} VALUES {val};"
        cursor.execute(req)
        connection.commit()
    connection.close()
    
# Добавление данных в таблицы, часть 2 (data - датафрейм с новыми данными, 
                                  # data_table_name - название таблицы в БД для этих данных,
                                  # data_metrics - датафрейм с данныи для исторических метрик,
                                  # data_metrics_table_name - название таблицы в БД для этих исторических данных)
def insert_values_p2(data, data_table_name, data_metrics, data_metrics_table_name):
    try:
        if len(pd.read_sql(data_table_name, con=create_engine(engine_param))) != 0:
            print(f'в {data_table_name} есть старые данные')
            exit()   
        else:
            None
    except:
        print(f'{data_table_name} еще не создана')
        exit() 
    try:
        insert_values(data, data_table_name)
    except:
        print(f'Даные в {data_table_name} не были добавлены, появилась ошибка')
        exit()
    date_filter = data['date'].unique()[0]
    try:
        date_list = list(pd.read_sql(data_metrics_table_name, con=create_engine(engine_param))['date'].unique())
    except:
        print(f'{data_metrics_table_name} еще не создана в БД')
        exit()
    if  date_filter not in date_list:
        try:
            insert_values(data_metrics, data_metrics_table_name)
        except:
            print(f'Даные в {data_metrics_table_name} не были добавлены, появилась ошибка')
            exit()
    else:
        try:
            connection = conn() 
            req = f"DELETE FROM {data_metrics_table_name} WHERE date = '{date_filter}'"
            connection\
            .cursor()\
            .execute(req)
            connection.commit()
            connection.close()
        except:
            print(f'Даные за определенный день в {data_metrics_table_name} не были удалены, появилась ошибка')
            exit()
        try:
            insert_values(data_metrics, data_metrics_table_name)
        except:
            print(f'Даные за определенный день в {data_metrics_table_name} не были удалены, появилась ошибка')
            exit()
            
# Создание или очистка таблиц в БД
def create_or_truncate_two_tables(data, 
                                  data_table_name, 
                                  data_unique_col, 
                                  data_metrics, 
                                  data_metrics_unique_col,
                                  data_metrics_table_name):
    try:
        connection = conn()
        req = """SHOW TABLES FROM ipolyakov_db"""
        cursor = connection.cursor()
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
    except:
        print('Ошибка в подключении к ipolyakov_db')
        exit() 
    if data_table_name not in list(df['Tables_in_ipolyakov_db']):
        if len(data_unique_col) == 1:
            try:
                req = create_table_from_dict_template(data_table_name,
                                                         make_dict_template(data.loc[0].to_dict()),
                                                         data_unique_col[0])
                connection\
                .cursor()\
                .execute(req)
            except:
                print(f'Ошибка в создании таблицы {data_table_name}')
                exit()
        else:
            try:
                req = create_table_unique_cols_sql_request(data_table_name, 
                                                           make_dict_template(data), 
                                                           data_unique_col)
                connection\
                .cursor()\
                .execute(req)
            except:
                print(f'Ошибка в создании таблицы {data_table_name}')
                exit()

    else:
        try:
            req = f"""TRUNCATE TABLE {data_table_name};"""  
            connection\
            .cursor()\
            .execute(req)
        except:
            print(f'Ошибка в очистке таблицы {data_table_name}')
            exit()
    if data_metrics_table_name not in list(df['Tables_in_ipolyakov_db']): 
        try:
            req = create_table_unique_cols_sql_request(data_metrics_table_name, 
                                                       make_dict_template(data_metrics), 
                                                       data_metrics_unique_col)
            connection\
            .cursor()\
            .execute(req)
        except:
            print(f'Ошибка в создании таблицы {data_metrics_table_name}')
            exit()                
    else:
        None
    connection.close()


# Создание или очистка таблицы в БД
def create_or_truncate_one_table(data, 
                                 data_table_name, 
                                 data_unique_col):
    try:
        connection = conn()
        req = """SHOW TABLES FROM ipolyakov_db"""
        cursor = connection.cursor()
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
    except:
        print('Ошибка в подключении к ipolyakov_db')
        exit() 
    if data_table_name not in list(df['Tables_in_ipolyakov_db']):
        if len(data_unique_col) == 1:
            try:
                req = create_table_from_dict_template(data_table_name,
                                                        make_dict_template(data.loc[0].to_dict()),
                                                        data_unique_col[0])
                connection\
                .cursor()\
                .execute(req)
            except:
                print(f'Ошибка в создании таблицы {data_table_name}')
                exit()
        else:
            try:
                req = create_table_unique_cols_sql_request(data_table_name, 
                                                            make_dict_template(data), 
                                                            data_unique_col)
                connection\
                .cursor()\
                .execute(req)
            except:
                print(f'Ошибка в создании таблицы {data_table_name}')
                exit()
            
    else:
        try:
            req = f"""TRUNCATE TABLE {data_table_name};"""  
            connection\
            .cursor()\
            .execute(req)
        except:
            print(f'Ошибка в очистке таблицы {data_table_name}')
            exit()
    connection.close()

# Создание таблицы в БД
def create_table(data_metrics, 
                 data_metrics_unique_col,
                 data_metrics_table_name):
    try:
        connection = conn()
        req = """SHOW TABLES FROM ipolyakov_db"""
        cursor = connection.cursor()
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
    except:
        print('Ошибка в подключении к ipolyakov_db')
        exit() 
    if data_metrics_table_name not in list(df['Tables_in_ipolyakov_db']): 
        try:
            req = create_table_unique_cols_sql_request(data_metrics_table_name, 
                                                        make_dict_template(data_metrics), 
                                                        data_metrics_unique_col)
            connection\
            .cursor()\
            .execute(req)
        except:
            print(f'Ошибка в создании таблицы {data_metrics_table_name}')
            exit()                 
    else:
        None
    connection.close()

# Добавление данных в таблицу, часть 2_1
def insert_values_p2_1(data, data_metrics_table_name):
    try:
        connection = conn()
        req = """SHOW TABLES FROM ipolyakov_db"""
        cursor = connection.cursor()
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        connection.close()
    except:
        print('Ошибка в подключении к ipolyakov_db')
        exit()  
    if data_metrics_table_name not in list(df['Tables_in_ipolyakov_db']):
        print(f'{data_metrics_table_name} нет в БД')
        exit()
    else:
        None
    try:
        date_list = list(pd.read_sql(data_metrics_table_name, con=create_engine(engine_param))['date'].unique())
    except:
        print(f'Ошибка в обращении к таблице {data_metrics_table_name}')
        exit()
    date_filter = data['date'].unique()[0]
    if  date_filter not in date_list:
        try:
            insert_values(data, data_metrics_table_name)
        except:
            print(f'Даные в {data_metrics_table_name} не были добавлены, появилась ошибка')
            exit()
    else:
        try:
            connection = conn()  
            req = f"DELETE FROM {data_metrics_table_name} WHERE date = '{date_filter}'"
            connection\
            .cursor()\
            .execute(req)
            connection.commit()
            connection.close()
        except:
            print(f'Даные за определенный день в {data_metrics_table_name} не были удалены, появилась ошибка')
            exit()
        try:
            insert_values(data, data_metrics_table_name)
        except:
            print(f'Даные за определенный день в {data_metrics_table_name} не были удалены, появилась ошибка')
            exit()

# Получение столбцов с признаком времени
def dt_col(data):
    date_col = []
    for row in data.columns:
        try:
            pd.to_datetime(row)
            date_col.append(row)
        except:
            None
    return date_col 

# Сортировка временных столбцов датафрейма
def data_columns_sorted(data):
    dt_col_data = dt_col(data)
    col_not_dt_data = [x for x in list(data.columns) if x not in dt_col_data]
    dt_col_data_s1 = sorted([pd.to_datetime(y) for y in dt_col_data])
    dt_col_data_s2 = [z.strftime('%d-%B-%Y') for z in dt_col_data_s1]
    col = col_not_dt_data + dt_col_data_s2
    dt_col_data = None
    col_not_dt_data = None
    dt_col_data_s1 = None
    dt_col_data_s2 = None
    data = data[col]
    return data
    
# Добавление новых столбюцов и обнолвение старых (с изменениями)
def add_new_col_and_reload_cols(data, data1, column, price=False):
    c = 0
    for col in data1.columns[1:]:
        if col not in data.columns:
            data = data.merge(data1[[column, col]], 
                              on=column, 
                              how='outer')
            if price==False:
                data.iloc[:, 1:] = data.iloc[:, 1:].fillna(0).astype(int)
                c+=1
            else:
                data.iloc[:, 1:] = data.iloc[:, 1:].fillna(0).astype(float)
                c+=1
        else:
            if int(data1[col].sum()) > int(data[col].sum()):
                data = data.drop(col, axis=1)
                data = data.merge(data1[[column, col]], 
                                         on=column, 
                                         how='outer')
                if price==False:
                    data.iloc[:, 1:] = data.iloc[:, 1:].fillna(0).astype(int)
                    c+=1
                else:
                    data.iloc[:, 1:] = data.iloc[:, 1:].fillna(0).astype(float)
                    c+=1
            else:
                None
    if c != 0:
        data = data_columns_sorted(data)
    return data, c

# # Получение таблицы с данными за последние дни
# def get_tabl_from_bd(table_name, date_column, start_date=start_date, end_date=end_date):
#     connection = conn()
#     cursor = connection.cursor()
#     req_0 = f"""SELECT * FROM {table_name} LIMIT 1;"""
#     cursor.execute(req_0)
#     rows = cursor.fetchall()
#     df_0 = pd.DataFrame(rows)
#     if 'Timestamp' in str(type( df_0.loc[0][date_column])) or 'T' in str(df_0.loc[0][date_column]):
#         req = f"""SELECT * FROM {table_name} WHERE CONVERT({date_column}, date) BETWEEN '{start_date}' AND '{end_date}';"""
#         cursor.execute(req)
#         rows = cursor.fetchall()
#         df = pd.DataFrame(rows)
#     else:
#         date_param = str([(pd.to_datetime(start_date) + datetime.timedelta(days=x))\
#                           .strftime('%d-%B-%Y') for x in range(0, 
#                           (pd.to_datetime(end_date)-pd.to_datetime(start_date)+datetime.timedelta(days=1))\
#                           .days)])\
#                           .replace("[", "(")\
#                           .replace("]", ")")
#         req = f"""SELECT * FROM {table_name} WHERE {date_column} in {date_param};"""
#         cursor.execute(req)
#         rows = cursor.fetchall()
#         df = pd.DataFrame(rows)
#         if len(df) == 0:
#             date_param = str([(pd.to_datetime(start_date) + datetime.timedelta(days=x))\
#                               .strftime('%Y-%m-%d') for x in range(0, 
#                               (pd.to_datetime(end_date)-pd.to_datetime(start_date)+datetime.timedelta(days=1))\
#                               .days)])\
#                               .replace("[", "(")\
#                               .replace("]", ")")
#             req = f"""SELECT * FROM {table_name} WHERE {date_column} in {date_param};"""
#             cursor.execute(req)
#             rows = cursor.fetchall()
#             df = pd.DataFrame(rows)    
#         else:
#             None
#     connection.close()
#     cursor.close()
#     return df

def get_tabl_from_bd(table_name, date_column='date', start_date=start_date, end_date=end_date, flag=0):
    connection = conn()
    cursor = connection.cursor()
    if flag == 0:
        req_0 = f"""SELECT * FROM {table_name} LIMIT 1;"""
        cursor.execute(req_0)
        rows = cursor.fetchall()
        df_0 = pd.DataFrame(rows)
        if 'Timestamp' in str(type( df_0.loc[0][date_column])) or 'T' in str(df_0.loc[0][date_column]):
            req = f"""SELECT * FROM {table_name} WHERE CONVERT({date_column}, date) BETWEEN '{start_date}' AND '{end_date}';"""
            cursor.execute(req)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)
        else:
            date_param = str([(pd.to_datetime(start_date) + datetime.timedelta(days=x))\
                              .strftime('%d-%B-%Y') for x in range(0, 
                              (pd.to_datetime(end_date)-pd.to_datetime(start_date)+datetime.timedelta(days=1))\
                              .days)])\
                              .replace("[", "(")\
                              .replace("]", ")")
            req = f"""SELECT * FROM {table_name} WHERE {date_column} in {date_param};"""
            cursor.execute(req)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)
            if len(df) == 0:
                date_param = str([(pd.to_datetime(start_date) + datetime.timedelta(days=x))\
                                  .strftime('%Y-%m-%d') for x in range(0, 
                                  (pd.to_datetime(end_date)-pd.to_datetime(start_date)+datetime.timedelta(days=1))\
                                  .days)])\
                                  .replace("[", "(")\
                                  .replace("]", ")")
                req = f"""SELECT * FROM {table_name} WHERE {date_column} in {date_param};"""
                cursor.execute(req)
                rows = cursor.fetchall()
                df = pd.DataFrame(rows)    
            else:
                None
    else:
        req = f"""SELECT * FROM {table_name};"""
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
    connection.close()
    cursor.close()
    return df


# Редактирование временного столбца(сдвигаем на один день)
def redact_date_column(data, date_column):
    data.loc[data.index, date_column]\
    = [(pd.to_datetime(x) + datetime.timedelta(days=1)) for x in list(data[date_column])]
    return data

# Создание списка дат в строковом формате от начала датафрейма до конца
def date_res_table(data):    
    date_column1 = dt_col(data)[0]
    date_column2 = dt_col(data)[-1]
    d1 = datetime.datetime(pd.to_datetime(date_column1).year, 
                           pd.to_datetime(date_column1).month, 
                           pd.to_datetime(date_column1).day)
    d2 = datetime.datetime(pd.to_datetime(date_column2).year, 
                           pd.to_datetime(date_column2).month, 
                           pd.to_datetime(date_column2).day)
    res = pd.date_range(
        min(d1, d2),
        max(d1, d2)
    ).strftime('%d-%B-%Y').tolist()
    return res

# # Дополнение датафрейма с остатками - заполняем пропущенные дни разницей
# def recovery_stocks_days(data):
#     data = data.fillna(0)
#     data.iloc[:, 1:] = data.iloc[:, 1:].astype(int)
#     data2 = data.copy()
#     for row in date_res_table(data):
#         if row not in list(data.columns):
#             data = pd.concat([data, 
#                               pd.DataFrame(len(data) * [0], 
#                                            columns=[row])], 
#                              axis=1)
#     data = data_columns_sorted(data)
#     for i in tqdm(range(len(data))):
#         start_column = [x for x in list(data.columns[1:]) if data.loc[i][x] > 0]
#         if start_column != []:
#             start_column = start_column[0]
#             date_cols = data.columns[data.columns.get_loc(start_column):]
#             if len(date_cols) > 10:
#                 R = None
#                 T = None
#                 c = 0
#                 for j in range(len(date_cols)-5):
#                     if data.loc[i][date_cols[j]] > 0:
#                         R = date_cols[j]
#                     else:
#                         if data.loc[i][date_cols[j+1]] > 0:
#                             T = date_cols[j+1]
#                             c = 1
#                         else:
#                             if data.loc[i][date_cols[j+2]] > 0:
#                                 T = date_cols[j+2]
#                                 c = 2
#                             else:
#                                 if data.loc[i][date_cols[j+3]] > 0:
#                                     T = date_cols[j+3]
#                                     c = 3
#                                 else:
#                                     if data.loc[i][date_cols[j+4]] > 0:
#                                         T = date_cols[j+4]
#                                         c = 4
#                                     else:
#                                         R = None
#                                         T = None
#                                         c = 0
#                         if R != None and T != None and c != 0:
#                             if data.loc[i][T] < data.loc[i][R]:
#                                 for x in list(range(c)):
#                                     data.loc[i, date_cols[j+x]] = data.loc[i][R]\
#                                     - (int((data.loc[i][R] - data.loc[i][T]) / (c+1))\
#                                     * (x+1))
#                             elif data.loc[i][T] == data.loc[i][R]:
#                                 for x in list(range(c)):
#                                     data.loc[i, date_cols[j+x]] = data.loc[i][T]
#                             else:
#                                 for x in list(range(c)):
#                                     data.loc[i, date_cols[j+x]] = data.loc[i][R]\
#                                     - (int(data.loc[i][R] / (c+1))\
#                                     * (x+1))
#                         else:
#                             None
#             else:
#                 None
#         else:
#             None
#     for row in data2.columns[1:]:
#         data[row] = data2[row]
#     data2 = None
#     data = data_columns_sorted(data)
#     return data


# Дополнение датафрейма с остатками - заполняем пропущенные дни разницей
def recovery_stocks_days(data):
    data = data.fillna(0)
    data.iloc[:, 1:] = data.iloc[:, 1:].astype(int)
    data2 = data.copy()
    for row in date_res_table(data):
        if row not in list(data.columns):
            data = pd.concat([data, 
                              pd.DataFrame(len(data) * [0], 
                                           columns=[row])], 
                             axis=1)
    data = data_columns_sorted(data)
    for i in tqdm(range(len(data))):
        start_column = [x for x in list(data.columns[1:]) if data.loc[i][x] > 0]
        if start_column != []:
            start_column = start_column[0]
            date_cols = data.columns[data.columns.get_loc(start_column):]
            if len(date_cols) > 15:
                R = None
                T = None
                c = 0        
                for j in range(len(date_cols)-10):
                    if data.loc[i][date_cols[j]] > 0:
                        R = date_cols[j]
                    else:         
                        if data.loc[i][date_cols[j+1]] > 0:
                            T = date_cols[j+1]
                            c = 1
                        else:        
                            if data.loc[i][date_cols[j+2]] > 0:
                                T = date_cols[j+2]
                                c = 2
                            else:
                                if data.loc[i][date_cols[j+3]] > 0:
                                    T = date_cols[j+3]
                                    c = 3
                                else:
                                    if data.loc[i][date_cols[j+4]] > 0:
                                        T = date_cols[j+4]
                                        c = 4
                                    else:
                                        if data.loc[i][date_cols[j+5]] > 0:
                                            T = date_cols[j+5]
                                            c = 5
                                        else:        
                                            if data.loc[i][date_cols[j+6]] > 0:
                                                T = date_cols[j+6]
                                                c = 6
                                            else:
                                                if data.loc[i][date_cols[j+7]] > 0:
                                                    T = date_cols[j+7]
                                                    c = 7
                                                else:
                                                    if data.loc[i][date_cols[j+8]] > 0:
                                                        T = date_cols[j+8]
                                                        c = 8
                                                    else:
                                                        if data.loc[i][date_cols[j+9]] > 0:
                                                            T = date_cols[j+9]
                                                            c = 9
                                                        else:
                                                            R = None
                                                            T = None
                                                            c = 0
                        if R != None and T != None and c != 0:
                            if data.loc[i][T] < data.loc[i][R]:
                                for x in list(range(c)):
                                    data.loc[i, date_cols[j+x]] = data.loc[i][R]\
                                    - (int((data.loc[i][R] - data.loc[i][T]) / (c+1))\
                                    * (x+1))
                            elif data.loc[i][T] == data.loc[i][R]:
                                for x in list(range(c)):
                                    data.loc[i, date_cols[j+x]] = data.loc[i][T]
                            else:
                                for x in list(range(c)):
                                    data.loc[i, date_cols[j+x]] = data.loc[i][R]\
                                    - (int(data.loc[i][R] / (c+1))\
                                    * (x+1))
                        else:
                            None
            else:
                None
        else:
            None
    for row in data2.columns[1:]:
        data[row] = data2[row]
    data2 = None
    data = data_columns_sorted(data)
    return data





# Дополнение датафрейма с ценами - заполняем пропущенные дни разницей
def recovery_price_days(data):
    data = data_columns_sorted(data)
    res = date_res_table(data)
    for i in tqdm(range(len(res))):
        if res[i] not in data.columns:
            try:
                j = 0
                while res[i+j] not in data.columns:
                    j += 1
                k = 0
                while res[i+k] not in data.columns:
                    k += -1            
                Q = []
                for l in range(len(data)):
                    pr_list = [data.loc[l][x] for x in [res[i+k], res[i+j]] if data.loc[l][x] != 0]
                    if len(pr_list) == 2:
                        Q.append(int(np.mean(pr_list)))
                    elif len(pr_list) == 1:
                        Q.append(int(pr_list[0]))
                    else:
                        Q.append(0)    
                data[res[i]] = Q
            except:
                None
    data = data_columns_sorted(data)
    return data

# Обновление файлов full_df
def unload_metrics(path_metrics, data1, column, price=False):
    metrics = pd.read_excel(path_metrics, 
                            dtype={'Артикул': 'str'})
    metrics, c = add_new_col_and_reload_cols(data=metrics, data1=data1, column=column, price=price)
    if c!=0:
        try:
            if path_metrics == path_le_price_metrics:
                metrics = data_columns_sorted(metrics)
                for i in range(len(metrics)):
                    price_0 = [x for x in list(metrics.iloc[i, 1:]) if x > 0]
                    if len(price_0) > 0:
                        price_0 = price_0[-1]
                        for col in metrics.columns[1:][::-1]:
                            if metrics.loc[i][col] != 0:
                                price_0 = metrics.loc[i][col]
                            else:
                                metrics.loc[i, col] = price_0
                    else:
                        None
            else:
                None
            metrics.to_excel(path_metrics, index=False)
            print('Были изменения, датафрейм выгружен')
        except:
            if path_metrics == path_le_price_metrics:
                metrics = data_columns_sorted(metrics)
                for i in range(len(metrics)):
                    price_0 = [x for x in list(metrics.iloc[i, 1:]) if x > 0]
                    if len(price_0) > 0:
                        price_0 = price_0[-1]
                        for col in metrics.columns[1:][::-1]:
                            if metrics.loc[i][col] != 0:
                                price_0 = metrics.loc[i][col]
                            else:
                                metrics.loc[i, col] = price_0
                    else:
                        None
            else:
                None
            metrics.to_excel(path_metrics, index=False)
            print('Были изменения, датафрейм выгружен')
    
# Получение из файлов full_df T_metrics
def get_T_metrics(path_metrics):
    T_metrics = pd.read_excel(path_metrics, 
                            dtype={'Артикул': 'str'})
    T_metrics.columns = ['Артикул']+[pd.to_datetime(x) for x in list(T_metrics.columns)[1:]]
    T_metrics = T_metrics[['Артикул']+sorted(T_metrics.columns[1:])[::-1]]
    T_metrics = T_metrics.T
    T_metrics = T_metrics.reset_index(drop=False)
    T_metrics.columns = list(T_metrics.loc[0])
    T_metrics = T_metrics.iloc[1:, :].reset_index(drop=True)
    T_metrics.index = T_metrics['Артикул']
    T_metrics = T_metrics.drop('Артикул', axis=1)
    return T_metrics
    
# Получение ненулевых суммированных остатков по РЦ и РЦ Иркутск
def RC():
    rc_irc = pd.read_excel(path_downloads + 'РЦ Иркутск.xlsx', 
                dtype={'Артикул': 'str'})
    rc = pd.read_excel(path_rc, 
                dtype={'Артикул': 'str'})
    RC = rc.merge(rc_irc, on='Артикул', how='outer')
    RC = RC.fillna(0)
    RC['Остаток на РЦ'] = [0 if x <= 0 else x for x in list(RC['Остаток на РЦ'])]
    RC['Остаток РЦ Иркутск'] = [0 if x <= 0 else x for x in list(RC['Остаток РЦ Иркутск'])]
    RC['РЦ'] = RC['Остаток на РЦ'] + RC['Остаток РЦ Иркутск']
    RC = RC[['Артикул', 'РЦ']]
    RC = RC.groupby('Артикул')['РЦ'].sum().reset_index(drop=False)
    RC = RC[RC['РЦ']>0].reset_index(drop=True)  
    return RC
        
# Смена статуса "товары в пути" на "новинка Даша"
def redact_st_1():
    RC_0 = RC()
    unique_items(RC_0)
    st = pd.read_excel(path_st, 
                    dtype={'Артикул': 'str'})
    unique_items(st)
    for i in range(len(st)):
        art = st.loc[i]['Артикул']
        if st.loc[i]['Статус'] == 'товары в пути':
            if art in list(set(RC_0['Артикул'])):
                st.loc[i, 'Статус'] = 'новинка Даша'
    unique_items(st)
    st.to_excel(path_st, index=False)

# Смена статуса "отказ" на "архив" и обратно
def redact_st_2():
    RC_0 = RC()
    unique_items(RC_0)
    Metrics = {path_full_WB_df: 'WB_full_df',
               path_full_OZON_df: 'OZON_full_df',
               path_full_YA_df: 'ya_full_df',
               path_full_SBER_df: 'sber_full_df',
               path_full_LE_df: 'le_full_df'}
    OST = pd.DataFrame(columns=['Артикул'])
    for M in Metrics:

        df = pd.read_excel(M, 
                           sheet_name=Metrics[M],
                           dtype={'Артикул': 'str'}).iloc[:-1, :].fillna(0)
        col = [x for x in list(df.columns) if x.lower() == 'остатки fbs' or x.lower() == 'остаток fbs']
        if len(col) == 1:
            col = col[0]
            cols = ['Остаток', col]
        else:
            cols = ['Остаток']
        df = df.groupby('Артикул')[cols].sum().reset_index(drop=False)
        df.columns = ['Артикул'] + [x+'_'+Metrics[M] for x in list(df.columns[1:])]
        OST = OST.merge(df, on='Артикул', how='outer')
    OST = OST.merge(RC_0, on='Артикул', how='outer')
    OST = OST.fillna(0)
    unique_items(OST)   
    OST_null = OST\
    .query(f"index in {[x for x in list(OST.index) if len([y for y in list(OST.iloc[x, 1:]) if y > 0]) == 0]}")\
    .reset_index(drop=True).copy()
    OST_not_null = OST\
    .query(f"index in {[x for x in list(OST.index) if len([y for y in list(OST.iloc[x, 1:]) if y > 0]) > 0]}")\
    .reset_index(drop=True).copy()
    st = pd.read_excel(path_st,
                   dtype={'Артикул':'str'})
    unique_items(st) 
    c = 0
    for i in range(len(st)):
        art = st.loc[i]['Артикул']
        if st.loc[i]['Статус'] == 'отказ':
            if art in list(set(OST_null['Артикул'])):
                st.loc[i, 'Статус'] = 'архив'
                c+=1
            else:
                None
        elif st.loc[i]['Статус'] == 'архив':
            if art in list(set(OST_not_null['Артикул'])):
                st.loc[i, 'Статус'] = 'отказ'
                c+=1
            else:
                None
    if c > 0:
        unique_items(st) 
        st.to_excel(path_st, index=False)

# Смена статуса "новинка Даша" на "текущий Дарья"
def redact_st_3():
    st = pd.read_excel(path_st, 
                       dtype={'Артикул': 'str'})
    unique_items(st)
    wb_orders = pd.read_excel(path_full_WB_df,
                              dtype={'Артикул': 'str'})
    wb_orders = wb_orders[['Артикул']+dt_col(wb_orders)].groupby('Артикул').sum().reset_index(drop=False)
    unique_items(wb_orders)
    wb_stocks = pd.read_excel(path_full_WB_df,
                              sheet_name='stocks_WB_metric',
                              dtype={'Артикул': 'str'})
    wb_stocks = wb_stocks[['Артикул']+dt_col(wb_stocks)].groupby('Артикул').sum().reset_index(drop=False)
    unique_items(wb_stocks)
    oz_orders = pd.read_excel(path_full_OZON_df,
                              dtype={'Артикул': 'str'})
    oz_orders = oz_orders[['Артикул']+dt_col(oz_orders)]
    unique_items(oz_orders)
    oz_stocks = pd.read_excel(path_full_OZON_df,
                              sheet_name='stocks_ozon_metrics',
                      dtype={'Артикул': 'str'})
    oz_stocks.columns = [x if 'Ost' not in x else x[:-4] for x in list(oz_stocks.columns)]
    oz_stocks = oz_stocks[['Артикул']+dt_col(oz_stocks)]
    unique_items(oz_stocks)
    c = 0
    for i in range(len(st)):
        if st.loc[i]['Статус'] == 'новинка Даша':
            art = st.loc[i]['Артикул']
            if art in list(wb_orders['Артикул']) \
            and art in list(oz_orders['Артикул']) \
            and art in list(wb_stocks['Артикул']) \
            and art in list(oz_stocks['Артикул']):
                if len([x for x in list(wb_orders[wb_orders['Артикул']==art].iloc[:, 1:].values[0]) if x > 0]) >= 30 \
                and len([x for x in list(oz_orders[oz_orders['Артикул']==art].iloc[:, 1:].values[0]) if x > 0]) > 0:
                    st.loc[i, 'Статус'] = 'текущий Дарья'
                    c+=1
                elif len([x for x in list(oz_orders[oz_orders['Артикул']==art].iloc[:, 1:].values[0]) if x > 0]) >= 30 \
                and len([x for x in list(wb_orders[wb_orders['Артикул']==art].iloc[:, 1:].values[0]) if x > 0]) > 0:
                    st.loc[i, 'Статус'] = 'текущий Дарья'
                    c+=1
                else:
                    None
        else:
            None
    if c > 0:
        unique_items(st)
        st.to_excel(path_st, index=False)



# Изменение имен датафрейма на основании Реестра
def rename_data_from_reestr(data, name_column, reestr):
    if 'Артикул' in list(data.columns):
        data = data.reset_index(drop=True)
        data['Артикул'] = data['Артикул'].astype(str)
        for i in range(len(data)):
            art = data.loc[i]['Артикул']
            if art in list(reestr['Артикул']):
                data.loc[i, name_column] = reestr[reestr['Артикул']==art]['Название'].values[0]
            else:
                None
    else:
        None
    return data



# Добавление в датафрейм временных столбцов по неделям
def weekly_amount_and_sort_columns(data):
    data_0 = data.copy()
    try:
        date_col_0  = dt_col(data)[0]
        date_cols = [y.strftime("%d-%B-%Y") for y in sorted([pd.to_datetime(x) for x in dt_col(data)])]
        str_cols_1 = []
        str_cols_2 = []
        for col in data.columns:
            if col not in date_cols:
                if list(data.columns).index(col) < list(data.columns).index(date_col_0):
                    str_cols_1.append(col)
                else:
                    str_cols_2.append(col)
        data = data[str_cols_1+date_cols+str_cols_2]
        date_cols_2 = []
        for i in range(len(date_cols)):
            if pd.to_datetime(date_cols[i]).dayofweek == 6 and i > 6:
                data.insert((data.columns.get_loc(date_cols[i]))+1, f"{pd.to_datetime(date_cols[i-6]):%d-%B}-{pd.to_datetime(date_cols[i]):%d-%B}", 0)
                date_cols_2.append(f"{pd.to_datetime(date_cols[i-6]):%d-%B}-{pd.to_datetime(date_cols[i]):%d-%B}")
        for row in date_cols_2:
            data[row] = [(data.iloc[x, data.columns.get_loc(row)-7:data.columns.get_loc(row)]).sum() \
                         for x in list(data.index)]
    except:
        data = data_0
        print('Ошика при добавлении столбцов')
        exit()
    return data





def re_oz_price(path_price):
    xlrd.xlsx.ensure_elementtree_imported(False, None)
    xlrd.xlsx.Element_has_iter = True
    wb = xlrd.open_workbook(path_price)
    sh = wb.sheet_by_name('Товары и цены')
    header = sh.row_values(2, start_colx=0, end_colx=None)
    header = header[:38]
    price = pd.DataFrame()
    for i in range(len(header)):
        price[header[i]] = sh.col_values(i, start_rowx=4, end_rowx=None)
    price['Артикул'] = price['Артикул'].astype(str)
    if len(price) - len(list(set(price['Артикул']))) != 0:
        print('В price есть неуникальные артикулы')
        exit()
    if 'Цена с учетом акции или стратегии, руб.' in price.columns:
        price = price.rename(columns={'Штрихкод': 'Баркод', 
                                      'Объемный вес, кг': 'Объемный вес',
                                      'Размер комиссии FBO, %': 'Комиссия в %',
                                      'Цена с учетом акции или стратегии, руб.': 'Цена',
                                      'Цена до скидки, руб.': 'Цена до скидки', 
                                      'Объем, л': 'Объем товара, л',
                                      'Размер комиссии FBS, %': 'Комиссия в % fbs',
                                      'Комиссия за эквайринг': 'Эквайринг'})
    elif 'Цена с учетом акции, руб.' in price.columns:
        price = price.rename(columns={'Ozon SKU ID': 'Баркод', 
                                      'Объемный вес, кг': 'Объемный вес',
                                      'Размер комиссии FBO, %': 'Комиссия в %',
                                      'Цена с учетом акции, руб.': 'Цена',
                                      'Цена до скидки, руб.': 'Цена до скидки', 
                                      'Объем, л': 'Объем товара, л',
                                      'Размер комиссии FBS, %': 'Комиссия в % fbs',
                                      'Комиссия за эквайринг': 'Эквайринг'})
    else:
        print('error')
        exit()
    for i in range(len(price)):
        if price['Объемный вес'][i] == '0':
            price['Объемный вес'][i] = '0,1'
    price['Объемный вес'] = price['Объемный вес'].astype('float')
    price['Баркод'] = price['Баркод'].astype('str')
    price['Комиссия в %'] = price['Комиссия в %'].astype('int')
    price['Цена'] = price['Цена'].astype('int')
    for i in range(len(price)):
        if price.loc[i]['Цена до скидки'] < price.loc[i]['Цена'] or price.loc[i]['Цена до скидки'] == 0:
            price.loc[i, ['Цена до скидки']] = price.loc[i]['Цена']
    price_dop = price[['Артикул', 'Рыночная цена конкурентов, руб.', 'Моя цена на других площадках, руб.', 'Индекс цен', 'Ценовой индекс товара на Ozon', 'Ценовой индекс товара на рынке', 'Ценовой индекс товара на рынке на мои товары', 'Ссылка на рыночную цену конкурентов']]
    price = price[['Артикул',
                   'Название',
                   'Ozon SKU ID',
           'Баркод',
           'Объемный вес',
           'Объем товара, л',
           'Цена',
           'Цена до скидки',
           'Эквайринг',
           'Комиссия в %',
           'Логистика Ozon, минимум, FBO',
           'Логистика Ozon, максимум, FBO',
           'Комиссия в % fbs',
           'Обработка отправления, минимум FBS',
           'Обработка отправления, максимум FBS',
           'Логистика Ozon, минимум, FBS',
           'Логистика Ozon, максимум, FBS']]
    return price


# Расчет итоговой наценки
def itogo_nac(data):
    data2 = data.copy()
    if 'Цена' not in data2.columns:
        print('Нет цены')
        exit()
    if 'Наценка' not in data2.columns:
        print('Нет наценки')
        exit()
    if 'Выручка' not in data2.columns:
        data2['Выручка'] = data2[[y.strftime('%d-%B-%Y') \
                                for y in sorted([pd.to_datetime(x) for x in dt_col(data2)])][-1]] * data2['Цена']
    data2 = data2[(data2['Наценка'].astype(str).isin(['inf', '-inf', 'nan', 'None'])==False) & \
              (data2['Выручка'].astype(str).isin(['nan', 'None'])==False)][['Наценка', 'Выручка']]
    if sum(data2['Выручка']) > 0:
        data2['Доля выручки'] = (data2['Выручка'] * 100) / sum(data2['Выручка'])
        data2['Итого наценка'] = data2['Доля выручки'] / 100 * data2['Наценка']
        S = sum(data2['Итого наценка'])
    else:
        S = 0 
    return S

# Расчет итоговой наценки 2
def itogo_nac2(data):
    df = data.copy()
    if 'Себестоимость' not in df.columns:
        print('Нет себестоимости')
        exit()
    if 'Прибыль' not in df.columns:
        print('Нет прибыли')
        exit()
    if 'Продажи' not in df.columns:
        print('Нет продаж')
        exit()
    df = df[(df['Себестоимость'].astype(str).isin(['0', 'nan', '0.0'])==False)\
            &(df['Продажи']>0)]\
            .reset_index(drop=True)
    if len(df) != 0:
        ITOG_NAC = round((((df['Прибыль'] * df['Продажи']).sum() / (df['Себестоимость'] *  df['Продажи']).sum()) * 100), 2)
        if str(ITOG_NAC) == 'nan':
            ITOG_NAC = 0
    else:
        ITOG_NAC = 0
    return ITOG_NAC



# Редактирование датафрейма для "_old" таблиц
def redact_df(df, result_column):
    
    final_df = pd.DataFrame()
    for col in dt_col(df)[::-1]:
        sub_df = df[['Артикул', col]].rename(columns={col: result_column}).copy()
        sub_df['date'] = col
        sub_df = sub_df.sort_values(by=['Артикул'])
        final_df = pd.concat([final_df, sub_df], ignore_index=True)
    df, final_df = final_df, None
    
    return df

# Редактирование датафрейма для "_old" таблиц, часть 2
def redact_df_2(PATH, 
                sheet_name, 
                result_column, 
                flag_dtc=False):
    
    df = pd.read_excel(PATH, 
                       sheet_name=sheet_name,
                       dtype={'Артикул': 'str'})
    
    if flag_dtc == True:
        df = df[['Артикул'] + [y.strftime('%d-%B-%Y') for y \
                               in sorted([pd.to_datetime(x) for x in dt_col(df)])][-3:]]
    else:
        df = df[['Артикул'] + [y.strftime('%d-%B-%Y') for y \
                               in sorted([pd.to_datetime(x) for x in dt_col(df)])][:]]
    df = df[df['Артикул'].astype(str).isin(['nan', 'NaN', 'NAN', 'Nan', 'None', ''])==False]
    unique_items(df)
    df = redact_df(df, result_column=result_column)
    
    return df


# Разворачивание датафрейма
def expand_df(df, 
              key_column,
              result_column,
              func='sum',
              date_column='date'):
    
    # df[date_column] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df[date_column])]
    Q = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df[date_column])].copy()
    for i in range(len(df)):
        df.loc[i, date_column] = Q[i]
    Q = None
    df_0 = pd.DataFrame(list(set(df[key_column])), columns=[key_column])
    for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) \
                                                        for x in list(df[date_column].unique())])]:
        if func == 'sum':
            df_0 = df_0.merge(df[df[date_column]==row].groupby(key_column)[result_column].sum()\
            .reset_index(drop=False).rename(columns={result_column: row}), on=key_column, how='left')
        elif func == 'mean':
            df_0 = df_0.merge(df[df[date_column]==row].groupby(key_column)[result_column].mean()\
            .reset_index(drop=False).rename(columns={result_column: row}), on=key_column, how='left')
        elif func == 'count':
            df_0 = df_0.merge(df[df[date_column]==row].groupby(key_column)[result_column].count()\
            .reset_index(drop=False).rename(columns={result_column: row}), on=key_column, how='left')
        else:
            print(f"{func} - такая функция не предусмотрена")
        
    df_0 = df_0.fillna(0)
    df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)

    if key_column.lower() in ['barcode', 'bar', 'баркод']:
        df_0.iloc[:, :] = df_0.iloc[:, :].astype(int)
    else:
        df_0[key_column] = df_0[key_column].astype(str)
        df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)
    df, df_0 = df_0, None
        
    return df

# Сворачивание датафрейма
def collapse_df(df, 
                key_column,
                result_column,
                date_column='date'):
    
    df_final = pd.DataFrame(columns=[key_column, date_column, result_column])
    for row in dt_col(df):
        sub_df = df[[key_column, row]].rename(columns={row: result_column}).copy()
        sub_df[date_column] = row
        df_final = pd.concat([df_final, sub_df], ignore_index=True)

    df, df_final = df_final, None
        
    return df

# Проверка дат
def check_dates(df, 
                date_column='date'):
    
    date_list = [y.strftime('%d-%B-%Y') for y in \
                 sorted([pd.to_datetime(x) for x in list(df[date_column].unique())])]

    res = mwm.date_range(start_date=datetime.datetime.strptime(date_list[0], '%d-%B-%Y').date(), 
                         end_date=datetime.datetime.strptime(date_list[-1], '%d-%B-%Y').date())
    
    if len(res) != len(date_list):
        print('Не хватает дней в датафрейме с заказами')
        print([x for x in res if x not in date_list])
        exit()


# Получить "Статус"
def get_st():
    st = pd.read_excel(path_st, 
                    dtype={'Артикул': 'str'})
    unique_items(st)
    return st

# Получение остортированного списка дат из названий столбцов датафрейма
def dt_col_s(df):
     return [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in dt_col(df)])]

# Проверка дат 2
def check_dates2(table_name, 
                 account, 
                 schema=mwm.dds_schema):
    
    req = f"""
    select count(distinct date), 
    (max(date) - min(date) + 1)
    from {schema}.{table_name}
    where account = '{account}';
    """
    
    df = mwm.select_table_from_request(req)
    if df.loc[0][0] == df.loc[0][1]:
        print('Даты не прерываются')
    else:
        print('Датафрейм имеет пропуски')
        exit()

# Убрать "Q" из артикула
def del_Q_art(data, art_col='Артикул'):
    data[art_col] = [str(x) if 'Q' not in str(x) else str(x).replace('Q', '') for x in list(data[art_col])]
    return data


def get_rec_last_date(flag_mp, schema=mwm.ir_schema):
    if flag_mp == 'WB':
        req = f"""
        with 
            msdt as (
            select distinct date::DATE as date
            from {schema}.marketing_statistic
            where marketplace = 'WB'
            order by date::DATE desc
            limit 1),
            ms as (
            select article_name::varchar, auto_advertising_costs::float, date::DATE
            from {schema}.marketing_statistic
            where marketplace = 'WB'
            and article_name is not null
            and auto_advertising_costs is not null 
            and auto_advertising_costs > 0
            order by date::DATE desc
            )
            select 
            article_name::varchar,  
            sum(auto_advertising_costs::bigint) as rec
            from ms
            where article_name::varchar is not null
            and auto_advertising_costs::bigint is not null
            and date::date = (select date::date from msdt)
            group by article_name::varchar
            having sum(auto_advertising_costs::bigint) > 0
            order by article_name::varchar;
        """
    elif flag_mp == 'OZ':
        req = """
        select article_name::varchar, 
        sum(auto_advertising_costs + search_advertising_costs) as rec
        from ir_db.marketing_statistic
        where marketplace = 'OZON'
        and date = (select distinct date::DATE as date
                    from ir_db.marketing_statistic
                    where marketplace = 'OZON'
                    order by date::DATE desc
                    limit 1)
        and auto_advertising_costs + search_advertising_costs > 0
        and article_name::varchar is not null
        group by article_name::varchar
        order by article_name::varchar;        
        """
    data_rec = mwm.select_table_from_request(req).rename(columns={0: 'Артикул', 1: 'Реклама'})
    data_rec['Артикул'] = data_rec['Артикул'].astype(str)
    data_rec['Реклама'] = data_rec['Реклама'].astype(int)
    mwm.unique_items(data_rec)

    return data_rec
    

# Возвращает путь к нумерованным файлам в папке "Загрузки Маслов"
def return_path(name_file, path_downloads=path_downloads):
    
    List_paths = sorted([py for py in glob.glob(f"{path_downloads}{name_file}")])
    if len(List_paths) > 1:
        if len([x for x in List_paths if '(' in x and ')' in x]) == 0:
            print('error')
            exit()
        else:
            List_paths = [x for x in List_paths if '(' in x and ')' in x]
        if len(List_paths) > 1:
            Dict_path = {}
            for x in List_paths:
                Dict_path[int(x.split('(')[1].split(')')[0])] = x
            path = sorted(Dict_path.items())[-1][1]
        else:
            path = List_paths[0]
    else:
        path = List_paths[0]

    return path
