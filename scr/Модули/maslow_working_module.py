import pandas as pd
import glob
import sys
import datetime
import re
import numpy as np
import psycopg2
import requests
from sys import exit
import time
link_to_table = 'https://docs.google.com/spreadsheets/d/1A1TyZPeq9r2HiDi-CHfmTfRpuAQYToib1vqyTx4ZyRE/edit#gid=0'
df = pd.read_excel('/'.join(link_to_table.split('/')[:-1])+'/export?format=xlsx')
PATH, df = [dict(df.loc[x]) for x in list(df.index)], None
i = 0
while len([py for py in glob.glob(f"{PATH[i]['path_downloads']}*.xlsx")]) == 0:
    i +=1
path_downloads = PATH[i]['path_downloads']
path_algoritm = PATH[i]['path_algoritm']
sys.path.append(f'{path_algoritm}Модули')
import constants_for_marketplace_metrics_dags as const


def date_range(start_date, end_date):
    res = pd.date_range(
        min(start_date, end_date), 
        max(start_date, end_date)
    ).strftime('%d-%B-%Y').tolist()
    return res


# Путь к папке с файлами для дагов
path_FD = path_downloads + 'Files for DAGs/'

# Пареметры БД
bd_param = {'host': '45.90.32.77',
            'port': 5432,
            'user': 'ir',
            'password': 'NBBTwzCpY46b',
            'dbname': 'ipolyakov_db'}

# Схема для работы
ir_schema = 'ir_db'
ip_schema = 'ipolyakov_db'
dds_schema = 'dds_ir_db'

args = {
    "owner": "Dmitry Maslow",
    'email_on_failure': False,
    'email_on_retry': False,
}

dt_now = const.dt_now
end_date =const.end_date
start_date = const.start_date
dt_yesterday =  const.dt_yesterday

# # Параметры для подключения к интерфейсу API (WB)
# WB_API_KEY = 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzc4ODM0MiwiaWQiOiI0Y2QwZTYxYi0zYmUyLTQ2ZTYtYTJhMy05Zjg5ZTk4OTE4MzkiLCJpaWQiOjE5NzA1MDg3LCJvaWQiOjMzNjk0LCJzIjo1MTAsInNpZCI6IjAwYjQ5NTAzLTgxODItNWY1Yy1hYzRlLWY2MDAyNTBhYTdmZCIsInVpZCI6MTk3MDUwODd9.GmfqqacCvqHMkLviIxi15bLVUqMlJ9_24CKSNzK_uS2wI6OUhe_HnuV1YtmjKjqsb8T7J1xW8es67adYdHci7g'
# # WB_API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjE0YzdjNTFmLTRjM2EtNDZlZi1hYTVmLWU4ZTAzNTM4OTVjZiJ9.64BLEDc9Q51f9JNMCxKjV-10SpvVjWwmcatpqWSK080'
# BASE_URL_WB = 'https://suppliers-api.wildberries.ru'
# BASE_URL_WB_2 = 'https://statistics-api.wildberries.ru'


# Подключение к БД
def conn(bd_param=bd_param):
    try:
        # пытаемся подключиться к базе данных
        connection = psycopg2.connect(dbname=bd_param['dbname'], 
                                user=bd_param['user'], 
                                password=bd_param['password'], 
                                host=bd_param['host'])
        cursor = connection.cursor()
        return connection, cursor
    except:
        # в случае сбоя подключения будет выведено сообщение
        print('Can`t establish connection to database')

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

# Редактирование столбцов в датафрейме для БД
def redact_columns_with_bd_table(data):
    data.columns = [re.sub("[^A-Za-z0-9_ а-яА-Я]", "", x).replace(' ', '_') for x in list(data.columns)]
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

# Редактирование имен столбцов датафрейма для работы с БД
def redact_columns_with_db_table(data):
    data.columns = [x.replace('-', '_')\
                    .replace('(', '').replace(')', '').replace('[', '').replace(']', '')\
                    .replace(',', '').replace('.', '').replace(' ', '_').replace('%', 'проц')\
                    for x in list(data.columns)]
    # data.columns = [re.sub('[(),].','', x) if '(' in x or ')' in x or ',' in x else x for x in list(data.columns)]
    # data.columns = [x.replace(' ', '_') if ' ' in x else x for x in list(data.columns)]
    # data.columns = [x.replace('-', '_') if '-' in x else x for x in list(data.columns)]
    return data

# Получение wb_list_of_nomenclatures и выгрузка
def get_wb_list_of_nomenclatures(path_downloads=path_downloads, 
                                 path_FD=path_FD):
    dict_report_paths = {}
    for path in [py for py in glob.glob(f"{path_downloads}report_*.xlsx.xlsx")]:
        dict_report_paths[pd.to_datetime(path.split('report')[-1][:-10], format='_%Y_%m_%d')] = path
    path_name_wb_list_of_nomenclatures = dict_report_paths[sorted(dict_report_paths.keys())[-1]]
    date_create_wb_list_of_nomenclatures = pd.to_datetime(path_name_wb_list_of_nomenclatures\
                                                          .split('report')[-1][:-10], 
                                                           format='_%Y_%m_%d').strftime('%d-%B-%Y')
    wb_list_of_nomenclatures = pd.read_excel(path_name_wb_list_of_nomenclatures)
    wb_list_of_nomenclatures = clear_article(wb_list_of_nomenclatures, "Артикул продавца")
    wb_list_of_nomenclatures['Баркод'] = wb_list_of_nomenclatures['Баркод'].astype(str)
    [x for x in list(wb_list_of_nomenclatures['Баркод']) if len(x) < 10]
    wb_list_of_nomenclatures = wb_list_of_nomenclatures[wb_list_of_nomenclatures['Баркод']\
                              .isin([x for x in list(wb_list_of_nomenclatures['Баркод']) if len(x) < 10])==False]\
                              .reset_index(drop=True)
    wb_list_of_nomenclatures['Баркод'] = wb_list_of_nomenclatures['Баркод'].astype(int)
    wb_list_of_nomenclatures = wb_list_of_nomenclatures.drop_duplicates().reset_index(drop=True)
    wb_list_of_nomenclatures['Артикул WB'] = wb_list_of_nomenclatures['Артикул WB'].astype(int)
    wb_list_of_nomenclatures = wb_list_of_nomenclatures.fillna('')
    wb_list_of_nomenclatures = redact_columns_with_db_table(wb_list_of_nomenclatures)
    if len(wb_list_of_nomenclatures) - len(list(set(wb_list_of_nomenclatures['Баркод']))) != 0:
        print('В wb_list_of_nomenclatures есть неуникальные баркоды')
        exit()
    else:
        None
    wb_list_of_nomenclatures = astype_df(wb_list_of_nomenclatures) # Преобразование типов столбцов
    wb_list_of_nomenclatures = redact_columns_with_bd_table(wb_list_of_nomenclatures) # Преобразование названий столбцов
    unique_barcode(wb_list_of_nomenclatures)
    wb_list_of_nomenclatures.to_excel(path_FD + 'wb_list_of_nomenclatures.xlsx', index=False)


# Получение wb_fbo_stocks_new
def get_wb_fbo_stocks_new(account='IR', 
                          path_FD=path_FD):

    if account == 'KZ':
        header_params = const.header_params_wb_kz
    elif account == 'IR':
        header_params = const.header_params_wb_fbo_stocks
    else:
        print('Ошибка в выборе кабинета вб')
        exit()

    url_param = const.url_param_wb_fbo_stocks
    api_params = const.api_params_wb_fbo_stocks

    response = requests.get(
                            url_param,
                            params=api_params,
                            headers=header_params,
                            )
    if response.status_code != 200:
        code = 0
        c = 0
        while code != 200:
            response = requests.get(
                                    url_param,
                                    params=api_params,
                                    headers=header_params,
                                    )
            code = response.status_code
            if code != 200:
                c += 50
                code = c
            time.sleep(10)
    else:
        None
    if response.status_code == 200:
        wb_fbo_stocks_new = pd.DataFrame(response.json())
        wb_fbo_stocks_new = clear_article(wb_fbo_stocks_new, 'supplierArticle')
        for row in wb_fbo_stocks_new['barcode']:
            try:
                int(row)
            except:
                wb_fbo_stocks_new[wb_fbo_stocks_new['barcode']!=row]
        wb_fbo_stocks_new = wb_fbo_stocks_new.reset_index(drop=True)
        wb_fbo_stocks_new['barcode'] = wb_fbo_stocks_new['barcode'].astype(str)
        wb_fbo_stocks_new = wb_fbo_stocks_new\
        .query(f"barcode in {[x for x in list(wb_fbo_stocks_new['barcode']) if len(x) > 5]}").reset_index(drop=True)
        wb_fbo_stocks_new['barcode'] = wb_fbo_stocks_new['barcode'].astype(int)
        wb_fbo_stocks_new = wb_fbo_stocks_new[wb_fbo_stocks_new['warehouseName']!=''].reset_index(drop=True)
        for bar in list(set(wb_fbo_stocks_new['barcode'])):
            for sklad in list(set(wb_fbo_stocks_new['warehouseName'])):
                if len(wb_fbo_stocks_new[(wb_fbo_stocks_new['barcode']==bar) & (wb_fbo_stocks_new['warehouseName']==sklad)]) > 1:
                    sum_quantity = wb_fbo_stocks_new[(wb_fbo_stocks_new['barcode']==bar)\
                                                 & (wb_fbo_stocks_new['warehouseName']==sklad)]['quantity'].sum()
                    for ind in list(wb_fbo_stocks_new[(wb_fbo_stocks_new['barcode']==bar)\
                                                  & (wb_fbo_stocks_new['warehouseName']==sklad)].index):
                        wb_fbo_stocks_new.loc[ind, ['quantity']] = sum_quantity
        wb_fbo_stocks_new = wb_fbo_stocks_new\
                        .drop_duplicates(subset=['warehouseName', 'barcode', 'quantity'], keep='first')\
                        .reset_index(drop=True)
        wb_fbo_stocks_new['date'] = const.dt_now.date().strftime('%d-%B-%Y')
    else:
        print('Запрос на получение wb_fbo_stocks_new выполнился с ошибкой')
        exit()  
    try:
        wb_fbo_stocks_new['barcode'] = wb_fbo_stocks_new['barcode'].astype(int)
    except:
        print('Некорректные баркоды в wb_fbo_stocks_new')
        exit()
    if len(wb_fbo_stocks_new) < 1000 and account == 'IR':
        print('Слишком мало данных в wb_fbo_stocks_new')
        exit()
    elif len(wb_fbo_stocks_new) < 10 and account == 'KZ':
        print('Слишком мало данных в wb_fbo_stocks_new')
        exit()
    else:
        None
    for row in list(wb_fbo_stocks_new['warehouseName'].unique()):
        if len(wb_fbo_stocks_new[wb_fbo_stocks_new['warehouseName']==row])\
           - len(list(set(wb_fbo_stocks_new[wb_fbo_stocks_new['warehouseName']==row]['barcode']))) != 0:
            print('В wb_fbo_stocks_new есть неуникальные barcode')
            exit()
        else:
            None
    wb_fbo_stocks_new = astype_df(wb_fbo_stocks_new)
    if account == 'IR':
        wb_fbo_stocks_new.to_excel(path_FD + 'wb_fbo_stocks_new.xlsx', index=False)
    else:
        wb_fbo_stocks_new.to_excel(path_FD + f'wb_{account.lower()}_fbo_stocks_new.xlsx', index=False)
  



# # Получение wb_price_new
# def get_wb_price_new(path_FD=path_FD):
#     url_param = const.url_param_wb_prices
#     header_params = const.header_params_wb_prices
#     response = requests.get(url_param,
#                             headers=header_params)
#     if response.status_code != 200:
#         code = 0
#         c = 0
#         while code != 200:
#             response = requests.get(url_param,
#                                     headers=header_params)
#             code = response.status_code
#             if code != 200:
#                 c += 50
#                 code = c
#             time.sleep(10)
#     else:
#         None
#     if response.status_code != 200:
#         print('Запрос на получение wb_price выполнился с ошибкой')
#         exit()
#     else:
#         wb_price_new = pd.DataFrame(response.json())
#     wb_list_of_nomenclatures = select_table('wb_list_of_nomenclatures')
#     unique_barcode(wb_list_of_nomenclatures)
#     try:
#         try:
#             wb_list_of_nomenclatures = wb_list_of_nomenclatures[['Артикул_wb', 'Баркод']]
#         except:
#             wb_list_of_nomenclatures = wb_list_of_nomenclatures[['Артикул_WB', 'Баркод']]
#     except:
#         print('Названия столбцов в отчете "wb_list_of_nomenclatures" изменились')
#         exit()
#     try:
#         wb_list_of_nomenclatures.columns = ['nmId', 'Баркод']
#         wb_price_new = wb_price_new.merge(wb_list_of_nomenclatures, on='nmId', how='left')
#         wb_price_new = wb_price_new[wb_price_new['Баркод'].astype(str) != 'nan'].reset_index(drop=True)
#         wb_price_new['Баркод'] = wb_price_new['Баркод'].astype(int)
#     except:
#         print('При обработке wb_list_of_nomenclatures и добавлении к wb_price_new баркодов произошла ошибка')
#         exit() 
#     unique_barcode(wb_price_new)
#     wb_price_new['date'] = const.dt_now.date().strftime('%d-%B-%Y')
#     wb_price_new = astype_df(wb_price_new)
#     if len(wb_price_new) < 1000:
#         print('Слишком мало данных в wb_price_new')
#         exit()
#     try:
#         for col in ['nmId', 'price', 'discount', 'promoCode', 'Баркод']:
#             wb_price_new[col] = wb_price_new[col].astype(int)
#     except:
#         print('Ощибка в измененнии типа данных в wb_price_new')
#         exit()
#     wb_price_new.to_excel(path_FD + 'wb_price_new.xlsx', index=False)




# Получение oz_price_new
def get_oz_price_new(path_FD=path_FD,
                     account='IR'):
    def requests_prices(last_id="", 
                        account=account):
        api_params = {
                  "filter": {
                    "visibility": "ALL"
                  },
                  "last_id": last_id,
                  "limit": 1000
                }
        url_param = const.url_param_oz_prices
        if account == 'KZ':
            header_params = const.header_params_oz_kz_prices
        elif account == 'IR':
            header_params = const.header_params_oz_prices
        elif account == 'SSY':
            header_params = const.header_params_oz_ssy_prices
        else:
            print('Ошибка в выборе кабинета озон')
            exit()
        response = requests.post(
                                 url_param,
                                 json=api_params,
                                 headers=header_params,
                                 )
        if response.status_code != 200:
            code = 0
            c = 0
            while code != 200:
                response = requests.post(
                                         url_param,
                                         json=api_params,
                                         headers=header_params,
                                         )
                code = response.status_code
                if code != 200:
                    c += 50
                    code = c
                time.sleep(10)
        else:
            None
        if response.status_code != 200:
            print('Запрос на получение oz_price_new выполнился с ошибкой')
            exit()
        else:
            None
        oz_price_new = pd.DataFrame(response.json()['result']['items'])
        if last_id == "":
            last_id = pd.DataFrame(response.json())['result'].loc['last_id']
        else:
            last_id = None
        return oz_price_new, last_id
    last_id = ""
    oz_price_new, last_id  = requests_prices(last_id=last_id)
    if len(oz_price_new) == 1000:
        while last_id != None:
            oz_price_new_0, last_id  = requests_prices(last_id=last_id)
            oz_price_new = pd.concat([oz_price_new, oz_price_new_0], ignore_index=True)
            oz_price_new_0 = None
        oz_price_new = oz_price_new.drop_duplicates(subset=['offer_id', 'product_id'])
    else:
        oz_price_new = oz_price_new.drop_duplicates(subset=['offer_id', 'product_id'])
    try:
        oz_price_new = oz_price_new.rename(columns={'price': 'price2'})\
                       .join(oz_price_new['price'].apply(pd.Series)).drop('price2', axis=1)\
                       .join(oz_price_new['price_indexes'].apply(pd.Series)['external_index_data']\
                       .apply(pd.Series))\
                       .drop('price_indexes', axis=1)\
                       .drop(['commissions', 
                              'marketing_actions', 
                              'currency_code', 
                              'minimal_price_currency', 
                              'premium_price', 
                              'recommended_price', 
                              'min_ozon_price'], axis=1)
    except:
        print('Ошибка в раскрытии признаков')
        exit()
    try:
        oz_price_new = clear_article(oz_price_new, 'offer_id')
        oz_price_new = astype_df(oz_price_new)
    except:
        print('Ошибка при редактировании oz_price_new')
        exit()
    if len(oz_price_new) > 0:
        if len(oz_price_new) - len(list(set(oz_price_new['offer_id']))) !=0:
            print('В датафрейме oz_price_new есть неуникальные offer_id (артикулы)')
            exit()
        else:
            None
    else:
        print('В oz_price_new нет данных')
        exit()
    oz_price_new = oz_price_new.fillna('')    
    oz_price_new['date'] = const.dt_now.date().strftime('%d-%B-%Y')
    try:
        for row in ['price', 'old_price', 'marketing_price', 'marketing_seller_price', 'price_index_value']:
            oz_price_new[row] = oz_price_new[row].astype(float)
    except:
        print('Ощибка в измененнии типа данных в oz_price_new')
        exit()
    if account == 'KZ':
        name = 'oz_kz_price_new.xlsx'
    elif account == 'IR':
        name = 'oz_price_new.xlsx'
    elif account == 'SSY':
        name = 'oz_ssy_price_new.xlsx'
    else:
        print('Ошибка в выборе кабинета озон')
        exit()
    oz_price_new.to_excel(path_FD + name, index=False)



# #  Получение wb_fbs_stocks_new
# def get_wb_fbs_stocks_new(account='IR',
#                           path_FD=path_FD):
#     if account == 'IR':
#         header_params = const.header_params_wb_fbs_stocks
#     elif account == 'KZ': 
#         header_params = const.header_params_wb_kz
#     else:
#         print('error')
#         exit()
#     # Получение warehouseId складов FBS вб
#     def get_warehouseId(header_params=header_params):
#         url_param = const.url_param_wb_fbs_warehouses
#         response = requests.get(
#                                 url_param,
#                                 headers=header_params,
#                                 )
#         if response.status_code != 200:
#             code = 0
#             c = 0
#             while code != 200:
#                 response = requests.get(
#                                         url_param,
#                                         headers=header_params,
#                                         )
#                 code = response.status_code
#                 if code != 200:
#                     c += 50
#                     code = c
#                 time.sleep(10)
#         else:
#             None
#         if response.status_code == 200:
#             list_warehouseId = {}
#             df = pd.DataFrame(response.json())
#             for row in df['name'].unique():
#                 list_warehouseId[row] = df[df['name']==row]['id'].values[0]
#             list_warehouseId_df = pd.DataFrame(list_warehouseId, index=[0])
#         if len(list_warehouseId_df) == 0:
#             print('В list_warehouseId_df нет данных')
#             exit()
#         return list_warehouseId_df
#     #  Получение list_bar в строковом формате из wb_list_of_nomenclatures
#     def get_wb_list_bar(account=account):
#         if account == 'IR':
#             try:
#                 wb_list_of_nomenclatures = select_table('wb_list_of_nomenclatures')
#                 unique_barcode(wb_list_of_nomenclatures)
#                 list_bar = list(set(wb_list_of_nomenclatures['Баркод'].astype(str)))
#             except:
#                 print('Ошибка в получении списка баркодов из wb_list_of_nomenclatures')
#                 exit()
#         elif account == 'KZ':
#             try:
#                 wb_kz_info = select_table('wb_kz_info')
#                 unique_barcode(wb_kz_info.rename(columns={'barcode': 'Баркод'}))
#                 list_bar = list(set(wb_kz_info['barcode'].astype(str)))
#             except:
#                 print('Ошибка в получении списка баркодов из wb_kz_info')
#                 exit()
#         else:
#             exit()
#         return list_bar
#     list_warehouseId = get_warehouseId().iloc[0, 0:].to_dict()
#     Sklad_list = list(list_warehouseId.keys())
#     list_bar = get_wb_list_bar()
#     wb_fbs_stocks_new = pd.DataFrame(columns=['Баркод', 'Остатки_FBS', 'Склад'])
#     for row in Sklad_list:
#         warehouseId = list_warehouseId[row]
#         url_param = f'{const.url_param_wb_fbs_stocks}{warehouseId}'
#         wb_fbs_stocks_sklad = None
#         if len(list_bar) > 1000:
#             for butch in list_bar[:1000], list_bar[1000:]:
#                 response = requests.post(
#                                         url_param,
#                                         headers=header_params,
#                                         json = {'skus': butch},
#                                         )
#                 if butch == list_bar[:1000]:
#                     wb_fbs_stocks_sklad = pd.DataFrame(response.json()['stocks'])
#                 else:
#                     wb_fbs_stocks_sklad = pd.concat([wb_fbs_stocks_sklad, pd.DataFrame(response.json()['stocks'])])
#             if len(wb_fbs_stocks_sklad) > 0:
#                 wb_fbs_stocks_sklad['Склад'] = row.replace(' ', '_')
#                 wb_fbs_stocks_sklad.columns = ['Баркод', 'Остатки_FBS', 'Склад']
#                 wb_fbs_stocks_new = pd.concat([wb_fbs_stocks_new, wb_fbs_stocks_sklad], ignore_index=True)
#             else:
#                 None
#         else:
#             response = requests.post(
#                                      url_param,
#                                      headers=header_params,
#                                      json = {'skus': list_bar},
#                                     )
#             wb_fbs_stocks_sklad = pd.DataFrame(response.json()['stocks'])
#             if len(wb_fbs_stocks_sklad) > 0:
#                 wb_fbs_stocks_sklad['Склад'] = row.replace(' ', '_')
#                 wb_fbs_stocks_sklad.columns = ['Баркод', 'Остатки_FBS', 'Склад']
#                 wb_fbs_stocks_new = pd.concat([wb_fbs_stocks_new, wb_fbs_stocks_sklad], ignore_index=True)
#             else:
#                 None  
#     wb_fbs_stocks_new['Баркод'] = wb_fbs_stocks_new['Баркод'].astype(int)
#     wb_fbs_stocks_new = wb_fbs_stocks_new.fillna(0)
#     wb_fbs_stocks_new['Остатки_FBS'] = wb_fbs_stocks_new['Остатки_FBS'].astype(int)
#     wb_fbs_stocks_new['date'] = const.dt_now.date().strftime('%d-%B-%Y')
#     for row in list(wb_fbs_stocks_new['Склад'].unique()):
#         if len(wb_fbs_stocks_new[wb_fbs_stocks_new['Склад']==row])\
#            - len(list(set(wb_fbs_stocks_new[wb_fbs_stocks_new['Склад']==row]['Баркод']))) != 0:
#             print('В wb_fbs_stocks_new есть неуникальные баркоды')
#             exit()
#         else:
#             None
#     if len(wb_fbs_stocks_new) == 0:
#         print('В wb_fbs_stocks нет данных')
#         exit()
#     if account == 'IR':
#         wb_fbs_stocks_new.to_excel(path_FD + 'wb_fbs_stocks_new.xlsx', index=False)
#     elif account == 'KZ': 
#         wb_fbs_stocks_new.to_excel(path_FD + 'wb_kz_fbs_stocks_new.xlsx', index=False)
#     else:
#         None
    

#  Получение wb_fbs_stocks_new
def get_wb_fbs_stocks_new(account='IR',
                          path_FD=path_FD):
    if account == 'IR':
        header_params = const.header_params_wb_fbs_stocks
    elif account == 'KZ': 
        header_params = const.header_params_wb_kz
    else:
        print('error')
        exit()
    # Получение warehouseId складов FBS вб
    def get_warehouseId(header_params=header_params):
        url_param = const.url_param_wb_fbs_warehouses
        response = requests.get(
                                url_param,
                                headers=header_params,
                                )
        if response.status_code != 200:
            code = 0
            c = 0
            while code != 200:
                response = requests.get(
                                        url_param,
                                        headers=header_params,
                                        )
                code = response.status_code
                if code != 200:
                    c += 50
                    code = c
                time.sleep(10)
        else:
            None
        if response.status_code == 200:
            list_warehouseId = {}
            df = pd.DataFrame(response.json())
            for row in df['name'].unique():
                list_warehouseId[row] = df[df['name']==row]['id'].values[0]
            list_warehouseId_df = pd.DataFrame(list_warehouseId, index=[0])
        if len(list_warehouseId_df) == 0:
            print('В list_warehouseId_df нет данных')
            exit()
        return list_warehouseId_df
    
    #  Получение list_bar в строковом формате из wb_list_of_nomenclatures
    def get_wb_list_bar(account=account):
        try:
            if account == 'IR':
                wb_info = select_table('wb_info').rename(columns={'nmid': 'nmId',
                                                                      'skus': 'Баркод'})
            else:
                wb_info = select_table(f'wb_{account.lower()}_info').rename(columns={'nmid': 'nmId',
                                                                                         'skus': 'Баркод'})
        except:
            print('Запрос на получение wb_info завершился ошибкой')
            exit()
        unique_barcode(wb_info)
        list_bar = list(set(wb_info['Баркод'].astype(str)))
        return list_bar
    
    list_warehouseId = get_warehouseId().iloc[0, 0:].to_dict()
    Sklad_list = list(list_warehouseId.keys())
    list_bar = get_wb_list_bar()
    wb_fbs_stocks_new = pd.DataFrame(columns=['Баркод', 'Остатки_FBS', 'Склад'])
    for row in Sklad_list:
        warehouseId = list_warehouseId[row]
        url_param = f'{const.url_param_wb_fbs_stocks}{warehouseId}'
        wb_fbs_stocks_sklad = None
        if len(list_bar) > 1000:
            for butch in list_bar[:1000], list_bar[1000:]:
                response = requests.post(
                                        url_param,
                                        headers=header_params,
                                        json = {'skus': butch},
                                        )
                if butch == list_bar[:1000]:
                    wb_fbs_stocks_sklad = pd.DataFrame(response.json()['stocks'])
                else:
                    wb_fbs_stocks_sklad = pd.concat([wb_fbs_stocks_sklad, pd.DataFrame(response.json()['stocks'])])
            if len(wb_fbs_stocks_sklad) > 0:
                wb_fbs_stocks_sklad['Склад'] = row.replace(' ', '_')
                wb_fbs_stocks_sklad.columns = ['Баркод', 'Остатки_FBS', 'Склад']
                wb_fbs_stocks_new = pd.concat([wb_fbs_stocks_new, wb_fbs_stocks_sklad], ignore_index=True)
            else:
                None
        else:
            response = requests.post(
                                     url_param,
                                     headers=header_params,
                                     json = {'skus': list_bar},
                                    )
            wb_fbs_stocks_sklad = pd.DataFrame(response.json()['stocks'])
            if len(wb_fbs_stocks_sklad) > 0:
                wb_fbs_stocks_sklad['Склад'] = row.replace(' ', '_')
                wb_fbs_stocks_sklad.columns = ['Баркод', 'Остатки_FBS', 'Склад']
                wb_fbs_stocks_new = pd.concat([wb_fbs_stocks_new, wb_fbs_stocks_sklad], ignore_index=True)
            else:
                None  
    wb_fbs_stocks_new['Баркод'] = wb_fbs_stocks_new['Баркод'].astype(int)
    wb_fbs_stocks_new = wb_fbs_stocks_new.fillna(0)
    wb_fbs_stocks_new['Остатки_FBS'] = wb_fbs_stocks_new['Остатки_FBS'].astype(int)
    wb_fbs_stocks_new['date'] = const.dt_now.date().strftime('%d-%B-%Y')
    for row in list(wb_fbs_stocks_new['Склад'].unique()):
        if len(wb_fbs_stocks_new[wb_fbs_stocks_new['Склад']==row])\
           - len(list(set(wb_fbs_stocks_new[wb_fbs_stocks_new['Склад']==row]['Баркод']))) != 0:
            print('В wb_fbs_stocks_new есть неуникальные баркоды')
            exit()
        else:
            None
    if len(wb_fbs_stocks_new) == 0:
        print('В wb_fbs_stocks нет данных')
        exit()
    if account == 'IR':
        wb_fbs_stocks_new.to_excel(path_FD + 'wb_fbs_stocks_new.xlsx', index=False)
    else:
        wb_fbs_stocks_new.to_excel(path_FD + f'wb_{account.lower()}_fbs_stocks_new.xlsx', index=False)


# Запрос на получение oz_product_info
def get_oz_product_info():
    header_params = const.header_params_oz_product_info
    url_param = const.url_param_oz_product_info
    # Запрос на получение oz_product_id
    def requests_oz_product_id(last_id="", 
                               header_params=header_params):
        url_param = const.url_param_oz_product_id
        api_params = {
                      'last_id':last_id,
                      'sort_by':'offer_id',
                      "sort_dir": "ASC"
                      }
        response = requests.post(
                                 url_param,
                                 json=api_params,
                                 headers=header_params,
                                 )
        if response.status_code != 200:
            code = 0
            c = 0
            while code != 200:
                response = requests.post(
                                         url_param,
                                         json=api_params,
                                         headers=header_params,
                                         )
                code = response.status_code
                if code != 200:
                    c += 50
                    code = c
                time.sleep(10)
        else:
            None
        if response.status_code != 200:
            print('Запрос на получение oz_product_id выполнился с ошибкой')
            exit()
        else:
            None
        oz_product_id = pd.DataFrame(response.json()['result']['items'])
        if last_id == "":
            last_id = pd.DataFrame(response.json())['result'].loc['last_id']
        else:
            last_id = None
        return oz_product_id, last_id
    # Получение oz_product_id
    def get_oz_product_id():
        try:
            last_id = ""
            oz_product_id, last_id  = requests_oz_product_id(last_id=last_id)
            while last_id != None:
                oz_product_id_0, last_id  = requests_oz_product_id(last_id=last_id)
                oz_product_id = pd.concat([oz_product_id, oz_product_id_0], ignore_index=True)
                oz_product_id_0 = None
        except:
            print('Ошибка при получении oz_product_id')
            exit()
        if len(oz_product_id) < 1000:
            print('Мало данных в oz_product_id')
            exit()
        if oz_product_id.isna().sum()['product_id'] != 0:
            print('Пропуски в oz_product_id')
            exit()
        else:
            return list(set(oz_product_id['product_id']))
    oz_product_id = get_oz_product_id()
    batchs = [oz_product_id[i:i + 1000] for i in range(0, len(oz_product_id), 1000)]
    for batch in batchs:
        api_params = {
                      'product_id':batch,
                      'sort_by':'offer_id',
                      "sort_dir": "ASC",
                      }        
        response = requests.post(
                                 url_param,
                                 json=api_params,
                                 headers=header_params,
                                 )
        if response.status_code != 200:
            code = 0
            c = 0
            while code != 200:
                response = requests.post(
                                         url_param,
                                         json=api_params,
                                         headers=header_params,
                                         )
                code = response.status_code
                if code != 200:
                    c += 50
                    code = c
                time.sleep(10)
        else:
            None    
        if response.status_code != 200:
            print('Запрос на получение oz_product_info выполнился с ошибкой')
            exit()
        else:
            if batch == oz_product_id[:1000]:
                oz_product_info_0 = pd.DataFrame(response.json()['result']['items'])
            else:
                oz_product_info = pd.DataFrame(response.json()['result']['items'])
                oz_product_info = pd.concat([oz_product_info_0, oz_product_info], ignore_index=True)
                oz_product_info_0 = None   
    if len(oz_product_info) < 1000:
        print('Мало данных в oz_product_info')
        exit()
    if oz_product_info['offer_id'].isna().sum() != 0:
        print('Пропуски в offer_id')
    if len(oz_product_info) - len(list(set(oz_product_info['offer_id']))) !=0:
        print('Есть неуникальные offer_id (артикулы) в oz_product_info')
        exit()
    try:
        oz_product_info = clear_article(oz_product_info, "offer_id")
    except:
        print('Ошибка при редактировании артикула')
        exit()
    try:
        oz_product_info['barcode'] = oz_product_info['barcode'].astype(str)
        oz_product_info = oz_product_info\
                         .query(f"barcode in {[x for x in list(oz_product_info['barcode']) if len(x) > 4]}")\
                         .reset_index(drop=True)
        oz_product_info[['barcode', 'id', 'fbo_sku', 'fbs_sku']]\
        = oz_product_info[['barcode', 'id', 'fbo_sku', 'fbs_sku']].astype(int) 
    except:
        print('Ошибка при редактировании oz_product_info')
        exit() 
    try:
        oz_product_info = oz_product_info.drop(['sources', 'price_indexes'], axis=1)\
                          .join(oz_product_info['stocks'].apply(pd.Series))\
                          .join(oz_product_info['visibility_details'].apply(pd.Series))\
                          .drop('state', axis=1)\
                          .join(oz_product_info['status'].apply(pd.Series))\
                          .drop('coming', axis=1)\
                          .rename(columns={'present': 'present_1'})\
                          .join(oz_product_info['discounted_stocks'].apply(pd.Series)\
                          .drop('reserved', axis=1))\
                          .drop(['stocks', 
                                 'visibility_details', 
                                 'status', 
                                 'discounted_stocks', 
                                 'reasons'], axis=1)   
    except:
        print('Ошибка при раскрытии столбцов в oz_product_info')
        exit()
    if len(oz_product_info) > 1000:
        if len(oz_product_info) - len(list(set(oz_product_info['offer_id']))) != 0:
            print('Неуникальные offer_id (артикулы) в oz_product_info')
            exit()
    else:
        print('Мало данных в oz_product_info')
        exit()
    oz_product_info.to_excel(path_FD + 'oz_product_info.xlsx', index=False)


# Получение oz_fbs_stocks_new
def get_oz_fbs_stocks_new(path_FD=path_FD):
    url_param = const.url_param_oz_fbs_stocks
    header_params = const.header_params_oz_fbs_stocks
    oz_product_info = select_table('oz_product_info')
    try:
        list_fbs_sku = [x for x in list(set(oz_product_info['fbs_sku'])) if x!=0]
        list_sku = [x for x in list(set(oz_product_info['sku'])) if x!=0]
        list_fbs_sku = list(set(list_fbs_sku + list_sku))
    except:
        print('Ошибка при получении list_fbs_sku')
        exit()
    oz_fbs_stocks_new = pd.DataFrame()
    step = 500
    i = 0
    while i < len(list_fbs_sku):
        api_params = {'sku':list_fbs_sku[i:i+step]}
        response = requests.post(
                                 url_param,
                                 json=api_params,
                                 headers=header_params,
                                 )
        answer_code = response.status_code
        if (answer_code != 200):
            break
        else:
            sub_oz_fbs_stocks_new = pd.DataFrame(response.json()['result'])
            oz_fbs_stocks_new = pd.concat([oz_fbs_stocks_new, sub_oz_fbs_stocks_new])\
            .drop_duplicates().reset_index(drop=True)
        i += step  
    oz_fbs_stocks_new['date'] =  const.dt_now.date().strftime('%d-%B-%Y')
    for row in list(oz_fbs_stocks_new['warehouse_name'].unique()):
        if len(oz_fbs_stocks_new[oz_fbs_stocks_new['warehouse_name']==row])\
           - len(list(set(oz_fbs_stocks_new[oz_fbs_stocks_new['warehouse_name']==row]['product_id']))) != 0:
            print('В oz_fbs_stocks_new есть неуникальные product_id')
            exit()
        else:
            None
    if len(oz_fbs_stocks_new) == 0:
        print('В oz_fbs_stocks_new нет данных')
        exit()
    oz_fbs_stocks_new.to_excel(path_FD + 'oz_fbs_stocks_new.xlsx', index=False)


# # Получение oz_fbo_stocks_new
# def get_oz_fbo_stocks_new(path_FD=path_FD):
#     url_param = const.url_param_oz_fbo_stocks
#     header_params = const.header_params_oz_fbo_stocks
#     offset = 0
#     oz_fbo_stocks_new = pd.DataFrame()
#     df_len = 1000
#     answer_code = 200
#     while df_len == 1000 and answer_code == 200:
#         api_params = {
#             "limit": 1000,
#             "offset": offset,
#             "warehouse_type": "ALL"
#         }
#         response = requests.post(
#                                  url_param,
#                                  json=api_params,
#                                  headers=header_params,
#                                 )
#         answer_code = response.status_code
#         if (answer_code != 200):
#             print(f'Код ответа - {answer_code}: {response.reason}')
#             print(response.text)
#             break
#         else:
#             sub_df = pd.DataFrame(response.json()['result']['rows'])
#         df_len = len(sub_df)    
#         offset += df_len
#         if df_len <= 1:
#             break
#         oz_fbo_stocks_new = (
#                              pd.concat([oz_fbo_stocks_new, sub_df])
#                              .drop_duplicates(subset=['sku','warehouse_name'])
#                              .sort_values(by='item_code')
#                              .reset_index(drop=True)
#                              )
#         if df_len <= 1:
#             break
#     if len(oz_fbo_stocks_new) == 0:
#         print('В oz_fbo_stocks_new нет данных')
#         exit()
#     oz_fbo_stocks_new = oz_fbo_stocks_new[['sku', 'warehouse_name', 'item_code', 'free_to_sell_amount']]      
#     oz_fbo_stocks_new['item_code'] = oz_fbo_stocks_new['item_code'].astype('str')
#     oz_fbo_stocks_new['warehouse_name'] = oz_fbo_stocks_new['warehouse_name'].str.upper()
#     oz_fbo_stocks_new['date'] = const.dt_now.date().strftime('%d-%B-%Y')
#     for row in list(oz_fbo_stocks_new['warehouse_name'].unique()):
#         if len(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row])\
#            - len(list(set(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row]['sku']))) != 0:
#             print('В oz_fbo_stocks_new есть неуникальные sku')
#             exit()
#     Q = []
#     for row in list(oz_fbo_stocks_new['warehouse_name'].unique()):
#         if len(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row])\
#            - len(list(set(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row]['item_code']))) != 0:
#             sub_df = oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row]
#             for art in list(set(sub_df['item_code'])):
#                 if len(sub_df[sub_df['item_code']==art]) > 1:
#                     sub_df_dop = sub_df[sub_df['item_code']==art]
#                     sub_df_dop = sub_df_dop.sort_values('free_to_sell_amount')
#                     for ind in list(sub_df_dop.index[:-1]):
#                         if sub_df_dop.loc[ind]['free_to_sell_amount'] > 5:
#                             print('Ошибка')
#                             exit()
#                         else:
#                             Q.append(ind)
#     Q = list(set(Q))
#     oz_fbo_stocks_new = oz_fbo_stocks_new.query('index not in @Q').reset_index(drop=True)
#     Q = None
#     for row in list(oz_fbo_stocks_new['warehouse_name'].unique()):
#         if len(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row])\
#            - len(list(set(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row]['item_code']))) != 0:
#             print('В oz_fbo_stocks_new есть неуникальные item_code')
#             exit()
#     try:
#         oz_fbo_stocks_new['sku'] = oz_fbo_stocks_new['sku'].astype(int)
#         oz_fbo_stocks_new['warehouse_name'] = oz_fbo_stocks_new['warehouse_name'].astype(str)
#         oz_fbo_stocks_new['item_code'] = oz_fbo_stocks_new['item_code'].astype(str)
#         oz_fbo_stocks_new['free_to_sell_amount'] = oz_fbo_stocks_new['free_to_sell_amount'].astype(int)
#     except:
#         print('Ощибка при изменении типа данных')
#         exit()
#     oz_fbo_stocks_new.to_excel(path_FD + 'oz_fbo_stocks_new.xlsx', index=False)
    

# Получение oz_fbo_stocks_new
def get_oz_fbo_stocks_new(account='IR', 
                          path_FD=path_FD):
    url_param = const.url_param_oz_fbo_stocks
    if account == 'KZ':
        header_params = const.header_params_oz_kz_prices
    elif account == 'IR':
        header_params = const.header_params_oz_product_info
    elif account == 'SSY':
        header_params = const.header_params_oz_ssy_prices
    else:
        print('Ошибка в выборе кабинета озон')
        exit()
    offset = 0
    oz_fbo_stocks_new = pd.DataFrame()
    df_len = 1000
    answer_code = 200
    while df_len == 1000 and answer_code == 200:
        api_params = {
            "limit": 1000,
            "offset": offset,
            "warehouse_type": "ALL"
        }
        response = requests.post(
                                 url_param,
                                 json=api_params,
                                 headers=header_params,
                                )
        answer_code = response.status_code
        if (answer_code != 200):
            print(f'Код ответа - {answer_code}: {response.reason}')
            print(response.text)
            break
        else:
            sub_df = pd.DataFrame(response.json()['result']['rows'])
        df_len = len(sub_df)    
        offset += df_len
        if df_len <= 1:
            break
        oz_fbo_stocks_new = (
                             pd.concat([oz_fbo_stocks_new, sub_df])
                             .drop_duplicates(subset=['sku','warehouse_name'])
                             .sort_values(by='item_code')
                             .reset_index(drop=True)
                             )
        if df_len <= 1:
            break
    if len(oz_fbo_stocks_new) == 0:
        print('В oz_fbo_stocks_new нет данных')
        exit()
    oz_fbo_stocks_new = oz_fbo_stocks_new[['sku', 'warehouse_name', 'item_code', 'free_to_sell_amount']]      
    oz_fbo_stocks_new['item_code'] = oz_fbo_stocks_new['item_code'].astype('str')
    oz_fbo_stocks_new['warehouse_name'] = oz_fbo_stocks_new['warehouse_name'].str.upper()
    oz_fbo_stocks_new['date'] = const.dt_now.date().strftime('%d-%B-%Y')
    for row in list(oz_fbo_stocks_new['warehouse_name'].unique()):
        if len(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row])\
           - len(list(set(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row]['sku']))) != 0:
            print('В oz_fbo_stocks_new есть неуникальные sku')
            exit()
    Q = []
    for row in list(oz_fbo_stocks_new['warehouse_name'].unique()):
        if len(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row])\
           - len(list(set(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row]['item_code']))) != 0:
            sub_df = oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row]
            for art in list(set(sub_df['item_code'])):
                if len(sub_df[sub_df['item_code']==art]) > 1:
                    sub_df_dop = sub_df[sub_df['item_code']==art]
                    sub_df_dop = sub_df_dop.sort_values('free_to_sell_amount')
                    for ind in list(sub_df_dop.index[:-1]):
                        if sub_df_dop.loc[ind]['free_to_sell_amount'] > 5:
                            print('Ошибка')
                            exit()
                        else:
                            Q.append(ind)
    Q = list(set(Q))
    oz_fbo_stocks_new = oz_fbo_stocks_new.query('index not in @Q').reset_index(drop=True)
    Q = None
    for row in list(oz_fbo_stocks_new['warehouse_name'].unique()):
        if len(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row])\
           - len(list(set(oz_fbo_stocks_new[oz_fbo_stocks_new['warehouse_name']==row]['item_code']))) != 0:
            print('В oz_fbo_stocks_new есть неуникальные item_code')
            exit()
    try:
        oz_fbo_stocks_new['sku'] = oz_fbo_stocks_new['sku'].astype(int)
        oz_fbo_stocks_new['warehouse_name'] = oz_fbo_stocks_new['warehouse_name'].astype(str)
        oz_fbo_stocks_new['item_code'] = oz_fbo_stocks_new['item_code'].astype(str)
        oz_fbo_stocks_new['free_to_sell_amount'] = oz_fbo_stocks_new['free_to_sell_amount'].astype(int)
    except:
        print('Ощибка при изменении типа данных')
        exit()
    if account == 'KZ':
        name = 'oz_kz_fbo_stocks_new.xlsx'
    elif account == 'IR':
        name = 'oz_fbo_stocks_new.xlsx'
    elif account == 'SSY':
        name = 'oz_ssy_fbo_stocks_new.xlsx'
    else:
        print('Ошибка в выборе кабинета озон')
        exit()
    oz_fbo_stocks_new.to_excel(path_FD + name, index=False)
    


# Определение типа данных
def recoginze_value_type(value, flag_str):
    if flag_str == False:
        default = 'VARCHAR'
        if isinstance(value, int):
            return 'BIGINT'
        elif isinstance(value, float):
            return 'FLOAT'
        elif isinstance(value, bool):
            return 'BOOL'
        elif isinstance(value, str):
            try:
                datetime.datetime.strptime(value, "%d-%B-%Y")
                t = 'date'
            except:
                t = default
            return t
        elif isinstance(value, datetime.date):
            return 'date'
        else:
            return default
    else:
        return 'VARCHAR'
# Создание блока в запросе sql с определением типов данных 
def make_dict_template(data, flag_str):
    fields_dict = data.loc[0].to_dict()
    res = dict()
    for key in fields_dict:
        res[key] = recoginze_value_type(fields_dict[key], flag_str=flag_str)
    return res

# Создание SQL-запроса на создание таблицы в БД по датафрейму
def create_table_from_dict_template(data, table_name, uniq_col, flag_str, schema=ir_schema):
    dict_template = make_dict_template(data, flag_str=flag_str)
    if len(uniq_col) == 1:
        fields_text = ''
        for key in dict_template:
            if key == uniq_col[0]:
                fields_text += f'{key} {dict_template[key]} PRIMARY KEY'
                if key != list(dict_template.keys())[-1]:
                    fields_text+=', '
            else:
                fields_text += f'{key} {dict_template[key]}'
                if key != list(dict_template.keys())[-1]:
                    fields_text+=', '
    elif len(uniq_col) > 1:
        fields_text = ''
        for key in dict_template:
            fields_text += f'{key} {dict_template[key]}, '
        fields_text += f"CONSTRAINT {'_'.join(uniq_col)+'_'+table_name+'_table_unique'} UNIQUE ({', '.join(uniq_col)})"
    elif len(uniq_col) == 0:
        fields_text = ''
        for key in dict_template:
            if key != list(dict_template.keys())[-1]:
                fields_text += f'{key} {dict_template[key]}, '
            else:
                fields_text += f'{key} {dict_template[key]}'
    sql_request = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({fields_text});"
    return  sql_request

# Показать все таблицв в базе данных в схеме
def show_table_name_from_schema(schema=ir_schema):
    try:
        connection, cursor = conn()
        req = """SELECT * FROM pg_catalog.pg_tables;"""
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        table_name_from_schema = list(df[df[0]==schema][1])
        connection.close()
        cursor.close()
        if len(table_name_from_schema) > 0:
            return table_name_from_schema
        else:
            print(f'В схеме {schema} нет таблиц')
    except:
        print('Ошибка в подключении к БД')
        
# Создание таблицы в БД
def create_table(data, 
                 table_name, 
                 uniq_col,
                 flag_str=False,
                 schema=ir_schema):
    try:
        req = create_table_from_dict_template(data=data, 
                                              table_name=table_name, 
                                              uniq_col=uniq_col, 
                                              flag_str=flag_str,
                                              schema=schema)
    except:
        print(f'Ошибка в создании sql запроса на создание таблицы {schema}.{table_name}')
        exit()
    connection, cursor = conn()
    try:
        cursor.execute(req)
        connection.commit()
    except:
        print(f'Ошибка в создании таблицы {schema}.{table_name}')
        exit()
    connection.close()
    cursor.close()
    
# Создание SQL-запроса на заполнение таблицы в БД данными датафрейма
def insert_values(data, table_name, schema=ir_schema, flag_NULL=False):
    connection, cursor = conn()
    try:
        cols = str(tuple(data.columns)).replace("'",'')
        if len(data) > 150:
            chunks = np.array_split(list(range(len(data))), len(data) // 100)
            for chunk in chunks:
                val = str([tuple(x) for x in (data.loc[chunk].to_numpy())])[1:-1]
                if flag_NULL == True:
                    val = ', '.join(["NULL" if x == "''" else x for x in val.split(', ')]) 
                    val = ', '.join(["NULL" if x == "nan" else x for x in val.split(', ')])
                req = f"INSERT INTO {schema}.{table_name} {cols} VALUES {val};"
                cursor.execute(req)
                connection.commit()
        else:
            val = str([tuple(x) for x in (data.loc[list(data.index)].to_numpy())])[1:-1]
            if flag_NULL == True:
                    val = ', '.join(["NULL" if x == "''" else x for x in val.split(', ')]) 
                    val = ', '.join(["NULL" if x == "nan" else x for x in val.split(', ')])
            req = f"INSERT INTO {schema}.{table_name} {cols} VALUES {val};"
            cursor.execute(req)
            connection.commit()
    except:
        print(f'Даные в {schema}.{table_name} не были добавлены, появилась ошибка')
        exit()
    connection.close()
    cursor.close()
    
# Очистка таблицы в БД
def truncate_table(table_name,
                   schema=ir_schema):
    try:
        req = f"""TRUNCATE TABLE {schema}.{table_name}"""
    except:
        print(f'Ошибка в запросе на очистку таблицы {schema}.{table_name}')
        exit()
    connection, cursor = conn()
    try:
        cursor.execute(req)
        connection.commit()
    except:
        print(f'Ошибка в очистке таблицы {schema}.{table_name}')
        exit()
    connection.close()
    cursor.close()

# Удаление таблицы в БД
def drop_table(table_name,
                   schema=ir_schema):
    try:
        req = f"""DROP TABLE IF EXISTS {schema}.{table_name}"""
    except:
        print(f'Ошибка в запросе на удаление таблицы {schema}.{table_name}')
        exit()
    connection, cursor = conn()
    try:
        cursor.execute(req)
        connection.commit()
    except:
        print(f'Ошибка в удалении таблицы {schema}.{table_name}')
        exit()
    connection.close()
    cursor.close()

# Удаление данных в таблице по фильтру
def delete_from_table_date_filter(table_name,
                                  date_column, 
                                  date_filter,
                                  schema=ir_schema):
    connection, cursor = conn()
    try:     
        try:
            req = f"DELETE FROM {schema}.{table_name} WHERE {date_column} = '{date_filter}'"
            cursor.execute(req)
            connection.commit()
        except:
            date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
            req = f"DELETE FROM {schema}.{table_name} WHERE {date_column} = '{date_filter}'"
            cursor.execute(req)
            connection.commit()
    except:
        print(f'Даные за определенный день в {schema}.{table_name} не были удалены, появилась ошибка')
        exit()
    connection.close()
    cursor.close()


# Обновление данных в таблице БД
def update_data_date_filter(data,
                            table_name,
                            date_column,
                            flag_NULL=False,
                            schema=ir_schema):
    date_filter = data[date_column].unique()[0]
    req = f"""SELECT * FROM {schema}.{table_name} WHERE {date_column} = '{date_filter}'"""
    connection, cursor = conn()
    try:
        cursor.execute(req)
        rows = cursor.fetchall()
        df_0 = pd.DataFrame(rows)
    except:
        print('Ошибка в обращении к БД')
        exit()
    connection.close()
    cursor.close()
    if len(df_0) == 0:
        insert_values(data,
                      table_name=table_name,
                      schema=schema,
                      flag_NULL=flag_NULL)
    else:
        try:
            connection, cursor = conn()
            req_columns = f"""SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                            AND table_schema='{schema}'
                            ORDER BY ordinal_position;"""
            cursor.execute(req_columns)
            rows_columns = cursor.fetchall()
            df_columns = pd.DataFrame(rows_columns)
            connection.close()
            cursor.close()
        except:
            print('Ошибка в получении столбцов таблицы')
            exit()
        df_0.columns = list(df_columns[0])
        date_list = list(df_0[date_column].unique())
        if isinstance(date_list[0], str) == False:
            date_list = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in date_list]
        else:
            None
        if date_filter not in date_list:
            insert_values(data,
                          table_name=table_name,
                          schema=schema, 
                          flag_NULL=flag_NULL)
        else:
            delete_from_table_date_filter(table_name=table_name,
                                          date_column=date_column,
                                          date_filter=date_filter,
                                          schema=schema)
            insert_values(data=data,
                          table_name=table_name,
                          schema=schema,
                          flag_NULL=flag_NULL)
            
# Добавление комментария к таблице
def add_comment_from_table(table_name,
                           name_column,
                           comment,
                           schema=ir_schema):
    connection, cursor = conn()
    req = f"COMMENT ON COLUMN {schema}.{table_name}.{name_column} IS '{comment}';"
    try:
        cursor.execute(req)
        connection.commit()  
    except:
        print('Не удалось добавить комментарий')
        exit()
    cursor.close()
    connection.close()

# Обновление / создание таблицы без исторических данных
def update_or_create_table_not_date(data, 
                                    table_name, 
                                    uniq_col, 
                                    schema=ir_schema, 
                                    flag_NULL=False):
    list_table_name_from_schema = show_table_name_from_schema(schema=schema)
    if table_name in list_table_name_from_schema:
        truncate_table(table_name=table_name,
                       schema=schema)
        try:
            insert_values(data=data,
                        table_name=table_name,
                        schema=schema,
                        flag_NULL=flag_NULL)
        except:
            drop_table(table_name=table_name, 
                       schema=schema)
            create_table(data=data, 
                         table_name=table_name, 
                         uniq_col=uniq_col,
                         schema=schema)
            insert_values(data=data,
                          table_name=table_name, 
                          schema=schema,
                          flag_NULL=flag_NULL)
    else:
        create_table(data=data, 
                     table_name=table_name, 
                     uniq_col=uniq_col,
                     schema=schema)
        insert_values(data=data,
                      table_name=table_name, 
                      schema=schema,
                      flag_NULL=flag_NULL)
        
# Обновление / создание таблицы с историческими данными
def update_or_create_table_date(data, 
                                table_name, 
                                uniq_col, 
                                date_column,
                                schema=ir_schema,
                                flag_NULL=False):
    list_table_name_from_schema = show_table_name_from_schema(schema=schema)
    if table_name in list_table_name_from_schema:
        update_data_date_filter(data=data,
                                table_name=table_name,
                                date_column=date_column,
                                schema=schema,
                                flag_NULL=flag_NULL)
    else:
        create_table(data=data, 
                     table_name=table_name, 
                     uniq_col=uniq_col,
                     schema=schema)
        insert_values(data=data,
                      table_name=table_name, 
                      schema=schema,
                      flag_NULL=flag_NULL)
        

# Получение данных из БД
def select_table(table_name, 
                 date_column=None,
                 date_filter=None,
                 limit=False,
                 schema=ir_schema):
    if date_column == None:
        if limit == True:
            req = f"""SELECT * FROM {schema}.{table_name} LIMIT 1;"""
        else:
            req = f"""SELECT * FROM {schema}.{table_name};"""
    else:
        if date_filter != None:
            if limit == True:
                req = f"""SELECT * FROM {schema}.{table_name} WHERE {date_column} = '{date_filter}' LIMIT 1;"""
            else:
                req = f"""SELECT * FROM {schema}.{table_name} WHERE {date_column} = '{date_filter}';"""
        else:
            print('Не задан фильтр по дате')
            exit()
    connection, cursor = conn() 
    try:
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
    except:
        print('Ошибка в обращении к БД')
        exit()
    try:
        req_columns = f"""SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                        AND table_schema='{schema}'
                        ORDER BY ordinal_position;"""
        cursor.execute(req_columns)
        rows_columns = cursor.fetchall()
        df_columns = pd.DataFrame(rows_columns)
    except:
        print('Ошибка в получении столбцов таблицы')
        exit()
    connection.close()
    cursor.close()
    df.columns = list(df_columns[0])
    return df


# Получение данных из БД c фильтром по первой дате (дата из первой добавленной строки в таблицу)
def select_table_with_first_date(table_name, 
                                date_column,
                                schema=ir_schema):
    df = select_table(table_name=table_name, 
                      schema=schema, 
                      limit=True)
    if date_column in df.columns:
        if len(list(df[date_column])) == 1:
            date_filter = list(df[date_column])[0]
        else:
            print('В таблице не корректная запись даты')
            exit()
    else:
        print('В таблице нет заданного столбца с датой')
        exit()
    df = select_table(table_name=table_name, 
                      schema=schema, 
                      date_column=date_column,
                      date_filter=date_filter)
    return df


# Получение данных из БД c фильтром по последней дате (дата - последний элемент отсортированного списка уникальных дат в столбце с датами)
def select_table_with_last_date_sort_list(table_name, 
                                          date_column,
                                          schema=ir_schema):
    req = f"""SELECT DISTINCT {date_column} FROM {schema}.{table_name};"""
    connection, cursor = conn() 
    try:
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
    except:
        print('Ошибка в обращении к БД')
        exit()
    connection.close()
    cursor.close()
    date_list = list(df[0])
    if isinstance(date_list[0], datetime.date) == True:
        date_list = sorted(date_list)
    else:
        if len([x for x in [datetime.date(1900, monthinteger, 1)\
               .strftime('%B') for monthinteger in list(range(1, 13))] if x in date_list[0]]) == 1:
            date_list = [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in date_list])]
        else:
            print('Такой формат даты не поддерживает алгоритм')
            exit()
    date_filter = date_list[-1]
    df = select_table(table_name=table_name, 
                      date_column=date_column,
                      date_filter=date_filter,
                      schema=schema)
    return df





def get_tabl_from_bd(table_name, 
                     date_column='date', 
                     start_date=start_date, 
                     end_date=end_date, 
                     flag=0, 
                     schema=ir_schema, 
                     flag_metrics=0):
    connection, cursor = conn()
    try:
        req_columns = f"""SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                        AND table_schema='{schema}'
                        ORDER BY ordinal_position;"""
        cursor.execute(req_columns)
        rows_columns = cursor.fetchall()
        df_columns = pd.DataFrame(rows_columns)
    except:
        print('Ошибка в получении столбцов таблицы')
        exit()
    if flag == 0:
        if flag_metrics != 0:
            start_date = (pd.to_datetime(start_date) - datetime.timedelta(days=21)).strftime('%Y-%m-%d')
        else:
            None
        req_0 = f"""SELECT * FROM {schema}.{table_name} LIMIT 1;"""
        cursor.execute(req_0)
        rows = cursor.fetchall()
        df_0 = pd.DataFrame(rows)
        df_0.columns = list(df_columns[0])
        if 'Timestamp' in str(type( df_0.loc[0][date_column])) or 'T' in str(df_0.loc[0][date_column]):
            req = f"""SELECT * FROM {schema}.{table_name} WHERE CAST({date_column} AS date) BETWEEN '{start_date}' AND '{end_date}';"""
            cursor.execute(req)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)
            df.columns = list(df_columns[0])
        else:
            date_param = str([(pd.to_datetime(start_date) + datetime.timedelta(days=x))\
                              .strftime('%d-%B-%Y') for x in range(0, 
                              (pd.to_datetime(end_date)-pd.to_datetime(start_date)+datetime.timedelta(days=1))\
                              .days)])\
                              .replace("[", "(")\
                              .replace("]", ")")
            req = f"""SELECT * FROM {schema}.{table_name} WHERE {date_column} in {date_param};"""
            cursor.execute(req)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)
            df.columns = list(df_columns[0])
            if len(df) == 0:
                date_param = str([(pd.to_datetime(start_date) + datetime.timedelta(days=x))\
                                  .strftime('%Y-%m-%d') for x in range(0, 
                                  (pd.to_datetime(end_date)-pd.to_datetime(start_date)+datetime.timedelta(days=1))\
                                  .days)])\
                                  .replace("[", "(")\
                                  .replace("]", ")")
                req = f"""SELECT * FROM {schema}.{table_name} WHERE {date_column} in {date_param};"""
                cursor.execute(req)
                rows = cursor.fetchall()
                df = pd.DataFrame(rows)  
                df.columns = list(df_columns[0])
            else:
                None
    else:
        req = f"""SELECT * FROM {schema}.{table_name};"""
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        df.columns = list(df_columns[0])
    connection.close()
    cursor.close()
    return df





def pg_stat_activity():
    return select_table('pg_stat_activity', schema='pg_catalog')

def closing_connection(pid):
    connection, cursor = conn()
    req = f"""SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE pid in ({pid})"""
    try:
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
    except:
        print('Ошибка в обращении к БД')
        exit()
    connection.close()
    cursor.close()




# Получение таблицы из готового запроса
def select_table_from_request(req,
                              table_name=None,
                              schema=ir_schema):
    if table_name == None:
        connection, cursor = conn()
        try:
            cursor.execute(req)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)
        except:
            print('Ошибка в обращении к БД')
            exit()
    else:
        connection, cursor = conn()
        try:
            cursor.execute(req)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)
        except:
            print('Ошибка в обращении к БД')
            exit()
        try:
            req_columns = f"""SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                            AND table_schema='{schema}'
                            ORDER BY ordinal_position;"""
            cursor.execute(req_columns)
            rows_columns = cursor.fetchall()
            df_columns = pd.DataFrame(rows_columns)
        except:
            print('Ошибка в получении столбцов таблицы')
            exit()
        connection.close()
        cursor.close()
        df.columns = list(df_columns[0])
    return df

# Выполение запроса
def request_from_db(req):
    connection, cursor = conn()
    try:
        cursor.execute(req)
        connection.commit()
    except:
        print('Ошибка в обращении к БД')
        exit()



# Получение wb_price_new
def get_wb_price_new(account='IR',
                     path_FD=path_FD):
    
    url_param = const.url_param_wb_prices
    if account == 'IR':
        header_params = const.header_params_wb_prices
    elif account == 'KZ':
        header_params = const.header_params_wb_kz
    else:
        print('Такого кабинета вб не существует')
        exit()
        
    response = requests.get(url_param,
                            headers=header_params)
    
    if response.status_code != 200:
        code = 0
        c = 0
        while code != 200:
            response = requests.get(url_param,
                                    headers=header_params)
            code = response.status_code
            if code != 200:
                c += 50
                code = c
            time.sleep(10)
    else:
        None
    if response.status_code != 200:
        print('Запрос на получение wb_price выполнился с ошибкой')
        exit()
    else:
        wb_price_new = pd.DataFrame(response.json())
    try:
        if account == 'IR':
            wb_info = select_table('wb_info').rename(columns={'nmid': 'nmId',
                                                                  'skus': 'Баркод'})
        else:
            wb_info = select_table(f'wb_{account.lower()}_info').rename(columns={'nmid': 'nmId',
                                                                                     'skus': 'Баркод'})
    except:
        print('Запрос на получение wb_info завершился ошибкой')
        exit()
    try:
        wb_price_new = wb_price_new.merge(wb_info[['nmId', 'Баркод']], on='nmId', how='inner')
        wb_price_new = wb_price_new[wb_price_new['Баркод'].astype(str) != 'nan'].reset_index(drop=True)
        wb_price_new = wb_price_new[wb_price_new['price'] > 0].reset_index(drop=True)
        wb_price_new['date'] = const.dt_now.date().strftime('%d-%B-%Y')
        wb_price_new = astype_df(wb_price_new)
        unique_barcode(wb_price_new)
    except:
        print('При обработке wb_price_new произошла ошибка')
        exit() 
    if len(wb_price_new) < 1000 and account == 'IR':
        print('Слишком мало данных в wb_price_new')
        exit()
    elif len(wb_price_new) < 10:
        print('Слишком мало данных в wb_price_new')
        exit()
    try:
        for col in ['nmId', 'price', 'discount', 'promoCode', 'Баркод']:
            wb_price_new[col] = wb_price_new[col].astype(int)
    except:
        print('Ощибка в измененнии типа данных в wb_price_new')
        exit()
    if account == 'IR':
        wb_price_new.to_excel(path_FD + 'wb_price_new.xlsx', index=False)
    else:
        wb_price_new.to_excel(path_FD + f'wb_{account.lower()}_price_new.xlsx', index=False)

def delete_from_request(req):
    connection, cursor = conn()
    cursor.execute(req)
    connection.commit()
    connection.close()
    cursor.close()

def drop_sql_activity_DBeaver():
    pg_stat_activity = pg_stat_activity()
    pg_stat_activity = pg_stat_activity[pg_stat_activity['application_name'].isin(\
                       [x for x in list(pg_stat_activity['application_name']) if 'DBeaver 23.3.0' in x])]
    for pid in list(pg_stat_activity['pid']):
        closing_connection(pid) 


# Получить платное хранение
def get_plat_hr(mp):
    req = f"""
          select article_name, sum(cost)
          from ir_db.cost_of_storage
          where marketplace = '{mp}'
          and date::DATE = (select distinct date::DATE
          from ir_db.cost_of_storage
          where marketplace = '{mp}'
          order by date::DATE desc
          limit 1)
          group by article_name
          having sum(cost) > 0
          order by article_name;
          """
    plat_hr = select_table_from_request(req).rename(columns={0: 'Артикул', 1: 'Платное хранение'})
    plat_hr['Артикул'] = plat_hr['Артикул'].astype(str)
    plat_hr['Платное хранение'] = plat_hr['Платное хранение'].astype(float)
    return plat_hr


# Получить "Закончится" - предсказание через сколько дней товар закончится
def get_df_zak():
    req = """
    SELECT 
      distinct art, 
    (case 
        when (MAX(Закончится_к::DATE) OVER(PARTITION BY art) - now()::DATE) < 0 then 0
        else (MAX(Закончится_к::DATE) OVER(PARTITION BY art) - now()::DATE)
    end) as zak
    FROM ir_db.preds_revenue_predictions
    order by art;
    """
    df_zak = select_table_from_request(req).rename(columns={0: 'Артикул', 1: 'Закончится'})
    df_zak['Артикул'] = df_zak['Артикул'].astype(str)
    df_zak['Закончится'] = df_zak['Закончится'].astype(int)
    return df_zak