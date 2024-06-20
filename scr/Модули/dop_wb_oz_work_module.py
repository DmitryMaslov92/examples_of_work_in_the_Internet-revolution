import pandas as pd
import glob
import sys
import datetime
import re
import numpy as np
import psycopg2
import requests
import time
from sys import exit
import requests
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
import maslow_working_module as mwm
import working_with_marketplace_metrics_dags as wwm

# Получение oz_fbs_orders_new
def get_oz_fbs_orders_new(account, # KZ, IR, SSY
                          date_to=None,
                          last_date=None,
                          path_FD=mwm.path_FD):
    if account == 'KZ':
        oz_api_key = const.OZON_KZ_API_KEY
        oz_klient_id = const.OZON_KZ_CLIENT_ID
    elif account == 'IR':
        oz_api_key = const.OZON_API_KEY
        oz_klient_id = const.OZON_CLIENT_ID
    elif account == 'SSY':
        oz_api_key = const.OZON_SSY_API_KEY
        oz_klient_id = const.OZON_SSY_CLIENT_ID
    else:
        print('Ошибка в выборе кабинета озон')
        exit()
    if date_to == None:
        date_to = datetime.datetime.now()
    date_to = pd.Timestamp(date_to).strftime('%FT%T.%fZ')
    if last_date == None:      
        last_date = datetime.datetime.now() - datetime.timedelta(days=1)
    date_from = pd.Timestamp(last_date).date().strftime('%FT%T.%fZ')
    df_len = 1000
    offset = 0
    answer_code = 200
    url_param = const.url_param_oz_fbs_orders
    header_params = {
        'Api-Key': oz_api_key,
        'Client-Id': oz_klient_id,
        'Content-Type': 'application/json'
        }
    fbs_df = pd.DataFrame()
    unique_cols = ['posting_number', 'in_process_at', 'products_offer_id', 'products_sku']
    while df_len > 10 and answer_code == 200:
        api_params = {
            "dir": "ASC",
            "filter": {
                "since": date_from,
                "status": "",
                "to": date_to,
            },
            "limit": 1000,
            "offset": offset,
            "with": {
                "analytics_data": True,
                "financial_data": False,
                "translit": False,
                "barcodes": False,
            }
        }
        response = requests.post(
            url_param,
            json=api_params,
            headers=header_params,
        )
        answer_code = response.status_code
        if (answer_code != 200):
            print('Error')
            print(f'Код ответа - {answer_code}: {response.reason}')
            exit()
        else:
            sub_df = pd.DataFrame(response.json()['result']['postings'])
        if sub_df.empty == True:
            print('Датафрейм пустой')
            exit()
        df_len = len(sub_df)    
        date_from = sub_df.loc[sub_df.index[-1], 'in_process_at']
        sub_df = sub_df.explode('products').reset_index(drop=True)
        for column in ['delivery_method', 'cancellation', 'products', 'analytics_data', 'requirements']:
            sub_sub_df = sub_df[column].apply(pd.Series)
            for col in sub_sub_df.columns:
                sub_sub_df.rename(columns={col:f'{column}_{col}'}, inplace=True)
            sub_df = sub_df.join(sub_sub_df)
            sub_df.drop(columns=column,inplace=True)
        fbs_df = pd.concat([fbs_df, sub_df]).drop_duplicates(subset=(unique_cols)).reset_index(drop=True)

        if df_len <= 1:
            break
    print(f'Загружено заказов FBS - {fbs_df.shape[0]}')

    fbs_df['in_process_at'] = [datetime.datetime.strptime(x[:10], '%Y-%m-%d') for x in list(fbs_df['in_process_at'])]
    fbs_df = fbs_df.sort_values(by='in_process_at', ascending=False).reset_index(drop=True)
    fbs_df['in_process_at'] = [x.strftime('%d-%B-%Y') for x in list(fbs_df['in_process_at'])]
    fbs_df = fbs_df.fillna('')
    for i in range(len(fbs_df)):
        for col in fbs_df.columns:
            s = str(fbs_df.loc[i][col])
            if '[' in s or ']' in s or '"' in s or "'" in s:
                fbs_df.loc[i, col] = s.replace('[', '').replace(']', '').replace('"', '').replace("'", "")  
    fbs_df.to_excel(path_FD + f'oz_{account.lower()}_fbs_orders_new.xlsx', index=False)



# Получение oz_fbo_orders_new
def get_oz_fbo_orders_new(account, # KZ, IR, SSY
                          date_to=None,
                          last_date=None,
                          path_FD=mwm.path_FD):
    if account == 'KZ':
        oz_api_key = const.OZON_KZ_API_KEY
        oz_klient_id = const.OZON_KZ_CLIENT_ID
    elif account == 'IR':
        oz_api_key = const.OZON_API_KEY
        oz_klient_id = const.OZON_CLIENT_ID
    elif account == 'SSY':
        oz_api_key = const.OZON_SSY_API_KEY
        oz_klient_id = const.OZON_SSY_CLIENT_ID
    else:
        print('Ошибка в выборе кабинета озон')
        exit()
    if date_to == None:
        date_to = datetime.datetime.now()
    date_to = pd.Timestamp(date_to).strftime('%FT%T.%fZ')
    if last_date == None:      
        last_date = datetime.datetime.now() - datetime.timedelta(days=1)
    date_from = pd.Timestamp(last_date).date().strftime('%FT%T.%fZ')
    df_len = 1000
    offset = 0
    answer_code = 200
    url_param = const.url_param_oz_fbo_orders
    header_params = {
        'Api-Key': oz_api_key,
        'Client-Id': oz_klient_id,
        'Content-Type': 'application/json'
        }
    fbo_df = pd.DataFrame()
    while df_len > 10 and answer_code == 200:
        api_params = {
            "dir": "ASC",
            "filter": {
                "since": date_from,
                "status": "",
                "to": date_to,
            },
            "limit": 1000,
            "offset": offset,
            "with": {
                "analytics_data": True,
                "financial_data": False,
                "translit": False,
                "barcodes": False,
            }
        }
        response = requests.post(
            url_param,
            json=api_params,
            headers=header_params,
        )
        answer_code = response.status_code
        if (answer_code != 200):
            print('Error')
            print(f'Код ответа - {answer_code}: {response.reason}')
            exit()
        else:
            sub_df = pd.DataFrame(response.json()['result'])
        if sub_df.empty == True:
            print('Датафрейм пустой')
            exit()
        df_len = len(sub_df) 
        date_from = sub_df.loc[sub_df.index[-1], 'created_at']
        sub_df = sub_df.explode('products').reset_index(drop=True)
        sub_df = sub_df.join(sub_df['products'].apply(pd.Series))
        sub_df = sub_df.join(sub_df['analytics_data'].apply(pd.Series))
        sub_df.drop(columns=['products','analytics_data'], inplace=True)
        fbo_df = pd.concat([fbo_df, sub_df]).reset_index(drop=True)
        if df_len <= 1:
            break
    print(f'Загружено заказов FBO - {fbo_df.shape[0]}')
    fbo_df['created_at'] = [datetime.datetime.strptime(x[:10], '%Y-%m-%d') for x in list(fbo_df['created_at'])]
    fbo_df = fbo_df.sort_values(by='created_at', ascending=False).reset_index(drop=True)
    fbo_df['created_at'] = [x.strftime('%d-%B-%Y') for x in list(fbo_df['created_at'])]
    fbo_df = fbo_df.fillna('')
    for i in range(len(fbo_df)):
        for col in fbo_df.columns:
            s = str(fbo_df.loc[i][col])
            if '[' in s or ']' in s or '"' in s or "'" in s:
                fbo_df.loc[i, col] = s.replace('[', '').replace(']', '').replace('"', '').replace("'", "") 
    fbo_df.to_excel(path_FD + f'oz_{account.lower()}_fbo_orders_new.xlsx', index=False)


# Запрос на получение oz_product_id
def requests_oz_product_id(header_params,
                           last_id=""):
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
    last_id = pd.DataFrame(response.json())['result'].loc['last_id']
    
    return oz_product_id, last_id

# Получение oz_product_id
def get_oz_product_id(account):
    try:
        if account == 'KZ':
            header_params = const.header_params_oz_kz_prices
        elif account == 'IR':
            header_params = const.header_params_oz_product_info
        elif account == 'SSY':
            header_params = const.header_params_oz_ssy_prices
        else:
            print('Ошибка в выборе кабинета озон')
            exit()
        oz_product_id, last_id  = requests_oz_product_id(header_params=header_params)
        len_df = len(oz_product_id)
        if len_df == 1000:
            while len_df > 0:
                oz_product_id_0, last_id  = requests_oz_product_id(header_params=header_params,
                                                                   last_id=last_id)
                len_df = len(oz_product_id_0)
                try:
                    oz_product_id = pd.concat([oz_product_id, oz_product_id_0], ignore_index=True)
                    oz_product_id_0 = None
                except:
                    None
        else:
            None
    except:
        print('Ошибка при получении oz_product_id')
        exit()
    if len(oz_product_id) < 1000 and account == 'IR':
        print('Мало данных в oz_product_id')
        exit()
    if len(oz_product_id) < 100 and account == 'KZ':
        print('Мало данных в oz_product_id')
        exit()
    if len(oz_product_id) < 400 and account == 'SSY':
        print('Мало данных в oz_product_id')
        exit()
        
    return oz_product_id


# Получение oz_product_info
def get_oz_product_info(account):
    if account == 'KZ':
        header_params = const.header_params_oz_kz_prices
    elif account == 'IR':
        header_params = const.header_params_oz_product_info
    elif account == 'SSY':
        header_params = const.header_params_oz_ssy_prices
    else:
        print('Ошибка в выборе кабинета озон')
        exit()
    url_param = const.url_param_oz_product_info
    try:
        oz_product_id = get_oz_product_id(account=account)
        oz_product_id = list(set(oz_product_id['product_id']))
        batchs = [oz_product_id[i:i + 1000] for i in range(0, len(oz_product_id), 1000)]
        oz_product_info = pd.DataFrame()
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
                oz_product_info_0 = pd.DataFrame(response.json()['result']['items'])
                oz_product_info = pd.concat([oz_product_info_0, oz_product_info], ignore_index=True)
                oz_product_info_0 = None
    except:
        print('Ошибка при получении oz_product_info')
        exit()
    if len(oz_product_info) < 1000 and account == 'IR':
        print('Мало данных в oz_product_info')
        exit()
    if len(oz_product_info) < 100 and account == 'KZ':
        print('Мало данных в oz_product_info')
        exit()
    if len(oz_product_info) < 400 and account == 'SSY':
        print('Мало данных в oz_product_info')
        exit()
    try:
        oz_product_info = mwm.clear_article(oz_product_info, "offer_id")
    except:
        print('Ошибка при редактировании артикула')
        exit()
    try:
        oz_product_info['barcode'] = oz_product_info['barcode'].astype(str)
        
        oz_product_info = oz_product_info\
                         .query(f"barcode in {[x for x in list(oz_product_info['barcode']) if len(x) > 4]}")\
                         .reset_index(drop=True)
        if account != 'SSY':
            oz_product_info[['barcode', 'id', 'fbo_sku', 'fbs_sku']]\
            = oz_product_info[['barcode', 'id', 'fbo_sku', 'fbs_sku']].astype(int) 
        else:
            oz_product_info[['id', 'fbo_sku', 'fbs_sku']]\
            = oz_product_info[['id', 'fbo_sku', 'fbs_sku']].astype(int) 
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
    if len(oz_product_info) - len(list(set(oz_product_info['offer_id']))) != 0:
        print('Неуникальные offer_id (артикулы) в oz_product_info')
        exit()
        
    return oz_product_info



# Получение oz_fbs_stocks_new
def get_oz_fbs_stocks_new(account):
    if account == 'KZ':
        header_params = const.header_params_oz_kz_prices
    elif account == 'IR':
        header_params = const.header_params_oz_product_info
    elif account == 'SSY':
        header_params = const.header_params_oz_ssy_prices
    else:
        print('Ошибка в выборе кабинета озон')
        exit()
    
    oz_product_info = get_oz_product_info(account=account)
    list_fbs_sku = [x for x in list(set(oz_product_info['fbs_sku'])) if x!=0]
    list_sku = [x for x in list(set(oz_product_info['sku'])) if x!=0]
    list_fbs_sku = list(set(list_fbs_sku + list_sku))

    oz_fbs_stocks_new = pd.DataFrame()
    step = 500
    i = 0
    while i < len(list_fbs_sku):
        api_params = {'sku':list_fbs_sku[i:i+step]}
        response = requests.post(
                                 const.url_param_oz_fbs_stocks,
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
    if len(oz_fbs_stocks_new) == 0:
        print('В oz_fbs_stocks_new нет данных')
        exit()
    oz_fbs_stocks_new['date'] =  const.dt_now.date().strftime('%d-%B-%Y')
    for row in list(oz_fbs_stocks_new['warehouse_name'].unique()):
        if len(oz_fbs_stocks_new[oz_fbs_stocks_new['warehouse_name']==row])\
           - len(list(set(oz_fbs_stocks_new[oz_fbs_stocks_new['warehouse_name']==row]['product_id']))) != 0:
            print('В oz_fbs_stocks_new есть неуникальные product_id')
            exit()
        else:
            None
    return oz_fbs_stocks_new


# Запрос на получение wb_orders
def get_wb_orders(account, 
                  full_flag='1',
                  path_FD=mwm.path_FD):
    
    url_param = const.url_param_wb_orders
    
    if full_flag == 'f':
        api_params = const.api_params_wb_fbo_stocks
    elif full_flag == '1':
        api_params = const.wb_api_params_yesterday
    elif full_flag == '2':
        api_params = const.wb_api_params_2

    if account == 'KZ':
        header_params = const.header_params_wb_kz
    elif account == 'IR':
        header_params = const.header_params_wb_fbo_stocks
    else:
        print('Ошибка в выборе кабинета вб')
        exit()
           
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
    if response.status_code != 200:
        print('Запрос на получение wb_orders выполнился с ошибкой')
        exit()
    else:
        None
        
    wb_orders = pd.DataFrame(response.json())
    wb_orders['date'] = [pd.to_datetime(x[:10], format='%Y-%m-%d')\
                         .strftime('%d-%B-%Y') for x in list(wb_orders['date'])]
    wb_orders['barcode'] = wb_orders['barcode'].astype(int)
    wb_orders = wb_orders.fillna('')
    if len(wb_orders) == 0:
        print('Ошибка в получении данных')
        exit()

    for i in range(len(wb_orders)):
        for col in wb_orders.columns:
            s = str(wb_orders.loc[i][col])
            if '[' in s or ']' in s or '"' in s or "'" in s:
                wb_orders.loc[i, col] = s.replace('[', '').replace(']', '').replace('"', '').replace("'", "") 
                
    wb_orders.to_excel(path_FD + f'wb_{account.lower()}_orders_new.xlsx', index=False)




# Получение wb_info
def get_wb_info(account='IR'):
    
    url_param = const.url_param_wb_info
    api_param = const.api_param_wb_info

    if account == 'KZ':
        header_params = const.header_params_wb_kz
    elif account == 'IR':
        header_params = const.header_params_wb_fbo_stocks
    else:
        print('Нет такого кабинета')
        exit()

    try:

        response = requests.post(url_param,
                                json=api_param,
                                headers=header_params)

        df = pd.DataFrame(response.json()['cards'])

        if len(df) == 1000:
            sub_df = pd.DataFrame()
            len_sub_df = len(df)
            last_updatedAt = response.json()['cursor']['updatedAt']
            last_nmID = response.json()['cursor']['nmID']
            while len_sub_df == 1000:
                sub_api_param = {'settings': {'cursor': {'updatedAt': last_updatedAt,
                                                         'nmID': last_nmID,
                                                         'limit': 1000},
                                              'filter': {'withPhoto': -1}}}
                response = requests.post(url_param,
                                        json=sub_api_param,
                                        headers=header_params)

                sub_df = pd.DataFrame(response.json()['cards'])
                last_updatedAt = response.json()['cursor']['updatedAt']
                last_nmID = response.json()['cursor']['nmID']
                df = pd.concat([df, sub_df], ignore_index=True)
                len_sub_df = len(sub_df)
    except:
        print('Запрос завершился с ошибкой')
        exit()
        
    if len(df) == 0:
        print('Нет данных')
        exit()
        
    try:
        df = df[['nmID', 'imtID', 'nmUUID', 'subjectID', 'subjectName', 'vendorCode', 'brand', 'title', 'description',
            'dimensions', 'sizes', 'createdAt', 'updatedAt']]
        df['vendorCode'] = [str(x).replace('/', '') for x in list(df['vendorCode'])]
        skus = []
        for i in range(len(df)):
            skus_sub = []
            for row in df.loc[i]['sizes']:
                skus_sub += row['skus']
            skus.append(skus_sub)
        skus = pd.DataFrame(skus, 
                            columns=['skus' if x == 0 else f'skus_{x}' \
                                    for x in list(range(max([len(x) for x in skus])))])
        df = df.join(df['dimensions'].apply(pd.Series).apply(pd.Series)).join(skus).drop(['dimensions', 'sizes'], axis=1)
        skus = None

        median_len = np.median([len(str(x)) for x in list(df['skus'])])
        df = df.query(f"index not in {[y for y in list(df.index) if len(str(df.loc[y]['skus'])) != median_len]}")\
            .reset_index(drop=True)
        median_len = None
        df['skus'] = df['skus'].astype(int)
        df = df.fillna('')
        mwm.unique_barcode(df.rename(columns={'skus': 'Баркод'}))
        mwm.unique_barcode(df.rename(columns={'nmID': 'Баркод'}))
    except:
        print('Обработка датафрейма завершилась с ошибкой')
        exit()
    
    return df


# Получение тарифов по вб (монопалета)
def get_wb_tariffs_pallet(account='IR'):
    
    api_param = const.wb_tariffs_api_param_taday
    url_param = const.url_param_wb_tariffs_pallet
    
    if account == 'KZ':
        header_params = const.header_params_wb_kz
    elif account == 'IR':
        header_params = const.header_params_wb_fbo_stocks
    else:
        print('Нет такого кабинета')
        exit()
    
    response = requests.get(url_param,
                            params=api_param,
                            headers=header_params)
    if response.status_code != 200:
        code = 0
        c = 0
        while code != 200:
            response = requests.get(url_param,
                                    params=api_param,
                                    headers=header_params)
            code = response.status_code
            if code != 200:
                c += 50
                code = c
            time.sleep(10)
    else:
        None
    if response.status_code != 200:
        print('Запрос на получение wb_tariffs_pallet выполнился с ошибкой')
        exit()
    else:
        None
    
    wb_tariffs_pallet = pd.DataFrame(response.json()['response']['data']['warehouseList'])
    if wb_tariffs_pallet.empty == True:
        print('Датафрейм пустой')
        exit()
        
    return wb_tariffs_pallet


# Получение тарифов по вб (короб)
def get_wb_tariffs_box(account='IR'):
    
    api_param = const.wb_tariffs_api_param_taday
    url_param = const.url_param_wb_tariffs_box
    
    if account == 'KZ':
        header_params = const.header_params_wb_kz
    elif account == 'IR':
        header_params = const.header_params_wb_fbo_stocks
    else:
        print('Нет такого кабинета')
        exit()
    
    response = requests.get(url_param,
                            params=api_param,
                            headers=header_params)
    if response.status_code != 200:
        code = 0
        c = 0
        while code != 200:
            response = requests.get(url_param,
                                    params=api_param,
                                    headers=header_params)
            code = response.status_code
            if code != 200:
                c += 50
                code = c
            time.sleep(10)
    else:
        None
    if response.status_code != 200:
        print('Запрос на получение wb_tariffs_box выполнился с ошибкой')
        exit()
    else:
        None
    
    wb_tariffs_box = pd.DataFrame(response.json()['response']['data']['warehouseList'])
    if wb_tariffs_box.empty == True:
        print('Датафрейм пустой')
        exit()
        
    return wb_tariffs_box

# Получение полного датафрейма по тарифам вб
def get_wb_full_tariffs():
    try:
        wb_tariffs_pallet = get_wb_tariffs_pallet()
        wb_tariffs_box = get_wb_tariffs_box()

        wb_kz_tariffs_pallet = get_wb_tariffs_pallet(account='KZ')
        wb_kz_tariffs_box = get_wb_tariffs_box(account='KZ')
    except:
        print('Ошибка в получении данных')
        exit()
    try:
        wb_tariffs_pallet = wb_tariffs_pallet.rename(columns={'palletDeliveryExpr': 'Delivery_Storage_Expr',
                                                            'palletDeliveryValueBase': 'Delivery_Value_Base',
                                                            'palletDeliveryValueLiter': 'Delivery_Value_Liter',
                                                            'palletStorageExpr': 'StorageExpr_Base',
                                                            'palletStorageValueExpr': 'StorageValueExpr_Liter'})
        wb_tariffs_box = wb_tariffs_box.rename(columns={'boxDeliveryAndStorageExpr': 'Delivery_Storage_Expr',
                                                        'boxDeliveryBase': 'Delivery_Value_Base',
                                                        'boxDeliveryLiter': 'Delivery_Value_Liter',
                                                        'boxStorageBase': 'StorageExpr_Base',
                                                        'boxStorageLiter': 'StorageValueExpr_Liter'})
        wb_kz_tariffs_pallet = wb_kz_tariffs_pallet.rename(columns={'palletDeliveryExpr': 'Delivery_Storage_Expr',
                                                                    'palletDeliveryValueBase': 'Delivery_Value_Base',
                                                                    'palletDeliveryValueLiter': 'Delivery_Value_Liter',
                                                                    'palletStorageExpr': 'StorageExpr_Base',
                                                                    'palletStorageValueExpr': 'StorageValueExpr_Liter'})
        wb_kz_tariffs_box = wb_kz_tariffs_box.rename(columns={'boxDeliveryAndStorageExpr': 'Delivery_Storage_Expr',
                                                            'boxDeliveryBase': 'Delivery_Value_Base',
                                                            'boxDeliveryLiter': 'Delivery_Value_Liter',
                                                            'boxStorageBase': 'StorageExpr_Base',
                                                            'boxStorageLiter': 'StorageValueExpr_Liter'})
    except:
        print('Ошибка в обработке данных')
        exit()
    wb_tariffs_pallet['tariffs'] = 'pallet'
    wb_tariffs_pallet['account'] = 'ir'
    wb_tariffs_box['tariffs'] = 'box'
    wb_tariffs_box['account'] = 'ir'
    wb_kz_tariffs_pallet['tariffs'] = 'pallet'
    wb_kz_tariffs_pallet['account'] = 'kz'
    wb_kz_tariffs_box['tariffs'] = 'box'
    wb_kz_tariffs_box['account'] = 'kz'
    wb_full_tariffs = pd.concat([wb_tariffs_pallet, wb_tariffs_box, wb_kz_tariffs_pallet, wb_kz_tariffs_box],
                                ignore_index=True)
    if len(wb_full_tariffs.columns) != 8:
        print('Ошибка в обработке столбцов')
        exit()
    for acc in wb_full_tariffs['account'].unique():
        for tr in  wb_full_tariffs['tariffs'].unique():
            if wb_full_tariffs[(wb_full_tariffs['account']==acc)\
                            &(wb_full_tariffs['tariffs']==tr)].value_counts('warehouseName')[0] != 1:
                print('Неуникальные склады')
                exit()
    try:
        for i in range(len(wb_full_tariffs)):
            for col in wb_full_tariffs.columns[:-3]:
                wb_full_tariffs.loc[i, col] = str(wb_full_tariffs.loc[i][col]).replace('-', '').replace(',', '.')
                if str(wb_full_tariffs.loc[i][col]) == '':
                    wb_full_tariffs.loc[i, col] = None
        wb_full_tariffs.iloc[:, :-3] = wb_full_tariffs.iloc[:, :-3].astype(float)
    except:
        print('Ошибка в обработке данных на втором этапе')
        exit()

    return wb_full_tariffs

# Получить датафрейм с заказами по вб из бд (данные из метрик до 25 января 2023 года)
def get_redact_wb_orders_old():
    wb_orders_old = mwm.select_table('wb_orders_old')
    wb_orders_old.columns = ['barcode', 'orders', 'date']
    wb_orders_old = wb_orders_old[['barcode', 'date', 'orders']]
    wb_orders_old['barcode'] = wb_orders_old['barcode'].astype(int)
    wb_orders_old = wb_orders_old.sort_values(by=['date', 'barcode']).reset_index(drop=True)
    wb_orders_old['date'] = [x.strftime('%d-%B-%Y') for x in list(wb_orders_old['date'])]
    wb_orders_old = wb_orders_old[wb_orders_old['date'].isin(\
                    mwm.date_range(start_date=datetime.datetime.strptime(wb_orders_old.loc[0]['date'], "%d-%B-%Y"), 
                    end_date=datetime.datetime(2023, 1, 25)))].reset_index(drop=True)
    res = mwm.date_range(start_date=datetime.datetime.strptime(wb_orders_old.loc[0]['date'], "%d-%B-%Y"), 
                         end_date=datetime.datetime.strptime(wb_orders_old.loc[len(wb_orders_old)-1]['date'], 
                                                             "%d-%B-%Y"))
    if len(res) != len(list(wb_orders_old['date'].unique())):
        print('Не хватает дней в датафрейме с заказами')
        print([x for x in res if x not in list(wb_orders_old['date'].unique())])
        exit()
    return wb_orders_old
    

# Получить отредактированный датафрейм по вб заказам
def get_redact_wb_orders_ir_or_kz(flag_full=False,
                                  date_filter=None,
                                  account='IR',
                                  schema=mwm.ir_schema):
    
    if date_filter != None:
        if type(date_filter) == datetime.date:
            date_filter = date_filter.strftime('%Y-%m-%d')
        else:
            date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
    else:
        None
    
    if account == 'IR':
        table_name = 'wb_orders2_copy'
        if flag_full == False:
            if date_filter == None:
                req = f"""
                SELECT * 
                FROM {schema}.{table_name}
                WHERE date::DATE = now()::DATE - 1;
                """
                df = mwm.select_table_from_request(req=req,
                                                   table_name=table_name,
                                                   schema=schema)
            else:
                req = f"""
                SELECT * 
                FROM {schema}.{table_name}
                WHERE date::DATE = '{date_filter}'
                """
                df = mwm.select_table_from_request(req=req,
                                                   table_name=table_name,
                                                   schema=schema)
        else:
            # Получить wb_orders2_copy полностью 
            df = mwm.select_table('wb_orders2_copy')
            
    elif account == 'KZ':
        table_name = 'wb_kz_orders'      
        if flag_full == False:
            if date_filter == None:
                req = f"""
                SELECT * 
                FROM {schema}.{table_name}
                WHERE date::DATE = now()::DATE - 1;
                """
                df = mwm.select_table_from_request(req=req,
                                                   table_name=table_name,
                                                   schema=schema)
            else:
                req = f"""
                SELECT * 
                FROM {schema}.{table_name}
                WHERE date::DATE = '{date_filter}'
                """
                df = mwm.select_table_from_request(req=req,
                                                   table_name=table_name,
                                                   schema=schema)
        else:
            # Получить wb_kz_orders полностью 
            df = mwm.select_table('wb_kz_orders')
            
    else:
        print('Такого кабинета не существует')
        exit()
        
    df = df[['date', 'barcode', 'supplierarticle']]
    df = df.dropna()
    df = df[df['barcode'].isin([x for x in list(df['barcode']) \
                                if str(x) != ''
                                and str(x)[0] != '1' 
                                and str(x)[0] != '2' 
                                and str(x) != '0'])].reset_index(drop=True)
    try:
        df['date'] = [x.date() for x in list(df['date'])]
    except:
        None
    df = mwm.clear_article(df, 'supplierarticle')
    df['barcode'] = df['barcode'].astype(int)

    df = df.groupby(['barcode', 'date']).agg({'barcode': ['count']}).reset_index()
    df.columns = ['barcode', 'date', 'orders']
    df = df.sort_values(by=['date', 'barcode']).reset_index(drop=True)
    df['date'] = [pd.to_datetime(x).strftime("%d-%B-%Y") for x in list(df['date'])]
    df = df[df['date'].isin(\
                            mwm.date_range(start_date=datetime.datetime(2023, 1, 26), 
                            end_date=datetime.datetime.now()))].reset_index(drop=True)
    for row in list(df['date'].unique()):
        mwm.unique_barcode(df[df['date']==row].rename(columns={'barcode': 'Баркод'}))
    res = mwm.date_range(start_date=datetime.datetime.strptime(df.loc[0]['date'], 
                                                               "%d-%B-%Y"), 
                         end_date=datetime.datetime.strptime(df.loc[len(df)-1]['date'], 
                                                             "%d-%B-%Y"))
    if len(res) != len(list(df['date'].unique())):
        print('Не хватает дней в датафрейме с заказами')
        print([x for x in res if x not in list(df['date'].unique())])
        exit()
        
    if account == 'IR':
        if flag_full != False:
            wb_orders_old = get_redact_wb_orders_old()
            df = pd.concat([df[::-1], 
                       wb_orders_old[::-1]], ignore_index=True)
    
    return df


# Получение полного датафрейма по вб заказам
def full_redact_wb_orders():
    wb_orders_ir = get_redact_wb_orders_ir_or_kz(flag_full=True,
                                                    date_filter=None,
                                                    account='IR',
                                                    schema=mwm.ir_schema)

    wb_orders_kz = get_redact_wb_orders_ir_or_kz(flag_full=True,
                                                    date_filter=None,
                                                    account='KZ',
                                                    schema=mwm.ir_schema)

    wb_orders_ir['account'] = 'ir'
    wb_orders_kz['account'] = 'kz'

    wwm.check_dates(wb_orders_ir)
    wwm.check_dates(wb_orders_kz)

    wb_orders = pd.concat([wb_orders_ir[::-1], wb_orders_kz], ignore_index=True)

    return wb_orders




# Получить датафрейм с остатками (фбо и фбс) по вб из бд (данные из метрик до 5 октября 2023 года)
def get_redact_wb_stocks_old():
    req = """
    with
    wbfbos as (
    select * 
    from ir_db.wb_fbo_stocks_old
    where Баркод is not null 
    and date is not null
    and date < '2023-10-06'
    ),
    wbfbss as (
    select *
    from ir_db.wb_fbs_stocks_old
    where Баркод is not null 
    and date is not null
    and date < '2023-10-06'
    )
    select 
    coalesce (wbfbos.Баркод, wbfbss.Баркод) as barcode,
    coalesce (wbfbos.date::DATE, wbfbss.date::DATE) as date,
    coalesce (wbfbos.Остатки_фбо, (0)) as fbo_stocks,
    coalesce (wbfbss.Остатки_фбс, (0)) as fbs_stocks
    from wbfbos
    full join wbfbss
    on wbfbos.Баркод = wbfbss.Баркод
    and wbfbos.date = wbfbss.date
    order by date desc, barcode;
    """
    wb_stocks_old = mwm.select_table_from_request(req=req,
                                                  table_name=None,
                                                  schema=mwm.ir_schema)
    wb_stocks_old.columns = ['barcode', 'date', 'fbo_stocks', 'fbs_stocks']
    wb_stocks_old['barcode'] = wb_stocks_old['barcode'].astype(int)
    
    wb_stocks_old = wb_stocks_old.sort_values(by=['date', 'barcode']).reset_index(drop=True)
    wb_stocks_old['date'] = [pd.to_datetime(x).strftime("%d-%B-%Y") for x in list(wb_stocks_old['date'])]
    
    for dt in list(wb_stocks_old['date'].unique()):
        mwm.unique_barcode(wb_stocks_old[wb_stocks_old['date']==dt].rename(columns={'barcode': 'Баркод'}))
    
    return wb_stocks_old


# Получение остатков по вб (фбо и фбс) по двум кабинетам (ir и kz)
def get_redact_wb_stocks(account='IR',
                         schema=mwm.ir_schema,
                         date_filter=None,
                         flag_full=False,
                         fbo_fbs_flag='fbo'):
    
    if date_filter != None:
        if type(date_filter) == datetime.date:
            date_filter = date_filter.strftime('%Y-%m-%d')
        else:
            date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
    else:
        None
    if account == 'IR':
        sklad_filter = "Склад = 'ПСР' and"
        if fbo_fbs_flag == 'fbo':
            table_name = 'wb_fbo_stocks'
        elif fbo_fbs_flag == 'fbs':
            table_name = 'wb_fbs_stocks'
        else:
            print('error')
            exit()
    elif account == 'KZ':
        sklad_filter = ""
        if fbo_fbs_flag == 'fbo':
            table_name = 'wb_kz_fbo_stocks'
        elif fbo_fbs_flag == 'fbs':
            table_name = 'wb_kz_fbs_stocks'
        else:
            print('error')
            exit()
    else:
        print('Нет такого кабинета на вб')
        exit()
        
    if fbo_fbs_flag == 'fbo':
        if flag_full == True:
            req = f"""
            select 
            barcode::bigint, 
            date::DATE as date,
            sum(quantityfull::int) as fbo_stocks
            from {schema}.{table_name}
            where barcode::varchar != '' 
            and date is not null 
            and barcode is not null 
            and date >= '2023-10-06'
            group by barcode, date
            having  barcode::varchar not LIKE '1%' 
            and barcode::varchar not LIKE '2%'
            order by date desc, barcode; 
            """
        else:
            if date_filter == None:
                req = f"""
                select 
                barcode::bigint, 
                date::DATE as date,
                sum(quantityfull::int) as fbo_stocks
                from {schema}.{table_name}
                where barcode::varchar != '' 
                and date is not null 
                and barcode is not null 
                and date >= '2023-10-06'
                and date::DATE = now()::DATE    
                group by barcode, date
                having  barcode::varchar not LIKE '1%' 
                and barcode::varchar not LIKE '2%'
                order by date desc, barcode; 
                """
            else:
                req = f"""
                select 
                barcode::bigint, 
                date::DATE as date,
                sum(quantityfull::int) as fbo_stocks
                from {schema}.{table_name}
                where barcode::varchar != '' 
                and date is not null 
                and barcode is not null 
                and date >= '2023-10-06'
                and date::DATE = '{date_filter}'     
                group by barcode, date
                having  barcode::varchar not LIKE '1%' 
                and barcode::varchar not LIKE '2%'
                order by date desc, barcode; 
                """ 
        wb_fbo_stocks = mwm.select_table_from_request(req=req,
                                                      table_name=None,
                                                      schema=mwm.ir_schema)
        wb_fbo_stocks.columns = ['barcode', 'date', 'fbo_stocks']
        wb_fbo_stocks['barcode'] = wb_fbo_stocks['barcode'].astype(int)
        wb_fbo_stocks = wb_fbo_stocks.fillna(0)
        for dt in list(wb_fbo_stocks['date'].unique()):
            mwm.unique_barcode(wb_fbo_stocks[wb_fbo_stocks['date']==dt].rename(columns={'barcode': 
                                                                                        'Баркод'}))
    elif fbo_fbs_flag == 'fbs':
        
        if flag_full == True:      
            req = f"""
            select 
            Баркод as barcode, 
            date, 
            sum(Остатки_fbs) as fbs_stocks
            from {schema}.{table_name}
            where {sklad_filter} Баркод::varchar != '' 
            and date is not null 
            and Баркод is not null 
            and date >= '2023-10-06'
            group by Баркод, date
            having  Баркод::varchar not LIKE '1%' 
            and Баркод::varchar not LIKE '2%'
            order by date desc, Баркод; 
            """
        else:
            if date_filter == None:
                req = f"""
                select 
                Баркод as barcode, 
                date, 
                sum(Остатки_fbs) as fbs_stocks
                from {schema}.{table_name}
                where {sklad_filter} Баркод::varchar != '' 
                and date is not null 
                and Баркод is not null 
                and date >= '2023-10-06'
                and date::DATE = now()::DATE 
                group by Баркод, date
                having  Баркод::varchar not LIKE '1%' 
                and Баркод::varchar not LIKE '2%'
                order by date desc, Баркод; 
                """
            else:
                req = f"""
                select 
                Баркод as barcode, 
                date, 
                sum(Остатки_fbs) as fbs_stocks
                from {schema}.{table_name}
                where {sklad_filter} Баркод::varchar != '' 
                and date is not null 
                and Баркод is not null 
                and date >= '2023-10-06'
                and date::DATE = '{date_filter}' 
                group by Баркод, date
                having  Баркод::varchar not LIKE '1%' 
                and Баркод::varchar not LIKE '2%'
                order by date desc, Баркод; 
                """
        wb_fbs_stocks = mwm.select_table_from_request(req=req,
                                                      table_name=None,
                                                      schema=mwm.ir_schema)
        wb_fbs_stocks.columns = ['barcode', 'date', 'fbs_stocks']
        wb_fbs_stocks['barcode'] = wb_fbs_stocks['barcode'].astype(int)
        wb_fbs_stocks = wb_fbs_stocks.fillna(0)
        for dt in list(wb_fbs_stocks['date'].unique()):
            mwm.unique_barcode(wb_fbs_stocks[wb_fbs_stocks['date']==dt].rename(columns={'barcode': 
                                                                                        'Баркод'}))

    else:
        print('Нет такого параметра остатков')
        exit()
        
    if fbo_fbs_flag == 'fbo':
        df = wb_fbo_stocks
    elif fbo_fbs_flag == 'fbs':
        df = wb_fbs_stocks
    else:
        print('error')
        exit()

    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    
    return df


# Получить полный датафрейм с остатками по вб(пропущенные даты заполнены разницей)
def get_full_redact_recovery_wb_stocks():
    
    wb_stocks_old = get_redact_wb_stocks_old()
    wb_stocks_old_fbs = wb_stocks_old[['barcode', 'date', 'fbs_stocks']].copy()
    wb_stocks_old_fbo = wb_stocks_old[['barcode', 'date', 'fbo_stocks']].copy()
    wb_stocks_old_fbs = wwm.expand_df(df=wb_stocks_old_fbs,
                                      key_column='barcode',
                                      result_column='fbs_stocks')
    wb_stocks_old_fbs = wwm.recovery_stocks_days(wb_stocks_old_fbs)
    wb_stocks_old_fbs = wwm.collapse_df(df=wb_stocks_old_fbs,
                                      key_column='barcode',
                                      result_column='fbs_stocks')
    wwm.check_dates(wb_stocks_old_fbs)
    wb_stocks_old_fbo = wwm.expand_df(df=wb_stocks_old_fbo,
                                      key_column='barcode',
                                      result_column='fbo_stocks')
    wb_stocks_old_fbo = wwm.recovery_stocks_days(wb_stocks_old_fbo)
    wb_stocks_old_fbo = wwm.collapse_df(df=wb_stocks_old_fbo,
                                      key_column='barcode',
                                      result_column='fbo_stocks')
    wwm.check_dates(wb_stocks_old_fbo)
    wb_stocks_old = wb_stocks_old_fbo.merge(wb_stocks_old_fbs, on=['barcode', 'date'], how='outer')
    wb_stocks_old['barcode'] = wb_stocks_old['barcode'].astype(int)
    wb_stocks_old = wb_stocks_old.fillna(0)

    wb_stocks_ir_fbo = get_redact_wb_stocks(account='IR',
                                                       date_filter=None,
                                                       flag_full=True,
                                                       fbo_fbs_flag='fbo')
    wb_stocks_ir_fbo = wwm.expand_df(df=wb_stocks_ir_fbo,
                                      key_column='barcode',
                                      result_column='fbo_stocks')
    wb_stocks_ir_fbo = wwm.recovery_stocks_days(wb_stocks_ir_fbo)
    wb_stocks_ir_fbo = wwm.collapse_df(df=wb_stocks_ir_fbo,
                                      key_column='barcode',
                                      result_column='fbo_stocks')
    wwm.check_dates(wb_stocks_ir_fbo)
    wb_stocks_ir_fbs = get_redact_wb_stocks(account='IR',
                                                       date_filter=None,
                                                       flag_full=True,
                                                       fbo_fbs_flag='fbs')
    wb_stocks_ir_fbs = wwm.expand_df(df=wb_stocks_ir_fbs,
                                      key_column='barcode',
                                      result_column='fbs_stocks')
    wb_stocks_ir_fbs = wwm.recovery_stocks_days(wb_stocks_ir_fbs)
    wb_stocks_ir_fbs = wwm.collapse_df(df=wb_stocks_ir_fbs,
                                      key_column='barcode',
                                      result_column='fbs_stocks')
    wwm.check_dates(wb_stocks_ir_fbs)
    wb_stocks_ir = wb_stocks_ir_fbo.merge(wb_stocks_ir_fbs, on=['barcode', 'date'], how='outer')
    wb_stocks_ir['barcode'] = wb_stocks_ir['barcode'].astype(int)
    wb_stocks_ir = wb_stocks_ir.fillna(0)
    wb_stocks_ir['fbo_stocks'] = wb_stocks_ir['fbo_stocks'].astype(int)
    wb_stocks_ir['fbs_stocks'] = wb_stocks_ir['fbs_stocks'].astype(int)
    wb_stocks_ir = pd.concat([wb_stocks_old[::-1], wb_stocks_ir], ignore_index=True)

    wb_stocks_kz_fbo = get_redact_wb_stocks(account='KZ',
                                                       date_filter=None,
                                                       flag_full=True,
                                                       fbo_fbs_flag='fbo')
    wb_stocks_kz_fbo = wwm.expand_df(df=wb_stocks_kz_fbo,
                                      key_column='barcode',
                                      result_column='fbo_stocks')
    wb_stocks_kz_fbo = wwm.recovery_stocks_days(wb_stocks_kz_fbo)
    wb_stocks_kz_fbo = wwm.collapse_df(df=wb_stocks_kz_fbo,
                                      key_column='barcode',
                                      result_column='fbo_stocks')
    wwm.check_dates(wb_stocks_kz_fbo)
    wb_stocks_kz_fbs = get_redact_wb_stocks(account='KZ',
                                                       date_filter=None,
                                                       flag_full=True,
                                                       fbo_fbs_flag='fbs')
    wb_stocks_kz_fbs = wwm.expand_df(df=wb_stocks_kz_fbs,
                                      key_column='barcode',
                                      result_column='fbs_stocks')
    wb_stocks_kz_fbs = wwm.recovery_stocks_days(wb_stocks_kz_fbs)
    wb_stocks_kz_fbs = wwm.collapse_df(df=wb_stocks_kz_fbs,
                                      key_column='barcode',
                                      result_column='fbs_stocks')
    wwm.check_dates(wb_stocks_kz_fbs)
    wb_stocks_kz = wb_stocks_kz_fbo.merge(wb_stocks_kz_fbs, on=['barcode', 'date'], how='outer')
    wb_stocks_kz['barcode'] = wb_stocks_kz['barcode'].astype(int)
    wb_stocks_kz = wb_stocks_kz.fillna(0)
    wb_stocks_kz['fbo_stocks'] = wb_stocks_kz['fbo_stocks'].astype(int)
    wb_stocks_kz['fbs_stocks'] = wb_stocks_kz['fbs_stocks'].astype(int)

    wb_stocks_ir['account'] = 'ir'
    wb_stocks_kz['account'] = 'kz'

    wb_stocks = pd.concat([wb_stocks_ir, wb_stocks_kz], ignore_index=True)
    
    return wb_stocks


# Получить датафрейм с ценами по вб из бд (данные из метрик до 5 октября 2023 года)
def get_redact_wb_prices_old():
    req = """
    select 
    Баркод, 
    date, 
    Цена as prices
    from ir_db.wb_prices_old
    where date < '2023-10-06' and date is not null and Баркод is not null and Баркод::varchar != ''
    order by date desc, Баркод
    """

    wb_prices_old = mwm.select_table_from_request(req=req,
                                                  table_name=None,
                                                  schema=mwm.ir_schema)\
                    .rename(columns={0: 'barcode', 1: 'date', 2: 'prices'})
    wb_prices_old['barcode'] = wb_prices_old['barcode'].astype(int)

    wb_prices_old = wb_prices_old.sort_values(by=['date', 'barcode']).reset_index(drop=True)
    wb_prices_old['date'] = [pd.to_datetime(x).strftime("%d-%B-%Y") for x in list(wb_prices_old['date'])]

    for dt in list(wb_prices_old['date'].unique()):
        mwm.unique_barcode(wb_prices_old[wb_prices_old['date']==dt].rename(columns={'barcode': 'Баркод'}))

    wb_prices_old['prices_before_discount'] = 0
    wb_prices_old['account'] = 'ir' 
    
    return wb_prices_old


# Получение цен вб по двум кабинетам (ir и kz)
def get_redact_wb_prices(account='IR',
                         schema=mwm.ir_schema,
                         date_filter=None,
                         flag_full=False):
    
    if date_filter != None:
        if type(date_filter) == datetime.date:
            date_filter = date_filter.strftime('%Y-%m-%d')
        else:
            date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
    else:
        None
    if account == 'IR':
        table_name = 'wb_prices'
    elif account == 'KZ':
        table_name = 'wb_kz_prices'
    else:
        print('Нет такого кабинета на вб')
        exit()
    
    if flag_full == True:
        req = f"""
        select 
        Баркод as barcode,
        date::DATE, 
        round(sum(price::float * ((100 - discount::float)/100)))::int as prices,
        round(sum(price))::int as prices_before_discount
        from {schema}.{table_name}
        where price > 0 and Баркод::varchar != '' and date is not null and Баркод is not null and date >= '2023-10-06'
        group by Баркод, date
        having  Баркод::varchar not LIKE '1%' and Баркод::varchar not LIKE '2%'
        """
    else:
        if date_filter == None:
            req = f"""
            select 
            Баркод as barcode,
            date::DATE, 
            round(sum(price::float * ((100 - discount::float)/100)))::int as prices,
            round(sum(price))::int as prices_before_discount
            from {schema}.{table_name}
            where price > 0 and Баркод::varchar != '' and date is not null and Баркод is not null and date >= '2023-10-06'
            and date::DATE = now()::DATE 
            group by Баркод, date
            having  Баркод::varchar not LIKE '1%' and Баркод::varchar not LIKE '2%'
            """ 
        else:
            if type(date_filter) == datetime.date:
                date_filter = date_filter.strftime('%Y-%m-%d')
            else:
                date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
            req = f"""
            select 
            Баркод as barcode,
            date::DATE, 
            round(sum(price::float * ((100 - discount::float)/100)))::int as prices,
            round(sum(price))::int as prices_before_discount
            from {schema}.{table_name}
            where price > 0 and Баркод::varchar != '' and date is not null and Баркод is not null and date >= '2023-10-06'
            and date::DATE = '{date_filter}'
            group by Баркод, date
            having  Баркод::varchar not LIKE '1%' and Баркод::varchar not LIKE '2%'
            """ 
            
    wb_prices = mwm.select_table_from_request(req=req,
                                              table_name=None,
                                              schema=mwm.ir_schema)\
                .rename(columns={0: 'barcode', 1: 'date', 2: 'prices', 3: 'prices_before_discount'})
    
    wb_prices['account'] = account.lower()
    wb_prices = wb_prices.sort_values(by=['date','barcode']).reset_index(drop=True)

    for dt in list(wb_prices['date'].unique()):
        mwm.unique_barcode(wb_prices[wb_prices['date']==dt].rename(columns={'barcode': 'Баркод'}))

    wb_prices['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(wb_prices['date'])]

    if flag_full == True:
        if account == 'IR':
            wb_prices_old = get_redact_wb_prices_old()
            wb_prices = pd.concat([wb_prices_old, wb_prices], ignore_index=True)
        else:
            None
        wb_prices_sub1 = wb_prices[['barcode', 'date', 'prices']].copy()
        wb_prices_sub2 = wb_prices[['barcode', 'date', 'prices_before_discount', 'account']].copy()
        wb_prices_sub1 = wwm.expand_df(df=wb_prices_sub1, 
                                       key_column='barcode',
                                       result_column='prices',
                                       func='mean',
                                       date_column='date')
        wb_prices_sub1 = wwm.recovery_price_days(wb_prices_sub1)
        wb_prices_sub1 = wwm.collapse_df(df=wb_prices_sub1, 
                                         key_column='barcode',
                                         result_column='prices',
                                         date_column='date')
        wb_prices = wb_prices_sub1.merge(wb_prices_sub2, on=['barcode', 'date'], how='left')
        wb_prices['prices_before_discount'] = wb_prices['prices_before_discount'].fillna(0).astype(int)
        wb_prices['account'] = wb_prices['account'].fillna(account.lower())

    wwm.check_dates(wb_prices)
            
    return wb_prices

# Получить полный датафрейм с ценами по вб(пропущенные даты заполнены средней)
def get_full_redact_recovery_wb_prices():

    wb_prices_ir = get_redact_wb_prices(account=const.account_ir,
                                        date_filter=None,
                                        flag_full=True)

    wb_prices_kz = get_redact_wb_prices(account=const.account_kz,
                                        date_filter=None,
                                        flag_full=True)

    wb_prices = pd.concat([wb_prices_ir, wb_prices_kz], ignore_index=True)

    return wb_prices



# Получить датафрейм с заказами по озону из бд (данные из метрик до 14 июля 2021 года)
def get_redact_oz_orders_old(schema=mwm.ir_schema):
    req = f"""
    select
    Артикул as item_code,
    date::DATE,
    Заказы::int as orders,
    'ir' as account
    from {schema}.oz_orders_old
    where date is not null and Артикул is not null and date < '2021-07-15'
    order by date, item_code;
    """
    oz_orders_old = mwm.select_table_from_request(req=req,
                                  table_name=None,
                                  schema=schema)\
                    .rename(columns={0: 'item_code', 1: 'date', 2: 'orders', 3: 'account'})
    oz_orders_old['item_code'] = oz_orders_old['item_code'].astype(str)
    wwm.check_dates(oz_orders_old)
    # oz_orders_old['date'] = [x.strftime('%d-%B-%Y') for x in list(oz_orders_old['date'])]
    
    for dt in oz_orders_old['date'].unique():
        mwm.unique_items(oz_orders_old[oz_orders_old['date']==dt].rename(columns={'item_code': 'Артикул'}))
        
    return oz_orders_old


# Получить отредактированный датафрейм по озон заказам
def get_redact_oz_orders(flag_full=False,
                         date_filter=None,
                         account=const.account_ir,
                         schema=mwm.ir_schema):
    
    if account == 'IR':
        req = f"""
        with
        ozon_orders_copy_redact as (
        select 
        products_offer_id, 
        created_at::DATE as date, 
        sum(products_quantity) as orders
        from {schema}.ozon_orders_copy -- Заказы fbo озон (таблица разрабов в нашей схеме)
        where status != 'cancelled'
        group by products_offer_id, date
        order by date desc, products_offer_id
        ),
        oz_fbs_orders_redact as (
        select 
        products_offer_id, 
        in_process_at::DATE as date, 
        sum(products_quantity) as orders
        from {schema}.oz_fbs_orders  -- Заказы fbs озон 
        where status != 'cancelled'
        group by products_offer_id, date
        order by date desc, products_offer_id
        ),
        oz_ir as (
        select *
        from ozon_orders_copy_redact
        union all 
        select * 
        from
        oz_fbs_orders_redact
        )
        select 
        products_offer_id as item_code,
        date::DATE,
        sum(orders) as orders,
        'ir' as account
        from oz_ir
        where date >= '2021-07-15'
        group by products_offer_id, date
        having products_offer_id is not null and date is not null
        order by date desc, item_code;
        """
        split_req = "where date >= '2021-07-15'"
        
    elif account == 'SSY':
        req = f"""
        WITH 
        oz_ssy as (
        select 
        offer_id as item_code,
        created_at as date,
        sum(quantity) as orders
        from {schema}.oz_ssy_fbo_orders
        where status != 'cancelled' 
        and offer_id is not null 
        and created_at is not null
        group by offer_id, created_at
        order by date desc, item_code
        )
        select 
        item_code,
        date,
        orders,
        'ssy' as account
        from oz_ssy
        where date is not null;
        """
        split_req = "date is not null"
        
    elif account == 'KZ':
        req = f"""
        WITH 
        oz_kz as (
        select 
        products_offer_id as item_code,
        in_process_at as date,
        sum(products_quantity) as orders
        from {schema}.oz_kz_fbs_orders
        where status != 'cancelled' 
        and in_process_at is not null 
        and products_offer_id is not null 
        group by products_offer_id, in_process_at
        order by date desc, item_code
        )
        select 
        item_code,
        date,
        orders,
        'kz' as account
        from oz_kz
        where date is not null;
        """
        split_req = "date is not null"
        
    else:
        print('Такого кабинета не существует')
        exit()
        
        
    if flag_full != True:
        if date_filter != None:
            if type(date_filter) == datetime.date:
                date_filter = date_filter.strftime('%Y-%m-%d')
            else:
                date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
            req = req.split(split_req)[0] + \
            split_req + \
            f"\nand date = '{date_filter}'\n" + \
            req.split(split_req)[1]
        else:
            req = req.split(split_req)[0] + \
            split_req + \
            f"\nand date = now()::DATE - 1\n" + \
            req.split(split_req)[1]
    else:
        None
    
    df = mwm.select_table_from_request(req=req,
                                       table_name=None,
                                        schema=schema)
    if len(df) == 0:
        print('Данных по таким условиям нет')
        df = pd.DataFrame(columns=['item_code', 'date', 'orders', 'account'])
    else:
        df.columns = ['item_code', 'date', 'orders', 'account']
        
    if account == 'IR':
        if flag_full == True:
            df = pd.concat([get_redact_oz_orders_old(), df[::-1]], ignore_index=True)

    for dt in df['date'].unique():
        wwm.unique_items(df[df['date']==dt].rename(columns={'item_code': 'Артикул'}))

    df['date'] = [x.strftime('%d-%B-%Y') for x in list(df['date'])]

    df['orders'] = df['orders'].astype(int)
    df['item_code'] = df['item_code'].astype(str)

    return df


# Получение полного датафрейма по озон заказам
def get_full_redact_oz_orders():    
    oz_orders_ir = get_redact_oz_orders(flag_full=True,
                                        date_filter=None,
                                        account=const.account_ir,
                                        schema=mwm.ir_schema)
    
    oz_orders_ssy = get_redact_oz_orders(flag_full=True,
                                         date_filter=None,
                                         account=const.account_ssy,
                                         schema=mwm.ir_schema)

    oz_orders_kz = get_redact_oz_orders(flag_full=True,
                                        date_filter=None,
                                        account=const.account_kz,
                                        schema=mwm.ir_schema)

    oz_orders = pd.concat([oz_orders_ir, oz_orders_ssy, oz_orders_kz], ignore_index=True)

    return oz_orders

# Полное обновление данных в dds_wb_prices
def full_del_and_upload_wb_prices(schema=mwm.dds_schema):
    #получить полный датафрейм с ценами по вб (пропуски заполняются автоматически)
    df = get_full_redact_recovery_wb_prices()
    if len(df) > 0:
        print('Датафрейм получен')
        req = f"""
        delete
        from {schema}.dds_wb_prices
        """
        #удаляем данные в таблице dds_wb_prices
        mwm.delete_from_request(req)
        print('Датафрейм удален')
        #загружаем данные в таблицу dds_wb_prices
        mwm.update_or_create_table_date(df, 
                                        'dds_wb_prices', 
                                        ['barcode', 'date', 'account'],
                                        'date',
                                        schema=schema)
        print('Данные в dds_wb_prices загружены')
    else:
        print('Данные по вб ценам не получены')

# Полное обновление данных в dds_wb_stocks
def full_del_and_upload_wb_stocks(schema=mwm.dds_schema):
    #получить полный датафрейм с остатками по вб (пропуски заполняются автоматически)
    df = get_full_redact_recovery_wb_stocks()
    if len(df) > 0:
        print('Датафрейм получен')
        req = f"""
        delete
        from {schema}.dds_wb_stocks
        """
        #удаляем данные в таблице dds_wb_stocks
        mwm.delete_from_request(req)
        print('Датафрейм удален')
        #загружаем данные в таблицу dds_wb_stocks
        mwm.update_or_create_table_date(df, 
                                        'dds_wb_stocks', 
                                        ['barcode', 'date', 'account'],
                                        'date',
                                        schema=schema)
        print('Данные в dds_wb_stocks загружены')
    else:
        print('Данные по вб остаткам не получены')

# Получить датафрейм с ценами по оз из бд (данные из метрик до 19 декабря 2023 года)
def get_redact_oz_prices_old():
    
    req = """
    select 
    Артикул::varchar, 
    date::date, 
    Цены as prices
    from ir_db.oz_prices_old
    where date < '2023-12-20' and date is not null and Артикул is not null and Артикул::varchar != ''
    order by date, Артикул
    """
    
    oz_prices_old = mwm.select_table_from_request(req)\
                    .rename(columns={0: 'item_code', 1: 'date', 2: 'prices'})
    
    oz_prices_old['item_code'] = oz_prices_old['item_code'].astype(str)
    oz_prices_old['prices'] = oz_prices_old['prices'].astype(int)
    oz_prices_old['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(oz_prices_old['date'])]

    wwm.check_dates(oz_prices_old)

    oz_prices_old['prices_before_discount'] = 0
    oz_prices_old['prices_before_discount'] = oz_prices_old['prices_before_discount'].astype(int)
    
    return oz_prices_old



# Получение цен озон по трем кабинетам (ir, kz, ssy)
def get_redact_oz_prices(account=const.account_ir,
                         schema=mwm.ir_schema,
                         date_filter=None):
    
    if account == const.account_ir:
        table_name = 'oz_prices'
    elif account == const.account_kz:
        table_name = 'oz_kz_prices'
    elif account == const.account_ssy:
        table_name = 'oz_ssy_prices'
    else:
        print('Нет такого кабинета на озоне')
        exit()
        
    req = f"""
    SELECT 
    offer_id::varchar,
    date::date,
    avg(marketing_seller_price) AS prices,
    avg(old_price) AS old_prices
    FROM {schema}.{table_name}
    GROUP BY offer_id, date
    HAVING offer_id IS NOT NULL AND date IS NOT NULL AND date::date >= '2023-12-20'
    order by date desc, offer_id;
    """

    if date_filter != None:
        if type(date_filter) == datetime.date:
            date_filter = date_filter.strftime('%Y-%m-%d')
        else:
            date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
            
        req = req.split('GROUP BY')[0] + \
              f"where date::DATE = '{date_filter}'\n" + 'GROUP BY \n' + \
              req.split('GROUP BY')[1]  
    else:
        req = req
        
    oz_prices = mwm.select_table_from_request(req=req,
                                              table_name=None,
                                              schema=schema)\
                .rename(columns={0: 'item_code', 1: 'date', 2: 'prices', 3: 'prices_before_discount'})
    
    oz_prices['prices'] = oz_prices['prices'].astype(int)
    oz_prices['prices_before_discount'] = oz_prices['prices_before_discount'].astype(int)
    oz_prices['item_code'] = oz_prices['item_code'].astype(str)
    
    if date_filter == None:
        
        oz_prices_sub1 = oz_prices[['item_code', 'date', 'prices']].copy()
        oz_prices_sub1 = wwm.expand_df(df=oz_prices_sub1, 
                                       key_column='item_code',
                                       result_column='prices',
                                       func='mean',
                                       date_column='date')
        oz_prices_sub1 = wwm.recovery_price_days(oz_prices_sub1)
        oz_prices_sub1 = wwm.collapse_df(df=oz_prices_sub1, 
                                         key_column='item_code',
                                         result_column='prices',
                                         date_column='date')

        oz_prices_sub2 = oz_prices[['item_code', 'date', 'prices_before_discount']].copy()
        oz_prices_sub2 = wwm.expand_df(df=oz_prices_sub2, 
                                       key_column='item_code',
                                       result_column='prices_before_discount',
                                       func='mean',
                                       date_column='date')
        oz_prices_sub2 = wwm.recovery_price_days(oz_prices_sub2)
        oz_prices_sub2 = wwm.collapse_df(df=oz_prices_sub2, 
                                         key_column='item_code',
                                         result_column='prices_before_discount',
                                         date_column='date')
        oz_prices = oz_prices_sub1.merge(oz_prices_sub2, on=['item_code', 'date'], how='left')
        oz_prices['prices_before_discount'] = oz_prices['prices_before_discount'].fillna(0).astype(int)
            
        if account == const.account_ir:
            oz_prices_old = get_redact_oz_prices_old()
            oz_prices = pd.concat([oz_prices_old, oz_prices], ignore_index=True)
        else:
            None
            
    oz_prices['account'] = account.lower()
    oz_prices['date'] = [x.strftime('%d-%B-%Y') for x in list(oz_prices['date'])]
    
    wwm.check_dates(oz_prices)
    
    return oz_prices


# Получить полный датафрейм с ценами по озон (пропущенные даты заполнены средней)
def get_full_redact_recovery_oz_prices():

    oz_prices_ir = get_redact_oz_prices(account=const.account_ir,
                                        schema=mwm.ir_schema,
                                        date_filter=None)
    wwm.check_dates(oz_prices_ir)

    oz_prices_kz = get_redact_oz_prices(account=const.account_kz,
                                        schema=mwm.ir_schema,
                                        date_filter=None)
    wwm.check_dates(oz_prices_kz)
    
    oz_prices_ssy = get_redact_oz_prices(account=const.account_ssy,
                                         schema=mwm.ir_schema,
                                         date_filter=None)
    wwm.check_dates(oz_prices_ssy)
    

    oz_prices = pd.concat([oz_prices_ir, oz_prices_kz, oz_prices_ssy], ignore_index=True)

    return oz_prices

# Полное обновление данных в dds_oz_prices
def full_del_and_upload_oz_prices(schema=mwm.dds_schema):
    #получить полный датафрейм с ценами по озон (пропуски заполняются автоматически)
    df = get_full_redact_recovery_oz_prices()
    if len(df) > 0:
        print('Датафрейм получен')
        req = f"""
        delete
        from {schema}.dds_oz_prices
        """
        #удаляем данные в таблице dds_oz_prices
        mwm.delete_from_request(req)
        print('Датафрейм удален')
        #загружаем данные в таблицу dds_oz_prices
        mwm.update_or_create_table_date(df, 
                                        'dds_oz_prices', 
                                        ['item_code', 'date', 'account'],
                                        'date',
                                        schema=schema)
        print('Данные в dds_oz_prices загружены')
    else:
        print('Данные по озон ценам не получены')


# Получить датафрейм с остатками (фбо и фбс) по озон из бд (до 2023-11-13)
def get_redact_oz_stocks_old():
    req = """
        WITH 
        ozfbssold as (
        SELECT 
        Артикул::varchar,
        date::date,
        Остатки_фбс::bigint
        FROM ir_db.oz_fbs_stocks_old
        WHERE date IS NOT NULL AND Артикул IS NOT NULL AND date::date < '2023-11-16'
        ORDER BY date DESC, Артикул
        ),
        ozfbosold as (
        SELECT 
        Артикул::varchar,
        date::date,
        Остатки_фбо::bigint
        FROM ir_db.oz_fbo_stocks_old
        WHERE date IS NOT NULL AND Артикул IS NOT NULL AND date::date < '2023-11-16'
        )
        SELECT 
        COALESCE(ozfbssold.Артикул::varchar, ozfbosold.Артикул::varchar),
        COALESCE(ozfbssold.date::date, ozfbosold.date::date),
        COALESCE(SUM(Остатки_фбо::bigint), (0)) as Остатки_фбо,
        COALESCE(SUM(Остатки_фбс::bigint), (0)) as Остатки_фбс
        from ozfbssold
        full join ozfbosold
        on ozfbssold.Артикул = ozfbosold.Артикул and ozfbssold.date = ozfbosold.date
        GROUP by COALESCE(ozfbssold.Артикул, ozfbosold.Артикул), COALESCE(ozfbssold.date, ozfbosold.date);
    """
    oz_stocks_old = mwm.select_table_from_request(req=req,
                                                  table_name=None,
                                                  schema=mwm.ir_schema)
    oz_stocks_old.columns = ['item_code', 'date', 'fbo_stocks', 'fbs_stocks']
    oz_stocks_old['item_code'] = oz_stocks_old['item_code'].astype(str)
    
    oz_stocks_old = oz_stocks_old.sort_values(by=['date', 'item_code']).reset_index(drop=True)
    oz_stocks_old['date'] = [pd.to_datetime(x).strftime("%d-%B-%Y") for x in list(oz_stocks_old['date'])]
    
    for dt in list(oz_stocks_old['date'].unique()):
        mwm.unique_items(oz_stocks_old[oz_stocks_old['date']==dt].rename(columns={'item_code': 'Артикул'}))
        
    oz_fbo_stocks_old = oz_stocks_old[['item_code', 'date', 'fbo_stocks']].copy()
    oz_fbs_stocks_old = oz_stocks_old[['item_code', 'date', 'fbs_stocks']].copy()
    oz_stocks_old = None
    
    oz_fbo_stocks_old = wwm.expand_df(df=oz_fbo_stocks_old, 
                                      key_column='item_code',
                                      result_column='fbo_stocks',
                                      date_column='date')
    oz_fbo_stocks_old = wwm.recovery_stocks_days(oz_fbo_stocks_old)
    oz_fbo_stocks_old = wwm.collapse_df(df=oz_fbo_stocks_old, 
                                        key_column='item_code',
                                        result_column='fbo_stocks',
                                        date_column='date')
    
    oz_fbs_stocks_old = wwm.expand_df(df=oz_fbs_stocks_old, 
                                      key_column='item_code',
                                      result_column='fbs_stocks',
                                      date_column='date')
    oz_fbs_stocks_old = wwm.recovery_stocks_days(oz_fbs_stocks_old)
    oz_fbs_stocks_old = wwm.collapse_df(df=oz_fbs_stocks_old, 
                                        key_column='item_code',
                                        result_column='fbs_stocks',
                                        date_column='date')
    
    oz_stocks_old = oz_fbo_stocks_old.merge(oz_fbs_stocks_old, on=['item_code', 'date'], how='outer')
    
    wwm.check_dates(oz_stocks_old)
    
    oz_stocks_old['account'] = const.account_ir.lower()

    oz_stocks_old = oz_stocks_old[oz_stocks_old['date']\
                                  .isin(['13-November-2023', 
                                         '14-November-2023', 
                                         '15-November-2023'])==False]\
                    .reset_index(drop=True)

    return oz_stocks_old


# Получение остатков озон по трем кабинетам (ir, kz, ssy) c '2023-11-13'
def get_redact_oz_stocks(account=const.account_ir,
                         schema=mwm.ir_schema,
                         date_filter=None,
                         fbo_fbs_flag='fbo'):
    
    table_name = 'oz_stocks'
    
    if account == const.account_ir:
        table_name = table_name = table_name[:2] + f'_{fbo_fbs_flag}' + table_name[2:]
    else:
        table_name = table_name = table_name[:2] + f'_{account.lower()}' + f'_{fbo_fbs_flag}' + table_name[2:]
        
    if table_name not in mwm.show_table_name_from_schema(schema=schema):
        print(f'Таблицы {table_name} нет в схеме {schema}')
        exit()
        
    if fbo_fbs_flag == 'fbs':
        dop_table_name = 'oz_prices'
        if account != const.account_ir:
            dop_table_name = dop_table_name[:2] + f'_{account.lower()}' + dop_table_name[2:]
            
        if dop_table_name not in mwm.show_table_name_from_schema(schema=schema):
            print(f'Таблицы {dop_table_name} нет в схеме {schema}')
            exit()
        else:
            dop_req = f"""
            SELECT DISTINCT product_id::bigint, offer_id::varchar
            FROM {schema}.{dop_table_name};
            """
        
    if fbo_fbs_flag == 'fbo':
        
        req = f"""
        SELECT 
        item_code::varchar,
        date::date,
        sum(free_to_sell_amount) AS fbo_stocks
        FROM {schema}.{table_name}
        GROUP BY item_code::varchar, date::date
        HAVING item_code IS NOT NULL 
        AND date IS NOT NULL 
        AND date::date >= '2023-11-13'
        ORDER BY date::date, item_code::varchar;
        """
        
    elif fbo_fbs_flag == 'fbs':
        
        req = f"""
        SELECT 
        product_id::bigint,
        date::date,
        sum(present::bigint) AS fbs_stocks
        FROM {schema}.{table_name}
        GROUP BY product_id::bigint, date::date
        HAVING product_id IS NOT NULL
        AND date IS NOT NULL
        AND date::date >= '2023-11-13'
        ORDER BY date::date, product_id::bigint;
        """
        
    else:
        print('Ошибка в обозначении флага фбо/фбс')
        exit()
        
    if date_filter != None:
        if type(date_filter) == datetime.date:
            date_filter = date_filter.strftime('%Y-%m-%d')
        else:
            date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
        if account == const.account_ir and fbo_fbs_flag == 'fbs':
            req = req.split('GROUP BY')[0] + \
                  f"where date::DATE = '{date_filter}'\nand warehouse_name::varchar = 'ПСР'\n" + 'GROUP BY \n' + \
                  req.split('GROUP BY')[1] 
        else:
            req = req.split('GROUP BY')[0] + \
                  f"where date::DATE = '{date_filter}'\n" + 'GROUP BY \n' + \
                  req.split('GROUP BY')[1] 
    else:
        if account == const.account_ir and fbo_fbs_flag == 'fbs':
            req = req.split('GROUP BY')[0] + \
                  f"where warehouse_name::varchar = 'ПСР'\n" + 'GROUP BY \n' + \
                  req.split('GROUP BY')[1] 
        else:
            req = req
    
    if fbo_fbs_flag == 'fbo':
        df = mwm.select_table_from_request(req)\
             .rename(columns={0: 'item_code', 1: 'date', 2: 'fbo_stocks'})
        if len(df) == 0:
            print('Данные не получены')
            exit()
    else:
        df = mwm.select_table_from_request(req)\
             .rename(columns={0: 'product_id', 1: 'date', 2: 'fbs_stocks'})
        if len(df) == 0:
            print('Данные не получены')
            exit()
        df['product_id'] = df['product_id'].astype(int)
        
        df_dop = mwm.select_table_from_request(dop_req)\
                 .rename(columns={0: 'product_id', 1: 'item_code'})
        if len(df_dop) == 0:
            print('Дополнительные данные не получены')
            exit()
        df_dop['product_id'] = df_dop['product_id'].astype(int)
        
        if len([x for x in list(df['product_id']) if x not in list(df_dop['product_id'])]) != 0:
            print(f'В {table_name} есть product_id, для которых нет артикулов соответствия')
            exit()

        df = df.merge(df_dop, on='product_id', how='left')[['item_code', 'date', 'fbs_stocks']]
        df_dop = None
        
    df['item_code'] = df['item_code'].astype(str)
    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    df[df.columns[-1]] = df[df.columns[-1]].astype(int)
    
    for dt in df['date'].unique():
        wwm.unique_items(df[df['date']==dt].rename(columns={'item_code': 'Артикул'}))
        
    return df


# Получить полный датафрейм с остатками по озон(пропущенные даты заполнены разницей)
def get_full_redact_recovery_oz_stocks():
    
    oz_stocks_old = get_redact_oz_stocks_old()

    oz_ir_fbo_stocks = get_redact_oz_stocks(account=const.account_ir,
                                            schema=mwm.ir_schema,
                                            date_filter=None,
                                            fbo_fbs_flag='fbo')
    oz_ir_fbo_stocks = wwm.expand_df(df=oz_ir_fbo_stocks,
                                     key_column='item_code',
                                     result_column='fbo_stocks')
    oz_ir_fbo_stocks = wwm.recovery_stocks_days(oz_ir_fbo_stocks)
    oz_ir_fbo_stocks = wwm.collapse_df(df=oz_ir_fbo_stocks,
                                       key_column='item_code',
                                       result_column='fbo_stocks')
    wwm.check_dates(oz_ir_fbo_stocks)


    oz_ir_fbs_stocks = get_redact_oz_stocks(account=const.account_ir,
                                            schema=mwm.ir_schema,
                                            date_filter=None,
                                            fbo_fbs_flag='fbs')
    oz_ir_fbs_stocks = wwm.expand_df(df=oz_ir_fbs_stocks,
                                     key_column='item_code',
                                     result_column='fbs_stocks')
    oz_ir_fbs_stocks = wwm.recovery_stocks_days(oz_ir_fbs_stocks)
    oz_ir_fbs_stocks = wwm.collapse_df(df=oz_ir_fbs_stocks,
                                       key_column='item_code',
                                       result_column='fbs_stocks')
    wwm.check_dates(oz_ir_fbs_stocks)


    oz_ssy_fbo_stocks = get_redact_oz_stocks(account=const.account_ssy,
                                             schema=mwm.ir_schema,
                                             date_filter=None,
                                             fbo_fbs_flag='fbo')
    oz_ssy_fbo_stocks = wwm.expand_df(df=oz_ssy_fbo_stocks,
                                      key_column='item_code',
                                      result_column='fbo_stocks')
    oz_ssy_fbo_stocks = wwm.recovery_stocks_days(oz_ssy_fbo_stocks)
    oz_ssy_fbo_stocks = wwm.collapse_df(df=oz_ssy_fbo_stocks,
                                        key_column='item_code',
                                        result_column='fbo_stocks')
    wwm.check_dates(oz_ssy_fbo_stocks)


    oz_kz_fbs_stocks = get_redact_oz_stocks(account=const.account_kz,
                                            schema=mwm.ir_schema,
                                            date_filter=None,
                                            fbo_fbs_flag='fbs')
    oz_kz_fbs_stocks = wwm.expand_df(df=oz_kz_fbs_stocks,
                                     key_column='item_code',
                                     result_column='fbs_stocks')
    oz_kz_fbs_stocks = wwm.recovery_stocks_days(oz_kz_fbs_stocks)
    oz_kz_fbs_stocks = wwm.collapse_df(df=oz_kz_fbs_stocks,
                                       key_column='item_code',
                                       result_column='fbs_stocks')
    wwm.check_dates(oz_kz_fbs_stocks)


    oz_stocks_old['item_code'] = oz_stocks_old['item_code'].astype(str)

    oz_ir_fbo_stocks['item_code'] = oz_ir_fbo_stocks['item_code'].astype(str)
    oz_ir_fbo_stocks['account'] = const.account_ir.lower()

    oz_ir_fbs_stocks['item_code'] = oz_ir_fbs_stocks['item_code'].astype(str)
    oz_ir_fbs_stocks['account'] = const.account_ir.lower()

    oz_ssy_fbo_stocks['item_code'] = oz_ssy_fbo_stocks['item_code'].astype(str)
    oz_ssy_fbo_stocks['account'] = const.account_ssy.lower()

    oz_kz_fbs_stocks['item_code'] = oz_kz_fbs_stocks['item_code'].astype(str)
    oz_kz_fbs_stocks['account'] = const.account_kz.lower()

    oz_ssy_fbo_stocks['fbs_stocks'] = 0
    oz_ssy_fbo_stocks = oz_ssy_fbo_stocks[list(oz_stocks_old.columns)]
    oz_kz_fbs_stocks['fbo_stocks'] = 0
    oz_kz_fbs_stocks = oz_kz_fbs_stocks[list(oz_stocks_old.columns)]

    oz_stocks = pd.concat([pd.concat([oz_stocks_old, 
                                      oz_ir_fbo_stocks\
                                      .merge(oz_ir_fbs_stocks, on=['item_code', 'date', 'account'], how='outer')\
                                      .fillna(0)], 
                                     ignore_index=True),
                          pd.concat([oz_ssy_fbo_stocks, oz_kz_fbs_stocks])], 
                          ignore_index=True)

    oz_stocks['fbo_stocks'] = oz_stocks['fbo_stocks'].fillna(0).astype(int)
    oz_stocks['fbs_stocks'] = oz_stocks['fbs_stocks'].fillna(0).astype(int)
    
    return oz_stocks



# Полное обновление данных в dds_oz_stocks
def full_del_and_upload_oz_stocks(schema=mwm.dds_schema):
    #получить полный датафрейм с остатками по вб (пропуски заполняются автоматически)
    df = get_full_redact_recovery_oz_stocks()
    if len(df) > 0:
        print('Датафрейм получен')
        req = f"""
        delete
        from {schema}.dds_oz_stocks
        """
        #удаляем данные в таблице dds_oz_stocks
        mwm.delete_from_request(req)
        print('Датафрейм удален')
        #загружаем данные в таблицу dds_oz_stocks
        mwm.update_or_create_table_date(df, 
                                        'dds_oz_stocks', 
                                        ['item_code', 'date', 'account'],
                                        'date',
                                        schema=schema)
        print('Данные в dds_oz_stocks загружены')
    else:
        print('Данные по озон остаткам не получены')



# Получить датафрейм с заказами по яндексу из бд (данные из метрик до 23 декабря 2023 года)
def get_redact_ya_orders_old(schema=mwm.ir_schema):
    req = f"""
    select
    Артикул as item_code,
    date::DATE,
    Заказы::int as orders,
    'ir' as account
    from {schema}.ya_orders_old
    where date is not null and Артикул is not null and date < '2023-12-23'
    order by date, item_code;
    """
    ya_orders_old = mwm.select_table_from_request(req=req,
                                  table_name=None,
                                  schema=schema)\
                    .rename(columns={0: 'item_code', 1: 'date', 2: 'orders', 3: 'account'})
    ya_orders_old['item_code'] = ya_orders_old['item_code'].astype(str)
    wwm.check_dates(ya_orders_old)
    
    for dt in ya_orders_old['date'].unique():
        mwm.unique_items(ya_orders_old[ya_orders_old['date']==dt].rename(columns={'item_code': 'Артикул'}))
        
    return ya_orders_old


# Получить отредактированный датафрейм по яндекс заказам
def get_redact_ya_orders(date_filter=None,
                         account=const.account_ir,
                         schema=mwm.ir_schema):

    req = f"""
    select 
    items_offerid::varchar as item_code,
    creationdate::date as date,
    sum(items_count::bigint) as orders,
    '{account.lower()}' as account
    from ir_db.yandex_orders2_copy
    where creationdate::date >= '2023-12-23'
    and items_offerid::varchar is not null 
    and creationdate::date is not null
    and items_count::bigint is not null
    and items_count::bigint > 0
    and status != 'CANCELLED'
    and fake = 0
    group by creationdate::date, items_offerid::varchar
    order by creationdate::date desc, items_offerid::varchar;
    """
    split_req = "where creationdate::date >= '2023-12-23'"
        
    if date_filter != None:
        if type(date_filter) == datetime.date:
            date_filter = date_filter.strftime('%Y-%m-%d')
        else:
            date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
        req = req.split(split_req)[0] + \
        split_req + \
        f"\nand creationdate::date = '{date_filter}'\n" + \
        req.split(split_req)[1]
    else:
        None
    
    df = mwm.select_table_from_request(req=req,
                                       table_name=None,
                                        schema=schema)
    if len(df) == 0:
        print('Данных по таким условиям нет')
        df = pd.DataFrame(columns=['item_code', 'date', 'orders', 'account'])
    else:
        df.columns = ['item_code', 'date', 'orders', 'account']
        
    if account == 'IR':
        if date_filter == None:
            df = pd.concat([get_redact_ya_orders_old(), df[::-1]], ignore_index=True)

    for dt in df['date'].unique():
        wwm.unique_items(df[df['date']==dt].rename(columns={'item_code': 'Артикул'}))

    df['date'] = [x.strftime('%d-%B-%Y') for x in list(df['date'])]

    df['orders'] = df['orders'].astype(int)
    df['item_code'] = df['item_code'].astype(str)
    
    wwm.check_dates(df)

    return df


# Получить датафрейм с ценами по яндекс из бд (данные из метрик до 28 декабря 2023 года)
def get_redact_ya_prices_old():
    
    req = """
    select 
    Артикул::varchar, 
    date::date, 
    Цены as prices
    from ir_db.ya_prices_old
    where date < '2023-12-28' and date is not null and Артикул is not null and Артикул::varchar != ''
    order by date, Артикул
    """
    
    ya_prices_old = mwm.select_table_from_request(req)\
                    .rename(columns={0: 'item_code', 1: 'date', 2: 'prices'})
    
    ya_prices_old['item_code'] = ya_prices_old['item_code'].astype(str)
    ya_prices_old['prices'] = ya_prices_old['prices'].fillna(0).astype(int)
    ya_prices_old['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(ya_prices_old['date'])]

    wwm.check_dates(ya_prices_old)

    ya_prices_old['prices_before_discount'] = 0
    ya_prices_old['prices_before_discount'] = ya_prices_old['prices_before_discount'].astype(int)
    
    return ya_prices_old


# Получение цен яндекс
def get_redact_ya_prices(account=const.account_ir,
                         schema=mwm.ir_schema,
                         date_filter=None):
    
    if account == const.account_ir:
        table_name = 'yandex_prices2_copy'
    else:
        print('Нет такого кабинета на озоне')
        exit()
        
    req = f"""
    select offerid::varchar as item_code,
    date::date,
    round(avg(price_value::bigint)) as prices,
    round(avg(price_discountbase::bigint)) as prices_before_discount
    from {schema}.{table_name}
    where offerid::varchar is not null 
    and date::date is not null
    and date::date >= '2023-12-28'
    and price_value::bigint is not null
    and price_value::bigint > 0
    group by date::date, offerid::varchar
    order by date::date desc, offerid::varchar;
    """

    if date_filter != None:
        if type(date_filter) == datetime.date:
            date_filter = date_filter.strftime('%Y-%m-%d')
        else:
            date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
            
        req = req.split('group by')[0] + \
              f"and date::DATE = '{date_filter}'\n" + 'group by \n' + \
              req.split('group by')[1]  
    else:
        req = req
        
    ya_prices = mwm.select_table_from_request(req=req,
                                              table_name=None,
                                              schema=schema)\
                .rename(columns={0: 'item_code', 1: 'date', 2: 'prices', 3: 'prices_before_discount'})
    
    ya_prices['prices'] = ya_prices['prices'].astype(int)
    ya_prices['prices_before_discount'] = ya_prices['prices_before_discount'].fillna(0).astype(int)
    ya_prices['item_code'] = ya_prices['item_code'].astype(str)
    
    if date_filter == None:
        
        ya_prices_sub1 = ya_prices[['item_code', 'date', 'prices']].copy()
        ya_prices_sub1 = wwm.expand_df(df=ya_prices_sub1, 
                                       key_column='item_code',
                                       result_column='prices',
                                       func='mean',
                                       date_column='date')
        ya_prices_sub1 = wwm.recovery_price_days(ya_prices_sub1)
        ya_prices_sub1 = wwm.collapse_df(df=ya_prices_sub1, 
                                         key_column='item_code',
                                         result_column='prices',
                                         date_column='date')

        ya_prices_sub2 = ya_prices[['item_code', 'date', 'prices_before_discount']].copy()
        ya_prices_sub2 = wwm.expand_df(df=ya_prices_sub2, 
                                       key_column='item_code',
                                       result_column='prices_before_discount',
                                       func='mean',
                                       date_column='date')
        ya_prices_sub2 = wwm.recovery_price_days(ya_prices_sub2)
        ya_prices_sub2 = wwm.collapse_df(df=ya_prices_sub2, 
                                         key_column='item_code',
                                         result_column='prices_before_discount',
                                         date_column='date')
        ya_prices = ya_prices_sub1.merge(ya_prices_sub2, on=['item_code', 'date'], how='left')
        ya_prices['prices_before_discount'] = ya_prices['prices_before_discount'].fillna(0).astype(int)
            
        if account == const.account_ir:
            ya_prices_old = get_redact_ya_prices_old()
            ya_prices = pd.concat([ya_prices_old, ya_prices], ignore_index=True)
        else:
            None
            
    else:
        ya_prices['date'] = [x.strftime('%d-%B-%Y') for x in list(ya_prices['date'])]
            
    ya_prices['account'] = account.lower()
    
    wwm.check_dates(ya_prices)
    
    return ya_prices


# Получить датафрейм с остатками (фбо и фбс) по яндексу из бд (до 2024-01-27)
def get_redact_ya_stocks_old():
    req = """
    WITH
    ya_stocks_old as (
        WITH 
        yafbssold as (
        SELECT 
        Артикул::varchar,
        date::date,
        Остатки_фбс::bigint
        FROM ir_db.ya_fbs_stocks_old
        WHERE date IS NOT NULL AND Артикул IS NOT NULL AND date::date < '2024-01-27'
        ORDER BY date DESC, Артикул
        ),
        yafbosold as (
        SELECT 
        Артикул::varchar,
        date::date,
        Остатки_фбо::bigint
        FROM ir_db.ya_fbo_stocks_old
        WHERE date IS NOT NULL AND Артикул IS NOT NULL AND date::date < '2024-01-27'
        )
        SELECT 
        COALESCE(yafbssold.Артикул::varchar, yafbosold.Артикул::varchar)::varchar as item_code,
        COALESCE(yafbssold.date::date, yafbosold.date::date)::date as date,
        COALESCE(SUM(Остатки_фбо::bigint), (0))::bigint as fbo_stocks,
        COALESCE(SUM(Остатки_фбс::bigint), (0))::bigint as fbs_stocks
        from yafbssold
        full join yafbosold
        on yafbssold.Артикул = yafbosold.Артикул and yafbssold.date = yafbosold.date
        GROUP by COALESCE(yafbssold.Артикул, yafbosold.Артикул), COALESCE(yafbssold.date, yafbosold.date)
    )
    SELECT 
    item_code,
    date,
    fbo_stocks,
    fbs_stocks
    FROM ya_stocks_old
    ORDER BY date, item_code;
    """
    ya_stocks_old = mwm.select_table_from_request(req=req,
                                                  table_name=None,
                                                  schema=mwm.ir_schema)
    ya_stocks_old.columns = ['item_code', 'date', 'fbo_stocks', 'fbs_stocks']
    ya_stocks_old['item_code'] = ya_stocks_old['item_code'].astype(str)
    
    ya_stocks_old = ya_stocks_old.sort_values(by=['date', 'item_code']).reset_index(drop=True)
    ya_stocks_old['date'] = [pd.to_datetime(x).strftime("%d-%B-%Y") for x in list(ya_stocks_old['date'])]
    
    for dt in list(ya_stocks_old['date'].unique()):
        mwm.unique_items(ya_stocks_old[ya_stocks_old['date']==dt].rename(columns={'item_code': 'Артикул'}))
        
    ya_fbo_stocks_old = ya_stocks_old[['item_code', 'date', 'fbo_stocks']].copy()
    ya_fbs_stocks_old = ya_stocks_old[['item_code', 'date', 'fbs_stocks']].copy()
    ya_stocks_old = None
    
    ya_fbo_stocks_old = wwm.expand_df(df=ya_fbo_stocks_old, 
                                      key_column='item_code',
                                      result_column='fbo_stocks',
                                      date_column='date')
    ya_fbo_stocks_old = wwm.recovery_stocks_days(ya_fbo_stocks_old)
    ya_fbo_stocks_old = wwm.collapse_df(df=ya_fbo_stocks_old, 
                                        key_column='item_code',
                                        result_column='fbo_stocks',
                                        date_column='date')
    
    ya_fbs_stocks_old = wwm.expand_df(df=ya_fbs_stocks_old, 
                                      key_column='item_code',
                                      result_column='fbs_stocks',
                                      date_column='date')
    ya_fbs_stocks_old = wwm.recovery_stocks_days(ya_fbs_stocks_old)
    ya_fbs_stocks_old = wwm.collapse_df(df=ya_fbs_stocks_old, 
                                        key_column='item_code',
                                        result_column='fbs_stocks',
                                        date_column='date')
    
    ya_stocks_old = ya_fbo_stocks_old.merge(ya_fbs_stocks_old, on=['item_code', 'date'], how='outer')
    
    wwm.check_dates(ya_stocks_old)
    
    ya_stocks_old['account'] = const.account_ir.lower()
    
    ya_stocks_old['date'] = [pd.to_datetime(x) for x in list(ya_stocks_old['date'])]
    
    ya_stocks_old = ya_stocks_old.sort_values(by=['date', 'item_code']).reset_index(drop=True)
    
    ya_stocks_old['date'] = [x.strftime("%d-%B-%Y") for x in list(ya_stocks_old['date'])]

    return ya_stocks_old


# Получение остатков яндекс c '2024-01-27'
def get_redact_ya_stocks(account=const.account_ir,
                         schema=mwm.ir_schema,
                         date_filter=None):
    
    if account == const.account_ir:
        table_name = 'yandex_stocks2_copy'
    else:
        print('Нет такого кабинета на яндексе')
        
    req = f"""
    with
    ya_stocks as (
        with
        ya_fbs_stocks as (
        select 
        offerid::varchar as item_code,
        date::date,
        sum(coalesce(stock_count_available::bigint, 0)::bigint) as fbs_stocks
        from {schema}.{table_name}
        where warehouse_name::varchar in ('SSY-Group', 'ПСР', 'Склад-Иркутск')
        and warehouse_name::varchar = 'ПСР'
        and offerid::varchar is not null 
        and date::date is not null 
        group by date::date, offerid::varchar
        order by date::date desc, offerid::varchar
        ),
        ya_fbo_stocks as (
        select 
        offerid::varchar as item_code,
        date::date,
        sum(coalesce(stock_count_available::bigint, 0)::bigint) as fbo_stocks
        from {schema}.{table_name}
        where warehouse_name::varchar not in ('SSY-Group', 'ПСР', 'Склад-Иркутск')
        and offerid::varchar is not null 
        and date::date is not null 
        group by date::date, offerid::varchar
        order by date::date desc, offerid::varchar
        )
        select 
        coalesce(ya_fbo_stocks.item_code, ya_fbs_stocks.item_code)::varchar as  item_code,
        coalesce(ya_fbo_stocks.date, ya_fbs_stocks.date)::date as  date,
        fbo_stocks::bigint,
        fbs_stocks::bigint,
        '{account.lower()}' as account
        from ya_fbo_stocks
        full join ya_fbs_stocks
        on ya_fbo_stocks.item_code = ya_fbs_stocks.item_code
        and ya_fbo_stocks.date = ya_fbs_stocks.date
    )
    select 
    item_code,
    date,
    fbo_stocks,
    fbs_stocks,
    account
    from ya_stocks
    order by account, date desc, item_code;
    """
        
    if date_filter != None:
        if type(date_filter) == datetime.date:
            date_filter = date_filter.strftime('%Y-%m-%d')
        else:
            date_filter = pd.to_datetime(date_filter).strftime('%Y-%m-%d')
            
        req = req.split('warehouse_name::varchar')[0] + f"date::DATE = '{date_filter}'\n"\
              + 'and warehouse_name::varchar' + req.split('warehouse_name::varchar')[1]\
              + 'warehouse_name::varchar' + req.split('warehouse_name::varchar')[2]\
              + f"date::DATE = '{date_filter}'\n"\
              + 'and warehouse_name::varchar' + req.split('warehouse_name::varchar')[3]
    else:
        None
        
    df = mwm.select_table_from_request(req)\
         .rename(columns={0: 'item_code', 1: 'date', 2: 'fbo_stocks', 3: 'fbs_stocks', 4: 'account'})
         
    df['item_code'] = df['item_code'].astype(str)
    
    if date_filter == None and account == const.account_ir:
        df = df.sort_values(by=['date', 'item_code']).reset_index(drop=True)
        df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
        ya_stocks_old = get_redact_ya_stocks_old()
        df = pd.concat([ya_stocks_old, df], ignore_index=True)
    else:
        df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
        
    df['fbo_stocks'] = df['fbo_stocks'].fillna(0).astype(int)
    df['fbs_stocks'] = df['fbs_stocks'].fillna(0).astype(int)
    
    for dt in df['date'].unique():
        wwm.unique_items(df[df['date']==dt].rename(columns={'item_code': 'Артикул'}))
        
    wwm.check_dates(df)
        
    return df


