import pymysql
import datetime
import pandas as pd

bd_param = {'host': 'ipolyakov.beget.tech',
            'port': 3306,
            'user': 'ipolyakov_db',
            'password': 'm761aUj$21b9',
            'database': 'ipolyakov_db',
            'cursorclass': pymysql.cursors.DictCursor}

engine_param =  'mysql+pymysql://' \
                + bd_param['user'] + ':' \
                + bd_param['password'] + '@' \
                + bd_param['host'] + ':' \
                + str(bd_param['port']) + '/' \
                + bd_param['database']

args = {
    "owner": "Dmitry Maslow",
    'email_on_failure': False,
    'email_on_retry': False,
}

account_ir = 'IR'
account_kz = 'KZ'
account_ssy = 'SSY'

maslow_json = 'my-test-project-396610-962484cc39dd.json'

# Параметры для подключения к интерфейсу API (WB)
WB_API_KEY = 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzc4ODM0MiwiaWQiOiI0Y2QwZTYxYi0zYmUyLTQ2ZTYtYTJhMy05Zjg5ZTk4OTE4MzkiLCJpaWQiOjE5NzA1MDg3LCJvaWQiOjMzNjk0LCJzIjo1MTAsInNpZCI6IjAwYjQ5NTAzLTgxODItNWY1Yy1hYzRlLWY2MDAyNTBhYTdmZCIsInVpZCI6MTk3MDUwODd9.GmfqqacCvqHMkLviIxi15bLVUqMlJ9_24CKSNzK_uS2wI6OUhe_HnuV1YtmjKjqsb8T7J1xW8es67adYdHci7g'
# WB_API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjE0YzdjNTFmLTRjM2EtNDZlZi1hYTVmLWU4ZTAzNTM4OTVjZiJ9.64BLEDc9Q51f9JNMCxKjV-10SpvVjWwmcatpqWSK080'
# WB_API_KEY2 = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3NJRCI6IjE0OWNhYmI4LWEyNWEtNGNjOC1iMGQzLTU2YzAyYWE1YjdmOSJ9.4QQv16bmUuDU0XvGIH0iZEjpjTRvZc70CdwQ-ND5hS4'
WB_API_KEY2 = WB_API_KEY
WB_KZ_API_KEY = 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMjI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcyMTg2ODIzMSwiaWQiOiIwYmIyYWYxNC1hZWE5LTQxMTItYWI2Ni0wODQzYjBmYWExZDIiLCJpaWQiOjEwNTQzOTY5NCwib2lkIjoxNDEwNTM5LCJzIjo1MTAsInNpZCI6IjQ3OGZhM2ZlLTk5NDktNDI5NC05MmQ0LTE2ZWVjMzQzNjNkNiIsInQiOmZhbHNlLCJ1aWQiOjEwNTQzOTY5NH0.n8dmSsUUtqujxrPStTL1HQpjGDVV-nIu9pFwiSnFOHedwxYpemhqI28Uz8GnDR-lTLsXAGsh022ygup0VZGaqw'
BASE_URL_WB = 'https://suppliers-api.wildberries.ru'
BASE_URL_WB_2 = 'https://statistics-api.wildberries.ru'

TODAY = pd.Timestamp(datetime.date.today())
OZON_API_KEY = '13b757fa-00fe-4c6d-99c2-c911d658c4a4'
OZON_CLIENT_ID = '3266'
BASE_URL_OZON = 'https://api-seller.ozon.ru'

OZON_KZ_API_KEY = '8b832e98-2b78-4f49-a59a-627671633c1c'
OZON_KZ_CLIENT_ID = '1423951'

OZON_SSY_API_KEY = '589705a6-5f4b-4256-89df-ecdd0d9993b1'
OZON_SSY_CLIENT_ID = '65958'

dt_now = datetime.datetime.now()
end_date = dt_now.strftime("%Y-%m-%d")
start_date = (dt_now - datetime.timedelta(days=9)).strftime("%Y-%m-%d")

dt_yesterday = dt_now.date() - datetime.timedelta(days=1)



end_day = dt_now.date() - datetime.timedelta(days=2)
start_day = (dt_now.date() - datetime.timedelta(days=11)).strftime('%Y-%m-%d')
end_day = end_day.strftime('%Y-%m-%d')




# Api параметры для запроса на wb_fbo_stocks
api_params_wb_fbo_stocks = {
                            'dateFrom': '2018-01-01',
                           }

# Api параметры для работы с вб, выгрузка за вчерашний день
wb_api_params_yesterday = {
                            'dateFrom': (datetime.datetime.now().date() - \
                                        datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                           'flag': 1
                           }


# Api параметры для работы с вб, выгрузка за последние 10 дней 
wb_api_params_2 = {
                            'dateFrom': (datetime.datetime.now().date() - \
                                        datetime.timedelta(days=9)).strftime('%Y-%m-%d'),
                           'flag': 0
                           }

# "url" параметры для запроса на информацию о товарах wb
url_param_wb_info = f'https://suppliers-api.wildberries.ru/content/v2/get/cards/list'


# Api параметры для запроса на информацию о товарах wb
api_param_wb_info = {
                      "settings": {
                      "cursor": {
                        "limit": 1000
                      },
                      "filter": {
                        "withPhoto": -1
                        }
                      }
                    }


# "header" параметры для запроса на wb_fbo_stocks
header_params_wb_fbo_stocks = {
                               'Authorization': WB_API_KEY,
                               'Content-Type': 'application/json'
                              }


# "header" параметры для запроса на wb_kz
header_params_wb_kz = {
                      'Authorization': WB_KZ_API_KEY,
                      'Content-Type': 'application/json'
                      }


# "url" параметры для запроса на wb_fbo_stocks
url_param_wb_fbo_stocks = f'{BASE_URL_WB_2}/api/v1/supplier/stocks'


#"url" параметры для запроса на wb_orders
url_param_wb_orders = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders'



# "header" параметры для запроса на wb_prices
header_params_wb_prices = header_params_wb_fbo_stocks

# "url" параметры для запроса на wb_prices
url_param_wb_prices = f'{BASE_URL_WB}/public/api/v1/info'


# "url" параметры для запроса на oz_prices
url_param_oz_prices = f'{BASE_URL_OZON}/v4/product/info/prices'

# "header" параметры для запроса на oz_prices
header_params_oz_prices = {
                           'Api-Key': OZON_API_KEY,
                           'Client-Id': OZON_CLIENT_ID,
                           'Content-Type': 'application/json'
                          }


# "url" параметры для вб тарифов (монопалета и короб)
url_param_wb_tariffs_pallet = 'https://common-api.wildberries.ru/api/v1/tariffs/pallet'
url_param_wb_tariffs_box = 'https://common-api.wildberries.ru/api/v1/tariffs/box'

# Api параметры для запроса на wb тарифы
wb_tariffs_api_param_taday = {'date': TODAY.strftime('%Y-%m-%d')}


# "url" параметры для запроса на wb_fbs_warehouses
url_param_wb_fbs_warehouses = f'{BASE_URL_WB}/api/v3/warehouses'

# "header" параметры для запроса на wb_fbs_stocks
header_params_wb_fbs_stocks = header_params_wb_fbo_stocks

# "url" параметры для запроса на wb_fbs_stocks
url_param_wb_fbs_stocks = 'https://suppliers-api.wildberries.ru/api/v3/stocks/'

# "url" параметры для запроса на oz_product_id
url_param_oz_product_id = f'{BASE_URL_OZON}/v2/product/list'

# "header" параметры для запроса на oz_prices
header_params_oz_product_info = header_params_oz_prices

# "url" параметры для запроса на oz_product_info
url_param_oz_product_info = f'{BASE_URL_OZON}/v2/product/info/list'

# "url" параметры для запроса на oz_fbs_stocks
url_param_oz_fbs_stocks = f'{BASE_URL_OZON}/v1/product/info/stocks-by-warehouse/fbs'

# "header" параметры для запроса на oz_fbs_stocks
header_params_oz_fbs_stocks = header_params_oz_prices

# "header" параметры для запроса на oz_fbo_stocks
header_params_oz_fbo_stocks = header_params_oz_prices

# "url" параметры для запроса на oz_fbo_stocks
url_param_oz_fbo_stocks = f'{BASE_URL_OZON}/v2/analytics/stock_on_warehouses'

# "url" параметры для запроса на oz_fbs_orders
url_param_oz_fbs_orders = f'{BASE_URL_OZON}/v3/posting/fbs/list'

# "url" параметры для запроса на oz_fbs_orders
url_param_oz_fbo_orders = f'{BASE_URL_OZON}/v2/posting/fbo/list'


# "header" параметры для запроса на oz_kz_prices
header_params_oz_kz_prices = {
                              'Api-Key': OZON_KZ_API_KEY,
                              'Client-Id': OZON_KZ_CLIENT_ID,
                              'Content-Type': 'application/json'
                             }

# "header" параметры для запроса на oz_ssy_prices
header_params_oz_ssy_prices = {
                              'Api-Key': OZON_SSY_API_KEY,
                              'Client-Id': OZON_SSY_CLIENT_ID,
                              'Content-Type': 'application/json'
                             }

# "header" параметры для запроса на oz_kz_fbs_stocks
header_params_oz_kz_fbs_stocks = header_params_oz_kz_prices

# "header" параметры для запроса на oz_kz_fbo_stocks
header_params_oz_kz_fbo_stocks = header_params_oz_kz_prices


# "header" параметры для запроса на oz_ssy_fbs_stocks
header_params_oz_ssy_fbs_stocks = header_params_oz_ssy_prices

# "header" параметры для запроса на oz_ssy_fbo_stocks
header_params_oz_ssy_fbo_stocks = header_params_oz_ssy_prices




schedule_interval_dag_dict = {
 'wb_info_dag': '8:30',
 'wb_kz_prices_dag': '8:32',
 'wb_kz_fbo_stocks_dag': '8:34',
 'wb_kz_fbs_stocks_dag': '8:36',
 'RC_stocks_dag': '8:37',
 'wb_kz_orders_dag': '8:38',
 'oz_kz_fbs_orders_dag': '8:40',
 'oz_kz_prices_dag': '8:42',
 'oz_kz_fbs_stocks_dag': '8:44',
 'oz_ssy_fbo_orders_dag': '8:46',
 'oz_ssy_fbo_stocks_dag': '8:48',
 'oz_ssy_prices_dag': '8:50',
 'wb_prices_dag': '9:10',
 'wb_fbo_stocks_dag': '9:12',
 'wb_fbs_stocks_dag': '9:14',
 'dds_wb_stocks_dag': '9:16',
 'dds_wb_prices_dag': '9:20',
 'wb_list_of_nomenclatures_dag': '9:40',
 'oz_product_info_dag': '9:42',
 'oz_products_dag': '9:44',
 'oz_fbs_orders_dag': '9:46',
 'oz_prices_dag': '9:48',
 'oz_fbo_stocks_dag': '9:50',
 'oz_fbs_stocks_dag': '9:52',
 'wb_tariffs_dag': '9:54',
 'dds_oz_prices_dag': '9:56',
 'dds_oz_stocks_dag': '10:00',
 'dds_ya_prices_dag': '10:04',
 'dds_ya_stocks_dag': '10:08',
 'mp_stats_categories_dag': '10:12',
 'ya_old_dag': '11:02',
 'sb_old_dag': '11:04',
 'le_old_dag': '11:06',
 'full_OZON_dag': '11:20',
 'full_YA_dag': '11:25',
 'full_SB_dag': '11:30',
 'full_LE_dag': '11:35',
 'dds_ya_orders_dag': '11:50',
 'dds_wb_orders_dag': '12:00',
 'dds_oz_orders_dag': '12:05',
 'refresh_materialized_view_data_mart_dag': '12:10',
 'full_WB_dag': '12:50',
}

sched_int = {}
for key in schedule_interval_dag_dict.keys():
    redact_val = schedule_interval_dag_dict[key].split(':')[1] + ' ' + \
                 ['0'+str(int(x) - 6) if len(str(int(x) - 6)) == 1 else str(int(x) - 6) \
                 for x in [schedule_interval_dag_dict[key].split(':')[0]]][0]  + ' * * *'
    sched_int[key] = redact_val

