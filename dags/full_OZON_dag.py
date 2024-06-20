# import re
# from typing import Any, Dict, List
# import pymysql
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# import os 
# import datetime
# import requests
# import time
# from sqlalchemy import create_engine
# import numpy as np
# import openpyxl
# import gspread
# import gspread_dataframe as gd
# from tqdm import tqdm
# from sys import exit
# import pandas as pd
# import sys
# import glob
# df = pd.read_excel(f'https://docs.google.com'+
# '/spreadsheets/d/e/2PACX-1vQyCtSf7tuz9520nSPiif43YpfQlaMsIbIrYwcsqzR2-TrKfx2bu-T5BUl595qeTrSWaMmLnNIzTKQF'+
# '/pub?output=xlsx')
# PATH, df = [dict(df.loc[x]) for x in list(df.index)], None
# i = 0
# while len([py for py in glob.glob(f"{PATH[i]['path_downloads']}*.xlsx")]) == 0:
#     i +=1
# path_downloads = PATH[i]['path_downloads']
# path_algoritm = PATH[i]['path_algoritm']
# sys.path.append(f'{path_algoritm}Модули')
# import constants_for_marketplace_metrics_dags as const
# import working_with_marketplace_metrics_dags as wwm

# bd_param = const.bd_param
# engine_param = const.engine_param
# maslow_json = path_downloads + const.maslow_json
# gc = gspread.service_account(filename=maslow_json)
# args = const.args
# os.environ['NO_PROXY'] = 'URL'

# def ozon_orders_metrics(path_ozon_orders_metrics):
#     df = wwm.get_tabl_from_bd('ozon_orders', date_column='created_at')
#     df = df[['products_offer_id', 'created_at', 'products_quantity']]
#     df = df.rename(columns={'products_offer_id': 'Артикул', 'created_at': 'date'})
#     df['Артикул'] = df['Артикул'].astype(str)
#     df = wwm.clear_article(df, 'Артикул')
#     df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
#     df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
#     for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
#         df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['products_quantity'].sum()\
#         .reset_index(drop=False).rename(columns={'products_quantity': row}), on='Артикул', how='left')
#     df_0 = df_0.fillna(0)
#     df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)
#     df, df_0 = df_0, None
#     wwm.unload_metrics(path_metrics=path_ozon_orders_metrics, 
#                        data1=df, 
#                        column='Артикул')
#     T_ozon_orders_metrics = wwm.get_T_metrics(path_metrics=path_ozon_orders_metrics)
#     sh = gc.open("full_OZON")
#     list_name = 'ozon_orders_metrics'
#     i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
#     ws = sh.get_worksheet(i)
#     ws.clear()
#     gd.set_with_dataframe(worksheet=ws,
#                           dataframe=T_ozon_orders_metrics, 
#                           include_index=True,
#                           include_column_header=True,
#                           resize=True)

# def ozon_fbo_stocks_metrics(path_ozon_fbo_stocks_metrics):
#     df = wwm.get_tabl_from_bd(table_name='ozon_stocks2', date_column='date')
#     df = wwm.redact_date_column(df, 'date')
#     df = df[['item_code', 'free_to_sell_amount', 'date']]
#     df = df.rename(columns={'item_code': 'Артикул'})
#     df['Артикул'] = df['Артикул'].astype(str)
#     df = wwm.clear_article(df, 'Артикул')
#     df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
#     df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
#     for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
#         df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['free_to_sell_amount'].sum()\
#         .reset_index(drop=False).rename(columns={'free_to_sell_amount': row}), on='Артикул', how='left')
#     df_0 = df_0.fillna(0)
#     df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)
#     df, df_0 = df_0, None
#     wwm.unload_metrics(path_metrics=path_ozon_fbo_stocks_metrics, 
#                        data1=df, 
#                        column='Артикул')
#     T_ozon_fbo_stocks_metrics = wwm.get_T_metrics(path_metrics=path_ozon_fbo_stocks_metrics)
#     sh = gc.open("full_OZON")
#     list_name = 'ozon_fbo_stocks_metrics'
#     i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
#     ws = sh.get_worksheet(i)
#     ws.clear()
#     gd.set_with_dataframe(worksheet=ws,
#                           dataframe=T_ozon_fbo_stocks_metrics, 
#                           include_index=True,
#                           include_column_header=True,
#                           resize=True)
    
# def ozon_fbs_stocks_metrics(path_ozon_fbs_stocks_metrics):
#     df = wwm.get_tabl_from_bd('oz__fbs_stocks_metrics', date_column='date')
#     df['product_id'] = df['product_id'].astype(int)
#     df = wwm.get_tabl_from_bd('oz__product_info', date_column='date')[['id', 'offer_id']]\
#          .rename(columns={'id': 'product_id'})\
#          .merge(df, 
#                 on='product_id',
#                 how='right')[['offer_id', 'present', 'date']]\
#          .rename(columns={'offer_id': 'Артикул'})
#     df['present'] = df['present'].astype(int)
#     df['Артикул'] = df['Артикул'].astype(str)
#     df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
#     for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
#         df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['present'].sum()\
#         .reset_index(drop=False).rename(columns={'present': row}), on='Артикул', how='left')
#     df_0 = df_0.fillna(0)
#     df, df_0 = df_0, None
#     df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
#     wwm.unload_metrics(path_metrics=path_ozon_fbs_stocks_metrics, 
#                        data1=df, 
#                        column='Артикул')
#     T_ozon_fbs_stocks_metrics = wwm.get_T_metrics(path_metrics=path_ozon_fbs_stocks_metrics)
#     sh = gc.open("full_OZON")
#     list_name = 'ozon_fbs_stocks_metrics'
#     i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
#     ws = sh.get_worksheet(i)
#     ws.clear()
#     gd.set_with_dataframe(worksheet=ws,
#                           dataframe=T_ozon_fbs_stocks_metrics, 
#                           include_index=True,
#                           include_column_header=True,
#                           resize=True)
    
# def ozon_price_metrics(path_ozon_price_metrics):
#     df = wwm.get_tabl_from_bd('ozon_prices', date_column='date')
#     df = wwm.redact_date_column(df, 'date')
#     df = df[['offer_id', 'price_price', 'date']]
#     df = df.rename(columns={'offer_id': 'Артикул'})
#     df['Артикул'] = df['Артикул'].astype(str)
#     df = wwm.clear_article(df, 'Артикул')
#     df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
#     df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
#     for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
#         df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['price_price'].mean()\
#         .reset_index(drop=False).rename(columns={'price_price': row}), on='Артикул', how='left')
#     df_0 = df_0.fillna(0)
#     df, df_0 = df_0, None
#     wwm.unload_metrics(path_metrics=path_ozon_price_metrics, 
#                        data1=df, 
#                        column='Артикул')
#     T_ozon_price_metrics = wwm.get_T_metrics(path_metrics=path_ozon_price_metrics)
#     sh = gc.open("full_OZON")
#     list_name = 'ozon_price_metrics'
#     i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
#     ws = sh.get_worksheet(i)
#     ws.clear()
#     gd.set_with_dataframe(worksheet=ws,
#                           dataframe=T_ozon_price_metrics, 
#                           include_index=True,
#                           include_column_header=True,
#                           resize=True)
    
# with DAG(
#     dag_id='full_OZON_dag',
#     default_args=args,
#     schedule_interval='45 05 * * *',
#     start_date=datetime.datetime(2023, 11, 13),
#     catchup=False,
#     dagrun_timeout=datetime.timedelta(minutes=10)
# ) as dag:

#     ozon_orders_metrics_task = PythonOperator(task_id = 'ozon_orders_metrics',
#                                             python_callable=ozon_orders_metrics, 
#                                             op_kwargs={'path_ozon_orders_metrics': wwm.path_ozon_orders_metrics},
#                                             dag=dag)
    
#     ozon_fbo_stocks_metrics_task = PythonOperator(task_id = 'ozon_fbo_stocks_metrics',
#                                           python_callable=ozon_fbo_stocks_metrics, 
#                                           op_kwargs={'path_ozon_fbo_stocks_metrics': wwm.path_ozon_fbo_stocks_metrics},
#                                           dag=dag)
    
#     ozon_fbs_stocks_metrics_task = PythonOperator(task_id = 'ozon_fbs_stocks_metrics',
#                                           python_callable=ozon_fbs_stocks_metrics, 
#                                           op_kwargs={'path_ozon_fbs_stocks_metrics': wwm.path_ozon_fbs_stocks_metrics},
#                                           dag=dag)
    
#     ozon_price_metrics_task = PythonOperator(task_id = 'ozon_price_metrics',
#                                            python_callable=ozon_price_metrics, 
#                                            op_kwargs={'path_ozon_price_metrics': wwm.path_ozon_price_metrics},
#                                            dag=dag)
    
#     ozon_orders_metrics_task >> \
#     ozon_fbo_stocks_metrics_task >> \
#     ozon_fbs_stocks_metrics_task >> \
#     ozon_price_metrics_task


import re
from typing import Any, Dict, List
import pymysql
from airflow import DAG
from airflow.operators.python import PythonOperator
import os 
import datetime
import requests
import time
from sqlalchemy import create_engine
import numpy as np
import openpyxl
import gspread
import gspread_dataframe as gd
from tqdm import tqdm
from sys import exit
import pandas as pd
import sys
import glob
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
import working_with_marketplace_metrics_dags as wwm
import maslow_working_module as mwm

bd_param = const.bd_param
engine_param = const.engine_param
maslow_json = path_downloads + const.maslow_json
gc = gspread.service_account(filename=maslow_json)
args = const.args
os.environ['NO_PROXY'] = 'URL'

def ozon_orders_metrics(path_ozon_orders_metrics):
    df = mwm.get_tabl_from_bd('ozon_orders', date_column='created_at', schema=mwm.ip_schema)
    df = df[['products_offer_id', 'created_at', 'products_quantity']]
    df = df.rename(columns={'products_offer_id': 'Артикул', 'created_at': 'date'})
    df['Артикул'] = df['Артикул'].astype(str)
    df = mwm.clear_article(df, 'Артикул')
    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
    for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
        df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['products_quantity'].sum()\
        .reset_index(drop=False).rename(columns={'products_quantity': row}), on='Артикул', how='left')
    df_0 = df_0.fillna(0)
    df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)
    df, df_0 = df_0, None
    df = df.iloc[:, :-1]
    wwm.unload_metrics(path_metrics=path_ozon_orders_metrics, 
                       data1=df, 
                       column='Артикул')
    T_ozon_orders_metrics = wwm.get_T_metrics(path_metrics=path_ozon_orders_metrics)
    sh = gc.open("full_OZON")
    list_name = 'ozon_orders_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_ozon_orders_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)

def ozon_fbo_stocks_metrics(path_ozon_fbo_stocks_metrics):
    df = mwm.get_tabl_from_bd(table_name='oz_fbo_stocks', date_column='date')
    df = df[['item_code', 'free_to_sell_amount', 'date']]
    df = df.rename(columns={'item_code': 'Артикул'})
    df['Артикул'] = df['Артикул'].astype(str)
    df = mwm.clear_article(df, 'Артикул')
    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
    for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
        df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['free_to_sell_amount'].sum()\
        .reset_index(drop=False).rename(columns={'free_to_sell_amount': row}), on='Артикул', how='left')
    df_0 = df_0.fillna(0)
    df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)
    df, df_0 = df_0, None
    wwm.unload_metrics(path_metrics=path_ozon_fbo_stocks_metrics, 
                       data1=df, 
                       column='Артикул')
    T_ozon_fbo_stocks_metrics = wwm.get_T_metrics(path_metrics=path_ozon_fbo_stocks_metrics)
    sh = gc.open("full_OZON")
    list_name = 'ozon_fbo_stocks_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_ozon_fbo_stocks_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)
    
def ozon_fbs_stocks_metrics(path_ozon_fbs_stocks_metrics):
    df = mwm.get_tabl_from_bd('oz_fbs_stocks', date_column='date')
    df['product_id'] = df['product_id'].astype(int)
    df = mwm.get_tabl_from_bd('oz_product_info', flag=1)[['id', 'offer_id']]\
         .rename(columns={'id': 'product_id'})\
         .merge(df, 
                on='product_id',
                how='right')[['offer_id', 'present', 'date']]\
         .rename(columns={'offer_id': 'Артикул'})
    df['present'] = df['present'].astype(int)
    df['Артикул'] = df['Артикул'].astype(str)
    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
    for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
        df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['present'].sum()\
        .reset_index(drop=False).rename(columns={'present': row}), on='Артикул', how='left')
    df_0 = df_0.fillna(0)
    df, df_0 = df_0, None
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
    wwm.unload_metrics(path_metrics=path_ozon_fbs_stocks_metrics, 
                       data1=df, 
                       column='Артикул')
    T_ozon_fbs_stocks_metrics = wwm.get_T_metrics(path_metrics=path_ozon_fbs_stocks_metrics)
    sh = gc.open("full_OZON")
    list_name = 'ozon_fbs_stocks_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_ozon_fbs_stocks_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)
    
def ozon_price_metrics(path_ozon_price_metrics):
    df = mwm.get_tabl_from_bd('oz_prices', date_column='date')
    df = df[['offer_id', 'price', 'date']]
    df = df.rename(columns={'offer_id': 'Артикул'})
    df['Артикул'] = df['Артикул'].astype(str)
    df = wwm.clear_article(df, 'Артикул')
    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
    for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
        df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['price'].mean()\
        .reset_index(drop=False).rename(columns={'price': row}), on='Артикул', how='left')
    df_0 = df_0.fillna(0)
    df, df_0 = df_0, None
    wwm.unload_metrics(path_metrics=path_ozon_price_metrics, 
                       data1=df, 
                       column='Артикул')
    T_ozon_price_metrics = wwm.get_T_metrics(path_metrics=path_ozon_price_metrics)
    sh = gc.open("full_OZON")
    list_name = 'ozon_price_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_ozon_price_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)
    
with DAG(
    dag_id='full_OZON_dag',
    default_args=args,
    schedule_interval=const.sched_int['full_OZON_dag'],
    start_date=datetime.datetime(2023, 11, 13),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

    ozon_orders_metrics_task = PythonOperator(task_id = 'ozon_orders_metrics',
                                            python_callable=ozon_orders_metrics, 
                                            op_kwargs={'path_ozon_orders_metrics': wwm.path_ozon_orders_metrics},
                                            dag=dag)
    
    ozon_fbo_stocks_metrics_task = PythonOperator(task_id = 'ozon_fbo_stocks_metrics',
                                          python_callable=ozon_fbo_stocks_metrics, 
                                          op_kwargs={'path_ozon_fbo_stocks_metrics': wwm.path_ozon_fbo_stocks_metrics},
                                          dag=dag)
    
    ozon_fbs_stocks_metrics_task = PythonOperator(task_id = 'ozon_fbs_stocks_metrics',
                                          python_callable=ozon_fbs_stocks_metrics, 
                                          op_kwargs={'path_ozon_fbs_stocks_metrics': wwm.path_ozon_fbs_stocks_metrics},
                                          dag=dag)
    
    ozon_price_metrics_task = PythonOperator(task_id = 'ozon_price_metrics',
                                           python_callable=ozon_price_metrics, 
                                           op_kwargs={'path_ozon_price_metrics': wwm.path_ozon_price_metrics},
                                           dag=dag)
    
    ozon_orders_metrics_task >> \
    ozon_fbo_stocks_metrics_task >> \
    ozon_fbs_stocks_metrics_task >> \
    ozon_price_metrics_task