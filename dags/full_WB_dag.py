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

# def wb_orders_metrics(path_wb_orders_metrics):
#     df = wwm.get_tabl_from_bd('wb_orders2', date_column='date')[['date', 'supplierArticle']]\
#     .rename(columns={'supplierArticle': 'Артикул'})
#     df['Артикул'] = df['Артикул'].astype(str)
#     df['Артикул'] = [x.replace('/', '') for x in list(df['Артикул'])]
#     df = wwm.clear_article(df, 'Артикул')
#     df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
#     df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
#     for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
#         df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['date'].count()\
#         .reset_index(drop=False).rename(columns={'date': row}), on='Артикул', how='left')
#     df_0 = df_0.fillna(0)
#     df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)
#     df, df_0 = df_0, None
#     wwm.unload_metrics(path_metrics=path_wb_orders_metrics, 
#                        data1=df, 
#                        column='Артикул')
#     T_wb_orders_metrics = wwm.get_T_metrics(path_metrics=path_wb_orders_metrics)
#     sh = gc.open("full_WB")
#     list_name = 'wb_orders_metrics'
#     i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
#     ws = sh.get_worksheet(i)
#     ws.clear()
#     gd.set_with_dataframe(worksheet=ws,
#                           dataframe=T_wb_orders_metrics, 
#                           include_index=True,
#                           include_column_header=True,
#                           resize=True)

# def wb_fbo_stocks_metrics(path_wb_fbo_stocks_metrics):
#     df = wwm.get_tabl_from_bd(table_name='wb_stocks_v1', date_column='date')
#     df = wwm.redact_date_column(df, 'date')
#     df = df[['supplierArticle', 'quantityFull', 'date']]
#     df = df.rename(columns={'supplierArticle': 'Артикул'})
#     df['Артикул'] = df['Артикул'].astype(str)
#     df['Артикул'] = [x.replace('/', '') for x in list(df['Артикул'])]
#     df = wwm.clear_article(df, 'Артикул')
#     df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
#     df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
#     for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
#         df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['quantityFull'].sum()\
#         .reset_index(drop=False).rename(columns={'quantityFull': row}), on='Артикул', how='left')
#     df_0 = df_0.fillna(0)
#     df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)
#     df, df_0 = df_0, None
#     wwm.unload_metrics(path_metrics=path_wb_fbo_stocks_metrics, 
#                        data1=df, 
#                        column='Артикул')
#     T_wb_fbo_stocks_metrics = wwm.get_T_metrics(path_metrics=path_wb_fbo_stocks_metrics)
#     sh = gc.open("full_WB")
#     list_name = 'wb_fbo_stocks_metrics'
#     i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
#     ws = sh.get_worksheet(i)
#     ws.clear()
#     gd.set_with_dataframe(worksheet=ws,
#                           dataframe=T_wb_fbo_stocks_metrics, 
#                           include_index=True,
#                           include_column_header=True,
#                           resize=True)

# def wb_fbs_stocks_metrics(path_wb_fbs_stocks_metrics):
#     wb_fbs_stocks_metrics = wwm.get_tabl_from_bd('wb__fbs_stocks_metrics', date_column='date')
#     wb_fbs_stocks_metrics['Баркод'] = wb_fbs_stocks_metrics['Баркод'].astype(int)
#     wb_list_of_nomenclatures = wwm.get_tabl_from_bd('wb__list_of_nomenclatures', date_column='date')
#     wb_list_of_nomenclatures['Баркод'] = wb_list_of_nomenclatures['Баркод'].astype(int)
#     wb_fbs_stocks_metrics, wb_list_of_nomenclatures = wb_fbs_stocks_metrics\
#                                                       .merge(wb_list_of_nomenclatures[['Артикул_продавца', 'Баркод']], 
#                                                              on='Баркод', 
#                                                              how='left')\
#                                                       .rename(columns={'Артикул_продавца': 'Артикул'})\
#                                                       .drop(['Баркод', 'Склад'], axis=1)\
#                                                       , None
#     wb_fbs_stocks_metrics['Артикул'] = wb_fbs_stocks_metrics['Артикул'].astype(str)
#     df, wb_fbs_stocks_metrics = wb_fbs_stocks_metrics, None
#     df['Остатки_FBS'] = df['Остатки_FBS'].astype(int)
#     df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
#     for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
#         df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['Остатки_FBS'].sum()\
#         .reset_index(drop=False).rename(columns={'Остатки_FBS': row}), on='Артикул', how='left')
#     df_0 = df_0.fillna(0)
#     df, df_0 = df_0, None
#     df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
#     wwm.unload_metrics(path_metrics=path_wb_fbs_stocks_metrics, 
#                        data1=df, 
#                        column='Артикул')
#     T_wb_fbs_stocks_metrics = wwm.get_T_metrics(path_metrics=path_wb_fbs_stocks_metrics)
#     sh = gc.open("full_WB")
#     list_name = 'wb_fbs_stocks_metrics'
#     i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
#     ws = sh.get_worksheet(i)
#     ws.clear()
#     gd.set_with_dataframe(worksheet=ws,
#                           dataframe=T_wb_fbs_stocks_metrics, 
#                           include_index=True,
#                           include_column_header=True,
#                           resize=True)
    
# def wb_price_metrics(path_wb_price_metrics):
#     df = wwm.get_tabl_from_bd('wb__list_of_nomenclatures', date_column='date')[['Артикул_WB', 
#                                                                             'Артикул_продавца']]\
#     .rename(columns={'Артикул_продавца': 'Артикул', 'Артикул_WB': 'nmId'})\
#     .merge(wwm.get_tabl_from_bd('wb_prices', date_column='date'), on='nmId', how='inner')[['Артикул', 
#                                                                                        'price', 
#                                                                                        'discount', 
#                                                                                        'date']]
#     df = wwm.redact_date_column(df, 'date')
#     df['Артикул'] = df['Артикул'].astype(str)
#     df['Цена'] = df['price'] * ((100 - df['discount'])/100)
#     df = df[['Артикул', 'Цена', 'date']]
#     df.loc[list(df.index), 'date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
#     df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
#     for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
#         df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['Цена'].mean()\
#         .reset_index(drop=False).rename(columns={'Цена': row}), on='Артикул', how='left')
#     df_0 = df_0.fillna(0)
#     df, df_0 = df_0, None
#     wwm.unload_metrics(path_metrics=path_wb_price_metrics, 
#                        data1=df, 
#                        column='Артикул')
#     T_wb_price_metrics = wwm.get_T_metrics(path_metrics=path_wb_price_metrics)
#     sh = gc.open("full_WB")
#     list_name = 'wb_price_metrics'
#     i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
#     ws = sh.get_worksheet(i)
#     ws.clear()
#     gd.set_with_dataframe(worksheet=ws,
#                           dataframe=T_wb_price_metrics, 
#                           include_index=True,
#                           include_column_header=True,
#                           resize=True)            
    
# with DAG(
#     dag_id='full_WB_dag',
#     default_args=args,
#     schedule_interval='40 05 * * *',
#     start_date=datetime.datetime(2023, 11, 13),
#     catchup=False,
#     dagrun_timeout=datetime.timedelta(minutes=10)
# ) as dag:

#     wb_orders_metrics_task = PythonOperator(task_id = 'wb_orders_metrics',
#                                             python_callable=wb_orders_metrics, 
#                                             op_kwargs={'path_wb_orders_metrics': wwm.path_wb_orders_metrics},
#                                             dag=dag)
    
#     wb_fbo_stocks_metrics_task = PythonOperator(task_id = 'wb_fbo_stocks_metrics',
#                                           python_callable=wb_fbo_stocks_metrics, 
#                                           op_kwargs={'path_wb_fbo_stocks_metrics': wwm.path_wb_fbo_stocks_metrics},
#                                           dag=dag)
    
#     wb_fbs_stocks_metrics_task = PythonOperator(task_id = 'wb_fbs_stocks_metrics',
#                                           python_callable=wb_fbs_stocks_metrics, 
#                                           op_kwargs={'path_wb_fbs_stocks_metrics': wwm.path_wb_fbs_stocks_metrics},
#                                           dag=dag)
    
#     wb_price_metrics_task = PythonOperator(task_id = 'wb_price_metrics',
#                                            python_callable=wb_price_metrics, 
#                                            op_kwargs={'path_wb_price_metrics': wwm.path_wb_price_metrics},
#                                            dag=dag)
    
#     wb_orders_metrics_task >> \
#     wb_fbo_stocks_metrics_task >> \
#     wb_fbs_stocks_metrics_task >> \
#     wb_price_metrics_task


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

def wb_orders_metrics(path_wb_orders_metrics):
    df = mwm.get_tabl_from_bd('wb_orders2', date_column='date', schema=mwm.ip_schema)[['date', 'supplierarticle']]\
    .rename(columns={'supplierarticle': 'Артикул'})
    df['Артикул'] = df['Артикул'].astype(str)
    df['Артикул'] = [x.replace('/', '') for x in list(df['Артикул'])]
    df = wwm.clear_article(df, 'Артикул')
    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
    for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
        df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['date'].count()\
        .reset_index(drop=False).rename(columns={'date': row}), on='Артикул', how='left')
    df_0 = df_0.fillna(0)
    df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)
    df, df_0 = df_0, None
    wwm.unload_metrics(path_metrics=path_wb_orders_metrics, 
                       data1=df, 
                       column='Артикул')
    T_wb_orders_metrics = wwm.get_T_metrics(path_metrics=path_wb_orders_metrics)
    sh = gc.open("full_WB")
    list_name = 'wb_orders_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_wb_orders_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)

def wb_fbo_stocks_metrics(path_wb_fbo_stocks_metrics):
    df = mwm.get_tabl_from_bd(table_name='wb_fbo_stocks', date_column='date')
    df = df[['supplierarticle', 'quantityfull', 'date']]
    df = df.rename(columns={'supplierarticle': 'Артикул'})
    df['Артикул'] = df['Артикул'].astype(str)
    df['Артикул'] = [x.replace('/', '') for x in list(df['Артикул'])]
    df = wwm.clear_article(df, 'Артикул')
    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    df['quantityfull'] = df['quantityfull'].astype(int)
    df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
    for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
        df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['quantityfull'].sum()\
        .reset_index(drop=False).rename(columns={'quantityfull': row}), on='Артикул', how='left')
    df_0 = df_0.fillna(0)
    df_0.iloc[:, 1:] = df_0.iloc[:, 1:].astype(int)
    df, df_0 = df_0, None
    wwm.unload_metrics(path_metrics=path_wb_fbo_stocks_metrics, 
                       data1=df, 
                       column='Артикул')
    T_wb_fbo_stocks_metrics = wwm.get_T_metrics(path_metrics=path_wb_fbo_stocks_metrics)
    sh = gc.open("full_WB")
    list_name = 'wb_fbo_stocks_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_wb_fbo_stocks_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)

def wb_fbs_stocks_metrics(path_wb_fbs_stocks_metrics):
    wb_fbs_stocks_metrics = mwm.get_tabl_from_bd('wb_fbs_stocks', date_column='date')
    wb_fbs_stocks_metrics['Баркод'] = wb_fbs_stocks_metrics['Баркод'].astype(int)
    wb_list_of_nomenclatures = mwm.get_tabl_from_bd('wb_list_of_nomenclatures', flag=1)
    wb_list_of_nomenclatures['Баркод'] = wb_list_of_nomenclatures['Баркод'].astype(int)
    wb_fbs_stocks_metrics, wb_list_of_nomenclatures = wb_fbs_stocks_metrics\
                                                      .merge(wb_list_of_nomenclatures[['Артикул_продавца', 'Баркод']], 
                                                             on='Баркод', 
                                                             how='left')\
                                                      .rename(columns={'Артикул_продавца': 'Артикул'})\
                                                      .drop(['Баркод', 'Склад'], axis=1)\
                                                      , None
    wb_fbs_stocks_metrics['Артикул'] = wb_fbs_stocks_metrics['Артикул'].astype(str)
    df, wb_fbs_stocks_metrics = wb_fbs_stocks_metrics, None
    df['Остатки_fbs'] = df['Остатки_fbs'].astype(int)
    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
    for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
        df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['Остатки_fbs'].sum()\
        .reset_index(drop=False).rename(columns={'Остатки_fbs': row}), on='Артикул', how='left')
    df_0 = df_0.fillna(0)
    df, df_0 = df_0, None
    df = df.fillna(0)
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
    wwm.unload_metrics(path_metrics=path_wb_fbs_stocks_metrics, 
                       data1=df, 
                       column='Артикул')
    T_wb_fbs_stocks_metrics = wwm.get_T_metrics(path_metrics=path_wb_fbs_stocks_metrics)
    sh = gc.open("full_WB")
    list_name = 'wb_fbs_stocks_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_wb_fbs_stocks_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)
    
def wb_price_metrics(path_wb_price_metrics):
    df = mwm.get_tabl_from_bd('wb_list_of_nomenclatures', flag=1)[['Артикул_wb', 
                                                                   'Артикул_продавца']]\
    .rename(columns={'Артикул_продавца': 'Артикул', 'Артикул_wb': 'nmid'})\
    .merge(mwm.get_tabl_from_bd('wb_prices', date_column='date'), on='nmid', how='inner')[['Артикул', 
                                                                                       'price', 
                                                                                       'discount', 
                                                                                       'date']]
    df['Артикул'] = df['Артикул'].astype(str)
    df['Цена'] = df['price'] * ((100 - df['discount'])/100)
    df = df[['Артикул', 'Цена', 'date']]
    df['date'] = [pd.to_datetime(x).strftime('%d-%B-%Y') for x in list(df['date'])]
    df_0 = pd.DataFrame(list(set(df['Артикул'])), columns=['Артикул'])
    for row in [y.strftime('%d-%B-%Y') for y in sorted([pd.to_datetime(x) for x in list(df['date'].unique())])]:
        df_0 = df_0.merge(df[df['date']==row].groupby('Артикул')['Цена'].mean()\
        .reset_index(drop=False).rename(columns={'Цена': row}), on='Артикул', how='left')
    df_0 = df_0.fillna(0)
    df, df_0 = df_0, None
    wwm.unload_metrics(path_metrics=path_wb_price_metrics, 
                       data1=df, 
                       column='Артикул')
    T_wb_price_metrics = wwm.get_T_metrics(path_metrics=path_wb_price_metrics)
    sh = gc.open("full_WB")
    list_name = 'wb_price_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_wb_price_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)            
    
with DAG(
    dag_id='full_WB_dag',
    default_args=args,
    schedule_interval=const.sched_int['full_WB_dag'],
    start_date=datetime.datetime(2023, 11, 13),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

    wb_orders_metrics_task = PythonOperator(task_id = 'wb_orders_metrics',
                                            python_callable=wb_orders_metrics, 
                                            op_kwargs={'path_wb_orders_metrics': wwm.path_wb_orders_metrics},
                                            dag=dag)
    
    wb_fbo_stocks_metrics_task = PythonOperator(task_id = 'wb_fbo_stocks_metrics',
                                          python_callable=wb_fbo_stocks_metrics, 
                                          op_kwargs={'path_wb_fbo_stocks_metrics': wwm.path_wb_fbo_stocks_metrics},
                                          dag=dag)
    
    wb_fbs_stocks_metrics_task = PythonOperator(task_id = 'wb_fbs_stocks_metrics',
                                          python_callable=wb_fbs_stocks_metrics, 
                                          op_kwargs={'path_wb_fbs_stocks_metrics': wwm.path_wb_fbs_stocks_metrics},
                                          dag=dag)
    
    wb_price_metrics_task = PythonOperator(task_id = 'wb_price_metrics',
                                           python_callable=wb_price_metrics, 
                                           op_kwargs={'path_wb_price_metrics': wwm.path_wb_price_metrics},
                                           dag=dag)
    
    wb_orders_metrics_task >> \
    wb_fbo_stocks_metrics_task >> \
    wb_fbs_stocks_metrics_task >> \
    wb_price_metrics_task