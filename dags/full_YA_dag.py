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

bd_param = const.bd_param
engine_param = const.engine_param
maslow_json = path_downloads + const.maslow_json
gc = gspread.service_account(filename=maslow_json)
args = const.args
os.environ['NO_PROXY'] = 'URL'

def ya_orders_metrics(path_ya_orders_metrics):
    df = pd.read_excel(wwm.path_full_YA_df,
                       sheet_name='ya_orders_metrics',
                       dtype={'Артикул': 'str'})
    df = wwm.data_columns_sorted(df[['Артикул']+wwm.dt_col(df)])[['Артикул']+list(df.columns[-10:])]
    df = wwm.clear_article(df, 'Артикул')
    if len(wwm.date_res_table(df)) != len(df.columns[1:]):
        exit()
    wwm.unload_metrics(path_metrics=path_ya_orders_metrics, 
                       data1=df, 
                       column='Артикул')
    T_ya_orders_metrics = wwm.get_T_metrics(path_metrics=path_ya_orders_metrics)
    sh = gc.open("full_YA")
    list_name = 'ya_orders_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_ya_orders_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)
    
def ya_fbs_stocks_metrics(path_ya_fbs_stocks_metrics):
    df = pd.read_excel(wwm.path_full_YA_df,
                       sheet_name='stocks_fbs_ya_metrics',
                       dtype={'Артикул': 'str'})
    df = wwm.data_columns_sorted(df[['Артикул']+wwm.dt_col(df)])[['Артикул']+list(df.columns[-10:])]
    df = wwm.clear_article(df, 'Артикул')
    df = df.fillna(0)
    df = df.groupby('Артикул').sum().reset_index(drop=False)
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
    df = wwm.recovery_stocks_days(df)
    wwm.unload_metrics(path_metrics=path_ya_fbs_stocks_metrics, 
                       data1=df, 
                       column='Артикул')
    if len(wwm.date_res_table(df)) != len(df.columns[1:]):
        df = wwm.recovery_stocks_days(df)
        wwm.unload_metrics(path_metrics=path_ya_fbs_stocks_metrics, 
                           data1=df, 
                           column='Артикул')
    else:
        None
    T_ya_fbs_stocks_metrics = wwm.get_T_metrics(path_metrics=path_ya_fbs_stocks_metrics)
    sh = gc.open("full_YA")
    list_name = 'ya_fbs_stocks_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_ya_fbs_stocks_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)
    
def ya_fbo_stocks_metrics(path_ya_fbo_stocks_metrics):    
    df = pd.read_excel(wwm.path_full_YA_df,
                       sheet_name='stocks_ya_metrics',
                       dtype={'Артикул': 'str'})
    df = wwm.data_columns_sorted(df[['Артикул']+wwm.dt_col(df)])[['Артикул']+list(df.columns[-10:])]
    df = wwm.clear_article(df, 'Артикул')
    df = df.fillna(0)
    df = df.groupby('Артикул').sum().reset_index(drop=False)
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
    df = wwm.recovery_stocks_days(df)
    if len(wwm.date_res_table(df)) != len(df.columns[1:]):
        exit()
    wwm.unload_metrics(path_metrics=path_ya_fbo_stocks_metrics, 
                       data1=df, 
                       column='Артикул')
    T_ya_fbo_stocks_metrics = wwm.get_T_metrics(path_metrics=path_ya_fbo_stocks_metrics)
    sh = gc.open("full_YA")
    list_name = 'ya_fbo_stocks_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_ya_fbo_stocks_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)
    
def ya_price_metrics(path_ya_price_metrics):
    df = pd.read_excel(wwm.path_full_YA_df,
                       sheet_name='ya_price_metrics',
                       dtype={'Артикул': 'str'})
    df = wwm.data_columns_sorted(df[['Артикул']+wwm.dt_col(df)])[['Артикул']+list(df.columns[-10:])].fillna(0)
    df = wwm.clear_article(df, 'Артикул').groupby('Артикул').mean().reset_index(drop=False)
    df.iloc[:, 1:] = df.iloc[:, 1:].astype(int)
    df = wwm.recovery_price_days(df) 
    if len(wwm.date_res_table(df)) != len(df.columns[1:]):
        exit()
    wwm.unload_metrics(path_metrics=path_ya_price_metrics, 
                       data1=df, 
                       column='Артикул')
    T_ya_price_metrics = wwm.get_T_metrics(path_metrics=path_ya_price_metrics)
    sh = gc.open("full_YA")
    list_name = 'ya_price_metrics'
    i = [x for x in list(range(len(sh.worksheets()))) if list_name in str(sh.worksheets()[x])][0]
    ws = sh.get_worksheet(i)
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,
                          dataframe=T_ya_price_metrics, 
                          include_index=True,
                          include_column_header=True,
                          resize=True)
    
with DAG(
    dag_id='full_YA_dag',
    default_args=args,
    schedule_interval=const.sched_int['full_YA_dag'],
    start_date=datetime.datetime(2023, 11, 13),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

    ya_orders_metrics_task = PythonOperator(task_id = 'ya_orders_metrics',
                                            python_callable=ya_orders_metrics, 
                                            op_kwargs={'path_ya_orders_metrics': wwm.path_ya_orders_metrics},
                                            dag=dag)
    
    ya_fbs_stocks_metrics_task = PythonOperator(task_id = 'ya_fbs_stocks_metrics',
                                          python_callable=ya_fbs_stocks_metrics, 
                                          op_kwargs={'path_ya_fbs_stocks_metrics': wwm.path_ya_fbs_stocks_metrics},
                                          dag=dag)
    
    ya_fbo_stocks_metrics_task = PythonOperator(task_id = 'ya_fbo_stocks_metrics',
                                          python_callable=ya_fbo_stocks_metrics, 
                                          op_kwargs={'path_ya_fbo_stocks_metrics': wwm.path_ya_fbo_stocks_metrics},
                                          dag=dag)
    
    ya_price_metrics_task = PythonOperator(task_id = 'ya_price_metrics',
                                           python_callable=ya_price_metrics, 
                                           op_kwargs={'path_ya_price_metrics': wwm.path_ya_price_metrics},
                                           dag=dag)
    
    ya_orders_metrics_task >> \
    ya_fbs_stocks_metrics_task >> \
    ya_fbo_stocks_metrics_task >> \
    ya_price_metrics_task