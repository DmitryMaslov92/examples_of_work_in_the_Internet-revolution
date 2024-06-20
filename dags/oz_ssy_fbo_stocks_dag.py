import pandas as pd
import glob
import sys
link_to_table = 'https://docs.google.com/spreadsheets/d/1A1TyZPeq9r2HiDi-CHfmTfRpuAQYToib1vqyTx4ZyRE/edit#gid=0'
df = pd.read_excel('/'.join(link_to_table.split('/')[:-1])+'/export?format=xlsx')
PATH, df = [dict(df.loc[x]) for x in list(df.index)], None
i = 0
while len([py for py in glob.glob(f"{PATH[i]['path_downloads']}*.xlsx")]) == 0:
    i +=1
path_downloads = PATH[i]['path_downloads']
path_algoritm = PATH[i]['path_algoritm']
sys.path.append(f'{path_algoritm}Модули')
import maslow_working_module as mwm
import constants_for_marketplace_metrics_dags as const
import dop_wb_oz_work_module as dm 
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import os
os.environ['NO_PROXY'] = 'URL'
from sys import exit 

def work_oz_ssy_fbo_stocks(schema):
    oz_ssy_fbo_stocks_new = pd.read_excel(mwm.path_FD + 'oz_ssy_fbo_stocks_new.xlsx')
    if len(oz_ssy_fbo_stocks_new) == 0:
        print('В oz_ssy_fbo_stocks_new нет данных')
        exit()
    try:
        oz_ssy_fbo_stocks_new['sku'] = oz_ssy_fbo_stocks_new['sku'].astype(int)
        oz_ssy_fbo_stocks_new['warehouse_name'] = oz_ssy_fbo_stocks_new['warehouse_name'].astype(str)
        oz_ssy_fbo_stocks_new['item_code'] = oz_ssy_fbo_stocks_new['item_code'].astype(str)
        oz_ssy_fbo_stocks_new['free_to_sell_amount'] = oz_ssy_fbo_stocks_new['free_to_sell_amount'].astype(int)
    except:
        print('Ошибка при изменении типа данных')
        exit()
    for row in list(oz_ssy_fbo_stocks_new['warehouse_name'].unique()):
        if len(oz_ssy_fbo_stocks_new[oz_ssy_fbo_stocks_new['warehouse_name']==row])\
           - len(list(set(oz_ssy_fbo_stocks_new[oz_ssy_fbo_stocks_new['warehouse_name']==row]['item_code']))) != 0:
            print('В oz_ssy_fbo_stocks_new есть неуникальные item_code')
            exit()
    mwm.update_or_create_table_date(oz_ssy_fbo_stocks_new, 
                                    'oz_ssy_fbo_stocks',
                                    ['item_code', 'warehouse_name', 'date'],
                                    'date',
                                    schema=schema)
    
with DAG(
    dag_id='oz_ssy_fbo_stocks_dag',
    default_args=mwm.args,
    schedule_interval=const.sched_int['oz_ssy_fbo_stocks_dag'],
    start_date=datetime.datetime(2024, 1, 18),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

  
    get_oz_ssy_fbo_stocks_new_task = PythonOperator(
                                                task_id = 'get_oz_ssy_fbo_stocks_new',
                                                python_callable=mwm.get_oz_fbo_stocks_new,
                                                op_kwargs={'account': 'SSY'},
                                                dag=dag
                                                )
    
    work_oz_ssy_fbo_stocks_task = PythonOperator(
                                             task_id = 'work_oz_ssy_fbo_stocks',
                                             python_callable=work_oz_ssy_fbo_stocks,
                                             op_kwargs={'schema':mwm.ir_schema},
                                             dag=dag
                                             )
    

    get_oz_ssy_fbo_stocks_new_task >> work_oz_ssy_fbo_stocks_task