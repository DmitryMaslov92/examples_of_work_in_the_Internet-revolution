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

def work_oz_kz_fbs_stocks(schema):
    oz_kz_fbs_stocks_new = dm.get_oz_fbs_stocks_new(account='KZ').fillna(0)
    mwm.update_or_create_table_date(oz_kz_fbs_stocks_new, 
                                    'oz_kz_fbs_stocks', 
                                    ['product_id', 'warehouse_name', 'date'],
                                    'date',
                                    schema=schema)
    
with DAG(
    dag_id='oz_kz_fbs_stocks_dag',
    default_args=mwm.args,
    schedule_interval=const.sched_int['oz_kz_fbs_stocks_dag'],
    start_date=datetime.datetime(2024, 1, 16),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

    
    work_oz_kz_fbs_stocks_task = PythonOperator(
                                             task_id = 'work_oz_kz_fbs_stocks',
                                             python_callable=work_oz_kz_fbs_stocks,
                                             op_kwargs={'schema':mwm.ir_schema},
                                             dag=dag
                                             )
    

    work_oz_kz_fbs_stocks_task