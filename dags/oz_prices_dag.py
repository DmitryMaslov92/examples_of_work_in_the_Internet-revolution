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
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import os
os.environ['NO_PROXY'] = 'URL'
from sys import exit

def work_oz_prices(schema):
    oz_price_new = pd.read_excel(mwm.path_FD + 'oz_price_new.xlsx',
                                 dtype={'offer_id': 'str'}).fillna('')
    if len(oz_price_new) == 0:
        print('В oz_price_new нет данных')
        exit()
    mwm.unique_items(oz_price_new.rename(columns={'offer_id': 'Артикул'}))
    mwm.update_or_create_table_date(oz_price_new, 
                                    'oz_prices', 
                                    ['offer_id', 'date'],
                                    'date',
                                    schema=schema)
    
with DAG(
    dag_id='oz_prices_dag',
    default_args=mwm.args,
    schedule_interval=const.sched_int['oz_prices_dag'],
    start_date=datetime.datetime(2023, 11, 13),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

  
    get_oz_price_new_task = PythonOperator(
                                           task_id = 'get_oz_price_new',
                                           python_callable=mwm.get_oz_price_new,
                                           dag=dag
                                          )
    
    work_oz_prices_task = PythonOperator(
                                         task_id = 'work_oz_prices',
                                         python_callable=work_oz_prices,
                                         op_kwargs={'schema':mwm.ir_schema},
                                         dag=dag
                                        )
    
    get_oz_price_new_task >> work_oz_prices_task