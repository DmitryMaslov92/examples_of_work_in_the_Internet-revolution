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

def work_oz_product_info(schema):
    oz_product_info = pd.read_excel(mwm.path_FD + 'oz_product_info.xlsx', 
                                    dtype={'offer_id': 'str'}).fillna('')
    for col in oz_product_info.columns:
        if str(oz_product_info[col].dtype) == 'object':
            oz_product_info[col] = [x.replace("'", "") if "'" in str(x) else x for x in list(oz_product_info[col])]
    if len(oz_product_info) == 0:
        print('В oz_product_info нет данных')
        exit()
    mwm.unique_items(oz_product_info.rename(columns={'offer_id': 'Артикул'}))
    mwm.update_or_create_table_not_date(oz_product_info, 
                                        'oz_product_info', 
                                        ['offer_id'],
                                        schema=schema)
    
with DAG(
    dag_id='oz_product_info_dag',
    default_args=mwm.args,
    schedule_interval=const.sched_int['oz_product_info_dag'],
    start_date=datetime.datetime(2023, 11, 13),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

  
    get_oz_product_info_task = PythonOperator(
                                              task_id = 'get_oz_product_info',
                                              python_callable=mwm.get_oz_product_info,
                                              dag=dag
                                              )
    
    work_oz_product_info_task = PythonOperator(
                                               task_id = 'work_oz_product_info',
                                               python_callable=work_oz_product_info,
                                               op_kwargs={'schema':mwm.ir_schema},
                                               dag=dag
                                               )
    

    get_oz_product_info_task >> work_oz_product_info_task