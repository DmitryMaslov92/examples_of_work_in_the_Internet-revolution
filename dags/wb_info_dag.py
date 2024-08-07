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

def work_wb_ir_info(schema):
    try:
        df = dm.get_wb_info()
    except:
        print('Ошибка в получении wb_info')
        exit()
    mwm.update_or_create_table_not_date(df, 
                                        'wb_info', 
                                        ['skus'], 
                                        schema=schema)

def work_wb_kz_info(account, 
                    schema):
    try:
        df = dm.get_wb_info(account=account)
    except:
        print(f'Ошибка в получении wb_{account.lower()}_info')
        exit()
    mwm.update_or_create_table_not_date(df, 
                                        f'wb_{account.lower()}_info', 
                                        ['skus'], 
                                        schema=schema)
    
    
with DAG(
    dag_id='wb_info_dag',
    default_args=mwm.args,
    schedule_interval=const.sched_int['wb_info_dag'],
    start_date=datetime.datetime(2023, 11, 13),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:
    
    work_wb_ir_info_task = PythonOperator(
                                          task_id = 'work_wb_ir_info',
                                          python_callable=work_wb_ir_info,
                                          op_kwargs={'schema':mwm.ir_schema},
                                          dag=dag
                                          )
    
    work_wb_kz_info_task = PythonOperator(
                                          task_id = 'work_wb_kz_info',
                                          python_callable=work_wb_kz_info,
                                          op_kwargs={'account': 'KZ',
                                                     'schema': mwm.ir_schema},
                                          dag=dag
                                          )
    

    work_wb_ir_info_task >> work_wb_kz_info_task