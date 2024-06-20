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


def work_wb_kz_fbs_stocks(schema):
    wb_fbs_stocks_new = pd.read_excel(mwm.path_FD + 'wb_kz_fbs_stocks_new.xlsx',
                                      dtype={'Баркод': 'int'})
    if len(wb_fbs_stocks_new) == 0:
        print('В wb_fbs_stocks_new нет данных')
        exit()
    for row in list(wb_fbs_stocks_new['Склад'].unique()):   
        if len(wb_fbs_stocks_new[wb_fbs_stocks_new['Склад']==row])\
           - len(list(set(wb_fbs_stocks_new[wb_fbs_stocks_new['Склад']==row]['Баркод']))) != 0:
            print('В wb_fbs_stocks_new есть неуникальные баркоды')
            exit()
    mwm.update_or_create_table_date(wb_fbs_stocks_new, 
                                    'wb_kz_fbs_stocks', 
                                    ['Баркод', 'Склад', 'date'],
                                    'date',
                                    schema=schema)
    
with DAG(
    dag_id='wb_kz_fbs_stocks_dag',
    default_args=mwm.args,
    schedule_interval=const.sched_int['wb_kz_fbs_stocks_dag'],
    start_date=datetime.datetime(2023, 11, 13),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

  
    get_wb_fbs_stocks_new_task = PythonOperator(
                                                task_id = 'get_wb_fbs_stocks_new',
                                                python_callable=mwm.get_wb_fbs_stocks_new,
                                                op_kwargs={'account': 'KZ'},
                                                dag=dag
                                                )
    
    work_wb_kz_fbs_stocks_task = PythonOperator(
                                             task_id = 'work_wb_kz_fbs_stocks',
                                             python_callable=work_wb_kz_fbs_stocks,
                                             op_kwargs={'schema':mwm.ir_schema},
                                             dag=dag
                                             )
    

    get_wb_fbs_stocks_new_task >> work_wb_kz_fbs_stocks_task