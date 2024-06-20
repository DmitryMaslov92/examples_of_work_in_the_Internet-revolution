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
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
os.environ['NO_PROXY'] = 'URL'
from sys import exit

def work_wb_list_of_nomenclatures(schema):
    wb_list_of_nomenclatures = pd.read_excel(mwm.path_FD + 'wb_list_of_nomenclatures.xlsx').fillna('')
    if len(wb_list_of_nomenclatures) == 0:
        print('Ошика в загрузке')
        exit()
    mwm.unique_barcode(wb_list_of_nomenclatures)
    mwm.update_or_create_table_not_date(wb_list_of_nomenclatures, 
                                        'wb_list_of_nomenclatures', 
                                        ['Баркод'], 
                                        schema=schema)
        
with DAG(
    dag_id='wb_list_of_nomenclatures_dag',
    default_args=mwm.args,
    schedule_interval=const.sched_int['wb_list_of_nomenclatures_dag'],
    start_date=datetime.datetime(2023, 11, 13),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

    get_wb_list_of_nomenclatures_task = PythonOperator(
                                                       task_id = 'get_wb_list_of_nomenclatures',
                                                       python_callable=mwm.get_wb_list_of_nomenclatures,
                                                       dag=dag
                                                      )
    
    work_wb_list_of_nomenclatures_task = PythonOperator(
                                                        task_id = 'work_wb_list_of_nomenclatures',
                                                        python_callable=work_wb_list_of_nomenclatures,
                                                        op_kwargs={'schema':mwm.ir_schema},
                                                        dag=dag
                                                       )
    

    get_wb_list_of_nomenclatures_task >> work_wb_list_of_nomenclatures_task