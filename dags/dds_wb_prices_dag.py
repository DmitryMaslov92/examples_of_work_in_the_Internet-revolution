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
import working_with_marketplace_metrics_dags as wwm
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
import datetime
import os
os.environ['NO_PROXY'] = 'URL'
from sys import exit

EXTERNAL_DAG_ID = "wb_prices_dag"

def get_most_recent_dag_run(dt):
    dag_runs = DagRun.find(dag_id="wb_prices_dag")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date
    
def update_dds_wb_prices(schema,
                         dds_schema):
    
    res = [datetime.datetime.strptime(x, '%d-%B-%Y').date() for x in \
           mwm.date_range(start_date=mwm.dt_now.date(),
           end_date=mwm.dt_now.date() - datetime.timedelta(days=9))]
    
    #IR
    req = f"""
    SELECT distinct date
    FROM {dds_schema}.dds_wb_prices
    WHERE account = '{const.account_ir.lower()}'
    order by date desc
    LIMIT 10;
    """
    list_date_dds_wb_prices_ir = list(mwm.select_table_from_request(req=req,
                                                                    table_name=None,
                                                                    schema=dds_schema)[0])

    for dt in [dt for dt in res if dt not in list_date_dds_wb_prices_ir]:
        try:
            wb_prices_ir = dm.get_redact_wb_prices(account=const.account_ir,
                                                   date_filter=dt,
                                                   flag_full=False)
        except:
            print(f'Данные за {dt} не были получены, account {const.account_ir}')
        try:
            mwm.insert_values(data=wb_prices_ir, 
                              table_name='dds_wb_prices', 
                              schema=dds_schema) 
        except:
            print(f'Данные за {dt} не были добавлены, account {const.account_ir}')

            
    #KZ
    req = f"""
    SELECT distinct date
    FROM {dds_schema}.dds_wb_prices
    WHERE account = '{const.account_kz.lower()}'
    order by date desc
    LIMIT 10;
    """
    list_date_dds_wb_prices_kz = list(mwm.select_table_from_request(req=req,
                                                                    table_name=None,
                                                                    schema=dds_schema)[0])

    for dt in [dt for dt in res if dt not in list_date_dds_wb_prices_kz]:
        try:
            wb_prices_kz = dm.get_redact_wb_prices(account=const.account_kz,
                                                   date_filter=dt,
                                                   flag_full=False)
        except:
            print(f'Данные за {dt} не были получены, account {const.account_kz}')
        try:
            mwm.insert_values(data=wb_prices_kz, 
                              table_name='dds_wb_prices', 
                              schema=dds_schema) 
        except:
            print(f'Данные за {dt} не были добавлены, account {const.account_kz}')

        
        
with DAG(
         dag_id='dds_wb_prices_dag',
         default_args=mwm.args,
         schedule_interval=const.sched_int['dds_wb_prices_dag'],
         start_date=datetime.datetime(2024, 1, 16),
         catchup=False,
         dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:
    

    ext_sensor = ExternalTaskSensor(
        task_id='sensor',
        external_dag_id=EXTERNAL_DAG_ID,
        execution_date_fn=get_most_recent_dag_run,
        check_existence=True
        )
    

  
    update_dds_wb_prices_task = PythonOperator(
                                               task_id='update_dds_wb_prices',
                                               python_callable=update_dds_wb_prices,
                                               op_kwargs={'schema':mwm.ir_schema,
                                                          'dds_schema': mwm.dds_schema},
                                               dag=dag
                                               )
    
    
    ext_sensor>>update_dds_wb_prices_task    