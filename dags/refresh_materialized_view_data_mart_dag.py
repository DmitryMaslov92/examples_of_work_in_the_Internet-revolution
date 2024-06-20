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

EXTERNAL_DAG_ID = "dds_oz_orders_dag"

def get_most_recent_dag_run(dt):
    dag_runs = DagRun.find(dag_id="dds_oz_orders_dag")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date
    
def refresh_materialized_view_data_mart(dds_schema):
    req = f"""
    REFRESH MATERIALIZED view {dds_schema}.data_mart;
    """
    mwm.delete_from_request(req)
    
    

        
        
with DAG(
         dag_id='refresh_materialized_view_data_mart_dag',
         default_args=mwm.args,
         schedule_interval=const.sched_int['refresh_materialized_view_data_mart_dag'],
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
    

  
    refresh_materialized_view_data_mart_task = PythonOperator(
                                               task_id='refresh_materialized_view_data_mart',
                                               python_callable=refresh_materialized_view_data_mart,
                                               op_kwargs={'dds_schema': mwm.dds_schema},
                                               dag=dag
                                               )
    
    
    ext_sensor>>refresh_materialized_view_data_mart_task 