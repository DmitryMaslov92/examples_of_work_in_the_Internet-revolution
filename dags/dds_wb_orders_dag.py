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

EXTERNAL_DAG_ID = "wb_kz_orders_dag"


def get_most_recent_dag_run(dt):
    dag_runs = DagRun.find(dag_id="wb_kz_orders_dag")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date

def update_dds_wb_orders(schema,
                         dds_schema):
    
    #IR
    req = f"""
    SELECT distinct date
    FROM {dds_schema}.dds_wb_orders
    WHERE account = '{const.account_ir.lower()}'
    order by date desc
    LIMIT 10;
    """
    list_date_dds_wb_orders_ir = list(mwm.select_table_from_request(req=req,
                                                                    table_name=None,
                                                                    schema=dds_schema)[0])

    req = f"""
    SELECT distinct date::DATE
    FROM {schema}.wb_orders2_copy
    order by date::DATE desc
    LIMIT 10;
    """
    list_date_wb_orders2_copy = list(mwm.select_table_from_request(req=req,
                                                                   table_name=None,
                                                                   schema=schema)[0])
    list_date_wb_orders2_copy = [x for x in list_date_wb_orders2_copy if x != None]
    
    for dt in list_date_wb_orders2_copy:
        if dt != mwm.dt_yesterday:
            if dt not in list_date_dds_wb_orders_ir:
                wb_orders2_copy_new = dm.get_redact_wb_orders_ir_or_kz(flag_full=False,
                                                                       date_filter=dt,
                                                                       account=const.account_ir,
                                                                       schema=schema)
                wb_orders2_copy_new['account'] = const.account_ir.lower()
                mwm.insert_values(data=wb_orders2_copy_new, 
                                  table_name='dds_wb_orders', 
                                  schema=dds_schema)
            else:
                None
        else: 
            wb_orders2_copy_new = dm.get_redact_wb_orders_ir_or_kz(flag_full=False,
                                                                   date_filter=dt,
                                                                   account=const.account_ir,
                                                                   schema=schema)
            wb_orders2_copy_new['account'] = const.account_ir.lower()
            
            if dt not in list_date_dds_wb_orders_ir:
                mwm.insert_values(data=wb_orders2_copy_new, 
                                  table_name='dds_wb_orders', 
                                  schema=dds_schema)
            else: 
                req = f"""
                DELETE
                FROM {dds_schema}.dds_wb_orders
                WHERE date::DATE = '{dt.strftime('%Y-%m-%d')}'
                and account = '{const.account_ir.lower()}'
                """
                mwm.delete_from_request(req)
                mwm.insert_values(data=wb_orders2_copy_new, 
                                  table_name='dds_wb_orders', 
                                  schema=dds_schema) 
        
        
    #KZ
    req = f"""
    SELECT distinct date
    FROM {dds_schema}.dds_wb_orders
    WHERE account = '{const.account_kz.lower()}'
    order by date desc
    LIMIT 10;
    """
    list_date_dds_wb_orders_kz = list(mwm.select_table_from_request(req=req,
                                                                    table_name=None,
                                                                    schema=dds_schema)[0])

    req = f"""
    SELECT distinct date::DATE
    FROM {schema}.wb_kz_orders
    order by date::DATE desc
    LIMIT 10;
    """
    list_date_wb_kz_orders = list(mwm.select_table_from_request(req=req,
                                                                table_name=None,
                                                                schema=schema)[0])
    list_date_wb_kz_orders = [x for x in list_date_wb_kz_orders if x != None]
    
    for dt in list_date_wb_kz_orders:
        if dt != mwm.dt_yesterday:
            if dt not in list_date_dds_wb_orders_kz:
                wb_kz_orders_new = dm.get_redact_wb_orders_ir_or_kz(flag_full=False,
                                                                    date_filter=dt,
                                                                    account=const.account_kz,
                                                                    schema=schema)
                wb_kz_orders_new['account'] = const.account_kz.lower()
                mwm.insert_values(data=wb_kz_orders_new, 
                                  table_name='dds_wb_orders', 
                                  schema=dds_schema)
            else:
                None
        else:
            wb_kz_orders_new = dm.get_redact_wb_orders_ir_or_kz(flag_full=False,
                                                                date_filter=dt,
                                                                account=const.account_kz,
                                                                schema=schema)
            wb_kz_orders_new['account'] = const.account_kz.lower()
            
            if dt not in list_date_dds_wb_orders_kz:
                mwm.insert_values(data=wb_kz_orders_new, 
                                  table_name='dds_wb_orders', 
                                  schema=dds_schema)
            else: 
                req = f"""
                DELETE
                FROM {dds_schema}.dds_wb_orders
                WHERE date::DATE = '{dt.strftime('%Y-%m-%d')}'
                and account = '{const.account_kz.lower()}'
                """
                mwm.delete_from_request(req)
                mwm.insert_values(data=wb_kz_orders_new, 
                                  table_name='dds_wb_orders', 
                                  schema=dds_schema) 
        
        
with DAG(
         dag_id='dds_wb_orders_dag',
         default_args=mwm.args,
         schedule_interval=const.sched_int['dds_wb_orders_dag'],
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
    

  
    update_dds_wb_orders_task = PythonOperator(
                                               task_id='update_dds_wb_orders',
                                               python_callable=update_dds_wb_orders,
                                               op_kwargs={'schema':mwm.ir_schema,
                                                          'dds_schema': mwm.dds_schema},
                                               dag=dag
                                               )
    
    
    ext_sensor>>update_dds_wb_orders_task