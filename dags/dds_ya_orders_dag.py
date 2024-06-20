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
from airflow.models import DagRun
import datetime
import os
os.environ['NO_PROXY'] = 'URL'
from sys import exit


    
def update_dds_ya_orders(schema,
                         dds_schema):
    
    res = [datetime.datetime.strptime(x, '%d-%B-%Y').date() for x in \
           mwm.date_range(start_date=mwm.dt_now.date(),
           end_date=mwm.dt_now.date() - datetime.timedelta(days=30))]
    
    for account in [const.account_ir]:
        for dt in res:
            ya_orders_acc_new = dm.get_redact_ya_orders(date_filter=dt,
                                                        account=account,
                                                        schema=mwm.ir_schema)
            if len(ya_orders_acc_new) > 0:
                try:
                    req = f"""
                    DELETE
                    FROM {dds_schema}.dds_ya_orders
                    WHERE date::DATE = '{dt.strftime('%Y-%m-%d')}'
                    and account = '{account.lower()}'
                    """
                    mwm.delete_from_request(req)
                    mwm.insert_values(data=ya_orders_acc_new, 
                                      table_name='dds_ya_orders', 
                                      schema=dds_schema)
                except:
                    print(f'Данные за {dt} таблицы dds_ya_orders аккаунта {account.lower()} не были обновлены')
            else:
                print(f'Нет данных за день: {dt} и аккаунт: {account}')
                        
with DAG(
         dag_id='dds_ya_orders_dag',
         default_args=mwm.args,
         schedule_interval=const.sched_int['dds_ya_orders_dag'],
         start_date=datetime.datetime(2024, 1, 16),
         catchup=False,
         dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:
    
  
    update_dds_ya_orders_task = PythonOperator(
                                               task_id='update_dds_ya_orders',
                                               python_callable=update_dds_ya_orders,
                                               op_kwargs={'schema':mwm.ir_schema,
                                                          'dds_schema': mwm.dds_schema},
                                               dag=dag
                                               )
    
    
    update_dds_ya_orders_task