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

EXTERNAL_DAG_ID = "oz_fbs_orders_dag"


def get_most_recent_dag_run(dt):
    dag_runs = DagRun.find(dag_id="oz_fbs_orders_dag")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date

    
def update_dds_oz_orders(schema,
                         dds_schema):
    
    res = [datetime.datetime.strptime(x, '%d-%B-%Y').date() for x in \
           mwm.date_range(start_date=mwm.dt_now.date(),
           end_date=mwm.dt_now.date() - datetime.timedelta(days=30))]
    
    for account in [const.account_ir, const.account_kz, const.account_ssy]:
        for dt in res:
            oz_orders_acc_new = dm.get_redact_oz_orders(flag_full=False,
                                                        date_filter=dt,
                                                        account=account,
                                                        schema=mwm.ir_schema)
            if len(oz_orders_acc_new) > 0:
                try:
                    req = f"""
                    DELETE
                    FROM {dds_schema}.dds_oz_orders
                    WHERE date::DATE = '{dt.strftime('%Y-%m-%d')}'
                    and account = '{account.lower()}'
                    """
                    mwm.delete_from_request(req)
                    mwm.insert_values(data=oz_orders_acc_new, 
                                      table_name='dds_oz_orders', 
                                      schema=dds_schema)
                except:
                    print(f'Данные за {dt} таблицы dds_oz_orders аккаунта {account.lower()} не были обновлены')
            else:
                print(f'Нет данных за день: {dt} и аккаунт: {account}')
                        
with DAG(
         dag_id='dds_oz_orders_dag',
         default_args=mwm.args,
         schedule_interval=const.sched_int['dds_oz_orders_dag'],
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
    

  
    update_dds_oz_orders_task = PythonOperator(
                                               task_id='update_dds_oz_orders',
                                               python_callable=update_dds_oz_orders,
                                               op_kwargs={'schema':mwm.ir_schema,
                                                          'dds_schema': mwm.dds_schema},
                                               dag=dag
                                               )
    
    
    ext_sensor>>update_dds_oz_orders_task




# import pandas as pd
# import glob
# import sys
# link_to_table = 'https://docs.google.com/spreadsheets/d/1A1TyZPeq9r2HiDi-CHfmTfRpuAQYToib1vqyTx4ZyRE/edit#gid=0'
# df = pd.read_excel('/'.join(link_to_table.split('/')[:-1])+'/export?format=xlsx')
# PATH, df = [dict(df.loc[x]) for x in list(df.index)], None
# i = 0
# while len([py for py in glob.glob(f"{PATH[i]['path_downloads']}*.xlsx")]) == 0:
#     i +=1
# path_downloads = PATH[i]['path_downloads']
# path_algoritm = PATH[i]['path_algoritm']
# sys.path.append(f'{path_algoritm}Модули')
# import maslow_working_module as mwm
# import constants_for_marketplace_metrics_dags as const
# import dop_wb_oz_work_module as dm
# import working_with_marketplace_metrics_dags as wwm
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.models import DagRun
# import datetime
# import os
# os.environ['NO_PROXY'] = 'URL'
# from sys import exit

# EXTERNAL_DAG_ID = "oz_fbs_orders_dag"


# def get_most_recent_dag_run(dt):
#     dag_runs = DagRun.find(dag_id="oz_fbs_orders_dag")
#     dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
#     if dag_runs:
#         return dag_runs[0].execution_date

    
# def update_dds_oz_orders(schema,
#                          dds_schema):

#     res = [datetime.datetime.strptime(x, '%d-%B-%Y').date() for x in \
#            mwm.date_range(start_date=mwm.dt_now.date() - datetime.timedelta(days=1),
#            end_date=mwm.dt_now.date() - datetime.timedelta(days=9))]

#     for account in [const.account_ir, const.account_kz, const.account_ssy]:
#         print(account)
#         print()
#         req = f"""
#         SELECT distinct date
#         FROM {dds_schema}.dds_oz_orders
#         WHERE account = '{account.lower()}'
#         order by date desc
#         LIMIT 10;
#         """
#         list_date_dds_oz_orders_acc = list(mwm.select_table_from_request(req=req,
#                                                                         table_name=None,
#                                                                         schema=dds_schema)[0])

#         for dt in res:
#             if dt != mwm.dt_yesterday:
#                 if dt not in list_date_dds_oz_orders_acc:
#                     oz_orders_acc_new = dm.get_redact_oz_orders(flag_full=False,
#                                                                 date_filter=dt,
#                                                                 account=account,
#                                                                 schema=mwm.ir_schema)
                    
# #                     # Блок на случай, если продаж по озон кз или озон ssy в этот день не было
# #                     if account == const.account_ssy or account == const.account_kz \
# #                     and len(oz_orders_acc_new) == 0:
# #                         oz_orders_acc_new = dm.get_redact_oz_orders(flag_full=False,
# #                                                                     date_filter=list_date_dds_oz_orders_acc[0]\
# #                                                                     .strftime('%d-%B-%Y'),
# #                                                                     account=account,
# #                                                                     schema=mwm.ir_schema)
# #                         oz_orders_acc_new['date'] = dt.strftime('%d-%B-%Y')
# #                         oz_orders_acc_new['orders'] = 0
            
#                     if len(oz_orders_acc_new) > 0:
#                         mwm.insert_values(data=oz_orders_acc_new, 
#                                           table_name='dds_oz_orders', 
#                                           schema=dds_schema)
#                     else:
#                         print(f'Нет данных за день: {dt} и аккаунт: {account}')
#                 else:
#                     None
#             else:
#                 oz_orders_acc_new = dm.get_redact_oz_orders(flag_full=False,
#                                                             date_filter=dt,
#                                                             account=account,
#                                                             schema=mwm.ir_schema)
                
# #                 # Блок на случай, если продаж по озон кз или озон ssy в этот день не было
# #                 if account == const.account_ssy or account == const.account_kz \
# #                 and len(oz_orders_acc_new) == 0:
# #                     oz_orders_acc_new = dm.get_redact_oz_orders(flag_full=False,
# #                                                                 date_filter=list_date_dds_oz_orders_acc[0]\
# #                                                                 .strftime('%d-%B-%Y'),
# #                                                                 account=account,
# #                                                                 schema=mwm.ir_schema)
# #                     oz_orders_acc_new['date'] = dt.strftime('%d-%B-%Y')
# #                     oz_orders_acc_new['orders'] = 0
                    
#                 if len(oz_orders_acc_new) > 0:
#                     if dt not in list_date_dds_oz_orders_acc:
#                         mwm.insert_values(data=oz_orders_acc_new, 
#                                           table_name='dds_oz_orders', 
#                                           schema=dds_schema)
#                     else: 
#                         req = f"""
#                         DELETE
#                         FROM {dds_schema}.dds_oz_orders
#                         WHERE date::DATE = '{dt.strftime('%Y-%m-%d')}'
#                         and account = '{account.lower()}'
#                         """
#                         mwm.delete_from_request(req)
#                         mwm.insert_values(data=oz_orders_acc_new, 
#                                           table_name='dds_oz_orders', 
#                                           schema=dds_schema)
#                 else:   
#                     print(f'Нет данных за день: {dt} и аккаунт: {account}')
                        
# with DAG(
#          dag_id='dds_oz_orders_dag',
#          default_args=mwm.args,
#          schedule_interval=const.sched_int['dds_oz_orders_dag'],
#          start_date=datetime.datetime(2024, 1, 16),
#          catchup=False,
#          dagrun_timeout=datetime.timedelta(minutes=10)
# ) as dag:
    

#     ext_sensor = ExternalTaskSensor(
#         task_id='sensor',
#         external_dag_id=EXTERNAL_DAG_ID,
#         execution_date_fn=get_most_recent_dag_run,
#         check_existence=True
#         )
    

  
#     update_dds_oz_orders_task = PythonOperator(
#                                                task_id='update_dds_oz_orders',
#                                                python_callable=update_dds_oz_orders,
#                                                op_kwargs={'schema':mwm.ir_schema,
#                                                           'dds_schema': mwm.dds_schema},
#                                                dag=dag
#                                                )
    
    
#     ext_sensor>>update_dds_oz_orders_task