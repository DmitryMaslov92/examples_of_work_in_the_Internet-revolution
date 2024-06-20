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

EXTERNAL_DAG_ID = "oz_fbs_stocks_dag"


def get_most_recent_dag_run(dt):
    dag_runs = DagRun.find(dag_id="oz_fbs_stocks_dag")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        return dag_runs[0].execution_date

def update_dds_oz_stocks(schema,
                         dds_schema):
    
    res = [datetime.datetime.strptime(x, '%d-%B-%Y').date() for x in \
           mwm.date_range(start_date=mwm.dt_now.date() ,
           end_date=mwm.dt_now.date() - datetime.timedelta(days=9))]

    for account in [const.account_ir, const.account_kz, const.account_ssy]:
        print(account)
        print()
        req = f"""
        SELECT distinct date
        FROM {dds_schema}.dds_oz_stocks
        WHERE account = '{account.lower()}'
        order by date desc
        LIMIT 10;
        """

        list_date_dds_oz_stocks_acc = list(mwm.select_table_from_request(req=req,
                                                                         table_name=None,
                                                                         schema=dds_schema)[0])
        for dt in [dt for dt in res if dt not in list_date_dds_oz_stocks_acc]:
            oz_stocks = pd.DataFrame()
            if account == const.account_ir:
                try:
                    oz_ir_fbo_stocks = dm.get_redact_oz_stocks(account=account,
                                                               schema=schema,
                                                               date_filter=dt,
                                                               fbo_fbs_flag='fbo')
                    oz_ir_fbo_stocks['item_code'] = oz_ir_fbo_stocks['item_code'].astype(str)
                    oz_ir_fbs_stocks = dm.get_redact_oz_stocks(account=account,
                                                               schema=schema,
                                                               date_filter=dt,
                                                               fbo_fbs_flag='fbs')
                    oz_ir_fbs_stocks['item_code'] = oz_ir_fbs_stocks['item_code'].astype(str)
                    oz_stocks = oz_ir_fbo_stocks.merge(oz_ir_fbs_stocks, 
                                                       on=['item_code', 'date'], 
                                                       how='outer').fillna(0)
                    oz_ir_fbo_stocks = None
                    oz_ir_fbs_stocks = None
                    oz_stocks['account'] = account.lower()
                    oz_stocks['fbo_stocks'] = oz_stocks['fbo_stocks'].astype(int)
                    oz_stocks['fbs_stocks'] = oz_stocks['fbs_stocks'].astype(int)
                except:
                    print(f'Данные за {dt} не были получены, account {account}')
            elif account == const.account_ssy:
                try:
                    oz_stocks = dm.get_redact_oz_stocks(account=account,
                                                        schema=schema,
                                                        date_filter=dt,
                                                        fbo_fbs_flag='fbo')               
                    oz_stocks['item_code'] = oz_stocks['item_code'].astype(str)
                    oz_stocks['account'] = account.lower()
                    oz_stocks['fbs_stocks'] = 0
                    oz_stocks = oz_stocks[['item_code', 'date', 'fbo_stocks', 'fbs_stocks', 'account']]
                    oz_stocks['fbo_stocks'] = oz_stocks['fbo_stocks'].astype(int)
                    oz_stocks['fbs_stocks'] = oz_stocks['fbs_stocks'].astype(int)
                except:
                    print(f'Данные за {dt} не были получены, account {account}')
            elif account == const.account_kz:
                try:
                    oz_stocks = dm.get_redact_oz_stocks(account=account,
                                                        schema=schema,
                                                        date_filter=dt,
                                                        fbo_fbs_flag='fbs')               
                    oz_stocks['item_code'] = oz_stocks['item_code'].astype(str)
                    oz_stocks['account'] = account.lower()
                    oz_stocks['fbo_stocks'] = 0
                    oz_stocks = oz_stocks[['item_code', 'date', 'fbo_stocks', 'fbs_stocks', 'account']]
                    oz_stocks['fbo_stocks'] = oz_stocks['fbo_stocks'].astype(int)
                    oz_stocks['fbs_stocks'] = oz_stocks['fbs_stocks'].astype(int)
                except:
                    print(f'Данные за {dt} не были получены, account {account}')
            else:
                print(f'Нет такого кабинета: {account}')
                exit()
            if len(oz_stocks) > 0:
                try:
                    mwm.insert_values(data=oz_stocks, 
                                      table_name='dds_oz_stocks', 
                                      schema=dds_schema)
                except:
                    print(f'Данные за {dt} не были добавлены, account {account}')
                    exit()
            else:
                print(f'Данных за {dt} аккаунта {account} нет в dds_oz_stocks')
                
                
with DAG(
         dag_id='dds_oz_stocks_dag',
         default_args=mwm.args,
         schedule_interval=const.sched_int['dds_oz_stocks_dag'],
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
    

  
    update_dds_oz_stocks_task = PythonOperator(
                                               task_id='update_dds_oz_stocks',
                                               python_callable=update_dds_oz_stocks,
                                               op_kwargs={'schema':mwm.ir_schema,
                                                          'dds_schema': mwm.dds_schema},
                                               dag=dag
                                               )
    
    
    ext_sensor>>update_dds_oz_stocks_task    