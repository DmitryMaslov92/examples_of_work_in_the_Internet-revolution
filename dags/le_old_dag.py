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
import working_with_marketplace_metrics_dags as wwm
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import os
os.environ['NO_PROXY'] = 'URL'
from sys import exit

le_inf_dict = {'le_orders_metrics': 'le_orders_old',
               'le_price_metrics': 'le_prices_old',
               'stocks_le_metrics': 'le_fbo_stocks_old'}

def work_le_orders_old(PATH,
                       schema,
                       inf_dict,
                       sheet_name='le_orders_metrics',
                       result_column='Заказы'):
    
    df = wwm.redact_df_2(PATH=PATH, 
                         sheet_name=sheet_name, 
                         result_column=result_column,
                         flag_dtc=True)
    
    for col in df['date'].unique():
        mwm.update_or_create_table_date(df[df['date']==col].reset_index(drop=True), 
                                        inf_dict[sheet_name], 
                                        ['Артикул', 'date'],
                                        'date',
                                        schema=schema,
                                        flag_NULL=True)
        
        
def work_le_prices_old(PATH,
                       schema,
                       inf_dict,
                       sheet_name='le_price_metrics',
                       result_column='Цены'):
    
    df = wwm.redact_df_2(PATH=PATH, 
                         sheet_name=sheet_name, 
                         result_column=result_column,
                         flag_dtc=True)
    
    for col in df['date'].unique():
        mwm.update_or_create_table_date(df[df['date']==col].reset_index(drop=True), 
                                        inf_dict[sheet_name], 
                                        ['Артикул', 'date'],
                                        'date',
                                        schema=schema,
                                        flag_NULL=True)
        
        
def work_le_fbo_stocks_old(PATH,
                       schema,
                       inf_dict,
                       sheet_name='stocks_le_metrics',
                       result_column='Остатки_фбо'):
    
    df = wwm.redact_df_2(PATH=PATH, 
                         sheet_name=sheet_name, 
                         result_column=result_column,
                         flag_dtc=True)
    
    for col in df['date'].unique():
        mwm.update_or_create_table_date(df[df['date']==col].reset_index(drop=True), 
                                        inf_dict[sheet_name], 
                                        ['Артикул', 'date'],
                                        'date',
                                        schema=schema,
                                        flag_NULL=True)
        
        
        
        
with DAG(
    dag_id='le_old_dag',
    default_args=mwm.args,
    schedule_interval=const.sched_int['le_old_dag'],
    start_date=datetime.datetime(2024, 1, 25),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

    work_le_orders_old_task = PythonOperator(
                                             task_id = 'work_le_orders_old',
                                             python_callable=work_le_orders_old,
                                             op_kwargs={'PATH': wwm.path_full_LE_df, 
                                                        'schema':mwm.ir_schema,
                                                        'inf_dict': le_inf_dict},
                                             dag=dag
                                             )
    
    work_le_prices_old_task = PythonOperator(
                                             task_id = 'work_le_prices_old',
                                             python_callable=work_le_prices_old,
                                             op_kwargs={'PATH': wwm.path_full_LE_df, 
                                                        'schema':mwm.ir_schema,
                                                        'inf_dict': le_inf_dict},
                                             dag=dag
                                             )
    
    work_le_fbo_stocks_old_task = PythonOperator(
                                                 task_id = 'work_le_fbo_stocks_old',
                                                 python_callable=work_le_fbo_stocks_old,
                                                 op_kwargs={'PATH': wwm.path_full_LE_df, 
                                                            'schema':mwm.ir_schema,
                                                            'inf_dict': le_inf_dict},
                                                 dag=dag
                                                 )
    

    
    work_le_orders_old_task>>\
    work_le_prices_old_task>>\
    work_le_fbo_stocks_old_task