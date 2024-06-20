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

def work_oz_products(schema,
                     path_downloads=path_downloads):
    # if len([py for py in glob.glob(f"{path_downloads}products (*).csv")]) == 0:
    #     if len([py for py in glob.glob(f"{path_downloads}products.csv")]) == 0:   
    #         path_name_oz_products = [py for py in glob.glob(f"{path_downloads}products.csv")][0]
    #     else:
    #         print('Ошибка в нахождении пути для oz_products')
    #         exit()
    # else:
    #     try:
    #         dict_report_paths = {}
    #         for path in [py for py in glob.glob(f"{path_downloads}products (*).csv")]:
    #             dict_report_paths[int(path.split('(')[1][:-5])] = path
    #         path_name_oz_products = dict_report_paths[sorted(dict_report_paths.keys())[-1]]
    #     except:
    #         print('Ошибка в нахождении пути для oz_products')
    #         exit()
    try:
        path_name_oz_products = wwm.return_path('products*.csv')
    except:
        print('Ошибка в нахождении пути для oz_products')
        exit()
    oz_products = pd.read_csv(path_name_oz_products, sep=';')
    if len(oz_products) == 0:
        print('В oz_products нет данных')
        exit()
    oz_products['Артикул'] = [x.replace("'", "") for x in list(oz_products['Артикул'])]
    oz_products = oz_products.fillna('')
    mwm.unique_items(oz_products)
    mwm.unique_barcode(oz_products.rename(columns={'Ozon Product ID': 'Баркод'}))
    oz_products = mwm.redact_columns_with_db_table(oz_products)
    for col in oz_products.columns:
        if str(oz_products[col].dtype) == 'object':
            oz_products[col] = [x.replace("'", "") if "'" in str(x) else x for x in list(oz_products[col])]
    mwm.update_or_create_table_not_date(oz_products,
                                        'oz_products',
                                        ['Артикул', 'Ozon_Product_ID'])
    
with DAG(
    dag_id='oz_products_dag',
    default_args=mwm.args,
    schedule_interval=const.sched_int['oz_products_dag'],
    start_date=datetime.datetime(2023, 11, 13),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:
    
    work_oz_products_task = PythonOperator(
                                           task_id = 'work_oz_products',
                                           python_callable=work_oz_products,
                                           op_kwargs={'schema':mwm.ir_schema},
                                           dag=dag
                                           )
    

    work_oz_products_task