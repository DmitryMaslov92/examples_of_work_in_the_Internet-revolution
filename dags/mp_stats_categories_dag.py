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
import gspread
import gspread_dataframe as gd


def mp_stats_categories_update(table_name, 
                               path_downloads, 
                               schema):
    
    mpcat = pd.DataFrame(gspread\
                         .service_account(filename=path_downloads + const.maslow_json)\
                         .open("MP_stats_categories")\
                         .worksheet("Категории")\
                         .get_all_records())

    dop_col = ['Наличие_И_WB', 'Наличие_ИЛИ', 'Исключение_ИЛИ_WB', 
               'Наличие_И_OZON', 'Исключение_ИЛИ_OZON', 'Наличие_ИЛИ_OZON', 'Группа']

    try:
        mpcat = mpcat[['Артикул', 'Категория_WB', 'Категория_OZON', 'Вариации']+dop_col]
    except:
        mpcat = mpcat[['Арт', 'Категория_WB', 'Категория_OZON', 'Вариации']+dop_col]

    mpcat.columns = ['Артикул', 'Категория_WB', 'Категория_OZON', 'Вариации']+dop_col

    for mp in ['WB', 'OZON']:   
        podcat_mp_cols = None
        podcat_mp_cols = [f'Подкатегория_{mp}' if y == 1 else f'Подкатегория_{mp}_{y}' for y in list(range(1, 
                          sorted([len(mpcat.loc[x][f'Категория_{mp}'].split('/')) for x in list(mpcat.index)])[-1]+1))]

        for row in podcat_mp_cols:
            mpcat[row] = None

        for i in range(len(mpcat)):
            cat = mpcat.loc[i][f'Категория_{mp}'].split('/')
            for col_i in range(len(podcat_mp_cols)):
                try:
                    mpcat.loc[i, list(podcat_mp_cols)[col_i]] = cat[col_i]
                except:
                    None

    Q = []
    for i in range(len(mpcat)):
        var = mpcat.loc[i]['Вариации']
        if len(var) > 0:
            var = [x.replace("'", '').replace(' ', '') for x in var.replace('[', '').replace(']', '').split(',')]
            var = sorted(list(set(var + [str(mpcat.loc[i]['Артикул']).replace(' ', '')])))
            Q.append(var)
        else:
            Q.append(None)
    mpcat['Вариации'] = Q
    Q = None

    unique_var = [x.split(',') for x in list(set([','.join(x) for x in list(mpcat['Вариации']) if x != None]))]
    unique_var_dict = {}
    unique_var_key = []
    for row in unique_var:
        uq_l = list(set([x[:3] for x in row]))
        if len(uq_l) > 1:
            uq = ''.join(uq_l)
        else:
            uq = uq_l[0]        
        i = 1
        if f'{uq}_{i}' not in unique_var_key:
            unique_var_key.append(f'{uq}_{i}')
        else:
            while f'{uq}_{i}' in unique_var_key:
                i+=1
            unique_var_key.append(f'{uq}_{i}')
    if len(unique_var) != len(unique_var_key):
        print('error')
        exit()
    for i in range(len(unique_var)):
        unique_var_dict[unique_var_key[i]] = unique_var[i]
    Q = []
    for i in range(len(mpcat)):
        var = mpcat.loc[i]['Вариации']
        if var != None:
            Q.append([x for x in list(unique_var_dict.keys()) if unique_var_dict[x] == var][0])
        else:
            Q.append(None)
    mpcat['Код_вариации'] = Q
    Q = None

    mpcat = mpcat[['Артикул', 'Категория_WB']\
                  +[x for x in list(mpcat.columns) if 'Подкатегория_WB' in x]\
                  +['Категория_OZON']\
                  +[x for x in list(mpcat.columns) if 'Подкатегория_OZON' in x]\
                  +['Вариации', 'Код_вариации']\
                  +dop_col]

    mpcat['Вариации'] = [', '.join(mpcat.loc[x]['Вариации']) if mpcat.loc[x]['Вариации'] != None else None for x in list(mpcat.index)]

    for col in mpcat.columns[1:]:
        mpcat[col] = mpcat[col].fillna('')
        
    # Создание/добавление данных в mp_stats_categories
    if table_name in mwm.show_table_name_from_schema():
        req = f"""
        SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}' AND table_name = '{table_name}'
                    ORDER BY ordinal_position ASC;
        """
        db_mpcat_cols = list(mwm.select_table_from_request(req)[0])
        db_mpcat_cols = [x.lower() for x in db_mpcat_cols]
        mpcat_cols = list(mpcat.columns)
        mpcat_cols = [x.lower() for x in mpcat_cols]
        if db_mpcat_cols !=  mpcat_cols:
            mwm.drop_table(table_name=table_name)

            req_mpcat = mwm.create_table_from_dict_template(data=mpcat, 
                                                            table_name=table_name, 
                                                            uniq_col=['Артикул'], 
                                                            flag_str=True,
                                                            schema=schema).rsplit(' ')
            for col in list(mpcat.columns[1:]):
                if col != list(mpcat.columns[1:])[-1]:
                    req_mpcat[req_mpcat.index(col)+1] = \
                    req_mpcat[req_mpcat.index(col)+1][:-1] + ' NULL,'
                else:
                    req_mpcat[req_mpcat.index(col)+1] = \
                    req_mpcat[req_mpcat.index(col)+1][:-2] + ' NULL)'      
            req_mpcat = ' '.join(req_mpcat)
            connection, cursor = mwm.conn()
            cursor.execute(req_mpcat)
            connection.commit()
            connection.close()
            cursor.close()

    else:

        req_mpcat = mwm.create_table_from_dict_template(data=mpcat, 
                                                        table_name=table_name, 
                                                        uniq_col=['Артикул'], 
                                                        flag_str=True,
                                                        schema=schema).rsplit(' ')
        for col in list(mpcat.columns[1:]):
            if col != list(mpcat.columns[1:])[-1]:
                req_mpcat[req_mpcat.index(col)+1] = \
                req_mpcat[req_mpcat.index(col)+1][:-1] + ' NULL,'
            else:
                req_mpcat[req_mpcat.index(col)+1] = \
                req_mpcat[req_mpcat.index(col)+1][:-2] + ' NULL)'      
        req_mpcat = ' '.join(req_mpcat)
        connection, cursor = mwm.conn()
        cursor.execute(req_mpcat)
        connection.commit()
        connection.close()
        cursor.close()

    mwm.truncate_table(table_name=table_name)
    mwm.insert_values(mpcat,
                      table_name=table_name,
                      flag_NULL=True)
    
with DAG(
         dag_id='mp_stats_categories_dag',
         default_args=mwm.args,
         schedule_interval=const.sched_int['mp_stats_categories_dag'],
         start_date=datetime.datetime(2024, 1, 16),
         catchup=False,
         dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

    mp_stats_categories_update_task = PythonOperator(task_id = 'mp_stats_categories_update',
                                            python_callable=mp_stats_categories_update, 
                                            op_kwargs={'schema': mwm.ir_schema, 
                                                       'path_downloads': path_downloads,
                                                       'table_name': 'mp_stats_categories'},
                                            dag=dag)
    
    mp_stats_categories_update_task