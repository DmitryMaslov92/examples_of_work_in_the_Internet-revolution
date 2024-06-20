# import pandas as pd
# import glob
# import sys
# df = pd.read_excel(f'https://docs.google.com'+
# '/spreadsheets/d/e/2PACX-1vQyCtSf7tuz9520nSPiif43YpfQlaMsIbIrYwcsqzR2-TrKfx2bu-T5BUl595qeTrSWaMmLnNIzTKQF'+
# '/pub?output=xlsx')
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
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# import datetime
# import os
# os.environ['NO_PROXY'] = 'URL'
# from sys import exit


# # Проверить таблицу oz_kz_fbs_orders в БД на пропущенные дни за последние 10 дней
# def check_oz_kz_fbs_orders(ti, 
#                            end_day, 
#                            start_day):
#     req = f"""select in_process_at
#               from ir_db.oz_kz_fbs_orders
#               where in_process_at between '{start_day}' 
#               and '{end_day}'
#               order by in_process_at DESC;"""
#     try:
#         connection, cursor = mwm.conn()
#         cursor.execute(req)
#         rows = cursor.fetchall()
#         df = pd.DataFrame(rows)
#         connection.close()
#         cursor.close()
#     except:
#         print('error')
#         exit()
#     if len(df[0].unique()) != 10:
#         print('В таблице oz_kz_fbs_orders пропущены дни, нужно обновить')
#         ti.xcom_push(key='update_marker', value=True)
#         dt_value_df = ', '.join([x.strftime('%d-%B-%Y') for x in list(df[0].unique())])
#         ti.xcom_push(key='dt_value', value=dt_value_df)
#     else:
#         ti.xcom_push(key='update_marker', value=False)
        
# #  Получить данные по api озона за последние 10 дней в случае пропусков в БД
# def get_get_oz_fbs_orders_new_p1(ti,
#                                  end_day, 
#                                  start_day):
#     update_marker = ti.xcom_pull(key='update_marker', task_ids=['check_oz_kz_fbs_orders'])[0]
#     if update_marker == True:
#         try:
#             dm.get_oz_fbs_orders_new(account='KZ',
#                                      date_to=datetime.datetime.strptime(end_day, '%Y-%m-%d').date()\
#                                      +datetime.timedelta(days=1),
#                                      last_date=datetime.datetime.strptime(start_day, '%Y-%m-%d').date())
#             ti.xcom_push(key='get_marker_1', value=True)
#         except:
#             print('error')
#             exit()
#     else:
#         ti.xcom_push(key='get_marker_1', value=False)

# # Загрузить полученные данные в случае пропусков
# def update_get_oz_fbs_orders_new_p1(ti,
#                                     schema):
#     update_marker = ti.xcom_pull(key='update_marker', task_ids=['check_oz_kz_fbs_orders'])[0]
#     if update_marker == True:
#         get_marker_1 = ti.xcom_pull(key='get_marker_1', task_ids=['get_get_oz_fbs_orders_new_p1'])[0]
#         if get_marker_1 == True:
#             try:
#                 oz_kz_fbs_orders_new = pd.read_excel(mwm.path_FD + 'oz_kz_fbs_orders_new.xlsx').fillna(0)
#             except:
#                 print('Загрузка oz_kz_fbs_orders_new завершилась с ошибкой')
#                 exit()
#             if oz_kz_fbs_orders_new.empty == True:
#                 print('Датафрейм пустой')
#                 exit()
#             dt_value_df = ti.xcom_pull(key='dt_value', task_ids=['check_oz_kz_fbs_orders'])[0].split(', ')
#             for dt in [y.strftime('%d-%B-%Y') for y in 
#                        sorted([pd.to_datetime(x) for x in list(oz_kz_fbs_orders_new['in_process_at'].unique())])]:
#                 if dt not in dt_value_df:
#                     mwm.insert_values(data=oz_kz_fbs_orders_new[oz_kz_fbs_orders_new['in_process_at']==dt]\
#                                            .reset_index(drop=True), 
#                                       table_name='oz_kz_fbs_orders', 
#                                       schema=schema)
#         else:
#             None
#     else:
#         None


# # Получить данные за последнее время
# def get_get_oz_fbs_orders_new_p2(ti):
#     try:
#         dm.get_oz_fbs_orders_new(account='KZ')
#         ti.xcom_push(key='get_marker_2', value=True)
#     except:
#         print('error')
#         exit()


# # Обновить-загрузить данные за последнее время
# def update_get_oz_fbs_orders_new_p2(ti,
#                                     schema):
#     get_marker_2 = ti.xcom_pull(key='get_marker_2', task_ids=['get_get_oz_fbs_orders_new_p2'])[0]
#     if get_marker_2 == True:
#         try:
#             oz_kz_fbs_orders_new = pd.read_excel(mwm.path_FD + 'oz_kz_fbs_orders_new.xlsx').fillna(0)
#         except:
#             print('Загрузка oz_kz_fbs_orders_new завершилась с ошибкой')
#             exit()
#         if oz_kz_fbs_orders_new.empty == True:
#             print('Датафрейм пустой')
#             exit()
#         for dt in [y.strftime('%d-%B-%Y') for y in 
#                    sorted([pd.to_datetime(x) for x in list(oz_kz_fbs_orders_new['in_process_at'].unique())])]:
#             try:
#                 mwm.update_data_date_filter(data=oz_kz_fbs_orders_new[oz_kz_fbs_orders_new['in_process_at']==dt]\
#                                                  .reset_index(drop=True),
#                                             table_name='oz_kz_fbs_orders',
#                                             date_column='in_process_at',
#                                             schema=schema)
#             except:
#                 print(f'День {dt} не обновлен')
#                 exit()
#     else:
#         exit()





    
# with DAG(
#          dag_id='oz_kz_fbs_orders_dag',
#          default_args=mwm.args,
#          schedule_interval='50 04 * * *',
#          start_date=datetime.datetime(2024, 1, 16),
#          catchup=False,
#          dagrun_timeout=datetime.timedelta(minutes=10)
# ) as dag:

  
#     check_oz_kz_fbs_orders_task = PythonOperator(
#                                                  task_id='check_oz_kz_fbs_orders',
#                                                  python_callable=check_oz_kz_fbs_orders,
#                                                  op_kwargs={'schema':mwm.ir_schema,
#                                                             'end_day': const.end_day,
#                                                             'start_day': const.start_day},
#                                                  dag=dag
#                                                 )
    
    
#     get_get_oz_fbs_orders_new_p1_task = PythonOperator(
#                                                        task_id='get_get_oz_fbs_orders_new_p1',
#                                                        python_callable=get_get_oz_fbs_orders_new_p1,
#                                                        op_kwargs={'schema':mwm.ir_schema,
#                                                                   'end_day': const.end_day,
#                                                                   'start_day': const.start_day},
#                                                        dag=dag
#                                                       )
    
    
#     update_get_oz_fbs_orders_new_p1_task = PythonOperator(
#                                                           task_id='update_get_oz_fbs_orders_new_p1',
#                                                           python_callable=update_get_oz_fbs_orders_new_p1,
#                                                           op_kwargs={'schema':mwm.ir_schema},
#                                                           dag=dag
#                                                          )
    
    
#     get_get_oz_fbs_orders_new_p2_task = PythonOperator(
#                                                        task_id='get_get_oz_fbs_orders_new_p2',
#                                                        python_callable=get_get_oz_fbs_orders_new_p2,
#                                                        dag=dag
#                                                       )
    
    
#     update_get_oz_fbs_orders_new_p2_task = PythonOperator(
#                                                           task_id='update_get_oz_fbs_orders_new_p2',
#                                                           python_callable=update_get_oz_fbs_orders_new_p2,
#                                                           op_kwargs={'schema':mwm.ir_schema},
#                                                           dag=dag
#                                                          )
    
#     check_oz_kz_fbs_orders_task>>\
#     get_get_oz_fbs_orders_new_p1_task>>\
#     update_get_oz_fbs_orders_new_p1_task>>\
#     get_get_oz_fbs_orders_new_p2_task>>\
#     update_get_oz_fbs_orders_new_p2_task




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
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import os
os.environ['NO_PROXY'] = 'URL'
from sys import exit


# Проверить таблицу oz_kz_fbs_orders в БД на пропущенные дни за последние 10 дней
def check_oz_kz_fbs_orders(ti, 
                           end_day, 
                           start_day):
    req = f"""select in_process_at
              from ir_db.oz_kz_fbs_orders
              where in_process_at between '{start_day}' 
              and '{end_day}'
              order by in_process_at DESC;"""
    try:
        connection, cursor = mwm.conn()
        cursor.execute(req)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        connection.close()
        cursor.close()
    except:
        print('error')
        exit()
    if len(df[0].unique()) != 10:
        print('В таблице oz_kz_fbs_orders пропущены дни, нужно обновить')
        ti.xcom_push(key='update_marker', value=True)
        dt_value_df = ', '.join([x.strftime('%d-%B-%Y') for x in list(df[0].unique())])
        ti.xcom_push(key='dt_value', value=dt_value_df)
    else:
        ti.xcom_push(key='update_marker', value=False)
        
#  Получить данные по api озона за последние 10 дней в случае пропусков в БД
def get_oz_fbs_orders_new_p1(ti,
                             end_day, 
                             start_day):
    update_marker = ti.xcom_pull(key='update_marker', task_ids=['check_oz_kz_fbs_orders'])[0]
    if update_marker == True:
        try:
            dm.get_oz_fbs_orders_new(account='KZ',
                                     date_to=datetime.datetime.strptime(end_day, '%Y-%m-%d').date()\
                                     +datetime.timedelta(days=1),
                                     last_date=datetime.datetime.strptime(start_day, '%Y-%m-%d').date())
            ti.xcom_push(key='get_marker_1', value=True)
        except:
            print('error')
            exit()
    else:
        ti.xcom_push(key='get_marker_1', value=False)

# Загрузить полученные данные в случае пропусков
def update_oz_fbs_orders_new_p1(ti,
                                    schema):
    update_marker = ti.xcom_pull(key='update_marker', task_ids=['check_oz_kz_fbs_orders'])[0]
    if update_marker == True:
        get_marker_1 = ti.xcom_pull(key='get_marker_1', task_ids=['get_oz_fbs_orders_new_p1'])[0]
        if get_marker_1 == True:
            try:
                oz_kz_fbs_orders_new = pd.read_excel(mwm.path_FD + 'oz_kz_fbs_orders_new.xlsx').fillna(0)
            except:
                print('Загрузка oz_kz_fbs_orders_new завершилась с ошибкой')
                exit()
            if oz_kz_fbs_orders_new.empty == True:
                print('Датафрейм пустой')
                exit()
            dt_value_df = ti.xcom_pull(key='dt_value', task_ids=['check_oz_kz_fbs_orders'])[0].split(', ')
            for dt in [y.strftime('%d-%B-%Y') for y in 
                       sorted([pd.to_datetime(x) for x in list(oz_kz_fbs_orders_new['in_process_at'].unique())])]:
                if dt not in dt_value_df:
                    mwm.insert_values(data=oz_kz_fbs_orders_new[oz_kz_fbs_orders_new['in_process_at']==dt]\
                                           .reset_index(drop=True), 
                                      table_name='oz_kz_fbs_orders', 
                                      schema=schema)
        else:
            None
    else:
        None


# Получить данные за последнее время
def get_oz_fbs_orders_new_p2(ti):
    try:
        dm.get_oz_fbs_orders_new(account='KZ')
        ti.xcom_push(key='get_marker_2', value=True)
    except:
        print('error')
        exit()


# Обновить-загрузить данные за последнее время
def update_oz_fbs_orders_new_p2(ti,
                                    schema):
    get_marker_2 = ti.xcom_pull(key='get_marker_2', task_ids=['get_oz_fbs_orders_new_p2'])[0]
    if get_marker_2 == True:
        try:
            oz_kz_fbs_orders_new = pd.read_excel(mwm.path_FD + 'oz_kz_fbs_orders_new.xlsx').fillna(0)
        except:
            print('Загрузка oz_kz_fbs_orders_new завершилась с ошибкой')
            exit()
        if oz_kz_fbs_orders_new.empty == True:
            print('Датафрейм пустой')
            exit()
        for dt in [y.strftime('%d-%B-%Y') for y in 
                   sorted([pd.to_datetime(x) for x in list(oz_kz_fbs_orders_new['in_process_at'].unique())])]:
            try:
                mwm.update_data_date_filter(data=oz_kz_fbs_orders_new[oz_kz_fbs_orders_new['in_process_at']==dt]\
                                                 .reset_index(drop=True),
                                            table_name='oz_kz_fbs_orders',
                                            date_column='in_process_at',
                                            schema=schema)
            except:
                print(f'День {dt} не обновлен')
                exit()
    else:
        exit()





    
with DAG(
         dag_id='oz_kz_fbs_orders_dag',
         default_args=mwm.args,
         schedule_interval=const.sched_int['oz_kz_fbs_orders_dag'],
         start_date=datetime.datetime(2024, 1, 16),
         catchup=False,
         dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:

  
    check_oz_kz_fbs_orders_task = PythonOperator(
                                                 task_id='check_oz_kz_fbs_orders',
                                                 python_callable=check_oz_kz_fbs_orders,
                                                 op_kwargs={'schema':mwm.ir_schema,
                                                            'end_day': const.end_day,
                                                            'start_day': const.start_day},
                                                 dag=dag
                                                )
    
    
    get_oz_fbs_orders_new_p1_task = PythonOperator(
                                                       task_id='get_oz_fbs_orders_new_p1',
                                                       python_callable=get_oz_fbs_orders_new_p1,
                                                       op_kwargs={'schema':mwm.ir_schema,
                                                                  'end_day': const.end_day,
                                                                  'start_day': const.start_day},
                                                       dag=dag
                                                      )
    
    
    update_oz_fbs_orders_new_p1_task = PythonOperator(
                                                          task_id='update_oz_fbs_orders_new_p1',
                                                          python_callable=update_oz_fbs_orders_new_p1,
                                                          op_kwargs={'schema':mwm.ir_schema},
                                                          dag=dag
                                                         )
    
    
    get_oz_fbs_orders_new_p2_task = PythonOperator(
                                                       task_id='get_oz_fbs_orders_new_p2',
                                                       python_callable=get_oz_fbs_orders_new_p2,
                                                       dag=dag
                                                      )
    
    
    update_oz_fbs_orders_new_p2_task = PythonOperator(
                                                          task_id='update_oz_fbs_orders_new_p2',
                                                          python_callable=update_oz_fbs_orders_new_p2,
                                                          op_kwargs={'schema':mwm.ir_schema},
                                                          dag=dag
                                                         )
    
    check_oz_kz_fbs_orders_task>>\
    get_oz_fbs_orders_new_p1_task>>\
    update_oz_fbs_orders_new_p1_task>>\
    get_oz_fbs_orders_new_p2_task>>\
    update_oz_fbs_orders_new_p2_task