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
import re
os.environ['NO_PROXY'] = 'URL'
from sys import exit

def letter_substitution(data):
    
    data = data.rename(columns={data.columns[0]: 'Артикул'})
    data['Артикул'] = data['Артикул'].astype(str)
    data = data.reset_index(drop=True)
    
    data['Артикул'] = [x.replace(' ', '') for x in list(data['Артикул'])]
    
    fa = list(filter(lambda x: re.compile('[^а-яА-Я ]').sub('', x), list(data['Артикул'])))

    if len(fa) > 0:

        Q = {'е': 'e', 
             'Е': 'E', 
             'Т': 'T', 
             'у': 'y', 
             'о': 'o', 
             'О': 'O', 
             'р': 'p', 
             'Р': 'P', 
             'а': 'a', 
             'А': 'A', 
             'Н': 'H',
             'К': 'K',
             'к': 'k',
             'х': 'x',
             'Х': 'X',
             'с': 'c',
             'С': 'C',
             'В': 'B',
             'М': 'M'}

        for i in range(len(data)):

            for row in list(Q.keys()):

                if row in data.loc[i]['Артикул']:

                    data.loc[i, 'Артикул'] = data.loc[i]['Артикул'].replace(row, Q[row])


    fa2 = list(filter(lambda x: re.compile('[^а-яА-Я ]').sub('', x), list(data['Артикул'])))

    if len(fa2) == 0:

        return data
    
def RC_stocks_update():
    if mwm.dt_now.date().isoweekday() != 6 or mwm.dt_now.date().isoweekday() != 7:
        try:
            rc2 = mwm.select_table_with_last_date_sort_list('ir_wh_stocks', 'Дата_обновления')
            if pd.to_datetime(rc2['Дата_обновления'].unique()[0])\
            .strftime('%d-%B-%Y') != pd.to_datetime(datetime.datetime.now())\
            .strftime('%d-%B-%Y'):
                print('РЦ в БД не обновлен')
                int('РЦ в БД не обновлен')
            else:
                None          
            rc2 = letter_substitution(rc2)
            rc_irc2 = rc2[rc2['Склад']=='Склад Иркутск'].copy()
            rc_irc2 = rc_irc2.groupby('Артикул')['Всего_Доступно'].sum().reset_index(drop=False)
            rc_irc2.columns = ['Артикул', 'Остаток РЦ Иркутск']
            # rc2 = rc2[rc2['Склад'].isin(['Склад некондиции Химки', 'Склад Иркутск', 'Склад брака Химки', 'Склад брака и некондиции Иркутск'])==False]
            rc2 = rc2[rc2['Склад'].isin(['СкладХимки Ячеистый'])]
            vr2 = rc2[['Артикул', 'Сейчас_Отгружается', 'Сейчас_В_резерве']].copy()
            vr2 = vr2.groupby('Артикул')[['Сейчас_Отгружается', 'Сейчас_В_резерве']].sum().reset_index(drop=False)
            vr2['В резерве'] = vr2['Сейчас_Отгружается'] + vr2['Сейчас_В_резерве']
            vr2 = vr2[['Артикул', 'В резерве']]
            vr2['В резерве'] = [None if x == 0 else x for x in list(vr2['В резерве'])]
            rc2 = rc2.groupby('Артикул')['Сейчас_В_наличии'].sum().reset_index(drop=False)
            rc2.columns = ['Артикул', 'Остаток на РЦ']

            if len(rc2) - len(list(set(rc2['Артикул']))) == 0:
                if len(vr2) - len(list(set(vr2['Артикул']))) == 0:
                    rc2.to_excel(path_algoritm + 'ОБЩИЕ ПРИЗНАКИ ДЛЯ МЕТРИК/РЦ/РЦ_обработанные.xlsx', index=False)
                    rc2.to_excel(path_downloads + 'Дима/' + 'РЦ_обработанные.xlsx', index=False)
                    rc2.to_excel(path_algoritm + 'WB_ORDERS/РЦ_обработанные.xlsx', index=False)
                    vr2.to_excel(path_algoritm + 'ОБЩИЕ ПРИЗНАКИ ДЛЯ МЕТРИК/В резерве/В резерве_обработанные.xlsx', index=False)
                    rc_irc2.to_excel(path_downloads + 'РЦ Иркутск.xlsx', index=False)
                else:
                    print('Есть дубли ')
            else:
                print('Есть дубли ')
        except:
            print('error')
            exit()
    else:
        None
        
        
with DAG(
         dag_id='RC_stocks_dag',
         default_args=mwm.args,
         schedule_interval=const.sched_int['RC_stocks_dag'],
         start_date=datetime.datetime(2024, 1, 16),
         catchup=False,
         dagrun_timeout=datetime.timedelta(minutes=10)
) as dag:
    


  
    RC_stocks_update_task = PythonOperator(
                                               task_id='RC_stocks_update',
                                               python_callable=RC_stocks_update,
                                               dag=dag
                                               )
    
    
    RC_stocks_update_task