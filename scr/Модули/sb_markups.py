import psycopg2
import pandas as pd
from sys import exit
import requests
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
import constants_for_marketplace_metrics_dags as const
import maslow_working_module as mwm
import working_with_marketplace_metrics_dags as wwm


def get_param_data():
    
    bd_param = {'host': '45.90.32.77',
                'port': 5432,
                'user': 'ir',
                'password': 'NBBTwzCpY46b',
                'dbname': 'ipolyakov_db'}

    connection = psycopg2.connect(dbname=bd_param['dbname'], 
                                  user=bd_param['user'], 
                                  password=bd_param['password'], 
                                  host=bd_param['host'])
    cursor = connection.cursor()

    req = """
          with
          cp as (
          SELECT 
          Артикул,
          coalesce(Себестоимость, 0) as Себестоимость
          FROM ir_db.cost_price
          WHERE date = (SELECT date 
                        FROM ir_db.cost_price
                        ORDER BY date DESC 
                        LIMIT 1)
          and Себестоимость > 0
          order by Артикул
          ),
          tr as (
          select * 
          from ir_db.sb_tariff_rate)
          select 
          cp.Артикул, 
          Себестоимость, 
          coalesce(tariff_rate, 0) as tariff_rate
          from cp left join tr 
          on cp.Артикул = tr.Артикул;
          """
    cursor.execute(req)
    rows = cursor.fetchall()
    connection.close()
    cursor.close()
    data = pd.DataFrame(rows)
    data.columns = ['Артикул', 'Себестоимость', 'тарифная ставка']
    data = data.drop_duplicates().reset_index(drop=True)
    data['Артикул'] = data['Артикул'].astype(str)
    data['Себестоимость'] = data['Себестоимость'].astype(float)
    data['тарифная ставка'] = data['тарифная ставка'].astype(float)
    if len(data) - len(list(set(data['Артикул']))) != 0:
        print('error')
        exit()
        
    return data
    
    
def creating_calculated_features(art_list, price_list, data=get_param_data(), nds_koeff=0.2, flag_profit=False):
    
    if len(price_list) != len(art_list):
        print('Длины списка артикулов и списка цен не идентичны')
        exit()
    data_price = pd.DataFrame(list(zip(art_list, price_list)), columns=['Артикул','Цена'])
    
    for art in art_list:
        if art not in list(data['Артикул']):
            print(f"Артикула {art} нет в таблице с параметрами для расчета наценки")
            exit()
            
    data = data[data['Артикул'].isin(art_list)].reset_index(drop=True)
    data = data.merge(data_price, on='Артикул', how='inner')
    
    def log(y):
        s = None
        if data.loc[y]['Комиссия за логистику, %'] < 30:
            s = 30
        elif data.loc[y]['Комиссия за логистику, %'] > 280:
            s = 280
        else:
            s = data.loc[y]['Комиссия за логистику, %']

        return s
    
    def dost_poc(y):
        s = None
        if data.loc[y]['Комиссия за доставку, %'] < 30:
            s = 30
        elif data.loc[y]['Комиссия за доставку, %'] > 215:
            s = 280
        else:
            s = data.loc[y]['Комиссия за доставку, %']

        return s
    
    data['тарифная ставка в рублях'] = (data['Цена'] / 100) * data['тарифная ставка']
    data['Комиссия за логистику, %'] = (data['Цена'] / 100) * 1
    data['Логистика'] = list(map(lambda x: log(x), list(data.index)))
    data['Комиссия за доставку, %'] = (data['Цена'] / 100) * 4
    data['Доставка до покупателя'] = list(map(lambda x: dost_poc(x), list(data.index)))
    data['Итого логистика'] = data['Логистика'] + data['Доставка до покупателя'] + 10
    data['обработка платежей'] = (data['Цена'] / 100) * 1.5
    data['Цена_ндс'] = data['Цена'] * nds_koeff / 1.2
    data['Себестоимость_ндс'] = data['Себестоимость'] * nds_koeff / 1.2
    data['тарифная ставка в рублях_ндс'] = data['тарифная ставка в рублях'] * nds_koeff / 1.2
    data['Итого логистика_ндс'] = data['Итого логистика'] * nds_koeff / 1.2
    data['обработка платежей_ндс'] = data['обработка платежей'] * nds_koeff / 1.2
    data['ндс'] = data['Цена_ндс'] \
                  - data['Себестоимость_ндс'] \
                  - data['тарифная ставка в рублях_ндс'] \
                  - data['Итого логистика_ндс'] \
                  - data['обработка платежей_ндс']
    data['Цена_ннп'] = data['Цена'] - data['Цена_ндс']
    data['Себестоимость_ннп'] = data['Себестоимость'] - data['Себестоимость_ндс']
    data['тарифная ставка в рублях_ннп'] = data['тарифная ставка в рублях'] - data['тарифная ставка в рублях_ндс']
    data['Итого логистика_ннп'] = data['Итого логистика'] - data['Итого логистика_ндс']
    data['обработка платежей_ннп'] = data['обработка платежей'] - data['обработка платежей_ндс']
    data['налог на пр'] = (data['Цена_ннп'] \
                           - data['Себестоимость_ннп'] \
                           - data['тарифная ставка в рублях_ннп'] \
                           - data['Итого логистика_ннп'] \
                           - data['обработка платежей_ннп']) * nds_koeff
    data['итого налог'] = data['ндс'] + data['налог на пр']
    data['% налога'] = data['итого налог'] * 100 / data['Цена']
    data['прибыль'] = data['Цена'] \
                      - data['тарифная ставка в рублях'] \
                      - data['Итого логистика'] \
                      - data['обработка платежей'] \
                      - data['итого налог'] - data['Себестоимость']
    data['Наценка'] = data['прибыль'] * 100 / data['Себестоимость']
    

    
    if flag_profit == False:
        return [list(x) for x in list(data[['Артикул', 'Наценка']].values)]
    else:
        return [list(x) for x in list(data[['Артикул', 'Наценка', 'Прибыль']].values)]  

def return_price(art_list, default_nc=100):
    data = get_param_data()
    for art in art_list:
        if art not in list(data['Артикул']):
            print(f"Артикула {art} нет в таблице с параметрами для расчета наценки")
            exit()
    SB_pr = {}
    for art in art_list:
        prices_list = list(range(50, 5000, 100))
        for price in prices_list:
            nc =  creating_calculated_features(art_list=[art], price_list=[price], data=data)[0][1]
            if nc > default_nc:
                break
        if nc > default_nc:
            while nc > default_nc:
                price-=1
                nc = creating_calculated_features(art_list=[art], price_list=[price], data=data)[0][1]
            price = price+1
        else:
            while nc < default_nc+1:
                price+=1
                nc = creating_calculated_features(art_list=[art], price_list=[price], data=data)[0][1]
            price = price-1
        SB_pr[art] = price
    return SB_pr