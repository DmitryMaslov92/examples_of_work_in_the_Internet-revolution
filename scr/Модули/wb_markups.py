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
    req = """select z.Артикул, Процент_комиссии, Стоимость_логистики, Себестоимость
    from 
    (SELECT * 
       FROM ir_db.wb_params_for_markups
       WHERE date = (SELECT date 
                     FROM ir_db.wb_params_for_markups
                     ORDER BY date DESC 
                     LIMIT 1)) as x
    inner join 
    (select Артикул_продавца as Артикул ,  Баркод from ir_db.wb_list_of_nomenclatures) as y on x.Баркод = y.Баркод
    inner join             
    (SELECT * 
    FROM ir_db.cost_price
       WHERE date = (SELECT date 
                     FROM ir_db.cost_price
                     ORDER BY date DESC 
                     LIMIT 1)) as z on z.Артикул = y.Артикул
    where Себестоимость > 0
    order by Артикул;"""
    cursor.execute(req)
    rows = cursor.fetchall()
    connection.close()
    cursor.close()
    data = pd.DataFrame(rows)
    data.columns = ['Артикул', 'Процент комиссии', 'Стоимость логистики', 'Себестоимость']
    data = data.drop_duplicates().reset_index(drop=True)
    
    if len(data) - len(list(set(data['Артикул']))) != 0:
        for i in range(len(data)):
            A = data.loc[i]['Артикул']
            if len(data[data['Артикул']==A]) > 1:
                PC = sorted(list(data[data['Артикул']==A]['Процент комиссии']))[-1]
                SL = sorted(list(data[data['Артикул']==A]['Стоимость логистики']))[-1]
                CP = sorted(list(data[data['Артикул']==A]['Себестоимость']))[-1]
                data.loc[i, 'Процент комиссии'] = PC
                data.loc[i, 'Стоимость логистики'] = SL
                data.loc[i, 'Себестоимость'] = CP
        data = data.drop_duplicates().reset_index(drop=True)
        if len(data) - len(list(set(data['Артикул']))) != 0:
            print('error')
            exit()
            
    req = """
    with 
    msdt as (
    select distinct date::DATE as date
    from ir_db.marketing_statistic
    where marketplace = 'WB'
    order by date::DATE desc
    limit 1),
    ms as (
    select article_name::varchar, auto_advertising_costs::float, date::DATE
    from ir_db.marketing_statistic
    where marketplace = 'WB'
    and article_name is not null
    and auto_advertising_costs is not null 
    and auto_advertising_costs > 0
    order by date::DATE desc
    ),
    dds_wb_orders as (
    select article_name,
    case 
        when orders = 0 then 1
        else orders
    end
    from
    (select article_name, sum(orders) as orders
    from
    (select barcode, orders
    from 
    (select distinct date::DATE as date
    from ir_db.marketing_statistic
    where marketplace = 'WB'
    order by date::DATE desc
    limit 1) as x
    left join 
    (select *
    from dds_ir_db.dds_wb_orders
    where account = 'ir') as y
    on x.date=y.date) as z
    left join 
    (select * 
    from ir_db.article_barcodes) as ab
    on z.barcode = ab.barcode
    group by article_name)
    )
    select 
    ms.article_name,
    round(auto_advertising_costs / coalesce (orders, (1)))::int as reclama
    from ms
    left join dds_wb_orders
    on ms.article_name = dds_wb_orders.article_name
    where date = (select date from msdt);
    """
    data_rec = mwm.select_table_from_request(req).rename(columns={0: 'Артикул', 1: 'Реклама'})
    mwm.unique_items(data_rec)
    
    data = data.merge(data_rec, on='Артикул', how='left').fillna(0)
    data['Реклама'] = data['Реклама'].astype(int)
    
    return data
    
    
def creating_calculated_features(art_list, price_list, data=get_param_data(), nds_koeff=0.2, flag_profit=False, 
                                 flag_reclama=False):
    
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
    data['Комиссия в рублях'] = (data['Цена'] / 100) * (data['Процент комиссии'] + 3)
    data['Эквайринг'] = data['Цена'] / 100 * 0.75

    if flag_reclama == False:
        data['НДС'] =  ((data['Цена'] * nds_koeff / 1.2) - (data['Себестоимость'] * nds_koeff / 1.2) - 
                        (data['Комиссия в рублях'] * nds_koeff / 1.2) - 
                        (data['Стоимость логистики'] * nds_koeff / 1.2))
    else:
        data['НДС'] =  ((data['Цена'] * nds_koeff / 1.2) - (data['Себестоимость'] * nds_koeff / 1.2) - 
                        (data['Комиссия в рублях'] * nds_koeff / 1.2) -
                        (data['Реклама'] * nds_koeff / 1.2) - 
                        (data['Стоимость логистики'] * nds_koeff / 1.2))

    if flag_reclama == False:   
        
        R = ((data['Цена'] - (data['Цена'] * nds_koeff / 1.2)) - 
            (data['Себестоимость'] - (data['Себестоимость'] * nds_koeff / 1.2)) - 
            (data['Комиссия в рублях'] - (data['Комиссия в рублях'] * nds_koeff / 1.2)) - 
            (data['Стоимость логистики'] - (data['Стоимость логистики'] * nds_koeff / 1.2))-
            (data['Эквайринг']))
    else:
        R = ((data['Цена'] - (data['Цена'] * nds_koeff / 1.2)) - 
            (data['Себестоимость'] - (data['Себестоимость'] * nds_koeff / 1.2)) - 
            (data['Комиссия в рублях'] - (data['Комиссия в рублях'] * nds_koeff / 1.2)) - 
            (data['Реклама'] - (data['Реклама'] * nds_koeff / 1.2)) - 
            (data['Стоимость логистики'] - (data['Стоимость логистики'] * nds_koeff / 1.2))-
            (data['Эквайринг']))
    
    data['Налог на прибыль'] = R*nds_koeff
    data['Налог'] = data['НДС'] + data['Налог на прибыль']
    
    if flag_reclama == False:
        
        data['Прибыль'] = (data['Цена'] - 
                           data['Себестоимость'] - 
                           data['Комиссия в рублях'] - 
                           data['Стоимость логистики'] - 
                           data['Налог']-
                           data['Эквайринг'])
        
    else:
        
        data['Прибыль'] = (data['Цена'] - 
                           data['Себестоимость'] - 
                           data['Комиссия в рублях'] - 
                           data['Стоимость логистики'] - 
                           data['Налог']-
                           data['Эквайринг']-
                           data['Реклама'])
        
    
    data['Наценка'] = (data['Прибыль'] / data['Себестоимость']) * 100
    data['Процент налога'] = (data['Налог'] * 100) / data['Цена']
    for i in range(len(data)):
        if data['Цена'][i] <= 0:
            data['Налог на прибыль'][i] = 0
            data['Прибыль'][i] = 0
            data['Процент налога'][i] = 0
            data['Процент налога'][i] = 0
    data['Наценка'] = data['Наценка'].round(2)
    
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
    WB_pr = {}
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
        WB_pr[art] = price
    return WB_pr