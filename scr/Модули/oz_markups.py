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
    req = """select x.Артикул, 
                    Объем_товара_л, 
                    Комиссия_в__fbs, 
                    Комиссия_в_процентах, 
                    Эквайринг, 
                    Себестоимость,
                    oz_general_localization_index,
                    oz_shipment_processing
             from
             (SELECT * 
              FROM ir_db.oz_params_for_markups
              WHERE date = (SELECT date 
                            FROM ir_db.wb_params_for_markups
                            ORDER BY date DESC 
                            LIMIT 1)) as x
              inner join             
              (SELECT * 
              FROM ir_db.cost_price
              WHERE date = (SELECT date 
                            FROM ir_db.cost_price
                            ORDER BY date DESC 
                            LIMIT 1)) as y on x.Артикул = y.Артикул
              where Себестоимость > 0
              order by x.Артикул;"""
    cursor.execute(req)
    rows = cursor.fetchall()
    connection.close()
    cursor.close()
    data = pd.DataFrame(rows)
    data.columns = ['Артикул', 'Объем товара, л', 'Комиссия в % fbs', 'Комиссия в %', 
                    'Эквайринг', 'Себестоимость', 'oz_general_localization_index', 'oz_shipment_processing']
    if len(data) - len(list(set(data['Артикул']))) != 0:
        Q = []
        for i in range(len(data)):
            A = data.loc[i]['Артикул']
            if len(data[data['Артикул']==A]) > 1:
                for row in list(data[data['Артикул']==A].sort_values('Себестоимость').index[:-1]):
                    Q.append(row)
        Q = list(set(Q))
        data, Q = data.query('index not in @Q').reset_index(drop=True), None
    if len(data) - len(list(set(data['Артикул']))) != 0:
        print('Не уникальные артикулы')
        exit()

    req = """
    select x.article_name,
    round(rec / coalesce (orders, (1)))::int as reclama
    from
    (select article_name, 
    auto_advertising_costs + search_advertising_costs as rec
    from ir_db.marketing_statistic
    where marketplace = 'OZON'
    and date = (select distinct date::DATE as date
    from ir_db.marketing_statistic
    where marketplace = 'OZON'
    order by date::DATE desc
    limit 1)
    and auto_advertising_costs + search_advertising_costs > 0) as x
    left join 
    (select item_code as article_name,
    orders
    from dds_ir_db.dds_oz_orders
    where item_code is not null 
    and orders is not null 
    and orders > 0
    and date::DATE = (select distinct date::DATE as date
    from ir_db.marketing_statistic
    where marketplace = 'OZON'
    order by date::DATE desc
    limit 1)) as y
    on x.article_name = y.article_name;
    """

    data_rec = mwm.select_table_from_request(req).rename(columns={0: 'Артикул', 1: 'Реклама'})
    mwm.unique_items(data_rec)

    data = data.merge(data_rec, on='Артикул', how='left').fillna(0)
    data['Реклама'] = data['Реклама'].astype(int)

    return data
        
        
def calc_features(art_list, price_list, data=get_param_data(), flag='fbo', flag_profit=False, nds_koeff=0.2, flag_reclama=False):
    
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
    
    def last_mile(data):
        if 'Последняя миля' not in data:
            R = []
            for i in range(len(data)):
                if data['Цена'][i] * 0.055 <= 20:
                    R.append(20)
                elif data['Цена'][i] * 0.055 >= 500:
                    R.append(500)
                else:
                    R.append(data['Цена'][i] * 0.055)
            data['Последняя миля'] = R
        else:
            None
        return data
    

    def oz_general_localization_index_discount(oz_general_localization_index):
        oz_general_localization_index = oz_general_localization_index - 5
        if oz_general_localization_index >= 0 and oz_general_localization_index <= 59:
            discount = 1.2
        elif oz_general_localization_index >= 60 and oz_general_localization_index <= 64:
            discount = 1.1
        elif oz_general_localization_index >= 65 and oz_general_localization_index <= 74:
            discount = 1.0
        elif oz_general_localization_index >= 75 and oz_general_localization_index <= 79:
            discount = 0.95
        elif oz_general_localization_index >= 80 and oz_general_localization_index <= 84:
            discount = 0.9
        elif oz_general_localization_index >= 85 and oz_general_localization_index <= 89:
            discount = 0.85
        elif oz_general_localization_index >= 90 and oz_general_localization_index <= 94:
            discount = 0.8
        elif oz_general_localization_index >= 95:
            discount = 0.5
        else:
            print('Ошибка в oz_general_localization_index')
            exit()
        return discount
    
    def redact_volume(data, flag='fbo', discount=1):
        if flag == 'fbo':
            if 'Объем товара, руб' not in data:
                R = []
                data['Объем товара, л'] = data['Объем товара, л'].round(2)
                for i in range(len(data)):
                    if data['Объем товара, л'][i] >= 0 and data['Объем товара, л'][i] <= 1.9:
                        R.append(58)
                    elif data['Объем товара, л'][i] > 1.9 and data['Объем товара, л'][i] <= 2.9:
                        R.append(61)
                    elif data['Объем товара, л'][i] > 2.9 and data['Объем товара, л'][i] <= 4.9:
                        R.append(63)
                    elif data['Объем товара, л'][i] > 4.9 and data['Объем товара, л'][i] <= 5.9:
                        R.append(67)
                    elif data['Объем товара, л'][i] > 5.9 and data['Объем товара, л'][i] <= 6.9:
                        R.append(69)
                    elif data['Объем товара, л'][i] > 6.9 and data['Объем товара, л'][i] <= 7.9:
                        R.append(71)
                    elif data['Объем товара, л'][i] > 7.9 and data['Объем товара, л'][i] <= 8.4:
                        R.append(73)
                    elif data['Объем товара, л'][i] > 8.4 and data['Объем товара, л'][i] <= 8.9:
                        R.append(75)
                    elif data['Объем товара, л'][i] > 8.9 and data['Объем товара, л'][i] <= 9.4:
                        R.append(76)
                    elif data['Объем товара, л'][i] > 9.4 and data['Объем товара, л'][i] <= 9.9:
                        R.append(77)
                    elif data['Объем товара, л'][i] > 9.9 and data['Объем товара, л'][i] <= 14.9:
                        R.append(85)
                    elif data['Объем товара, л'][i] > 14.9 and data['Объем товара, л'][i] <= 19.9:
                        R.append(111)
                    elif data['Объем товара, л'][i] > 19.9 and data['Объем товара, л'][i] <= 24.9:
                        R.append(126)
                    elif data['Объем товара, л'][i] > 24.9 and data['Объем товара, л'][i] <= 29.9:
                        R.append(141)
                    elif data['Объем товара, л'][i] > 29.9 and data['Объем товара, л'][i] <= 34.9:
                        R.append(166)
                    elif data['Объем товара, л'][i] > 34.9 and data['Объем товара, л'][i] <= 39.9:
                        R.append(191)
                    elif data['Объем товара, л'][i] > 39.9 and data['Объем товара, л'][i] <= 44.9:
                        R.append(216)
                    elif data['Объем товара, л'][i] > 44.9 and data['Объем товара, л'][i] <= 49.9:
                        R.append(231)
                    elif data['Объем товара, л'][i] > 49.9 and data['Объем товара, л'][i] <= 54.9:
                        R.append(271)
                    elif data['Объем товара, л'][i] > 54.9 and data['Объем товара, л'][i] <= 59.9:
                        R.append(296)
                    elif data['Объем товара, л'][i] > 59.9 and data['Объем товара, л'][i] <= 64.9:
                        R.append(321)
                    elif data['Объем товара, л'][i] > 64.9 and data['Объем товара, л'][i] <= 69.9:
                        R.append(356)
                    elif data['Объем товара, л'][i] > 69.9 and data['Объем товара, л'][i] <= 74.9:
                        R.append(376)
                    elif data['Объем товара, л'][i] > 74.9 and data['Объем товара, л'][i] <= 99.9:
                        R.append(406)
                    elif data['Объем товара, л'][i] > 99.9 and data['Объем товара, л'][i] <= 124.9:
                        R.append(531)
                    elif data['Объем товара, л'][i] > 124.9 and data['Объем товара, л'][i] <= 149.9:
                        R.append(706)
                    elif data['Объем товара, л'][i] > 149.9 and data['Объем товара, л'][i] <= 174.9:
                        R.append(906)
                    elif data['Объем товара, л'][i] > 174.9:
                        R.append(1106)
                    else:
                        R.append(None)
                data['Объем товара, руб'] = R 
                data['Объем товара, руб'] = data['Объем товара, руб'] * discount     
            else:
                None
        elif flag == 'fbs': 
            if 'Объем товара, руб' not in data:
                R = []
                data['Объем товара, л'] = data['Объем товара, л'].round(2)
                for i in range(len(data)):
                    if data['Объем товара, л'][i] >= 0 and data['Объем товара, л'][i] <= 1.9:
                        R.append(70)
                    elif data['Объем товара, л'][i] > 1.9 and data['Объем товара, л'][i] <= 2.9:
                        R.append(73)
                    elif data['Объем товара, л'][i] > 2.9 and data['Объем товара, л'][i] <= 4.9:
                        R.append(76)
                    elif data['Объем товара, л'][i] > 4.9 and data['Объем товара, л'][i] <= 5.9:
                        R.append(80)
                    elif data['Объем товара, л'][i] > 5.9 and data['Объем товара, л'][i] <= 6.9:
                        R.append(83)
                    elif data['Объем товара, л'][i] > 6.9 and data['Объем товара, л'][i] <= 7.9:
                        R.append(85)
                    elif data['Объем товара, л'][i] > 7.9 and data['Объем товара, л'][i] <= 8.4:
                        R.append(88)
                    elif data['Объем товара, л'][i] > 8.4 and data['Объем товара, л'][i] <= 8.9:
                        R.append(90)
                    elif data['Объем товара, л'][i] > 8.9 and data['Объем товара, л'][i] <= 9.4:
                        R.append(91)
                    elif data['Объем товара, л'][i] > 9.4 and data['Объем товара, л'][i] <= 9.9:
                        R.append(92)
                    elif data['Объем товара, л'][i] > 9.9 and data['Объем товара, л'][i] <= 14.9:
                        R.append(102)
                    elif data['Объем товара, л'][i] > 14.9 and data['Объем товара, л'][i] <= 19.9:
                        R.append(133)
                    elif data['Объем товара, л'][i] > 19.9 and data['Объем товара, л'][i] <= 24.9:
                        R.append(151)
                    elif data['Объем товара, л'][i] > 24.9 and data['Объем товара, л'][i] <= 29.9:
                        R.append(169)
                    elif data['Объем товара, л'][i] > 29.9 and data['Объем товара, л'][i] <= 34.9:
                        R.append(199)
                    elif data['Объем товара, л'][i] > 34.9 and data['Объем товара, л'][i] <= 39.9:
                        R.append(229)
                    elif data['Объем товара, л'][i] > 39.9 and data['Объем товара, л'][i] <= 44.9:
                        R.append(259)
                    elif data['Объем товара, л'][i] > 44.9 and data['Объем товара, л'][i] <= 49.9:
                        R.append(277)
                    elif data['Объем товара, л'][i] > 49.9 and data['Объем товара, л'][i] <= 54.9:
                        R.append(325)
                    elif data['Объем товара, л'][i] > 54.9 and data['Объем товара, л'][i] <= 59.9:
                        R.append(355)
                    elif data['Объем товара, л'][i] > 59.9 and data['Объем товара, л'][i] <= 64.9:
                        R.append(385)
                    elif data['Объем товара, л'][i] > 64.9 and data['Объем товара, л'][i] <= 69.9:
                        R.append(427)
                    elif data['Объем товара, л'][i] > 69.9 and data['Объем товара, л'][i] <= 74.9:
                        R.append(451)
                    elif data['Объем товара, л'][i] > 74.9 and data['Объем товара, л'][i] <= 99.9:
                        R.append(487)
                    elif data['Объем товара, л'][i] > 99.9 and data['Объем товара, л'][i] <= 124.9:
                        R.append(637)
                    elif data['Объем товара, л'][i] > 124.9:
                        R.append(847)
                    else:
                        R.append(None)
                data['Объем товара, руб'] = R
            else:
                None  
        else:
            None
        return data
    
    def add_settlement_features_new(data, nds_koeff=nds_koeff, flag='fbo', flag_reclama=flag_reclama):
        if flag == 'fbs':
            if 'Обработка' not in list(data.columns):
                data['Обработка'] = data.loc[0]['oz_shipment_processing']
            else:
                None
        else:
            None
        if flag == 'fbs': 
            data['Комиссия в процентах'] = data['Комиссия в % fbs'] 
        elif flag == 'fbo':
            data['Комиссия в процентах'] = data['Комиссия в %']
        else:
            None
        if 'Комиссия' not in list(data.columns):
            data['Комиссия'] = data['Цена'] * data['Комиссия в процентах'] / 100
            for i in range(len(data)):
                if data.loc[i]['Комиссия'] < 0.12:
                    data['Комиссия'].loc[i] = 0.12
        else:
            None
        if flag == 'fbo':   
            if 'Логистика' not in list(data.columns):
                data['Логистика'] = data['Объем товара, руб'] + data['Последняя миля']
            else:
                None
        elif flag == 'fbs':
            if 'Логистика' not in list(data.columns):
                data['Логистика'] = data['Объем товара, руб'] + data['Последняя миля'] + data['Обработка']
            else:
                None
        if 'НДС' not in list(data.columns):
            if flag_reclama == False:
                data['НДС'] = ((data['Цена'] * nds_koeff / 1.2) - 
                               (data['Себестоимость'] * nds_koeff / 1.2) - 
                               (data['Комиссия'] * nds_koeff / 1.2) - 
                               (data['Логистика'] * nds_koeff / 1.2))
            else:
                data['НДС'] = ((data['Цена'] * nds_koeff / 1.2) - 
                               (data['Себестоимость'] * nds_koeff / 1.2) - 
                               (data['Комиссия'] * nds_koeff / 1.2) - 
                               (data['Логистика'] * nds_koeff / 1.2) - 
                               (data['Реклама'] * nds_koeff / 1.2))                
        else:
            None     
        if 'Налог на прибыль'  not in list(data.columns):
            if flag_reclama == False:
                data['Налог на прибыль'] = (((data['Цена'] - (data['Цена'] * nds_koeff / 1.2)) - 
                                             (data['Себестоимость'] - (data['Себестоимость'] * nds_koeff / 1.2)) - 
                                             (data['Комиссия'] - (data['Комиссия'] * nds_koeff / 1.2)) - 
                                             (data['Эквайринг']) - 
                                             (data['Логистика'] - (data['Логистика'] * nds_koeff / 1.2))) * nds_koeff)
            else:
                data['Налог на прибыль'] = (((data['Цена'] - (data['Цена'] * nds_koeff / 1.2)) - 
                                             (data['Себестоимость'] - (data['Себестоимость'] * nds_koeff / 1.2)) - 
                                             (data['Комиссия'] - (data['Комиссия'] * nds_koeff / 1.2)) -
                                             (data['Реклама'] - (data['Реклама'] * nds_koeff / 1.2)) - 
                                             (data['Эквайринг']) - 
                                             (data['Логистика'] - (data['Логистика'] * nds_koeff / 1.2))) * nds_koeff)
        else:
            None       
        if 'Налог' not in list(data.columns):
            data['Налог'] = data['НДС'] + data['Налог на прибыль']
        else:
            None    
        if '% налога' not in list(data.columns):
            data['% налога'] = data['Налог'] * 100 / data['Цена']
        else:
            None    
        if 'Прибыль' not in list(data.columns):   
            if flag_reclama == False:
                 
                data['Прибыль'] = (data['Цена'] - data['Себестоимость'] - 
                                data['Комиссия'] - data['Логистика'] - data['Налог'] - data['Эквайринг'])

            else:
            
                data['Прибыль'] = (data['Цена'] - data['Себестоимость'] - 
                                data['Комиссия'] - data['Логистика'] - data['Налог'] - data['Эквайринг'] - data['Реклама'])

        else:
            None     
        if 'Наценка' not in list(data.columns):
            data['Наценка'] = data['Прибыль'] / data['Себестоимость'] * 100
            data['Наценка'] = data['Наценка'].round(2)
        else:
            None
        return data


    data = redact_volume(data, 
                         flag=flag, 
                         discount=oz_general_localization_index_discount(data.loc[0]['oz_general_localization_index']))
    data = last_mile(data)
    data = add_settlement_features_new(data, nds_koeff=nds_koeff, flag=flag)
    
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
    OZ_pr = {}
    for art in art_list:
        prices_list = list(range(50, 5000, 100))
        for price in prices_list:
            nc =  calc_features(art_list=[art], price_list=[price], data=data)[0][1]
            if nc > default_nc:
                break
        if nc > default_nc:
            while nc > default_nc:
                price-=1
                nc = calc_features(art_list=[art], price_list=[price], data=data)[0][1]
            price = price+1
        else:
            while nc < default_nc+1:
                price+=1
                nc = calc_features(art_list=[art], price_list=[price], data=data)[0][1]
            price = price-1
        OZ_pr[art] = price
    return OZ_pr