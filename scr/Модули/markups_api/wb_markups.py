import psycopg2
import pandas as pd
from sys import exit
def creating_calculated_features(art, price):
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
    data = pd.DataFrame(rows)
    data.columns = ['Артикул', 'Процент комиссии', 'Стоимость логистики', 'Себестоимость']
    data = data.drop_duplicates().reset_index(drop=True)
    if len(data) - len(list(set(data['Артикул']))) != 0:
        Q = []
        for i in range(len(data)):
            art = data.loc[i]['Артикул']
            if len(data[data['Артикул']==art]) > 1:
                for row in list(data[data['Артикул']==art].sort_values('Себестоимость').index[:-1]):
                    Q.append(row)
        Q = list(set(Q))
        data, Q = data.query('index not in @Q').reset_index(drop=True), None
    if len(data) - len(list(set(data['Артикул']))) != 0:
        print('Не уникальные артикулы')
        exit()
    
    if art in list(data['Артикул']):
        data = data[data['Артикул']==art].reset_index(drop=True)
        data['Цена'] = price
        data['Комиссия в рублях'] = (data['Цена'] / 100) * data['Процент комиссии']
        data['НДС'] =  ((data['Цена'] * 0.2 / 1.2) - (data['Себестоимость'] * 0.2 / 1.2) - 
                        (data['Комиссия в рублях'] * 0.2 / 1.2) - 
                        (data['Стоимость логистики'] * 0.2 / 1.2))
        R = ((data['Цена'] - (data['Цена'] * 0.2 / 1.2)) - 
             (data['Себестоимость'] - (data['Себестоимость'] * 0.2 / 1.2)) - 
             (data['Комиссия в рублях'] - (data['Комиссия в рублях'] * 0.2 / 1.2)) - 
             (data['Стоимость логистики'] - (data['Стоимость логистики'] * 0.2 / 1.2)))
        data['Налог на прибыль'] = R*0.2
        data['Налог'] = data['НДС'] + data['Налог на прибыль']
        data['Прибыль'] = (data['Цена'] - 
                           data['Себестоимость'] - 
                           data['Комиссия в рублях'] - 
                           data['Стоимость логистики'] - 
                           data['Налог'])
        data['Наценка'] = (data['Прибыль'] / data['Себестоимость']) * 100
        data['Процент налога'] = (data['Налог'] * 100) / data['Цена']
        for i in range(len(data)):
            if data['Цена'][i] <= 0:
                data['Налог на прибыль'][i] = 0
                data['Прибыль'][i] = 0
                data['Процент налога'][i] = 0
                data['Процент налога'][i] = 0
        data['Наценка'] = data['Наценка'].round(2)
        return list(data[['Артикул', 'Наценка']].values[0])
    else:
        return f'Для артикула {art} нет расчетных параметров в базе'