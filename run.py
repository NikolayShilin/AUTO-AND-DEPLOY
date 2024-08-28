import os
import configparser #чтобы прочитать данные из congig.ini

import pandas as pd
from datetime import datetime, timedelta

from yahoo_fin.stock_info import get_data

from pgdb import PGDatabase

dirname = os.path.dirname(__file__)

config = configparser.ConfigParser()
config.read(os.path.join(dirname, 'config.ini'))

SALES_PATH = config["Files"]["SALES_PATH"]
COMPANIES = eval(config['Companies']['COMPANIES'])
DATABASE_CREDS = config['Database']

sales_df = pd.DataFrame()
#существует ли файл проверка
if os.path.exists(SALES_PATH):
    sales_df = pd.read_csv(SALES_PATH)
    os.remove(SALES_PATH)


historical_dict = {}

for company in COMPANIES:
    historical_dict[company] = get_data(company, 
                                        start_date=(datetime.today()-timedelta(days=1)).strftime('%m/%d/%Y'), 
                                        end_date=datetime.today()).reset_index()

#перед тем как закинуть данные в бд, создаем экземпляр
database = PGDatabase(
    host=DATABASE_CREDS['HOST'],
    database=DATABASE_CREDS['DATABASE'],
    user=DATABASE_CREDS['USER'],
    password=DATABASE_CREDS['PASSWORD']
)
    
#теперь закидываем данные в базу, автоматизируем этот процесс
for i, row in sales_df.iterrows():
    query = f"insert into sales values('{row['dt']}', '{row['company']}', '{row['transaction_type']}', {row['amount']})"
    database.post(query)  
    
for company, data in historical_dict.items():
    for i, row in data.iterrows():
        query = f"insert into stock values ('{row['index']}', '{row['ticker']}', {row['open']}, {row['close']})"   
        database.post(query)