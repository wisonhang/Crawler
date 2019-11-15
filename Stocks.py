import tushare as ts
import os 
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
import urllib
import csv
import datetime
import json
import sqlite3


def historical_intinal():
    con=sqlite3.connect('stocks_database.db')
    stock_info=ts.get_stock_basics()
    for code in stock_info.index:
        try:
            temp=ts.get_k_data(code,autype='qfq',start='2006-01-01',pause=1)
            temp['date']=temp['date'].map(lambda x: dt.strptime(x,'%Y-%m-%d'))
            temp.to_sql(code,con,if_exists='replace',index=False)
        except:
            print(code)
            next
            
            
def historical_update():
    con=sqlite3.connect('stocks_database.db')
    cursor = con.cursor()
    cursor=cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables=list(cursor.fetchall())
    weekday=dt.today().weekday()
    hour=dt.today().hour
    if weekday<5 & hour <=15 & hour>=9:
        print('It is trading time, please update later')
        return  
    for code_name in tables:
        code=code_name[0]
        query='select * from "code" order by date desc limit 5'
        data=pd.read_sql(query.replace('code',code),con)
        start=data.date.head(1).values[0]
        try:
            temp=ts.get_k_data(code,autype='qfq',start=start)
            temp['date']=temp['date'].map(lambda x: dt.strptime(x,'%Y-%m-%d'))
        except Exception as e:
            print(e)
            return
        else:   
            if start in set(temp['date'].astype(str)):
                if data.close.head(1)[0]==temp.close.iloc[0]:
                    temp=temp.drop(temp.index[0])
                    temp.to_sql(code,con,if_exists='append',index=False)
                else:
                    temp=ts.get_k_data(code,autype='qfq',start='2006-01-01')
                    temp['date']=temp['date'].map(lambda x: dt.strptime(x,'%Y-%m-%d'))
                    temp.to_sql(code,con,if_exists='replace',index=False)
            elif len(temp)>0:
                temp.to_sql(code,con,if_exists='append',index=False)
                print(code,start)
                
                
                
historical_update()