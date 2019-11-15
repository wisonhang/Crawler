import tushare as ts
import os 
import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import urllib
import csv
import datetime
import json
import sqlite3
import time
import schedule

def index_download(code=''):
    hostname=url='http://www.csindex.com.cn/uploads/file/autofile/cons/%s'
    dis='/home/dwhang/project/dwhang/Industry_index/content'
    tt=code+'cons.xls'
    os.chdir(dis)
    url=hostname%tt
    try:
        req = urllib.request.Request(url)
        webpage = urllib.request.urlopen(req)  
    except Exception as e:
        print(code +' does not exist')
        pass
    else:
        filename=tt.replace('cons','')
        f=open(filename,'wb')
        ans = webpage.read() 
        f.write(ans)
        f.close()
        temp=pd.read_excel(filename)
        temp=temp.iloc[:,[0,2,4,5]]
        temp.columns=['日期','指数名称','成分券代码','成分券名称']
        temp=temp.set_index('日期')
        temp.to_csv(filename.replace('xls','csv'),encoding='utf-8')
        os.remove(filename)

def get_hist_data(code,b):
    temp=pd.DataFrame()
    try:
        temp=ts.get_k_data(code,index=True)
        #temp['指数名称']=b
        #temp['指数代码']=a
        temp=temp.set_index('date')
        temp.to_csv(dis+code+'.csv',encoding='utf-8')
    except Exception as e:
        print(e)
'''        
def indexdate_update():
    newstart=dt.strptime(pd.read_csv('data/'+
                                           os.listdir('data/')[0])['date'].tail(1).values[0],'%Y-%m-%d')
    if dt.now()<=newstart+timedelta(2):
        print('Already New data')
    else:
        for filename in os.listdir('data/'):
            new_index=ts.get_k_data(filename.replace('.csv',''),index=True,start=str(newstart+timedelta(1+2/3))
            if(new_index.shape[0]==0):
                print('no update data')
                break
            else:
                with open('data/'+filename, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['date', 'open', 'close', 'high', 'low', 'volume', 'code'])
                    writer.writerows(json.loads(new_index.to_json(orient='records')))
                    print('finish update',filename)    
     
'''


def historical_intinal():
    con=sqlite3.connect('index_database.db')
    stock_info=pd.read_csv('index.csv',dtype={'指数代码':str}).set_index('指数代码')
    for code in stock_info.index:
        try:
            temp=ts.get_k_data(code,index=True,start='2006-01-01',pause=1)
            temp['date']=temp['date'].map(lambda x: dt.strptime(x,'%Y-%m-%d'))
            temp.to_sql(code,con,if_exists='replace',index=False)
        except:
            print(code)
            next
            
            
def historical_update():
    con=sqlite3.connect('index_database.db')
    cursor = con.cursor()
    cursor=cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables=list(cursor.fetchall())
    weekday=dt.today().weekday()
    hour=dt.today().hour+8
    if weekday<=5 & hour <=15 & hour>=9:
        print(weekday,hour)
        print('It is trading time, please update later')
        return  
    for code_name in tables:
        code=code_name[0]
        query='select * from "code" order by date desc limit 5'
        data=pd.read_sql(query.replace('code',code),con)
        start=data.date.head(1).values[0]
        try:
            temp=ts.get_k_data(code,index=True,start=start)
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
                    temp=ts.get_k_data(code,index=True,start='2006-01-01')
                    temp['date']=temp['date'].map(lambda x: dt.strptime(x,'%Y-%m-%d'))
                    temp.to_sql(code,con,if_exists='replace',index=False)
            elif len(temp)>0:
                temp.to_sql(code,con,if_exists='append',index=False)
                print(code,start)  
                
                
def historical_update2():
    con=sqlite3.connect('index_database.db')
    cursor = con.cursor()
    cursor=cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables=list(cursor.fetchall())
    weekday=dt.today().weekday()
    hour=dt.today().hour+8
    if weekday<=5 and hour <=15 and hour>=9:
        print(weekday,hour)
        print('It is trading time, please update later')
        #return  
    for code_name in tables:
        time.sleep(10)
        code=code_name[0]
        query='select * from "code" order by date desc limit 5'
        data=pd.read_sql(query.replace('code',code),con)
        start=data.date.head(1).values[0]
        try:
            temp=ts.get_h_data(code,index=True,start=start,pause=10).reset_index().sort_values('date')
            temp['code']=data['code'][0]
            temp['volume']=temp['volume']/100
            temp=temp.drop('amount',axis=1)
        except Exception as e:
            print(e)
            #return
        else:   
            if start in list(temp['date'].map(lambda x: str(x))):
                if np.round(data.close.head(1)[0],2)==np.round(temp.close.iloc[0],2):
                    temp=temp.drop(temp.index[0])
                    temp.to_sql(code,con,if_exists='append',index=False)
                else:
                    temp=ts.get_h_data(code,index=True,start='2006-01-01',pause=100).reset_index().sort_values('date')
                    temp['code']=data['code'][0]
                    temp['volume']=temp['volume']/100
                    temp=temp.drop('amount',axis=1)
                    temp.to_sql(code,con,if_exists='replace',index=False)
            elif len(temp)>0:
                temp.to_sql(code,con,if_exists='append',index=False)
        print(code,start) 
       
                                    
'''        
dis=os.getcwd()+'/data/'
data=pd.read_csv('index.csv',index_col=0,dtype={'指数代码':str})
for a in data['指数代码']:
    get_hist_data(a,b=None)
'''

if __name__ == "__main__":
    #path=os.getcwd()
    #os.chdir(path+'/Data')
    schedule.every().day.at('00:00').do(historical_update)
    while True:
        schedule.run_pending()
        time.sleep(10)

                                    
#historical_update()                                    
                                    
#indexdate_update()