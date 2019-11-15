from pyquery import PyQuery as pq
import pandas as pd
import numpy as np
import urllib
from urllib.request import urlopen, Request
#urllib.request.Request(url)
import time
import json
from datetime import datetime,date
from tushare.stock import cons as ct
import os 
import threading
import sqlite3
from bs4 import BeautifulSoup
import re

lock = threading.Lock()
class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):  
        self.lock = threading.RLock() #锁  
        self.path = path #数据库连接参数  

        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)

    def get_conn(self):  
        conn = sqlite3.connect(self.path)#,check_same_thread=False)  
        conn.text_factory=str
        return conn   

    def conn_close(self,conn=None):  
        conn.close()  

    def conn_trans(func):  
        def connection(self,*args,**kwargs):  
            self.lock.acquire()  
            conn = self.get_conn()  
            kwargs['conn'] = conn  
            rs = func(self,*args,**kwargs)  
            self.conn_close(conn)  
            self.lock.release()  
            return rs  
        return connection  

    @conn_trans    
    def execute(self,command,method_flag=0,conn=None):  
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except sqlite3.IntegrityError as e:
            print (e)
            return -1
        except Exception as e:
            print (e)
            return -2
        return 0

    @conn_trans
    def fetchall(self,command="select name from xiaoqu",conn=None):
        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception as e:
            print (e)
            pass
        return lists

    
def gen_fundmental_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list=[ '基金代码','基金全称','基金简称', '基金类型',  '份额规模', '资产规模',
        #'基金经理','起始期', '截止期','任职期间','任职回报', 
        '业绩比较基准','跟踪标的','发行日期', '成立日期/规模','成立来分红',
        '基金托管人','托管费率', '基金管理人','管理费率', '销售服务费率']
    #info_list=[]
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into fundmental_info values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command 
 
def gen_manager_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list=['起始期','截止期','基金经理','任职期间','任职回报','基金代码']
    #info_list=[]
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into fund_managers values(?,?,?,?,?,?)",t)
    return command 

####################basic fund level data ##################################    
def get_fund_id(tp='all',retry_count=3,pause=0.001):
    '''
    tp: string
    ['ETF-场内', 'QDII', 'QDII-ETF', 'QDII-指数', '保本型', '债券创新-场内', '债券型',
       '债券指数', '其他创新', '分级杠杆', '固定收益', '定开债券', '封闭式', '混合型', '理财型', '联接基金',
       '股票型', '股票指数', '货币型']
    '''
    url='http://fund.eastmoney.com/js/fundcode_search.js'
    datatype=['ETF-场内', 'QDII', 'QDII-ETF', 'QDII-指数', '保本型', '债券创新-场内', '债券型',
       '债券指数', '其他创新', '分级杠杆', '固定收益', '定开债券', '封闭式', '混合型', '理财型', '联接基金',
       '股票型', '股票指数', '货币型']
    if (tp!='all') & (tp not in datatype):
        print('input type error')
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            request = Request(url)
            lines = urlopen(request, timeout = 10).read().decode('utf-8')
        except Exception as e:
            print(e)
        else:
            lines='{"data":'+lines[lines.index('[['):lines.rindex(']]')+2]+'}'
            lines=json.loads(lines)
            data=pd.DataFrame(lines['data'],columns=['代码','缩写','名称','基金类型','基金拼写'])
            data=data.sort_values('名称')
            data=data.drop_duplicates(subset='缩写')
            data=data.sort_values('代码')
            del data['基金拼写']
            if  tp=='all':
                return(data)
            elif tp in datatype:
                data=data[data['基金类型']==tp]
                data.index=range(0,data.shape[0])
                return(data)
    raise IOError(ct.NETWORK_URL_ERROR_MSG)
    
    
def get_fund_fundmental(code,db=None,retry_count=3,pause=0.01):
    tempdata=None
    temp=None
    url='http://fund.eastmoney.com/f10/jbgk_%s.html'% code
    #jjjl_url='http://fund.eastmoney.com/f10/jjjl_%s.html'% code
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            request = Request(url)
            lines = urlopen(request, timeout = 10).read()
            doc=pq(lines.decode('utf-8'))
            data=pd.read_html(str(doc('table')))
            data=data[1]
            tempdata=np.append(data[[0,1]].values,data[[2,3]].values,axis=0)
            tempdata=pd.DataFrame(tempdata).set_index(0).T
            temp=tempdata.loc[1].to_dict()
        except Exception as e:
            print(e)
            return(False)
        else:
            if db:
                if isinstance(temp,dict):     
                    command=gen_fundmental_insert_command(temp)
                    db.execute(command,1)
                    return(True)
            else :
                return (tempdata)

    
def get_fund_fumdmental_threading(codes,db=None):
    for code in codes:
        get_fund_fundmental(code,db)

def get_fund_manager(code,db=None,retry_count=3,pause=0.01):
    tempdata=None
    jjjl_url='http://fund.eastmoney.com/f10/jjjl_%s.html'% code
    for _ in range(retry_count):
        time.sleep(pause)
        #print('a')
        try:
            request = Request(jjjl_url)
            lines = urlopen(request, timeout = 10).read()
            doc=pq(lines.decode('utf-8'))
            data=pd.read_html(str(doc('table')))
            data=data[1]
            data['基金代码']=code
            tempdata=data.to_dict(orient='record')
        except Exception as e:
            print(e)
            return
        else:
            print(code)
            if db:
                for i in tempdata:
                    command=gen_manager_insert_command(i)
                    db.execute(command,1)
                    return
        #funds_manager.append(data)
            else:
                return(data)

def get_fund_manager_threading(codes,db):
    for code in codes:
        get_fund_manager(code,db)

def get_manager_info(retry_count=3,pause=0.001):
    '''
    获取东方财富上基金经理的管理基金数据
    '''
    url='http://fund.eastmoney.com/Data/FundDataPortfolio_Interface.aspx?dt=14&mc=returnjson&ft=all&pn=2000&pi=1&sc=abbname&st=asc'
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            request = Request(url)
            lines = urlopen(request, timeout = 10).read().decode('utf-8')
        except Exception as e:
            print(e)
        else:
            lines=lines[lines.index('{'):lines.index(']]')+2]+'}'
            data=lines.replace('data','"data"')
            DDa=pd.DataFrame(json.loads(data)['data'])
            DD=DDa[[0,1,3,6,7,10,11]].copy()
            DD.columns=['经理代码','基金经理','基金公司','从业时间','现任基金最佳回报(%)','管理规模(亿元)','历任基金最佳回报(%)']
            DD['管理基金数']=DDa[4].apply(lambda x: len(x.split(',')))
            DD['管理规模(亿元)']=DD['管理规模(亿元)'].apply(lambda x : float(x[0:x.index('亿')]) if x.find('亿')>0 else np.nan)
            DD[['现任基金最佳回报(%)','历任基金最佳回报(%)']]=DD[['现任基金最佳回报(%)','历任基金最佳回报(%)']].applymap(
                                                       lambda x : float(x[0:x.index('%')]) if x.find('%')>0 else np.nan)
            return(DD)
                   
def get_fund_rank(ft='all',qdii='',sd='',ed='',pi=1,pn=100,retry_count=3,pause=0.001):
    """
        获取基金收益
    Parameters
    ------
      ft:string
          全部：all, 股票型：gp, 混合型：hh, 债券型：zq, 指数型:zz , 保本型:bb , 
          QDII:qdii, LOF: lof 
      qdii: num
           债券型——长期纯债:041,短期纯债:042,混合纯债:043, 定期开放债:008, 可转债:045
           指数型——沪深指数:053,行业指数:054,大盘指数:01, 中盘指数:02, 小盘指数:03,股票指数:001,债券指数:003
           QDII——全球股票:311,亚太股票：312,大中华区：313,新兴市场：314,金砖国家：315,美国股票：317,
                   全球指数：318,股债混合：320,债券：330, 商品：340
      sd:string
                  开始日期 format：YYYY-MM-DD 为空时取到去年今日的交易数据
      ed:string
                  结束日期 format：YYYY-MM-DD 为空时取到今日交易说句
      pn: num
          基金数
      retry_count : int, 默认 3
                 如遇网络等问题重复执行的次数 
      pause : int, 默认 0
                重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
    return
    -------
    """
    url='http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=%s&rs=&gs=0&sc=zzf&st=desc&sd=%s&ed=%s&qdii=%s&tabSubtype=,,,,,&pi=%s&pn=%s'
    #url='http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=&gs=0&sc=zzf&st=desc&sd=start&ed=end&qdii=&tabSubtype=,,,,,&pi=1&pn=7000&'
    colnames=['基金代码','基金简称','日期','单位净值','累计净值','日增长率','近1周',
              '近1月','近3月','近6月','近1年','近2年' ,'近3年' ,'今年来','成立来','成立日']
    if (ed=='' )& (sd==''):
        ed=date.today().strftime('%Y-%m-%d')
        sd=(date.today()-pd.Timedelta(days=365)).strftime('%Y-%m-%d')
    url=url%(ft,sd,ed,qdii,pi,pn)
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            request = Request(url)
            lines = urlopen(request, timeout = 10).read().decode('utf-8')
        except Exception as e:
            print(e)
        else:
            lines=lines[lines.index('{'):lines.index(']')+1]+']}'
            data=lines.replace('","','"],["').replace(',','","').replace(']","[','],[').replace('datas:[','"datas":[[')
            js=json.loads(data)
            fundinfo=pd.DataFrame(js['datas'])
            fundinfo=fundinfo[[0,1]+list(range(3,17))]
            fundinfo.columns=colnames
            return(fundinfo)
    raise IOError(ct.NETWORK_URL_ERROR_MSG)    
    
    
    

    
    
    
##################### fund report download##################        
def get_quarter_report(url,retry_count=3,pause=0.01):
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            request = Request(url)
            lines = urlopen(request, timeout = 10).read()
            doc=pq(lines.decode('utf-8'))
            data=pd.read_html(str(doc('table')))
            names=[x.text for x in BeautifulSoup(str(doc('label.right')),'lxml').findAll('font')]
        except Exception as e:
            continue
        else:
            data= [x.join(pd.DataFrame({'报告期':[y]})).fillna(method='ffill') for x,y in zip(data,names)]
            return(data)
    #raise IOError(ct.NETWORK_URL_ERROR_MSG)
    return


def get_fund_stocks(code,st=2010,detail=False):
    if detail:
        url='http://fund.eastmoney.com/f10/FundArchivesDatas.aspx?type=jjcc&code=%s&topline=10&year=%s&month=3,6,9,12'
    else:
        url='http://fund.eastmoney.com/f10/FundArchivesDatas.aspx?type=jjcc&code=%s&topline=10&year=%s'
    end=date.today().year
    stocks=[]
    for year in range(end,st-1,-1):
        url_get=url%(code,year)
        #print(year)
        res=get_quarter_report(url_get)
        if res is None:
            break
        else:
            stocks=stocks+res
    stocks_report=[]
    for i,df in enumerate(stocks):
        if i==0:
            stocks_report=df
        else:
            stocks_report=stocks_report.append(df)
    return(stocks_report)
        
        
def get_fund_bonds(code,st=2010):
    url='http://fund.eastmoney.com/f10/FundArchivesDatas.aspx?type=zqcc&code=%s&topline=10&year=%s&month=3,6,9,12'
    end=date.today().year
    bonds=[]
    for year in range(end,st-1,-1):
        url_get=url%(code,year)
        #print(year)
        res=get_quarter_report(url_get)
        if res is None:
            break
        else:
            bonds=bonds+res
    bonds_report=[]
    for i,df in enumerate(bonds):
        if i==0:
            bonds_report=df
        else:
            bonds_report=bonds_report.append(df)
    return(bonds_report)

def get_fund_industry(code,st=2010):
    url='http://fund.eastmoney.com/f10/F10DataApi.aspx?type=hypz&code=%s&year=%s'
    end=date.today().year
    industry=[]
    for year in range(end,st-1,-1):
        url_get=url%(code,year)
        #print(year)
        res=get_quarter_report(url_get)
        if res is None:
            break
        else:
            industry=industry+res
    industry_report=[]
    for i,df in enumerate(industry):
        if i==0:
            industry_report=df
        else:
            industry_report=industry_report.append(df)
    return(industry_report)

##################################################
def get_fund_history(code=None,sdate='',edate='',page=1,per=3000,retry_count=3,
            pause=0.001):
    """
        获取基金历史净值分红记录
    Parameters
    ------
      code:string
                  基金代码 e.g. 600848
      sdate:string
                  开始日期 format：YYYY-MM-DD 为空时取到API所提供的最早日期数据
      edate:string
                  结束日期 format：YYYY-MM-DD 为空时取到最近一个交易日数据
      retry_count : int, 默认 3
                 如遇网络等问题重复执行的次数 
      pause : int, 默认 0
                重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
    return
    -------
      DataFrame
          属性:净值日期 单位净值 累计净值 日增长率 申购状态 赎回状态 分红送配
    """
    url='http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=%s&page=%s&per=%s&sdate=%s&edate=%s&rt=0.08760689579229652'
    url=url%(code,page,per,sdate,edate)
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            request = Request(url)
            lines = urlopen(request, timeout = 10).read()
        except Exception as e:
            print(e)
        else:
            doc=pq(lines.decode('utf-8'))
            data=pd.read_html(str(doc('table')))[0]
            data=data.iloc[::-1]
            data.index=range(0,len(data))
            return(data)
    raise IOError(ct.NETWORK_URL_ERROR_MSG)        
    
    
######################### operation function#######

def initinal():
    con=sqlite3.connect('funds_database.db')
    fund_id=get_fund_id()
    fund_id.to_sql('fundsID',con,if_exists='replace',index=False)
    managerInfo=get_manager_info()
    managerInfo.to_sql('managerInfo',con,if_exists='replace',index=False)




def update_fundmental():
    con=sqlite3.connect('funds_database.db')
    con.execute('delete from fundmental_info')
    con.commit()
    command="create table if not exists fundmental_info (基金代码 Text,基金全称 Text,基金简称 Text,基金类型 Text,份额规模 Text,资产规模 Text,业绩比较基准 Text,跟踪标的 Text,发行日期 Text,成立日期[/]规模 Text,成立来分红 Text,基金托管人 Text,托管费率 Text,基金管理人 Text,管理费率 Text,销售服务费率 Text)"
    db=SQLiteWraper('funds_database.db',command)
    fund_id=pd.read_sql('select * from fundsID',con)
    funds=fund_id.loc[['后端' not in x for x in  fund_id['名称']]].copy()
    lock = threading.Lock()
    threads=[]  
    num=100
    codes_len=int(len(funds['代码'])/num)
    codes = [funds['代码'].iloc[(i*codes_len):(i+1)*codes_len] for i in range(num)]+[funds['代码'].iloc[(num*codes_len):]]
    for i in codes:  
        t = threading.Thread(  
                target=get_fund_fumdmental_threading,args=(i,db)  
            )   
        threads.append(t)  
    for i in range(0,len(codes)):  
        threads[i].start()  
    for i in range(0,len(codes)):  
        threads[i].join()  

def update_fundmanager():
    con=sqlite3.connect('funds_database.db')
    con.execute('delete from fund_managers')
    con.commit()
    command="create table if not exists fund_managers (起始期 Text,截止期 Text,基金经理 Text,任职期间 Text,任职回报 Text,基金代码 Text)"
    db=SQLiteWraper('funds_database.db',command)
    fund_id=pd.read_sql('select * from fundsID',con)
    funds=fund_id.loc[['后端' not in x for x in  fund_id['名称']]].copy()
    lock = threading.Lock()
    threads=[]  
    num=100
    codes_len=int(len(funds['代码'])/num)
    codes = [funds['代码'].iloc[(i*codes_len):(i+1)*codes_len] for i in range(num)]+[funds['代码'].iloc[(num*codes_len):]]
    for i in codes:  
        t = threading.Thread(  
                target=get_fund_manager_threading,args=(i,db)  
            )   
        threads.append(t)  
    for i in range(0,len(codes)):  
        threads[i].start()  
    for i in range(0,len(codes)):  
        threads[i].join()  
        
          
    
def update_fundsrank():
    con=sqlite3.connect('funds_database.db')
    fund_return=get_fund_rank(pn=8000)
    fund_return.to_sql('funds_return',con,if_exists='replace',index=False)
    fund_manager=pd.read_sql('select * from fund_managers',con)
    fund_mental=pd.read_sql('select 基金代码,资产规模,基金类型,基金管理人 from fundmental_info',con)
    fund_mental['基金代码']=fund_mental['基金代码'].apply(lambda x: re.findall('\w*[0-9]',x)[0])
    base_info=['基金代码','基金简称','基金类型','基金管理人','资产规模',
                     '基金经理','起始期']
    select_info=['单位净值','累计净值','近1周','近3月','近6月','近1年',
                       '近2年','近3年','成立来']
    fund_rank=pd.merge(pd.merge(fund_return,fund_manager,on='基金代码',how='inner'),fund_mental,on='基金代码',how='inner')
    def format_value(x):
        try:
            val=np.round(float(x),2)
            return(val)
        except Exception as e:
            return(np.nan)
    fund_rank[select_info]=fund_rank[select_info].applymap(format_value)
    fund_rank['资产规模']=fund_rank['资产规模'].apply(lambda x: float(str(x).replace("---","NaN").split("亿元")[0]))
    fund_rank['任职回报']=fund_rank['任职回报'].apply(lambda x: float( x.replace('%','')) if isinstance(x,str) else np.nan)
    fund_rank.to_sql('funds_rank',con,if_exists='replace',index=False)
    
def funds_position_initial():
    data=get_fund_id()
    stock=data
    bond=data
    funds_stocks={}
    funds_bonds={}
    funds_industry={}  
    stocks_list=[]
    bonds_list=[]
    industry_list=[]
    for i,code in enumerate(stock['代码']):
        tempdata=get_fund_stocks(code)
        if isinstance(tempdata,pd.DataFrame):
            tempdata['基金代码']=code
            tempdata=tempdata
            if i==0:
                funds_stocks=tempdata
            else:
                funds_stocks=funds_stocks.append(tempdata)
            print('fininsh stock',code)
        else:
            stocks_list.append(code)

        tempdata0=get_fund_industry(code)
        if isinstance(tempdata0,pd.DataFrame):
            tempdata0['基金代码']=code
            tempdata0=tempdata0 
            if i==0:
                funds_industry=tempdata0
            else:
                funds_industry=funds_industry.append(tempdata0)
        else:
            industry_list.append(code)

    for i,code in enumerate(bond['代码']):
        tempdata=get_fund_bonds(code)
        if isinstance(tempdata,pd.DataFrame):
            tempdata['基金代码']=code
            tempdata=tempdata
            if i==0:
                funds_bonds=tempdata
            else:
                funds_bonds=funds_bonds.append(tempdata)
            print('fininsh bond',code)
        else:
            bonds_list.append(code)
    con=sqlite3.connect('funds_database.db')
    funds_bonds.drop_duplicates().set_index(['基金代码','报告期','序号']).to_sql('funds_bonds',con,if_exists='replace')
    funds_stocks.drop_duplicates().set_index(['基金代码','报告期','序号']).to_sql('funds_stocks',con,if_exists='replace')
    funds_industry.drop_duplicates().set_index(['基金代码','报告期','序号']).to_sql('funds_industry',con,if_exists='replace')

def update_position(year=2018):    
    con=sqlite3.connect('funds_database.db')
    industry=pd.read_sql('select * from funds_industry',con)
    stocks=pd.read_sql('select * from funds_stocks',con)
    bonds=pd.read_sql('select * from funds_bonds',con)
    stocks_list=[]
    bonds_list=[]
    industry_list=[]
    for i,code in enumerate(stock['基金代码'].unique()):
        tempdata=get_fund_stocks(code,st=year)
        if isinstance(tempdata,pd.DataFrame):
            tempdata['基金代码']=code
            tempdata=tempdata
            stocks=stocks.append(tempdata)
            print('fininsh stock',code)
        else:
            stocks_list.append(code)
            
        tempdata0=get_fund_industry(code,st=year)
        if isinstance(tempdata0,pd.DataFrame):
            tempdata0['基金代码']=code
            tempdata0=tempdata0 
            industry=industry.append(tempdata0)
        else:
            industry_list.append(code)

    for i,code in enumerate(bonds['基金代码'].unique()):
        tempdata=get_fund_bonds(code,st=year)
        if isinstance(tempdata,pd.DataFrame):
            tempdata['基金代码']=code
            tempdata=tempdata
            bonds=bonds.append(tempdata)
            print('fininsh bond',code)
        else:
            bonds_list.append(code)
            

    industry.drop_duplicates(subset=['基金代码','报告期','行业类别']).set_index(['基金代码','报告期','序号']).to_sql('funds_industry',con,if_exists='replace')
    stocks.drop_duplicates(subset=['基金代码','报告期','股票名称']).set_index(['基金代码','报告期','序号']).to_sql('funds_stocks',con,if_exists='replace')
    bonds.drop_duplicates(subset=['基金代码','报告期','债券名称']).set_index(['基金代码','报告期','序号']).to_sql('funds_industry',con,if_exists='replace')

    
#update_fundmental()    
update_fundmanager()    
update_fundsrank()