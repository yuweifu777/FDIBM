# -*- coding:utf-8 -*-
"""
    78行 .to_sql() 报错，没有解决。
"""
import logging
import pymysql
import requests
import sqlalchemy
import pandas as pd
import tushare as ts
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


CALENDAR = pd.read_csv("CONSTANT_DATA/TRADE_CALENDAR.csv", index_col=0, encoding="utf-8")["Date"].tolist()
STOCK_DATA = pd.read_csv("CONSTANT_DATA/STOCK_DATA.csv", index_col=0, encoding="utf-8")
MYSQL_CONN = 'mysql+pymysql://root:password@localhost:3306/StockQuote'
KYB_DTYPE = {
    u'amount':"float64", 
    u'avgChangeRate':"float64", 
    u'avgPrice':"float64", 
    u'buy':"float64", 
    u'buyprice1':"float64", 
    u'buyprice2':"float64", 
    u'buyprice3':"float64", 
    u'buyprice4':"float64", 
    u'buyprice5':"float64", 
    u'buyvol1':"float64", 
    u'buyvol2':"float64", 
    u'buyvol3':"float64", 
    u'buyvol4':"float64", 
    u'buyvol5':"float64", 
    u'highChangeRate':"float64", 
    u'mktcode':"int64", 
    u'openChange':"float64", 
    u'openChangeRate':"float64", 
    u'price':"float64", 
    u'priceChange2':"float64", 
    u'priceChangeRate2':"float64", 
    u'priceHigh':"float64", 
    u'priceHighLowRate':"float64", 
    u'priceLast':"float64", 
    u'priceLow':"float64", 
    u'priceOpen':"float64", 
    u'rt':"bool", 
    u'sell':"float64", 
    u'sellprice1':"float64", 
    u'sellprice2':"float64", 
    u'sellprice3':"float64", 
    u'sellprice4':"float64", 
    u'sellprice5':"float64", 
    u'sellvol1':"float64", 
    u'sellvol2':"float64", 
    u'sellvol3':"float64", 
    u'sellvol4':"float64", 
    u'sellvol5':"float64", 
    u'stkCode':"unicode", 
    u'stkName':"unicode", 
    u'tdate':"int64", 
    u'tradeDate':"int64", 
    u'volume':"float64", 
}
logging.basicConfig(level=logging.INFO)


a = tick_df.astype({u'amount':"float", u'avgChangeRate':"float"})


def tick(engine):

    r = requests.post("http://www.kanyanbao.com.cn/websocket/quotes.json", data={"stkcodes":",".join(STOCK_DATA.index.tolist())})
    content = r.json()
    tick_df = pd.DataFrame(content).astype(dtype=KYB_DTYPE).set_index('key')
    minute = datetime.now().strftime("%Y-%m-%d %H:%M:00")
    tick_df["postTime"] = minute

    for stock in tick_df["key"]:
        tick_df.loc[stock, :].to_sql(stock, engine, index_label="postTime", if_exists='append')

    logging.info("Downloading tick data {}.".format(minute))


if __name__ == '__main__':

    today = datetime.now().strftime("%Y-%m-%d")
    
    # test
    CALENDAR.append(today)

    if today in CALENDAR:

        print("#"*50 + "\nBegin getting data for {}".format(today))

        # MySql connection
        engine = sqlalchemy.create_engine(MYSQL_CONN)
        
        # Trigger every minute
        global scheduler
        scheduler = BlockingScheduler()
        scheduler.add_job(tick, 'interval', minutes=1, start_date='{} 15:17:00'.format(today), end_date='{} 15:20:00'.format(today), args=[engine])
        scheduler.add_job(tick, 'interval', minutes=1, start_date='{} 15:30:00'.format(today), end_date='{} 15:40:00'.format(today), args=[engine])

        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

    else:
        print("Today is not a trade day.")


def test():
    
    engine = create_engine('mysql+pymysql://root:password@localhost:3306/StockQuote')

    test_df.to_sql('testdb', engine, if_exists='append')

    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='password', db='StockQuote')
    # conn = pymysql.connect(host='139.224.228.178', port=3306, user='root', passwd='4Hu#Epe7Ejeze!@6', db='gtja20')

    cursor = conn.cursor()

    SQL = """
    create table user (id varchar(20) primary key, name varchar(20))
    """
    cursor.execute(SQL)
    cursor.execute('insert into user (id, name) values (%s, %s)', ['1', 'Michael'])

    conn.commit()
    cursor.close()

    conn.close()

