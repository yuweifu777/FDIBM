#!/root/Fudan/kybdata/bin/python
# -*- coding:utf-8 -*-
import time
import logging
import pymysql
import requests
import pandas as pd
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


CALENDAR = pd.read_csv("/root/Fudan/kybdata/CONSTANT_DATA/TRADE_CALENDAR.csv", index_col=0, encoding="utf-8")["Date"].tolist()
STOCK_DATA = pd.read_csv("/root/Fudan/kybdata/CONSTANT_DATA/STOCK_DATA.csv", index_col=0, encoding="utf-8")
logging.basicConfig(level=logging.INFO)
PASSWORD = "4Hu#Epe7Ejeze!@6"


def tick(conn, cursor):
    r = requests.post("http://www.kanyanbao.com.cn/websocket/quotes.json", data={"stkcodes":",".join(STOCK_DATA.index.tolist())})
    content = r.json()
    minute = datetime.now().strftime("%Y-%m-%d %H:%M:00")
    for i in content[:]:
        stock = i.pop("key")
        i['postTime'] = minute
        placeholders = ', '.join(['%s'] * len(i))
        columns = ', '.join(i.keys())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (stock, columns, placeholders)
        cursor.execute(sql, list(i.values()))
    conn.commit()


def create_tables():
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd=PASSWORD, db='StockQuote', charset='utf8')
    cursor = conn.cursor()
    for stock in STOCK_DATA.index[:]:
        SQL = """ CREATE TABLE `{}` (
            `postTime` datetime NOT NULL DEFAULT '2000-01-01 00:00:00',  
            `stkCode` varchar(10) DEFAULT NULL, 
            `price` float DEFAULT NULL,  
            `amount` float DEFAULT NULL,  
            `volume` float DEFAULT NULL,  
            `avgChangeRate` float DEFAULT NULL,  
            `avgPrice` float DEFAULT NULL,  
            `buy` float DEFAULT NULL,  
            `buyprice1` float DEFAULT NULL,  
            `buyprice2` float DEFAULT NULL,  
            `buyprice3` float DEFAULT NULL,  
            `buyprice4` float DEFAULT NULL,  
            `buyprice5` float DEFAULT NULL,  
            `buyvol1` float DEFAULT NULL,  
            `buyvol2` float DEFAULT NULL,  
            `buyvol3` float DEFAULT NULL,  
            `buyvol4` float DEFAULT NULL,  
            `buyvol5` float DEFAULT NULL,  
            `highChangeRate` float DEFAULT NULL,   
            `openChange` float DEFAULT NULL,  
            `openChangeRate` float DEFAULT NULL,  
            `priceChange2` float DEFAULT NULL,  
            `priceChangeRate2` float DEFAULT NULL, 
            `priceHigh` float DEFAULT NULL,   
            `priceHighLowRate` float DEFAULT NULL,  
            `priceLast` float DEFAULT NULL,  
            `priceLow` float DEFAULT NULL,  
            `priceOpen` float DEFAULT NULL,  
            `rt` tinyint(1) DEFAULT NULL,  
            `sell` float DEFAULT NULL,  
            `sellprice1` float DEFAULT NULL,  
            `sellprice2` float DEFAULT NULL,  
            `sellprice3` float DEFAULT NULL,  
            `sellprice4` float DEFAULT NULL,  
            `sellprice5` float DEFAULT NULL,  
            `sellvol1` float DEFAULT NULL,  
            `sellvol2` float DEFAULT NULL,  
            `sellvol3` float DEFAULT NULL,  
            `sellvol4` float DEFAULT NULL,  
            `sellvol5` float DEFAULT NULL,  
            `stkName` text,  
            `mktcode` varchar(10) DEFAULT NULL, 
            `tdate` float DEFAULT NULL,  
            `tradeDate` varchar(20) DEFAULT NULL,  
            PRIMARY KEY (`postTime`)) 
            ENGINE=InnoDB DEFAULT CHARSET=utf8;
            """.format(stock)
        cursor.execute(SQL)
        logging.info("Finished {}".format(stock))
    conn.commit()
    cursor.close()
    conn.close()


def run():
    today = datetime.now().strftime("%Y-%m-%d")
    if today in CALENDAR:
        logging.info("#"*50 + "\nBegin getting data for {}".format(today))
        conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd=PASSWORD, db='StockQuote', charset='utf8')
        cursor = conn.cursor()

        # Trigger every minute
        global scheduler
        scheduler = BlockingScheduler()        
        scheduler.add_job(tick, 'interval', minutes=1, start_date='{} 09:30:00'.format(today), end_date='{} 11:30:00'.format(today), args=[conn, cursor])
        scheduler.add_job(tick, 'interval', minutes=1, start_date='{} 13:00:00'.format(today), end_date='{} 15:00:00'.format(today), args=[conn, cursor])
        try:
            scheduler.start()
            cursor.close()
            conn.close()
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            conn.commit()
            cursor.close()
            conn.close()
    else:
        logging.info("Today is not a trade day.")


if __name__ == "__main__":

    run()

    # create_tables()
