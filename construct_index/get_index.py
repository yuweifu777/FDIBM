# -*- coding:utf-8 -*-
from __future__ import division
from datetime import datetime
from sqlalchemy import create_engine
import os
import logging
import pymysql
import pandas as pd


PWD = os.getcwd()
STOCK_DATA = pd.read_csv("{}/CONSTANT_DATA/STOCK_DATA.csv".format(PWD), index_col=0, encoding="utf-8")
TRADE_CALENDAR = pd.read_csv("{}/CONSTANT_DATA/TRADE_CALENDAR.csv".format(PWD), index_col=0, encoding="utf-8")["Date"].tolist()
INDUSTRIES = list(set(STOCK_DATA.IndCode))
UPDATE_NETVALUE = pd.read_csv("{}/CONSTANT_DATA/Update_Netvalue.csv".format(PWD), index_col=0, encoding="utf-8")
Today = datetime.now()
PASSWORD = "4Hu#Epe7Ejeze!@6"
logging.basicConfig(level=logging.INFO)


def get_data():

    try:
        os.mkdir("Daily_Data")
    except OSError as e:
        pass

    conn = pymysql.connect(host="127.0.0.1", port=3306, user="root", passwd=PASSWORD, db="StockQuote", charset="utf8")
    cursor = conn.cursor()

    for num, stock_code in enumerate(STOCK_DATA.index):
        logging.info("Getting data for {} -- {}/{}".format(stock_code, num+1, STOCK_DATA.shape[0]))
        SQL = "select postTime, price from {0} where postTime between '{1} 00:00:00' and '{1} 23:59:59'".format(stock_code, Today.strftime("%Y-%m-%d"))
        cursor.execute(SQL)
        data = cursor.fetchall()
        df = pd.DataFrame(list(data), columns=["postTime", "price"]).set_index("postTime").sort_index()
        df["mktvalue"] = df["price"] * STOCK_DATA.loc[stock_code, "TotalShares"] / 10**8
        df["minpct"] = df["price"].pct_change().fillna(0)
        df.to_csv("{}/Daily_Data/{}.csv".format(PWD, stock_code), encoding="utf-8")

    cursor.close()
    conn.close()


def get_index():

    try:
        os.mkdir("IndIndex") 
    except OSError as e:
        pass

    engine = create_engine("mysql+pymysql://root:{}@127.0.0.1:3306/StockIndex".format(PASSWORD))

    for industry in INDUSTRIES:
        logging.info("Calculating for index {}".format(industry))
        industry_stocks = STOCK_DATA.loc[STOCK_DATA["IndCode"] == industry, :].index.tolist()
        sum_series = sum([pd.read_csv("{}/Daily_Data/{}.csv".format(PWD, stock), index_col=0)["mktvalue"] for stock in industry_stocks])
        minpct = []
        for stock in industry_stocks:
            stock_df = pd.read_csv("{}/Daily_Data/{}.csv".format(PWD, stock), index_col=0)
            minpct.append(stock_df['mktvalue'] / sum_series * stock_df['minpct'])
        industry_df = sum(minpct).to_frame(name='minpct')
        industry_df['netvalue'] = industry_df.add(1).cumprod() * UPDATE_NETVALUE.loc[industry, 'netvalue']
        # industry_df.to_csv("IndIndex/{}.csv".format(industry), encoding="utf-8")
        industry_df.reset_index().to_sql(industry, engine, if_exists='append', index=False)
        UPDATE_NETVALUE.loc[industry, 'netvalue'] = industry_df.iloc[-1, -1]
    UPDATE_NETVALUE.to_csv('{}/CONSTANT_DATA/Update_Netvalue.csv'.format(PWD))



if __name__ == '__main__':

    if Today.strftime("%Y-%m-%d") in TRADE_CALENDAR:
        logging.info("Getting Index For {}".format(Today.strftime("%Y-%m-%d"))) 
        get_data()
        get_index()
    else:
        print("Today is not a trading day.")
