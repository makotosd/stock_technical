#!/usr/bin/python
# coding: utf-8
#
from urllib.parse import urlparse
import mysql.connector
import pandas_datareader.data as web
from datetime import datetime, timedelta
import requests
import io
import pandas as pd

# company codeのリストを返す
def company_codes():
    url = 'http://kabusapo.com/dl-file/dl-stocklist.php'
    res = requests.get(url).content
    df = pd.read_csv(io.StringIO(res.decode('utf-8')), header=0, index_col=0)
    return df.index
    # return ['6701.JP', '6702.JP']


class stockdb():

    #
    def __init__(self):
        url = urlparse('mysql://stockdb:bdkcots@192.168.1.11:3306/stockdb')
        self.mydb = mysql.connector.connect(
            host=url.hostname,
            port=url.port,
            user=url.username,
            database=url.path[1:],
            password=url.password
        )
        self.mycursor = self.mydb.cursor(buffered=True)
        sql = 'CREATE TABLE IF NOT EXISTS stockdb ('
        sql += 'date DATE NOT NULL, '
        sql += 'cc VARCHAR(16) NOT NULL, '
        sql += 'open FLOAT, close FLOAT, high FLOAT, low FLOAT, volume INT,'
        sql += 'PRIMARY KEY(date, cc)'
        sql += ')'
        self.mycursor.execute(sql)

    def __del__(self):
        self.mydb.close()

    def get_start_date(self, company_code):
        sql = 'SELECT date from %s where cc = "%s" ORDER BY date DESC limit %d;' % ('stockdb', company_code, 1)
        self.mycursor.execute(sql)
        if self.mycursor.rowcount == 0:
            return ""
        else:
            lastday = self.mycursor.fetchone()[0]
            return lastday + timedelta(days=1)


    #
    def get_data_from_stooq(self, company_code, start_date):
        if start_date == "":
            start_date = datetime(2010, 1, 1)

        # startオプションをちゃんと動かすには、pandas-datareader 0.9.0が必要。
        print("  gathering data since: ", start_date.strftime('%Y/%m/%d'))
        tsd = web.DataReader(company_code, "stooq", start_date.strftime('%Y/%m/%d')).dropna()
        if tsd.index.name == 'Exceeded the daily hits limit':
            print("## " + tsd.index.name + " ##")
            exit(-1)
        else:
            return tsd

    def insert_data(self, company_code, data):
        for date in data.index:
            row = data.loc[date]
            sql = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES ("%s", "%s", %f, %f, %f, %f, %d)' % \
                  ('stockdb',
                   'date', 'cc', 'open', 'close', 'high', 'low', 'volume',
                   date, company_code,
                   row['Open'], row['Close'], row['High'], row['Low'], row['Volume']
                   )
            try:
                self.mycursor.execute(sql)
            except mysql.connector.IntegrityError as e:
                print("history already exist: %s" % e)


        self.mydb.commit()

    # DBの株価を更新する
    def update_stockdb(self, company_code):
        print("CompanyCode: ", cc)

        # DBにアクセスしてDB内の最新の日付をゲット
        startdate = self.get_start_date(company_code)

        # stooqにアクセスして、株価を入手
        stooq_data = self.get_data_from_stooq(company_code, startdate)

        # DBの更新
        self.insert_data(company_code, stooq_data)


if __name__ == "__main__":
    stockdb = stockdb()
    for cc in company_codes():
        stockdb.update_stockdb(str(cc) + ".JP")
