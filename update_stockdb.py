#!/usr/bin/python
# coding: utf-8
#
from urllib.parse import urlparse
import mysql.connector
from datetime import datetime, timedelta
import requests
import io
import pandas as pd
import time
import yfinance as yf
import logging
import urllib.request
from sqlalchemy import create_engine
import argparse
from bs4 import BeautifulSoup

class Stockdb():

    #
    def __init__(self, m, n):
        if args.initdb:
            self.initdb(args.url_db)

        # data_jの更新
        url = urllib.request.urlopen('https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls')
        with open("data_j.xls", "wb") as f:
            f.write(url.read())
        df = pd.read_excel("data_j.xls", header=0, index_col=1)

        if m == 0:
            engine = create_engine(args.url_db)
            df.to_sql("data_j", con=engine.connect(), index=True, index_label="cc", if_exists="replace")

        start = int(len(df.index) / n * m)
        end = int(len(df.index) / n * (m+1))
        self.CompanyCode = []
        for index, row in df[start:end].iterrows():
            self.CompanyCode.append(str(index) + ".JP")

    def __del__(self):
        self.mydb.close()

    def initdb(self, url_str):
        url = urlparse(url_str)
        self.mydb = mysql.connector.connect(
            host=url.hostname,
            port=url.port,
            user=url.username,
            database=url.path[1:],
            password=url.password
        )
        # stockdbの削除
        self.mycursor = self.mydb.cursor(buffered=True)
        sql = 'DROP TABLE IF EXISTS %s ;' % (args.stockdb)
        self.mycursor.execute(sql)

        # stockdbの作成
        self.mycursor = self.mydb.cursor(buffered=True)
        sql = 'CREATE TABLE IF NOT EXISTS %s (' % (args.stockdb)
        sql += 'date DATE NOT NULL, '
        sql += 'cc VARCHAR(16) NOT NULL, '
        sql += 'open FLOAT, close FLOAT, high FLOAT, low FLOAT, volume BIGINT,'
        sql += 'PRIMARY KEY(date, cc)'
        sql += ') PARTITION BY KEY(cc) PARTITIONS 4096;'
        self.mycursor.execute(sql)

    def company_codes(self):
        '''
        sql = 'SELECT cc FROM getdatatimedb WHERE valid=1 ORDER BY datagettime ASC;'
        self.mycursor.execute(sql)
        ret = self.mycursor.fetchall()
        return ret
        '''
        return self.CompanyCode

    def get_start_date(self, company_code):
        sql = 'SELECT date from %s where cc = "%s" ORDER BY date DESC limit %d;' % (args.stockdb, company_code, 1)
        self.mycursor.execute(sql)
        if self.mycursor.rowcount == 0:
            return datetime(2010, 1, 1).date()
        else:
            lastday = self.mycursor.fetchone()[0]
            return lastday + timedelta(days=1)


    def insert_data(self, company_code, data, tablename):
        for date in data.index:
            row = data.loc[date]
            sql = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES ("%s", "%s", %f, %f, %f, %f, %d)' % \
                  (tablename,
                   'date', 'cc', 'open', 'close', 'high', 'low', 'volume',
                   date, company_code,
                   row['Open'], row['Close'], row['High'], row['Low'], int(row['Volume'])
                   )
            try:
                self.mycursor.execute(sql)
            except mysql.connector.IntegrityError as e:
                logging.error("history already exist: %s" % e)
            except mysql.connector.DataError as e:
                logging.error("dataerror exist: %s\n" % e)
                logging.error("date: %s, volume: %d\n" % (date, row['Volume']))

        self.mydb.commit()

        '''
        today = datetime.utcnow()
        sql = 'UPDATE getdatatimedb SET cc="%s", valid=1, datagettime="%s" ' \
              'WHERE cc="%s" ' % (company_code, today, company_code)

        #sql = 'INSERT INTO getdatatimedb (cc, valid, datagettime) ' \
        #      'VALUES ("%s", %d, "%s") ' \
        #      'ON DUPLICATE KEY UPDATE cc=VALUES(cc);' % (company_code, 1, today)
        self.mycursor.execute(sql)
        self.mydb.commit()
        '''

    def remove_old_data(self, tablename, num_date):
        dt = datetime.now().date() - timedelta(days=num_date)
        sql = 'DELETE FROM %s WHERE date<"%s"' % (tablename, dt)
        self.mycursor.execute(sql)
        self.mydb.commit()

    def initdb_kabuoji3(self, company_code):
        t1 = datetime.now().date()  # today
        headers = {
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
        }
        df = pd.DataFrame()

        for year in range(2010, t1.year):
            url = 'https://kabuoji3.com/stock/%s/%s/' % (company_code.replace('.JP', ''), year)
            html = requests.get(url, headers=headers)
            soup = BeautifulSoup(html.text,'html.parser')
            table = soup.find(class_="stock_table stock_data_table")
            rows = table.findAll('tr')

            for row in rows:
                cells = row.findAll('td')
                if len(cells) > 0:
                    items = []
                    for i in range(len(cells)):
                        cell = cells[i]
                        if i == 0: # 日付
                            items.append(cell.get_text())
                        else:      # 数字
                            items.append(int(cell.get_text()))

                    se = pd.Series(items[1:6], index = ["Open", "High", "Low", "Close", "Volume"], name=items[0])
                    df = df.append(se)
            time.sleep(2)

        return df

    # DBの株価を更新する
    def update_stockdb(self, company_code):
        logging.info("CompanyCode: %s", cc)

        if args.initdb:
            stock_data = self.initdb_kabuoji3(company_code)
            self.insert_data(company_code, stock_data, args.stockdb)

        # DBにアクセスしてDB内の最新の日付をゲット
        start_date = self.get_start_date(company_code)

        t = datetime.now().date()  # today
        if start_date <= t:
            # yahoo financeにアクセスして、株価を入手
            stock_data = self.yfinace(company_code, start_date).dropna()

            # DBの更新
            self.insert_data(company_code, stock_data, args.stockdb)

    def yfinace(self, companycode, start):
        logging.info("  gathering data since: %s", start)
        companycode = companycode.replace('.JP', '.T')
        msft = yf.Ticker(companycode)

        # get historical market data
        hist = msft.history(start=start)
        # hist_rename = hist.rename(columns={'Open':'open', 'High':'high', 'Low':'low', 'Close':'close', 'Volume':'volume'})
        return hist

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='stockdbの作成、更新')
    parser.add_argument('--initdb', action='store_true')
    parser.add_argument('--url_db', default='mysql+mysqlconnector://stockdb:bdkcots@192.168.1.11:3306/stockdb')
    parser.add_argument('--stockdb', default='stockdb')
    args = parser.parse_args()

    formatter = '%(levelname)s : %(asctime)s : %(message)s'
    logging.basicConfig(filename='./update_stockdb.log', level=logging.INFO, format=formatter)

    stockdb = Stockdb(0, 1)

    for cc in stockdb.company_codes():
        stockdb.update_stockdb(cc)
        time.sleep(1)
    # stockdb.remove_old_data("stockdb_sub", 7)
