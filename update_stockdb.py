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


class stockdb():

    #
    def __init__(self, m, n):
        url = urlparse('mysql://stockdb:bdkcots@192.168.1.11:3306/stockdb')
        self.mydb = mysql.connector.connect(
            host=url.hostname,
            port=url.port,
            user=url.username,
            database=url.path[1:],
            password=url.password
        )
        # stockdb の作成
        self.mycursor = self.mydb.cursor(buffered=True)
        sql = 'CREATE TABLE IF NOT EXISTS stockdb ('
        sql += 'date DATE NOT NULL, '
        sql += 'cc VARCHAR(16) NOT NULL, '
        sql += 'open FLOAT, close FLOAT, high FLOAT, low FLOAT, volume BIGINT,'
        sql += 'PRIMARY KEY(date, cc)'
        sql += ')'
        self.mycursor.execute(sql)

        '''
        # getdatatimedb の作成
        sql = 'CREATE TABLE IF NOT EXISTS getdatatimedb ('
        sql += 'cc VARCHAR(16) NOT NULL, '
        sql += 'datagettime DATETIME, '
        sql += 'valid boolean,'
        sql += 'PRIMARY KEY(cc)'
        sql += ')'
        self.mycursor.execute(sql)

        # 一旦全部invalid
        sql = 'UPDATE getdatatimedb SET valid = 0;'
        self.mycursor.execute(sql)

        # あればUPDATE、なければInsert
        for cc in ccs:
            cc = str(cc) + ".JP"
            sql = 'INSERT INTO getdatatimedb (cc, valid) VALUES ("%s", 1) ' \
                  'ON DUPLICATE KEY UPDATE cc=VALUES(cc), valid=1;' % (cc)
            self.mycursor.execute(sql)
        self.mydb.commit()
        '''

        url = 'http://kabusapo.com/dl-file/dl-stocklist.php'
        res = requests.get(url).content
        df = pd.read_csv(io.StringIO(res.decode('utf-8')), header=0, index_col=0)

        start = int(len(df.index) / n * m)
        end = int(len(df.index) / n * (m+1))
        self.CompanyCode = []
        for cc in list(df.index)[start:end]:
            self.CompanyCode.append(str(cc) + ".JP")

    def __del__(self):
        self.mydb.close()

    def company_codes(self):
        '''
        sql = 'SELECT cc FROM getdatatimedb WHERE valid=1 ORDER BY datagettime ASC;'
        self.mycursor.execute(sql)
        ret = self.mycursor.fetchall()
        return ret
        '''
        return self.CompanyCode

    def get_start_date(self, company_code):
        sql = 'SELECT date from %s where cc = "%s" ORDER BY date DESC limit %d;' % ('stockdb', company_code, 1)
        self.mycursor.execute(sql)
        if self.mycursor.rowcount == 0:
            return datetime(2010, 1, 1).date()
        else:
            lastday = self.mycursor.fetchone()[0]
            return lastday + timedelta(days=1)


    def insert_data(self, company_code, data):
        for date in data.index:
            row = data.loc[date]
            sql = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES ("%s", "%s", %f, %f, %f, %f, %d)' % \
                  ('stockdb',
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

    # DBの株価を更新する
    def update_stockdb(self, company_code):
        logging.info("CompanyCode: %s", cc)

        # DBにアクセスしてDB内の最新の日付をゲット
        start_date = self.get_start_date(company_code)

        t = datetime.now().date()  # today
        if start_date <= t:
            # yahoo financeにアクセスして、株価を入手
            stock_data = self.yfinace(company_code, start_date).dropna()

            # DBの更新
            self.insert_data(company_code, stock_data)

    def yfinace(self, companycode, start):
        logging.info("  gathering data since: %s", start)
        companycode = companycode.replace('.JP', '.T')
        msft = yf.Ticker(companycode)

        # get historical market data
        hist = msft.history(start=start)
        # hist_rename = hist.rename(columns={'Open':'open', 'High':'high', 'Low':'low', 'Close':'close', 'Volume':'volume'})
        return hist

if __name__ == "__main__":
    formatter = '%(levelname)s : %(asctime)s : %(message)s'
    logging.basicConfig(filename='./update_stockdb.log', level=logging.INFO, format=formatter)

    stockdb = stockdb(0, 2)

    for cc in stockdb.company_codes():
        stockdb.update_stockdb(cc)
        time.sleep(1)
