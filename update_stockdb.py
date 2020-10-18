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
import re
import os
import sys

def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

class Stockdb():

    #
    def __init__(self, m, n):
        
        url = urlparse(args.url_db)
        self.mydb = mysql.connector.connect(
            host=url.hostname,
            port=url.port,
            user=url.username,
            database=url.path[1:],
            password=url.password
        )
        self.mycursor = self.mydb.cursor(buffered=True)

        if args.dropdb:
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
        # self.mydb.close()
        pass

    def initdb(self, url_str):
        # stockdbの削除
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

    # dbからデータ取得。なければNoneを返す
    def get_data_in_db(self, tablename, cc, date):
        sql = 'SELECT * FROM %s WHERE cc="%s" AND date="%s" LIMIT 1;' % (tablename, cc, date)
        self.mycursor.execute(sql)
        if self.mycursor.rowcount == 0:
            return None
        else:
            items = self.mycursor.fetchone()
            se = pd.Series(items[2:7], index = ["Open", "Close", "High", "Low", "Volume"], name=items[0])

            return se

    # 二つのデータの比較
    def compare_data(self, a, b, cc, date):
        # logging.info(a.sort_index())
        # logging.info(b.sort_index())
        if (a.sort_index() == b.sort_index()).all():
            return True
        else:
            return False


    # dbにデータ挿入
    def insert_data(self, company_code, data, tablename):
        if len(data) == 0:
            logging.info("  No data for %s" % (company_code))
            return

        for date in data.index:
            data_in_db = self.get_data_in_db(tablename, company_code, date)
            if data_in_db is not None:
                if self.compare_data(data_in_db, data.loc[date], company_code, date):
                    continue
                else:
                    logging.fatal("data is different from db. cc: %s, date: %s" % (cc, date))
                    sys.exit(1)

            else:
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

    def remove_old_data(self, tablename, num_date):
        dt = datetime.now().date() - timedelta(days=num_date)
        sql = 'DELETE FROM %s WHERE date<"%s"' % (tablename, dt)
        self.mycursor.execute(sql)
        self.mydb.commit()

    # https://kabuoji3.com の過去データからDBの初期化
    # すぐにアクセス拒否された。翌日には復活したが。
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
            if table is None:
                continue

            rows = table.findAll('tr')
            if rows is None:
                continue

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

    # yahooからdownloadしたcsvからdataframeにへんかん
    def csv2df(self, company_code):
        df = pd.DataFrame()

        saveFilePath = os.path.join("./history/", "%s.T.csv" % (company_code.replace('.JP', '')))
        try:
            df = pd.read_csv(saveFilePath, index_col=0, encoding="shift-jis", header=0, names=('date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj'))
        except UnicodeDecodeError as e:
            logging.info("  no valid data in csv. see following error.")
            logging.info(e)
            return df

        df['Volume'] = df['Volume'] * round(df['Close'] / df['Adj'], 4) # Volumeだけ逆数
        df['Open']   = df['Open']   * round(df['Adj'] / df['Close'], 4)
        df['High']   = df['High']   * round(df['Adj'] / df['Close'], 4)
        df['Low']    = df['Low']    * round(df['Adj'] / df['Close'], 4)
        df['Close']  = df['Close']  * round(df['Adj'] / df['Close'], 4) # Closeは最後にやりましょう
        df = df.drop('Adj', axis=1)

        return df

    # nikkeiのサイトから最新株価を取得
    def latest_stock_data_from_nikkei(self, company_code):

        headers = {
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
        }
        url = 'https://www.nikkei.com/nkd/company/history/dprice/?scode=%s&ba=1' % (company_code.replace('.JP', ''))
        html = requests.get(url, headers=headers)
        soup = BeautifulSoup(html.text,'html.parser')
        table = soup.find(class_="m-tableType01_table")
        if table is None:
            logging.info("no table for %s" % (company_code))

        df = pd.DataFrame()
        rows = table.findAll('tr')
        if rows is None:
            logging.info("no tr for %s" % (company_code))

        for row in rows:
            t = row.find('th').get_text()
            mm = re.search(r'(\d+)/', t)  # 月
            dd = re.search(r'/(\d+)', t)  # 日
            if mm is None or dd is None:
                continue

            cells = row.findAll('td')
            if len(cells) > 0:
                items = []
                flag_novalue = False
                for i in range(6): #open, high, low, close, vol, adj
                    v = cells[i].get_text().replace(',', '')
                    if is_float(v):
                        items.append(float(v))
                    else:
                        flag_novalue = True
                        break

                if flag_novalue:
                    continue

                items[4] = items[4] * (items[3]/items[5])     #修正後終値で補正(volumeは逆数)
                items[0] = items[0] * (items[5]/items[3])     #修正後終値で補正
                items[1] = items[1] * (items[5]/items[3])     #修正後終値で補正
                items[2] = items[2] * (items[5]/items[3])     #修正後終値で補正
                items[3] = items[3] * (items[5]/items[3])     #修正後終値で補正

                today = datetime.now()
                mm = int(mm.groups()[0])
                dd = int(dd.groups()[0])
                if today.month < mm:
                    yy = today.year - 1
                else:
                    yy = today.year
                vday = datetime(yy, mm, dd)

                se = pd.Series(items[0:5], index = ["Open", "High", "Low", "Close", "Volume"], name=vday.strftime('%Y-%m-%d'))
                df = df.append(se)
        return df


    # info
    def info_db(self, cc):
        sql = 'SELECT date FROM %s WHERE cc="%s" ORDER BY date DESC LIMIT 1;' % (args.stockdb, cc)
        self.mycursor.execute(sql)
        latest = self.mycursor.fetchone()
        if latest == None:
            latest = ""
        else:
            latest = latest[0]

        sql = 'SELECT date FROM %s WHERE cc="%s" ORDER BY date ASC LIMIT 1;' % (args.stockdb, cc)
        self.mycursor.execute(sql)
        oldest = self.mycursor.fetchone()
        if oldest == None:
            oldest = ""
        else:
            oldest = oldest[0]

        sql = 'SELECT count(*) FROM %s WHERE cc="%s"' % (args.stockdb, cc)
        self.mycursor.execute(sql)
        num = self.mycursor.fetchone()
        if num == None:
            num = 0
        else:
            num = int(num[0])

        logging.info("  cc: %s, %s - %s, %d data", cc, oldest, latest, num)

    # 最新株価を返す
    def get_latest_stock_data(self, company_code):
        if args.update_by_nikkei:
            return self.latest_stock_data_from_nikkei(company_code)

        else:
            # DBにアクセスしてDB内の最新の日付をゲット
            start_date = self.get_start_date(company_code)

            t = datetime.now().date()  # today
            if start_date <= t:
                # yahoo financeにアクセスして、株価を入手
                return self.yfinace(company_code, start_date).dropna()

    # DBの株価を更新する
    def update_stockdb(self, company_code):
        logging.info("CompanyCode: %s", cc)
        self.info_db(company_code)

        # DBの作成、初期データの挿入
        if args.historydb:
            # stock_data = self.initdb_kabuoji3(company_code)
            stock_data = self.csv2df(company_code)
            self.insert_data(company_code, stock_data, args.stockdb)

        # 最新株価の入手とDBの更新
        stock_data = self.get_latest_stock_data(company_code)
        self.insert_data(company_code, stock_data, args.stockdb)

        # 情報出力
        self.info_db(company_code)

    # yahoo financeから株価入手
    # 数年前の株価が怪しい、、、最新は大丈夫そうかな。
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
    parser.add_argument('--dropdb', action='store_true')
    parser.add_argument('--historydb', action='store_true')
    parser.add_argument('--url_db', default='mysql+mysqlconnector://stockdb:bdkcots@192.168.1.11:3306/stockdb')
    parser.add_argument('--stockdb', default='stockdb')
    parser.add_argument('--update_by_nikkei', action='store_true')
    args = parser.parse_args()

    formatter = '%(levelname)s : %(asctime)s : %(message)s'
    logging.basicConfig(filename='./update_stockdb.log', level=logging.INFO, format=formatter)

    stockdb = Stockdb(0, 1)

    skip = False
    for cc in stockdb.company_codes():
        if cc == '1605.JP':
            skip = False
        if skip:
            continue

        stockdb.update_stockdb(cc)
        time.sleep(3)
