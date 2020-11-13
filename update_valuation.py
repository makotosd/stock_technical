#!/usr/bin/python
# coding: utf-8
#
import argparse
import logging
from urllib.parse import urlparse
import mysql.connector
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime
import logging
from sqlalchemy import create_engine

class dbconnector():

    def __init__(self):
        url = urlparse(args.url_db)
        self.mydb = mysql.connector.connect(
            host=url.hostname,
            port=url.port,
            user=url.username,
            database=url.path[1:],
            password=url.password
        )
        self.mycursor = self.mydb.cursor(buffered=True)

        if args.droptable :
            sql = 'DROP TABLE IF EXISTS %s ;' % (args.tablename)
            self.mycursor.execute(sql)

        # 予想株価収益率: PER
        # 予想配当利回り: Dividend yield, DY
        # 株価純資産倍率: PBR
        # 予想自己資本利益率: ROE
        # (株式益回り: PERの逆数)
        # 株式数: Number of Shares, NOS
        # 時価総額: Market Capitalization, MC
        self.mycursor = self.mydb.cursor(buffered=True)
        sql = 'CREATE TABLE IF NOT EXISTS %s (' % (args.tablename)
        sql += 'date DATE NOT NULL, '
        sql += 'cc VARCHAR(16) NOT NULL, '
        sql += 'PER FLOAT, DY FLOAT, PBR FLOAT, ROE FLOAT, NOS BIGINT, MC FLOAT,'
        sql += 'PRIMARY KEY(date, cc)'
        sql += ') PARTITION BY KEY(cc);'
        # sql += ') PARTITION BY KEY(cc) PARTITIONS 4096;'
        self.mycursor.execute(sql)

    def __del__(self):
        pass

class company():
    def __init__(self, cc):
        self.companycode = str(cc) + ".JP"
        self.valuation = self.get_valuation(self.companycode)

    def __del__(self):
        pass

    def get_valuation(self, cc):
        headers = {
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
        }
        url = 'https://www.nikkei.com/nkd/company/?scode=%s' % (cc.replace('.JP', ''))
        html = requests.get(url, headers=headers)
        soup = BeautifulSoup(html.text,'html.parser')

        ###########################################################################
        dt = soup.find(class_='m-stockInfo_date')
        if dt is None:
            return {}
        dt = datetime.strptime(dt.get_text(), '%Y/%m/%d')

        ###########################################################################
        detail_right = soup.find(class_="m-stockInfo_detail_right")
        if detail_right is None:
            return {}

        detail_right_value = detail_right.find(class_="m-stockInfo_detail_list")
        if detail_right_value is None:
            return {}

        detail_right_value_li = detail_right_value.findAll('li')
        if detail_right_value_li is None:
            return {}

        # 売買高, 予想PER, 予想配当利回り(DY)
        per = re.sub('([\-0-9.]+) .+', r'\1', detail_right_value_li[1].find(class_="m-stockInfo_detail_value").get_text().replace(',', ''))
        if per == "--" :
            per = None
        else:
            per = float(per)

        dy = re.sub('([\-0-9.]+) .+', r'\1', detail_right_value_li[1].find(class_="m-stockInfo_detail_value").get_text().replace(',', ''))
        if dy == "--" :
            dy = None
        else:
            dy = float(dy)

        ############################################################################
        detail_left = soup.find(class_="m-stockInfo_detail m-stockInfo_detail_left")
        if detail_left is None:
            return {}

        detail_left_value = detail_left.find(class_="m-stockInfo_detail_list")
        if detail_left_value is None:
            return {}

        detail_left_value_li = detail_left_value.findAll('li')
        if detail_left_value_li is None:
            return {}

        # PBR, ROE, 株式益回り（予想）, 普通株式数(NOS), 時価総額(MC)
        pbr = re.sub('([\-0-9.]+) .+', r'\1', detail_left_value_li[0].find(class_="m-stockInfo_detail_value").get_text().replace(',', ''))
        if pbr == "--":
            pbr = None
        else:
            pbr = float(pbr)

        roe = re.sub('([\-0-9.]+) .+', r'\1', detail_left_value_li[1].find(class_="m-stockInfo_detail_value").get_text().replace(',', ''))
        if roe == "N/A":
            roe = None
        else:
            roe = float(roe)

        nos = re.sub('([\-0-9.]+) .+', r'\1', detail_left_value_li[3].find(class_="m-stockInfo_detail_value").get_text().replace(',', ''))
        nos = int(nos)

        mc = re.sub('([\-0-9.]+) .+', r'\1', detail_left_value_li[4].find(class_="m-stockInfo_detail_value").get_text().replace(',', ''))
        mc = int(mc)

        # return (dt, pd.Series([per, dy, pbr, roe, nos, mc], index = ["PER", "DY", "PBR", "ROE", "NOS", "MC"], name=cc))
        return {
            'cc': self.companycode,
            'date': dt.strftime('%Y-%m-%d'),
            'PER': per,
            'DY': dy,
            'PBR': pbr,
            'ROE': roe,
            'NOS': nos,
            'MC': mc
        }

    def insertdb(self, conn):
        logging.info(self.valuation)
        # sql = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) VALUES ("%s", "%s", %f, %f, %f, %f, %d, %d)'
        sql1 = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s) VALUES ' % (args.tablename, 'date', 'cc', 'PER', 'DY', 'PBR', 'ROE', 'NOS', 'MC')
        sql2 = '(%s, %s, %s, %s, %s, %s, %s, %s)'
        sql = sql1 + sql2
        # v = self.valuation.where(pd.notnull(self.valuation), None)
        # v = v.astype({'PER': float, 'DY': float, 'PBR': float, 'ROE': float, 'NOS': int, 'MC': int})
        # self.valuation = v
        if len(self.valuation) == 0:
            return

        try:
            conn.mycursor.execute(sql, 
                (self.valuation['date'], self.valuation['cc'],
                self.valuation['PER'], self.valuation['DY'], self.valuation['PBR'],
                self.valuation['ROE'], self.valuation['NOS'], self.valuation['MC']))
        except mysql.connector.IntegrityError as e:
            logging.error("history already exist: %s" % e)
        except mysql.connector.DataError as e:
            logging.error("dataerror exist: %s\n" % e)
        '''
        engine = create_engine(args.url_db)
        self.valuation.to_sql(args.tablename, con=engine.connect(), index=True, if_exists="replace")
        '''
        conn.mydb.commit()

class companies():
    def __init__(self, conn):
        sql = "SELECT cc FROM %s" % ('data_j')
        conn.mycursor.execute(sql)
        self.list = []

        skip = False
        if args.skipuntil != '':
            skip = True

        for cc in conn.mycursor.fetchall():
            if skip == True and str(cc[0]) == args.skipuntil:
                skip = False
            if skip:
                continue
        
            co = company(cc[0])
            co.insertdb(conn)
            self.list.append(co)
            time.sleep(args.sleep)

    def __del__(self):
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='stockdbの作成、更新')
    parser.add_argument('--droptable', action='store_true')
    parser.add_argument('--tablename', default='valuation')
    parser.add_argument('--url_db', default='mysql+mysqlconnector://stockdb:bdkcots@192.168.1.11:3306/stockdb')
    parser.add_argument('--sleep', default=4)
    parser.add_argument('--skipuntil', default='')
    args = parser.parse_args()

    formatter = '%(levelname)s : %(asctime)s : %(message)s'
    logging.basicConfig(filename='./update_valuation.log', level=logging.INFO, format=formatter)

    dbconn = dbconnector()
    companies = companies(dbconn)
