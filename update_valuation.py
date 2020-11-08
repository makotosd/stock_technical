#!/usr/bin/python
# coding: utf-8
#
import argparse
import logging
from urllib.parse import urlparse
import mysql.connector

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
        self.companycode = cc

    def __del__(self):
        pass

class companies():
    def __init__(self, conn):
        sql = "SELECT cc FROM %s"
        conn.mycursor.execute(sql)
        self.list = []
        for cc in conn.mycursor.fetchall():
            co = company(cc)
            self.list.append(co)

    def __del__(self):
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='stockdbの作成、更新')
    parser.add_argument('--droptable', action='store_true')
    parser.add_argument('--tablename', default='valuation')
    args = parser.parse_args()

    dbconnctor = dbconnector()
    companies = companies(dbconnector)
