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

    def __del__(self):
        pass

class company():
    def __init__(self):
        pass
    def __del__(self):
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='stockdbの作成、更新')
    parser.add_argument('--dropdb', action='store_true')
    args = parser.parse_args()

    dbconnctor = dbconnector()
