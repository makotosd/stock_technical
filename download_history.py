# -*- coding: utf-8 -*-

import logging
import time
import argparse
import io
import os.path
import logging

import pandas as pd

import requests
from selenium import webdriver
import chromedriver_binary
import html5lib
from bs4 import BeautifulSoup

from urllib.parse import urlparse
import mysql.connector

_dir = os.path.dirname(os.path.abspath(__file__))

def create_session():
    s = requests.Session()
    s.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
        "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:55.0) Gecko/20100101 Firefox/55.0"
    })
    return s


def yahoojp_session(target_url, login_id, password):

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
        "Connection": "keep-alive",
        "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:55.0) Gecko/20100101 Firefox/55.0"
    }

    cap = webdriver.DesiredCapabilities.PHANTOMJS
    for key, val in headers.items():
        cap["phantomjs.page.customHeaders." + key] = val
    cap["phantomjs.page.settings.userAgent"] = headers["User-Agent"]

    try:
        # <class 'selenium.webdriver.phantomjs.webdriver.WebDriver'>
        driver = webdriver.Chrome()  # webdriver.PhantomJS()
    except Exception as e:
        logging.error(e)
        return None

    # クッキーを先に取得する
    driver.get(target_url)
    bs4 = BeautifulSoup(driver.page_source, "html5lib")
    login_url = bs4.find("a", attrs={"id": "msthdLogin"})["href"]

    driver.get(login_url) # login_urlに↑のbs4いらないのでは、、、
    time.sleep(1)

    driver.find_element_by_name("login").send_keys(login_id)
    driver.find_element_by_name("btnNext").click()  # 次へボタン
    time.sleep(1)

    driver.find_element_by_name("passwd").send_keys(password)
    driver.find_element_by_name("btnSubmit").click()  # ログインボタン
    time.sleep(1)
    # driver.save_screenshot(_dir + "/login.png")   # ログイン済みを画像で確認できます

    # セッション情報をrequestsに移す
    s = create_session()
    for c in driver.get_cookies():
        s.cookies[c["name"]] = c["value"]

    driver.close()
    return s

def ccs():
    url = urlparse("mysql+mysqlconnector://stockdb:bdkcots@localhost:3306/stockdb")
    mydb = mysql.connector.connect(
        host=url.hostname,
        port=url.port,
        user=url.username,
        database=url.path[1:],
        password=url.password
    )
    mycursor = mydb.cursor(buffered=True)

    sql = 'SELECT cc FROM %s ORDER BY cc;' % ("data_j")
    mycursor.execute(sql)
    ret = []
    for cc in mycursor.fetchall():
        ret.append(str(cc[0]))
    return ret


# 以下の二つのサイトの情報から作成。
#  <https://qiita.com/toshi32y/items/463d5566e1efaf370f4f>
#  <https://www.shibuya24.info/entry/parse_logined_html>

# ただ、こんなことしなくても、seleniumでログイン後、
# url = 'https://raw.githubusercontent.com/ground0state/for_qiita/master/data/iris.csv'
# res = session.get(url).content
# df = pd.read_csv(io.StringIO(res.decode('utf-8')), header=0, index_col=0)
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='stockdbの作成、更新')
    parser.add_argument('--yahoo_id')
    parser.add_argument('--yahoo_pw')
    args = parser.parse_args()

    formatter = '%(levelname)s : %(asctime)s : %(message)s'
    logging.basicConfig(filename='./get_csv_yahoo.log', level=logging.INFO, format=formatter)

    logging.info("#### start download csv ####")

    session = yahoojp_session("https://finance.yahoo.co.jp/", args.yahoo_id, args.yahoo_pw)

    for cc in ccs():
        cc = cc.replace(".JP", "")
        saveFilePath = os.path.join("./history/", "%s.T.csv" % (cc))
        url = "https://download.finance.yahoo.co.jp/common/history/%s.T.csv" % (cc)
        response = session.get(url)

        if response.status_code == requests.codes.ok:
            with open(saveFilePath, 'wb') as saveFile:
                saveFile.write(response.content)
        else:
            logging.info("no csv for %s" % (cc))

        time.sleep(1)