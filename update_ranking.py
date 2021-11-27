import html5lib
from bs4 import BeautifulSoup
from urllib import request
import pandas as pd
import datetime
from sqlalchemy import create_engine
import argparse

Features = ["date", "ranking", "市場・商品区分", "cc", "mkt", "name", "price", "diff_rate", "diff", "volume"]

def crawl_single_page(url):
    response = request.urlopen(url)
    soup = BeautifulSoup(response)
    response.close()

    table=soup.find('table', class_="rankingTable")
    rows=table.find_all('tr', class_="rankingTabledata yjM")

    today = datetime.datetime.now().strftime('%Y-%m-%d')

    ret = pd.DataFrame(columns = Features)
    for row in rows:
        cols = row.find_all('td')
        ranking = int(cols[0].text)
        cc = cols[1].text + ".JP"
        mkt = cols[2].text
        name = cols[3].text
        price = float(cols[5].text.replace(',', ''))
        diff_rate = float(cols[6].text.replace('%', '')) / 100
        diff = float(cols[7].text.replace(',', '').replace('---', '0'))
        volume = float(cols[8].text.replace(',', ''))
        s = pd.Series([today, ranking, "ranking", cc, mkt, name, price, diff_rate, diff, volume], 
            index=Features)
        ret = ret.append(s, ignore_index=True)
        print(cols[0].text, cols[1].text, cols[2].text, cols[3].text, cols[8].text)

    return ret


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='stockdbの作成、更新')
    parser.add_argument('--url_db', default='mysql+mysqlconnector://user:pass@192.168.1.1:3306/stockdb')
    args = parser.parse_args()

    df = pd.DataFrame(columns = Features)

    for page in range(1, 81): # 80
        url = 'https://info.finance.yahoo.co.jp/ranking/?kd=31&tm=d&vl=a&mk=1&p=' + str(page)
        df_page = crawl_single_page(url)
        df = pd.concat([df, df_page])

    print(df)

    try:
        engine = create_engine(args.url_db)
        df.to_sql('cc_flow', con=engine, if_exists='append', index=False)
    except Exception as e:
        print(e)
