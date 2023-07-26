import pymysql
import pandas as pd
import time

from bs4 import BeautifulSoup
from datetime import timedelta
from urllib import request as req
from yahoo_fin import stock_info as si

from db_vars import *
from asap_logger import *


''' Vars for Web-scraping '''
headers = ('User-Agent', 'Mozilla/5.0')

class USAUpdater:
    def __init__(self):
        """ 생성자: MariaDB 연결하는 conn 생성 및 (cods:company)관계의 dict 생성 """
        self.conn = pymysql.connect(host='localhost', user='root', password='tpghks981!',
                                    db='sehwan_inv', charset='utf8')
        self.cur = self.conn.cursor()
        self.dict_code_company = dict()
        return

    def update_usa_comp_info(self):
        self.update_nas_comp_info()
        self.update_other_comp_info()

    def update_nas_comp_info(self):
        today = datetime.today().strftime("%Y-%m-%d")
        nas_tickers = si.tickers_nasdaq()

        for ticker in nas_tickers:
            sql = f"SELECT max(last_update) FROM {table_name_usa_comp_info} WHERE ticker='{ticker}'"
            self.cur.execute(sql)
            result = self.cur.fetchone()
            sql = ''
            if result[0] is None:
                print(f"{ticker} isn't in DB")
                sql = f"INSERT INTO {table_name_usa_comp_info}(ticker, market, last_update) " \
                      f"VALUES('{ticker}', 'nasdaq', '{today}')"
            elif result[0].strftime('%Y-%m-%d') < today:
                print(f"{ticker} is in DB")
                sql = f"UPDATE {table_name_usa_comp_info} SET last_update='{today}' WHERE ticker='{ticker}'"
            else:
                print(f"{ticker} already exists")

            if len(sql) > 0:
                self.cur.execute(sql)

        self.conn.commit()

    def update_other_comp_info(self):
        today = datetime.today().strftime("%Y-%m-%d")
        other_tickers = si.tickers_other()

        for ticker in other_tickers:
            sql = f"SELECT max(last_update) FROM {table_name_usa_comp_info} WHERE ticker='{ticker}'"
            self.cur.execute(sql)
            result = self.cur.fetchone()
            sql = ''
            if result[0] is None:
                print(f"{ticker} isn't in DB")
                sql = f"INSERT INTO {table_name_usa_comp_info}(ticker, market, last_update) " \
                      f"VALUES('{ticker}', 'other', '{today}')"
            elif result[0].strftime('%Y-%m-%d') < today:
                print(f"{ticker} is in DB")
                sql = f"UPDATE {table_name_usa_comp_info} SET last_update='{today}' WHERE ticker='{ticker}'"
            else:
                print(f"{ticker} already exists")

            if len(sql) > 0:
                self.cur.execute(sql)

        self.conn.commit()

    def get_all_nas_ticker(self):
        url = f'https://www.nasdaq.com/market-activity/stocks/screener?exchange=nasdaq'
        opener = req.build_opener()
        opener.addheaders = [headers]
        response = opener.open(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        tickers = []
        for row in soup.find_all('tbody')[0].find_all('tr'):
            ticker = row.find_all('td')[0].text.strip()
            tickers.append(ticker)
            return

    def update_usa_price(self):
        start_time = datetime.today().strftime("%Y-%m-%d")
        self._update_usa_price(start_time=start_time, market='nasdaq')
        self._update_usa_price(start_time=start_time, market='other')
        return

    def _update_usa_price(self, start_date=None, end_date=None, start_time="", market=None):
        if not market:
            market = 'nasdaq'

        sql = f"SELECT ticker FROM {table_name_usa_comp_info} WHERE market='{market}'"

        ###
        starttime = time.time()
        ###
        self.cur.execute(sql)
        tickers = self.cur.fetchall()

        count = 0

        for ticker in tickers:
            tic = ticker[0]
            sql = f"SELECT EXISTS(SELECT 1 FROM {table_name_usa_daily_price} " \
                  f"WHERE ticker='{tic}' and update_date='{start_time}') as cnt"
            self.cur.execute(sql)
            result = self.cur.fetchone()

            if result[0] != 0:
                print(f"Update {tic}'s price already done at {start_time}")
                continue

            if start_date is None:
                sql = f"SELECT max(market_date) FROM {table_name_usa_daily_price} WHERE ticker='{tic}'"
                self.cur.execute(sql)
                last_update = self.cur.fetchone()[0]
                if last_update is None:
                    start_date = None
                else:
                    start_date = last_update + timedelta(1)

            try:
                price = si.get_data(ticker=tic, start_date=start_date, end_date=end_date)
            except Exception as e:
                print(e)
                continue

            if price is None:
                continue

            price.index.name = 'market_date'
            price['today_diff'] = price['close'] - price['open']
            price['yesterday_diff'] = (price.close - price.close.shift()).fillna(0)
            price = price.dropna()

            for idx, daily_price in price.iterrows():
                market_time = idx.strftime('%Y-%m-%d')
                if last_update and idx <= last_update:
                    sql = f"UPDATE {table_name_usa_daily_price} SET ticker='{tic}', market_date='{market_time}', " \
                          f"update_date='{start_time}', market='{market}', open='{daily_price['open']}', " \
                          f"high='{daily_price['high']}', low='{daily_price['low']}', close='{daily_price['close']}', " \
                          f"today_diff='{daily_price['today_diff']}', yesterday_diff='{daily_price['yesterday_diff']}', " \
                          f"volume='{daily_price['volume']}' WHERE ticker='{tic}' and market_date='{market_time}'"
                else:
                    sql = f"INSERT INTO {table_name_usa_daily_price}(ticker, market_date, update_date, market, open, " \
                          f"high, low, close, today_diff, yesterday_diff, volume) " \
                          f"VALUES('{tic}', '{market_time}', '{start_time}', '{market}', '{daily_price['open']}', " \
                          f"'{daily_price['high']}', '{daily_price['low']}', '{daily_price['close']}', " \
                          f"'{daily_price['today_diff']}', '{daily_price['yesterday_diff']}', '{daily_price['volume']}')"

                try:
                    self.cur.execute(sql)
                except Exception as e:
                    print(f"{ticker} has error while inserting execution")
                    print(f"market_update in DB: {last_update} / start_date: {start_date}")
                    print(f"SQL: {sql}")
                    print(e)
            count += 1
            print(f"Ticker: {tic}, Count: {count} is done")
            self.conn.commit()
        endtime = time.time()
        print(f"Elapsed time = {endtime-starttime}")

    def update_nas_price(self, start_date=None, end_date=None, start_time=""):
        sql = f"SELECT ticker FROM {table_name_usa_comp_info} WHERE market='nasdaq'"

        ###
        starttime = time.time()
        ###
        self.cur.execute(sql)
        tickers = self.cur.fetchall()

        count = 0

        for ticker in tickers:
            tic = ticker[0]
            sql = f"SELECT EXISTS(SELECT 1 FROM {table_name_usa_daily_price} " \
                  f"WHERE ticker='{tic}' and update_date='{start_time}') as cnt"
            self.cur.execute(sql)
            result = self.cur.fetchone()

            if result[0] != 0:
                print(f"Update {tic}'s price already done at {start_time}")
                continue

            if start_date is None:
                sql = f"SELECT max(market_date) FROM {table_name_usa_daily_price} WHERE ticker='{tic}'"
                self.cur.execute(sql)
                last_update = self.cur.fetchone()[0]
                if last_update is None:
                    start_date = None
                else:
                    start_date = last_update + timedelta(1)

            try:
                price = si.get_data(ticker=tic, start_date=start_date, end_date=end_date)
            except Exception as e:
                print(e)
                continue

            if price is None:
                continue

            price.index.name = 'market_date'
            date = price.index.strftime("%Y-%m-%d")
            date1 = date[0]
            price['today_diff'] = price['close'] - price['open']
            price['yesterday_diff'] = (price.close - price.close.shift()).fillna(0)
            price = price.dropna()

            for idx, daily_price in price.iterrows():
                market_time = idx.strftime('%Y-%m-%d')
                if last_update and idx <= last_update:
                    sql = f"UPDATE {table_name_usa_daily_price} SET ticker='{tic}', market_date='{market_time}', " \
                          f"update_date='{start_time}', market='nasdaq', open='{daily_price['open']}', " \
                          f"high='{daily_price['high']}', low='{daily_price['low']}', close='{daily_price['close']}', " \
                          f"today_diff='{daily_price['today_diff']}', yesterday_diff='{daily_price['yesterday_diff']}', " \
                          f"volume='{daily_price['volume']}' WHERE ticker='{tic}' and market_date='{market_time}'"
                else:
                    sql = f"INSERT INTO {table_name_usa_daily_price}(ticker, market_date, update_date, market, open, " \
                          f"high, low, close, today_diff, yesterday_diff, volume) " \
                          f"VALUES('{tic}', '{market_time}', '{start_time}', 'nasdaq', '{daily_price['open']}', " \
                          f"'{daily_price['high']}', '{daily_price['low']}', '{daily_price['close']}', " \
                          f"'{daily_price['today_diff']}', '{daily_price['yesterday_diff']}', '{daily_price['volume']}')"

                try:
                    self.cur.execute(sql)
                except Exception as e:
                    print(f"{ticker} has error while inserting execution")
                    print(f"market_update in DB: {last_update} / start_date: {start_date}")
                    print(f"SQL: {sql}")
                    print(e)
            count += 1
            print(f"Ticker: {tic}, Count: {count} is done")
            self.conn.commit()
        endtime = time.time()
        print(f"Elapsed time = {endtime-starttime}")

    def update_other_price(self, start_date=None, end_date=None, start_time=""):
        sql = f"SELECT ticker FROM {table_name_usa_comp_info} WHERE market='other'"

        ###
        starttime = time.time()
        ###
        self.cur.execute(sql)
        tickers = self.cur.fetchall()

        count = 0

        for ticker in tickers:
            tic = ticker[0]
            sql = f"SELECT EXISTS(SELECT 1 FROM {table_name_usa_daily_price} " \
                  f"WHERE ticker='{tic}' and update_date='{start_time}') as cnt"
            self.cur.execute(sql)
            result = self.cur.fetchone()

            if result[0] != 0:
                print(f"Update {tic}'s price already done at {start_time}")
                continue

            if start_date is None:
                sql = f"SELECT max(market_date) FROM {table_name_usa_daily_price} WHERE ticker='{tic}'"
                self.cur.execute(sql)
                last_update = self.cur.fetchone()[0]
                if last_update is None:
                    start_date = None
                else:
                    start_date = last_update + timedelta(1)

            try:
                price = si.get_data(ticker=tic, start_date=start_date, end_date=end_date)
            except Exception as e:
                print(e)
                continue

            if price is None:
                continue

            price.index.name = 'market_date'
            date = price.index.strftime("%Y-%m-%d")
            date1 = date[0]
            price['today_diff'] = price['close'] - price['open']
            price['yesterday_diff'] = (price.close - price.close.shift()).fillna(0)
            price = price.dropna()

            for idx, daily_price in price.iterrows():
                market_time = idx.strftime('%Y-%m-%d')
                if last_update and idx <= last_update:
                    sql = f"UPDATE {table_name_usa_daily_price} SET ticker='{tic}', market_date='{market_time}', " \
                          f"update_date='{start_time}', market='other', open='{daily_price['open']}', " \
                          f"high='{daily_price['high']}', low='{daily_price['low']}', close='{daily_price['close']}', " \
                          f"today_diff='{daily_price['today_diff']}', yesterday_diff='{daily_price['yesterday_diff']}', " \
                          f"volume='{daily_price['volume']}' WHERE ticker='{tic}' and market_date='{market_time}'"
                else:
                    sql = f"INSERT INTO {table_name_usa_daily_price}(ticker, market_date, update_date, market, open, " \
                          f"high, low, close, today_diff, yesterday_diff, volume) " \
                          f"VALUES('{tic}', '{market_time}', '{start_time}', 'other', '{daily_price['open']}', " \
                          f"'{daily_price['high']}', '{daily_price['low']}', '{daily_price['close']}', " \
                          f"'{daily_price['today_diff']}', '{daily_price['yesterday_diff']}', '{daily_price['volume']}')"

                try:
                    self.cur.execute(sql)
                except Exception as e:
                    print(f"{ticker} has error while inserting execution")
                    print(f"market_update in DB: {last_update} / start_date: {start_date}")
                    print(f"SQL: {sql}")
                    print(e)
            count += 1
            self.conn.commit()
        endtime = time.time()
        print(f"Elapsed time = {endtime-starttime}")

    def update_sp500_price(self):
        starttime = time.time()

        sql = f"SHOW FULL COLUMNS from sp500_indv_price"
        cols = pd.read_sql(sql, self.conn)
        sp500_tickers = si.tickers_sp500()

        # Column 과 ticker 매칭 확인 후 없는 ticker 추가
        for idx, ticker in enumerate(sp500_tickers):
            if not ticker in cols['Field'].values:
                try:
                    sql = f"ALTER TABLE sp500_indv_price ADD {ticker} FLOAT Default -1"
                    self.cur.execute(sql)
                    self.conn.commit()
                except Exception as e:
                    print(f"[{idx}]{ticker} failed to update")
                    print(e)

        # ticker별로 close 값 추가하기. PK는 datetime이다.
        for idx, ticker in enumerate(sp500_tickers):
            print(f"idx: {idx} / ticker: {ticker}")
            sql = f"SELECT * FROM usa_daily_price where ticker='{ticker}'"
            price_df = pd.read_sql(sql, self.conn)
            price_df.index = price_df['market_date']

            if price_df.shape[0] == 0:
                continue

            for idx, row in price_df.iterrows():
                sql = f"SELECT EXISTS(SELECT 1 FROM sp500_indv_price WHERE market_date='{idx}') as cnt"
                self.cur.execute(sql)
                result = self.cur.fetchone()
                if result[0] == 0:
                    sql = f"INSERT INTO sp500_indv_price(market_date) VALUES('{idx}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    continue

                sql = f"UPDATE sp500_indv_price SET {ticker}='{row['close']}' WHERE market_date='{idx}'"
                self.cur.execute(sql)
                self.conn.commit()

        endtime = time.time()
        print(f"Elapsed time = {endtime-starttime}")