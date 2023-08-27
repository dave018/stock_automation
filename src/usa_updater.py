import pymysql
import pandas as pd
import time

from bs4 import BeautifulSoup
from datetime import timedelta
from urllib import request as req
from yahoo_fin import stock_info

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

    def update_usa_comp_info(self):
        markets = ['nasdaq', 'other']

        for market in markets:
            self.__update_usa_comp_info(market)

    def __update_usa_comp_info(self, market):
        sql = f"SELECT ticker FROM {TABLE_NAME_USA_COMP_INFO}"
        self.cur.execute(sql)
        result = self.cur.fetchall()
        existed_companies = set([row[0] for row in result])

        today = datetime.today().strftime("%Y-%m-%d")
        current_nasdaq_tickers = set(stock_info.tickers_nasdaq())

        new_tickers = current_nasdaq_tickers - existed_companies

        if not new_tickers:
            print(f"There is not new tickers in {market}")
        else:
            for new_ticker in new_tickers:
                print(f"{new_ticker} is a new company")
                sql = f"INSERT INTO {TABLE_NAME_USA_COMP_INFO}(ticker, market, last_update) " \
                      f"VALUES('{new_ticker}', '{market}', '{today}')"
                self.cur.execute(sql)

        sql = f"UPDATE {TABLE_NAME_USA_COMP_INFO} SET last_update='{today}' WHERE market='{market}'"
        self.cur.execute(sql)
        self.conn.commit()

    def update_usa_price(self):
        start_time = datetime.today().strftime("%Y-%m-%d")
        self._update_usa_price(start_time=start_time, market='nasdaq')
        self._update_usa_price(start_time=start_time, market='other')

    def _update_usa_price(self, start_date=None, end_date=None, start_time="", market=None):
        if not market:
            market = 'nasdaq'

        sql = f"SELECT ticker FROM {TABLE_NAME_USA_COMP_INFO} WHERE market='{market}'"

        ###
        starttime = time.time()
        ###
        self.cur.execute(sql)
        result = self.cur.fetchall()
        tickers = [ticker[0] for ticker in result]

        update_done_count = 0
        for ticker in tickers:
            # 이미 실행한 날에 가격 업데이트를 마쳤는지 확인
            sql = f"SELECT EXISTS(SELECT 1 FROM {TABLE_NAME_USA_DAILY_PRICE} " \
                  f"WHERE ticker=%s and update_date=%s) as cnt"
            self.cur.execute(sql, (ticker, start_time))
            result = self.cur.fetchone()

            if result[0] != 0:
                print(f"Update {ticker}'s price already done at {start_time}")
                continue

            sql = f"SELECT max(market_date) FROM {TABLE_NAME_USA_DAILY_PRICE} WHERE ticker='{ticker}'"
            self.cur.execute(sql)
            last_update = self.cur.fetchone()[0]

            if start_date is None and last_update is not None:
                start_date = last_update + timedelta(1)

            try:
                price = stock_info.get_data(ticker=ticker, start_date=start_date, end_date=end_date)
                if price is None:
                    continue
            except Exception as e:
                print(e)
                continue

            price.index.name = 'market_date'
            price['today_diff'] = price['close'] - price['open']
            price['yesterday_diff'] = (price.close - price.close.shift()).fillna(0)
            price = price.dropna()

            for idx, daily_price in price.iterrows():
                market_time = idx.strftime('%Y-%m-%d')

                # daily_price와 다른 필요한 변수들을 하나의 dict에 모읍니다.
                data = {
                    'ticker': ticker,
                    'market_date': market_time,
                    'update_date': start_time,
                    'market': market,
                    'open': daily_price['open'],
                    'high': daily_price['high'],
                    'low': daily_price['low'],
                    'close': daily_price['close'],
                    'today_diff': daily_price['today_diff'],
                    'yesterday_diff': daily_price['yesterday_diff'],
                    'volume': daily_price['volume'],
                }

                if last_update and idx <= last_update:
                    # 이 dict를 사용하여 UPDATE 쿼리를 생성합니다.
                    sql = f"UPDATE {TABLE_NAME_USA_DAILY_PRICE} SET " + ', '.join(f"{k}=%({k})s" for k in data.keys()) \
                          + " WHERE ticker=%(ticker)s and market_date=%(market_date)s"
                else:
                    # 이 dict를 사용하여 INSERT 쿼리를 생성합니다.
                    sql = f"INSERT INTO {TABLE_NAME_USA_DAILY_PRICE}(" + ', '.join(data.keys()) + \
                          ") VALUES (" + ', '.join(f"%({k})s" for k in data.keys()) + ")"

                try:
                    self.cur.execute(sql)
                except Exception as e:
                    print(f"{ticker} has error while inserting execution")
                    print(f"market_update in DB: {last_update} / start_date: {start_date}")
                    print(f"SQL: {sql}")
                    print(e)

            update_done_count += 1
            print(f"Ticker: {ticker}, Count: {update_done_count} is done")
            self.conn.commit()

        endtime = time.time()
        print(f"Elapsed time = {endtime-starttime}")

    def update_sp500_price(self):
        starttime = time.time()

        sql = f"SHOW FULL COLUMNS from sp500_indv_price"
        cols = pd.read_sql(sql, self.conn)
        sp500_tickers = stock_info.tickers_sp500()

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
