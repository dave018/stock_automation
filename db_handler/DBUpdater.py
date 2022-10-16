import json
import pymysql
import pandas as pd
from bs4 import BeautifulSoup
from threading import Timer
from datetime import datetime, timedelta
from urllib import request as req
from db_handler.DBConnector import DBConnector
import yfinance as yf
import requests
import io
import re

from db_handler.db_vars import *

''' Vars for Web-scraping '''
headers = ('User-Agent', 'Mozilla/5.0')

class DBUpdater:
    def __init__(self):
        """ 생성자: MariaDB 연결하는 conn 생성 및 (cods:company)관계의 dict 생성 """
        self.conn = pymysql.connect(host='localhost', user='root', password='tpghks981!',
                                    db='sehwan_inv', charset='utf8')
        self.dict_code_company = dict()

    def __del__(self):
        self.conn.close()

    def set_env(self):
        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                self.pages_to_fetch_daily = config['pages_to_fetch_daily']
                self.pages_to_fetch_all = config['pages_to_fetch_all']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                self.pages_to_fetch_daily = 10
                self.pages_to_fetch_all = 1000
                config = {'pages_to_fetch_daily': self.pages_to_fetch_daily, 'pages_to_fetch_all': self.pages_to_fetch_all}
                json.dump(config, out_file)

    def check_connection(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT VERSION();")
        result = cursor.fetchone()
        print("MariaDB version : {}".format(result))
        return

    def make_table(self, table_name):
        cursor = self.conn.cursor()
        sql = "CREATE TABLE IF NOT EXISTS "
        if table_name == "daily_price":
            sql += table_name_daily_price + """
                (
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date))
            """
        elif table_name == "company_info":
            sql += table_name_comp_info + """
                (
                code VARCHAR(20),
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (code))
            """
        else:
            print("making {} is not ready".format(table_name))

        cursor.execute(sql)
        self.conn.commit()
        return

    def read_krx_code(self):
        """ KRX로부터 상장기업 목록 파일을 읽어와서 데이터프레임으로 변환 """
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx = pd.read_html(url, header=0)[0] # 위 url에 대해서 그냥 read_html()을 호출하면, DF의 list가 반환된다. 그래서 0번 원소 DF를 특정 필요
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드': 'code', '회사명': 'company'}) # 내 DB에서 사용하는 col 명으로 변경
        krx.code = krx.code.map('{:06d}'.format) # 6자리 digit으로 변경하며, 빈자리는 '0'으로 채움
        return krx

    def update_comp_info(self):
        """ 종목코드를 company_info 테이블에 업데이트 한 후, 딕셔너리(self.codes)에 저장 """
        sql = 'SELECT * FROM ' + table_name_comp_info
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]

        """ company_info 테이블의 'last_update' col 업데이트 """
        with self.conn.cursor() as curs: # 왜 with문을 쓰나?
            sql = 'SELECT max(last_update) FROM ' + table_name_comp_info
            curs.execute(sql)
            result = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')
            print(today)
            if result[0] == None or result[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    """ f-string으로, 문자열 포매팅 방법이다. python v3.6 이후부터 사용 가능하며, str.format보다 최근 """
                    sql = f"REPLACE INTO " + table_name_comp_info + " (code, company, last_update) " \
                          f"VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f'[{tmnow}] #{idx+1:04d} REPLACE INTO ' + table_name_comp_info + \
                          f'VALUES ({code}, {company}, {today})')
                self.conn.commit()
                print('')

        return

    def get_comp_info(self):
        sql = "SELECT * FROM " + table_name_comp_info
        return pd.read_sql(sql, self.conn)

    def get_daily_price(self, code, day):
        start_day = datetime.today() - timedelta(days=day)

        sql = "SELECT * FROM " + table_name_daily_price + " WHERE code ='" + code + "' AND date > '" + \
              start_day.strftime("%Y-%m-%d") + "'"

        return pd.read_sql(sql, self.conn)

    def calc_diff(self, df_price):
        l = len(df_price)
        for i in range(l - 1):
            df_price['diff'].iloc[i] = df_price['close'].iloc[i] - df_price['close'].iloc[i + 1]
        return

    def read_naver_kr(self, code, company, pages_to_fetch):
        try:
            url = f'http://finance.naver.com/item/sise_day.naver?code={code}'
            opener = req.build_opener()
            opener.addheaders = [headers]
            response = opener.open(url)
            doc = BeautifulSoup(response, 'lxml')
            last_page = doc.find('td', class_='pgRR').a['href'].split('=')[-1]

            df = pd.DataFrame()
            pages = min(int(last_page), pages_to_fetch)

            for page in range(1, pages + 1):
                pg_url = '{}&page={}'.format(url, page)
                response = opener.open(pg_url)
                df = df.append(pd.read_html(response, header=0)[0])
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'\
                      .format(tmnow, company, code, page, pages), end="\r")

            df = df.rename(columns={'날짜':'date', '종가':'close', '전일비':'diff', '시가':'open',
                                    '고가':'high', '저가':'low', '거래량':'volume'})
            df['date'] = df['date'].replace('.', '-')
            df = df.dropna()
            # pandas의 데이터프레임 데이터타입 한번에 바꾸기
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = \
                df[['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)
            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
            self.calc_diff(df)
        except Exception as e:
            print('Exception occured', str(e))
            return None

        return df

    def replace_price_db(self, df, idx, code, company):
        with self.conn.cursor() as curs:
            for data in df.itertuples():
                sql = f"REPLACE INTO " + table_name_daily_price + " VALUES ('{code}', " \
                      f"'{data.date}', {data.open}, {data.high}, {data.low}, {data.close}, " \
                      f"{data.diff}, {data.volume})"
                curs.execute(sql)
            self.conn.commit()
        print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO ' + table_name_daily_price + \
              '[OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), idx + 1, company, code, len(df)))

    def check_last_update_price(self):
        sql = f"SELECT date from {table_name_daily_price}"
        return

    def update_stock_price(self, is_all):
        """ KRX 상장 법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트 """
        print("Start function name: ", self.update_stock_price.__name__)
        pages_to_fetch = self.pages_to_fetch_all if is_all else self.pages_to_fetch_daily

        company_info = self.get_comp_info()
        idx = 0
        for idx, row in company_info.iterrows():
            df = self.read_naver_kr(row['code'], row['company'], pages_to_fetch)
            if df is None:
                continue
            self.replace_price_db(df, idx, row['code'], row['company'])

    def new_update_nas_company_info(self):
        url = "https://pkgstore.datahub.io/core/nasdaq-listings/nasdaq-listed_csv/data/7665719fb51081ba0bd834fde71ce822/nasdaq-listed_csv.csv"
        s = requests.get(url).content
        companies = pd.read_csv(io.StringIO(s.decode('utf-8')))

        sql = 'SELECT * FROM ' + table_name_nas_comp_info
        company_df = pd.read_sql(sql, self.conn)
        company_code = dict()
        for idx in range(len(company_df)):
            company_code[company_df['code'].values[idx]] = company_df['company'].values[idx]

        with self.conn.cursor() as curs:
            sql = 'SELECT max(last_update) FROM ' + table_name_nas_comp_info
            curs.execute(sql)
            result = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')
            print(today)
            if result[0] == None or result[0].strftime('%Y-%m-%d') < today:
                for idx in range(len(companies)):
                    code = companies['Symbol'][idx]
                    name = companies['Company Name'][idx]
                    name = re.sub("[!',.@#$]", '', name)
                    sql = f"REPLACE INTO nas_company_info (code, company, last_update) " \
                          f"VALUES ('{code}', '{name}', '{today}')"
                    curs.execute(sql)
                    company_code[code] = name
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f'[{tmnow}] #{idx + 1:04d} REPLACE INTO company_info ' \
                        f'VALUES ({code}, {name}, {today})')
                self.conn.commit()
        print(companies)

    def tmp_update_nas_company_info(self):
        try:
            with open('db_handler/nasdaq_stock.json', 'r', encoding="utf-8") as in_file:
                nas = json.load(in_file)
                print(nas)
        except FileNotFoundError:
            print("There isn't a json file for NASDAQ company info")

        sql = 'SELECT * FROM nas_company_info'
        df = pd.read_sql(sql, self.conn)
        nas_codes = dict()
        for idx in range(len(df)):
            nas_codes[df['code'].values[idx]] = df['company'].values[idx]

        with self.conn.cursor() as curs:
            sql = 'SELECT max(last_update) FROM nas_company_info'
            curs.execute(sql)
            result = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')
            print(today)
            if result[0] == None or result[0].strftime('%Y-%m-%d') < today:
                for idx, company in enumerate(nas):
                    code = nas[company]
                    sql = f"REPLACE INTO nas_company_info (code, company, last_update) " \
                          f"VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    nas_codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f'[{tmnow}] #{idx + 1:04d} REPLACE INTO company_info ' \
                          f'VALUES ({code}, {company}, {today})')
                self.conn.commit()

        return

    def replace_nas_price_db(self, df, code):
        try:
            with self.conn.cursor() as curs:
                for data in df.itertuples():
                    date = datetime(data.Index.year, data.Index.month, data.Index.day).strftime('%Y-%m-%d')
                    sql = f"REPLACE INTO " + table_name_nas_daily_price + " VALUES ('{code}', " \
                          f"'{date}', {data.Open}, {data.High}, {data.Low}, {data.Close}, " \
                          f"{data.Diff}, {data.Volume})"
                    curs.execute(sql)
                self.conn.commit()
            print('[{}] {} : rows > REPLACE INTO ' + table_name_nas_daily_price + ' [OK]' \
                  .format(datetime.now().strftime('%Y-%m-%d %H:%M'),  code))
        except Exception as e:
            print(e)

    def read_nas_company_quotes(self, company, start, end):
        try:
            price = []
            price_final = pd.DataFrame()
            if start is not None and end is not None:
                price = yf.download(company, start=start, end=end, progress=False)
#                date = price_df.index
            elif start is not None and end is None:
                price = yf.download(company, start=start, progress=False)
            elif start is None and end is not None:
                price = yf.download(company, end=end, progress=False)
            else:
                price = yf.download(company, progress=False)

            if len(price) == 0:
                None
            else:
                price['Name'] = company
                price['Diff'] = price['Close'] - price['Open']

            price_final = price_final.append(price, sort=False)
            price_final.index.name = 'Date'
            print(price_final)
        except Exception as e:
            print(e)
        return price_final

    def update_nas_stock_price(self, start=None, end=None, is_all=False):
        sql = "SELECT * FROM " + table_name_nas_comp_info
        nas_comp_info = pd.read_sql(sql, self.conn)

        if is_all:
            start = None
            end = None

        for comp_code in nas_comp_info['code']:
            if comp_code == 'AFAM':
                continue
            df_quotes = self.read_nas_company_quotes(comp_code, start, end)
            self.replace_nas_price_db(df_quotes, comp_code)

        return

    def old_update_nas_stock_price(self):
        """ Read historical qutoes of NASDAQ by using yfinance and update it to DB """
        print("Get all-time stock price for all companies?(y/n)")
        ans = input()
        if ans == 'y':
            start = None
            end = None
        if ans == "n":
            print("Set start day(%YYYY-%mm-%dd)")
            ans = input()
            ymd = ans.split('-')
            start = datetime(int(ymd[0]), int(ymd[1]), int(ymd[2]))
            print("Set end day(%YYYY-%mm-%dd")
            ans = input()
            ymd = ans.split('-')
            end = datetime(int(ymd[0]), int(ymd[1]), int(ymd[2]))

        sql = 'SELECT * FROM ' + table_name_comp_info
        nas_comp_info = pd.read_sql(sql, self.conn)
        for company in nas_comp_info['code']:
            if company == 'AFAM': # pymysql.err.OperationalError: (1054, "Unknown column 'nan' in 'field list'")
                continue
            df = self.read_nas_company_quotes(company, start, end)
            self.replace_nas_price_db(df, company)

    def get_nas_daily_price(self, code, day):
        start_day = datetime.today() - timedelta(days=day)

        sql = "SELECT * FROM " + table_name_nas_daily_price + " WHERE code ='" + code + "' AND date > '" + \
              start_day.strftime("%Y-%m-%d") + "'"

        return pd.read_sql(sql, self.conn)