import json
import pymysql
import pandas as pd
from bs4 import BeautifulSoup
from datetime import timedelta
from urllib import request as req

from db_vars import *
from asap_logger import *

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
            sql += TABLE_NAME_DAILY_PRICE + """
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
            sql += TABLE_NAME_COMP_INFO + """
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
        sql = 'SELECT * FROM ' + TABLE_NAME_COMP_INFO
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]

        """ company_info 테이블의 'last_update' col 업데이트 """
        with self.conn.cursor() as curs: # 왜 with문을 쓰나?
            sql = 'SELECT max(last_update) FROM ' + TABLE_NAME_COMP_INFO
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
                    sql = f"REPLACE INTO " + TABLE_NAME_COMP_INFO + " (code, company, last_update) " \
                          f"VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f'[{tmnow}] #{idx+1:04d} REPLACE INTO ' + TABLE_NAME_COMP_INFO + \
                          f'VALUES ({code}, {company}, {today})')
                self.conn.commit()
                print('')

        return

    def get_comp_info(self):
        sql = "SELECT * FROM " + TABLE_NAME_COMP_INFO
        return pd.read_sql(sql, self.conn)

    def get_daily_price(self, code, day):
        start_day = datetime.today() - timedelta(days=day)

        sql = "SELECT * FROM " + TABLE_NAME_DAILY_PRICE + " WHERE code ='" + code + "' AND date > '" + \
              start_day.strftime("%Y-%m-%d") + "'"

        return pd.read_sql(sql, self.conn)

    def get_daily_price_all(self):
        sql = "SELECT * FROM " + TABLE_NAME_DAILY_PRICE
        return pd.read_sql(sql, self.conn)

    def calc_diff(self, df_price):
        l = len(df_price)
        for i in range(l - 1):
            df_price['diff'].iloc[i] = df_price['close'].iloc[i] - df_price['close'].iloc[i + 1]

    def read_naver_kr(self, code, company, pages_to_fetch, last_update=None):
        try:
            url = f'http://finance.naver.com/item/sise_day.naver?code={code}'
            opener = req.build_opener()
            opener.addheaders = [headers]
            response = opener.open(url)
            doc = BeautifulSoup(response, 'lxml')
            last_page = doc.find('td', class_='pgRR').a['href'].split('=')[-1]

            df = pd.DataFrame()
            pages = min(int(last_page), pages_to_fetch)

            is_last = False

            for page in range(1, pages + 1):
                pg_url = '{}&page={}'.format(url, page)
                response = opener.open(pg_url)
                df_tmp = pd.read_html(response, header=0)[0].dropna()

                for idx, row in df_tmp.iterrows():
                    date = datetime.strptime(row['날짜'], "%Y.%m.%d")
                    if last_update is not None and date > last_update:
                        print(f"{date} is new")
                        df = df.append(row)
                    else:
                        is_last = True
                        break

                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                print(f'[{tmnow}] {company} ({code}) : {page:04d}/{pages:04d} pages are downloading...', end="\r")

                if is_last:
                    break

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
                sql = f"REPLACE INTO {TABLE_NAME_DAILY_PRICE} VALUES ('{code}', " \
                      f"'{data.date}', {data.open}, {data.high}, {data.low}, {data.close}, " \
                      f"{data.diff}, {data.volume})"
                curs.execute(sql)
            self.conn.commit()
        print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO ' + TABLE_NAME_DAILY_PRICE + \
              '[OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), idx + 1, company, code, len(df)))

    def get_last_update_price(self, code):
        sql = f"SELECT date from {TABLE_NAME_DAILY_PRICE} WHERE " + \
              f"date=(SELECT MAX(date) FROM {TABLE_NAME_DAILY_PRICE} WHERE code='{code}') and code={code}"
        date = pd.read_sql(sql, self.conn)['date'][0]
        return datetime(date.year, date.month, date.day)

    def update_stock_price(self, is_all):
        """ KRX 상장 법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트 """
        print("Start function name: ", self.update_stock_price.__name__)
        pages_to_fetch = self.pages_to_fetch_all if is_all else self.pages_to_fetch_daily

        company_info = self.get_comp_info()

        for idx, row in company_info.iterrows():
            last_update_date = self.get_last_update_price(row['code'])
            df = self.read_naver_kr(row['code'], row['company'], pages_to_fetch, last_update_date)
            if df is None:
                continue
            self.replace_price_db(df, idx, row['code'], row['company'])
