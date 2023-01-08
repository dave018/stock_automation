from datetime import datetime
from datetime import timedelta
import pandas as pd
import matplotlib.pyplot as plt
from db_handler.DBConnector import DBConnector
import re
import pymysql


class DBAnalyzer:

    def __init__(self):
        self.stock_list = []
        return

    def test_conn(self, cur):
        sql = 'SELECT VERSION();'
        cur.execute(sql)
        result = cur.fetchone()
        print("MariaDB's version : {}".format(result))

    def get_krx_comp_info(self, conn):
        """ 회사의 종목코드 및 종목명을 dictionary 형태로 리턴 """
        comps = dict()
        sql = 'SELECT * FROM company_info'

        comps = pd.read_sql(sql, conn)

        return comps

    def get_nas_comp_info(self, conn):
        """ 회사의 종목코드 및 종목명을 dictionary 형태로 리턴 """
        comps = dict()
        sql = 'SELECT * FROM nas_company_info'

        comps = pd.read_sql(sql, conn)

        return comps


    def get_stock_info(self, start='', end='', codes=[]):
        # Get all-time data of selected company
        dbc = DBConnector()
        conn = dbc.tmp_connect()
        cursor = conn.cursor()
        self.test_conn(cursor)
        sql = 'SELECT * FROM daily_price WHERE code='
        for i in range(len(codes)):
            com_sql = sql + '"' + codes[i] + '";'
            self.stock_list.append(pd.read_sql(com_sql, conn))
        dbc.tmp_discon()

    def set_date(self, date):
        if date is None:
            one_year_ago = datetime.today() - timedelta(day=365)
            date = one_year_ago.strftime('%Y-%m-%d')
            print("date is initialized to '{}'".format(date))
        else:
            # 정규표현식 사용. \D는 숫자가 온다는 것. +는 1개 이상이라는 것.
            start_lst = re.split('\D+', date)
            if date[0] == '':
                start_lst = start_lst[1:]
            start_year = int(start_lst[0])
            start_month = int(start_lst[1])
            start_day = int(start_lst[2])

            if start_year < 1900 or start_year > 2200:
                print(f"ValueError: start_year({start_year:d}) is wrong")
                return
            if start_month < 1 or start_month > 12:
                print(f"ValueError: start_month({start_month:d}) is wrong")
                return
            if start_day < 1 or start_day > 31:
                print(f"ValueError: start_day({start_day:d}) is wrong")
                return

            date = f"{start_year:04d}-{start_month:02d}-{start_day:02d}"

        return date

    def get_daily_price(self, code, start_date=None, end_date=None):
        """ daily_price, nas_daily_price 테이블에서 읽어와서 데이터프레임으로 변환
            1) KRX 종목의 일별 시세를 데이터프레임 형태로 반환
            2) NASDAQ 종목의 일별 시세를 데이터프레임 형태로 반환
        """
        start_date = self.set_date(start_date)
        end_date = self.set_date(end_date)

        conn = pymysql.connect(host='localhost', user='root', password='tpghks981!', db='sehwan_inv',
                               charset='utf8')

        if code.isdigit():
            db = 'daily_price'
        else:
            db = 'nas_daily_price'

        sql = f"SELECT * FROM {db} WHERE code = '{code}'" \
              f" and date >= '{start_date}' and date <= '{end_date}'"

        """ BETWEEN을 쓴 것 보다 <=, >= 사용한 쿼리의 처리 속도가 더 빠르다고 한다."""
#              f" and date between '{start_date}' and '{end_date}'"
        stock_df = pd.read_sql(sql, conn)
        conn.close()

        return stock_df

    def draw_chart(self, start_date, end_date, codes=[]):
        start = self.set_date(start_date)
        end = self.set_date(end_date)

        dbc = DBConnector()
        conn = dbc.tmp_connect()

        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(1, 1, 1)

        for i in range(len(codes)):
            sql = f"SELECT * FROM daily_price WHERE code='{codes[i]}' and date >= '{start}' " \
                  f"and date <= '{end}'"
            tmp_df = pd.read_sql(sql, conn)

            subset = tmp_df.loc[0:, ['date', 'close']]

            ax.plot(subset['date'], subset['close'])

        plt.show()

        return
