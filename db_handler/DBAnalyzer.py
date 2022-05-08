from datetime import datetime
import backtrader as bt
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from db_handler.DBConnector import DBConnector


class DBAnalyzer:

    def __init__(self):
        self.stock_list = []
        return

    def test_conn(self, cur):
        sql = 'SELECT VERSION();'
        cur.execute(sql)
        result = cur.fetchone()
        print("MariaDB's version : {}".format(result))

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

    def draw_chart(self, codes=[], start='', end=''):
        # 인자로 넘어온 code에 해당하는 기업의 주가 정보를 차트로 보여준다.
        dbc = DBConnector()
        conn = dbc.tmp_connect()
        cursor = conn.cursor()

        self.test_conn(cursor)
        sql = 'SELECT * FROM daily_price WHERE code='
        for i in range(len(codes)):
            com_sql = sql + '"' + codes[i] + '";'
            tmp_df = pd.read_sql(com_sql, conn)

            fig = plt.figure(figsize=(12,8))
            ax = fig.add_subplot(1, 1, 1)
            ax.plot(tmp_df['date'], tmp_df['close'])
            plt.show()
