from datetime import datetime
import backtrader as bt
import yfinance as yf
import pandas as pd

class DBAnalyzer:

    def __init__(self):
        return

    def test_conn(self, cur):
        sql = 'SELECT VERSION();'
        cur.execute(sql)
        result = cur.fetchone()
        print("MariaDB's version : {}".format(result))

    def get_stock_info(self, dbc, start='', end='', codes=[]):
        # Get all-time data of selected company
        conn = dbc.tmp_connect()
        cursor = conn.cursor()
        self.test_conn(cursor)

        sql = 'SELECT * FROM daily_price WHERE code='
        for i in range(len(codes)):
            com_sql = sql + '"' + codes[i] + '";'
            df = pd.read_sql(com_sql, conn)
            date = df['date']
            open = df['open']
            high = df['high']
            low = df['low']
        conn = dbc.tmp_discon()

        return
