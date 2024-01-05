import numpy as np
import pandas as pd
import pymysql
import matplotlib.pyplot as plt
import time

from datetime import timedelta, date
from yahoo_fin import stock_info as si


class MKAnalyzer:
    def __init__(self):
        """ 생성자: MariaDB 연결하는 conn 생성 및 (cods:company)관계의 dict 생성 """
        self.conn = pymysql.connect(host='localhost', user='root', password='tpghks981!',
                                    db='sehwan_inv', charset='utf8')
        self.cur = self.conn.cursor()
        self.dict_code_company = dict()
        return

    def get_sharp_index(self, ticker_list, start_date, end_date, period):
        start_time = time.time()

        ticker_list = ["AAPL", "NVDA", "MSFT", "KO", "TSLA", "TSM", "QCOM", "GOOGL"]
        ticker_list = ["AAPL", "MSFT", "KO", "TSM", "QCOM", "GOOGL"]
        ticker_list = ["AAPL", "MSFT", "TSM", "QCOM", "GOOGL"]
        #ticker_list = ["AAPL", "MSFT", "GOOGL"]

        str_where = "("
        for idx, ticker in enumerate(ticker_list):
            str_where += f"'{ticker}', "
        str_where = str_where.strip(', ')
        str_where += ")"

        if period:
            sql = f"select * From usa_daily_price where market_date>'{period}' AND ticker in {str_where}"
        elif not start_date is None and not end_date is None:
            sql = f"select * From usa_daily_price where market_date>='{start_date}' AND market_date<='{end_date}' " \
                  f"AND ticker in {str_where}"

        df_result = pd.read_sql(sql, self.conn)

        endtime = time.time()
        print(f"Elapsed time = {endtime - start_time}")

        df_nas = pd.DataFrame()

        for idx, ticker in enumerate(ticker_list):
            df_tmp = df_result[df_result['ticker'] == ticker]
            df_tmp.index = df_tmp['market_date']
            df_nas = pd.concat((df_nas, df_tmp['close'].rename(ticker)), axis=1)
        endtime2 = time.time()

        # 수정 필요하다. df_nas에 Nan 데이터가 많이 포함되어있어.
        df_nas = df_nas.dropna(axis=0)

        daily_ret = df_nas.pct_change()
        annual_ret = daily_ret.mean() * 252
        daily_cov = daily_ret.cov()
        annual_cov = daily_cov * 252

        port_ret = []
        port_risk = []
        port_weights = []
        sharp_ratio = []

        for _ in range(20000):
            weights = np.random.random(len(annual_ret.index.values))
            weights /= np.sum(weights)

            returns = np.dot(weights, annual_ret)
            risk = np.sqrt(np.dot(weights.T, np.dot(annual_cov, weights)))

            port_ret.append(returns)
            port_risk.append(risk)
            port_weights.append(weights)
            sharp_ratio.append(returns/risk)

        portfolio = {'Returns': port_ret, 'Risk': port_risk, 'Sharp': sharp_ratio}
        for i, s in enumerate(annual_ret.index.values):
            portfolio[s] = [weight[i] for weight in port_weights]

        df = pd.DataFrame(portfolio)
        df = df[['Returns', 'Risk', 'Sharp'] + [s for s in annual_ret.index.values]]
        max_sharp = df.loc[df['Sharp'] == df['Sharp'].max()]
        min_risk = df.loc[df['Risk'] == df['Risk'].min()]

        df.plot.scatter(x='Risk', y='Returns', c='Sharp', cmap='viridis', edgecolor='k', figsize=(11,7), grid=True)
        plt.scatter(x=max_sharp['Risk'], y=max_sharp['Returns'], c='r', marker='*', s=300)
        plt.scatter(x=min_risk['Risk'], y=min_risk['Returns'], c='r', marker='X', s=200)
        plt.title('Porfolio Optimization')
        plt.xlabel('Risk')
        plt.ylabel('Expected Returns')
        plt.show()
        end_time = time.time()

        print(f"Elapsed time = {end_time - start_time}")

    def get_sharp_index_sp500(self):
        sp500_tickers = si.tickers_sp500()

        today = date.today()
        one_year_ago = today - timedelta(365)
        two_year_ago = today - timedelta(365 * 2)
        three_year_ago = today - timedelta(365 * 3)
        four_year_ago = today - timedelta(365 * 4)
        five_year_ago = today - timedelta(365 * 5)

        self.get_sharp_index(ticker_list=sp500_tickers, start_date=three_year_ago, end_date=two_year_ago, period='')
