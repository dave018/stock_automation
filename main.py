from src.db_updater import DBUpdater
from src.db_analyzer import DBAnalyzer
from src.usa_updater import USAUpdater

from asap_logger import *


def execute_test(dbu: DBUpdater, dba: DBAnalyzer):
    print("Test function starts")
#    dbu.check_connection()
#    dbu.make_table("daily_price")
#    dbu.read_krx_code()
#    dbu.update_comp_info()
    dbu.set_env()
    dbu.update_stock_price(False)
#    dbu.read_naver_kr('005930', '삼성전자', 1)
#    dba.get_stock_info(codes=['005930', '035420'])
#    dba.draw_chart(['005930', '035420'], '2018-01-02')
    print("Test function ends")


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        if sys.argv[1] == "daily_update":
            if sys.argv[2] == "KOSPI":
                dbu = DBUpdater()
                dbu.set_env()
                dbu.update_stock_price(is_all=True)
            elif sys.argv[2] == "USA":
                usau = USAUpdater()
                logger_init("update_nas_company_info")
                usau.update_usa_comp_info()
                logger_init("update_nas_stock_price")
                usau.update_usa_price()
    else:
        sys.exit()

    while True:
        print("Select job to do\n"
              "1: Update KRX stock item info\n"
              "2: Update NAS stock item info\n"
              "3: Get price of KRX stock item price\n"
              "4: Get price of NAS stock item price\n"
              "5: Draw stock price\n"
              "exit: Exit")

        job = input()

        if job == '1':
            dbu = DBUpdater()
            dba = DBAnalyzer()
            execute_test(dbu, dba)
            print("Update KRX stock item info is selected")
        elif job == '2':
            print("Update NAS stock item info is selected")
        elif job == '3':
            print("Get price of KRX stock item price is selected")
        elif job == '4':
            print("Get price of NAS stock item price is selected")
        elif job == '5':
            print("Draw stock price is selected")
            codes = []
            print("Enter code for items to draw chart\n"
                  "(Enter 'done' to exit")
            while True:
                code = input()
                if code == 'done':
                    break
                codes.append(code)

            print("Enter date for start\n"
                  "Format: YYYY-MM-DD")
            start_date = input()
            print("Enter date for end\n"
                  "Format: YYYY-MM-DD")
            end_date = input()

            dba = DBAnalyzer()
            dba.draw_chart(start_date=start_date, end_date=end_date, codes=codes)

        elif job == 'exit':
            break
        else:
            print("Wrong input")
