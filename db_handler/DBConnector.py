import pymysql


class DBStruct:
    def __init__(self):
        self.host = ''
        self.user = ''
        self.password = ''
        self.db = ''
        self.charset = ''


class DBConnector:
    def __init__(self):
        return

    def make_db(self):
        return

    def get_db(self, db_name):
        return

    def db_connect(self, host, user, password, db, charset):
        self.conn = pymysql.connect(host=host, user=user, password=password,
                                    db=db, charset=charset)
        return self.conn

    def db_connect_need_input(self):
        params = ['host', 'user', 'password', 'db', 'charset']
        args = []
        for i in range(len(params)):
            print("Enter {}".format(params[i]))
            args.append(input())

        self.conn = pymysql.connect(host=args[0], user=args[1], password=args[2],
                                    db=args[3], charset=args[4])

        return self.conn

    def tmp_connect(self):
        #self.conn = pymysql.connect(host='localhost', user='root', password='tpghks981!',
        #                            db='sehwan_inv', charset='utf8')
        self.conn = pymysql.connect(host='localhost', user='root', password='tpghks981!',
                                    charset='utf8')
        return self.conn

    def set_cmd_mode(self):
        conn = self.db_connect_need_input()
        cursor = conn.cursor()

        while True:
            print("Enter SQL cmd")
            print("(Enter 'exit' if want to exit)")
            sql = input()
            if sql == "exit":
                break
            try:
                cursor.execute(sql)
                result = cursor.fetchall()
                print(result)
            except Exception as e:
                print(e)
                print("Please check SQL")

        return

    def tmp_discon(self):
        self.conn.close()

