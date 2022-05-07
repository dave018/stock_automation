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

    def make_db(self, ):
        return

    def get_db(self, db_name):
        return

    def tmp_connect(self):
        self.conn = pymysql.connect(host='localhost', user='root', password='tpghks981!',
                                    db='sehwan_inv', charset='utf8')
        return self.conn

    def tmp_discon(self):
        self.conn.close()


