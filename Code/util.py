import pymysql
import json
import time
import traceback
from string import Template

LIMIT = 10

def dbinfo():
    with open('dbinfo.json', 'r', encoding='utf8') as f:
        dic = json.loads(f.read())
    return dic


class dbconnection(object):
    __instance = None

    @staticmethod
    def instance():
        if dbconnection.__instance is None:
            dbconnection()
        return dbconnection.__instance

    @staticmethod
    def reset():
        if dbconnection.__instance:
            dbconnection.__instance.close()
        dbconnection.__instance = None

    def __init__(self): # private constructor
        if dbconnection.__instance:
            raise Exception("This class is a singleton!")
        try_count = 0
        info = dbinfo()
        while 1:
            try:
                conn = pymysql.connect(
                    host=info['host'],
                    port=info['port'],
                    user=info['user'],
                    password=info['password'],
                    database=info['database']
                )
                dbconnection.__instance = conn
                break
            except:
                time.sleep(.5)
                try_count += 1
                print("retry to connect DB... try(s) : %d" % try_count)


class dbhelper(object):

    def __init__(self):
        self.__conn = dbconnection.instance()

    @property
    def conn(self):
        return self.__conn

    def open(self):
        dbconnection.reset()
        self.__conn = dbconnection.instance()

    def close(self):
        if self.__conn:
            self.__conn.close()
            

    def select(self, q='''select 1;''', vars=None, isone=False):
        with self.__conn.cursor(pymysql.cursors.DictCursor) as curs:
            if vars:
                curs.execute(q, tuple(vars))
            else:
                curs.execute(q)
            ret = curs.fetchone() if isone else curs.fetchall()
            self.__conn.commit()
        return ret

    def rselect(self, q='''select 1;''', vars=None, isone=False):
        return self.repeat(self.select, q=q, vars=vars, isone=isone)
    
    def update(self, q='''select 1;''', vars=None, commit=True):
        with self.__conn.cursor(pymysql.cursors.DictCursor) as curs:
            if vars:
                curs.execute(q, tuple(vars))
            else:
                curs.execute(q)
            if commit:
                self.__conn.commit()
    
    def rupdate(self, q='''select 1;''', vars=None, commit=True):
        self.repeat(self.update, q=q, vars=vars, commit=commit)

    def insert(self, table, dic):
        keys = list(dic.keys())
        q = '''insert into {table}({cols}) values({fms});'''.format(
            table=table,
            cols=",".join(['{}'.format(key) for key in keys]),
            fms=",".join(["%s" for i in keys]))
        vars = [dic[key] for key in keys]

        self.rupdate(q, vars)

    def insert_bulk(self, table, dic_list):
        strs = ''

        for k in dic_list:
            strs += "({cols}),".format(cols=",".join(['{}'.format("\'" + str(key) + '\'') for key in k.values()]))

        strs = strs[0:-1]
        
        s = Template("insert into {table} ({cols}) values $var".format(table=table,cols=",".join(['{}'.format(key) for key in k])))
    
        s = s.substitute(var=strs)
        
        self.rupdate(s)

    def insert_ignore(self, table, dic):
        keys = list(dic.keys())
        q = '''insert IGNORE into {table}({cols}) values({fms});'''.format(
            table=table,
            cols=",".join(['{}'.format(key) for key in keys]),
            fms=",".join(["%s" for i in keys]))
        vars = [dic[key] for key in keys]

        self.rupdate(q, vars)

    def repeat(self, f, **kwargs):
        cnt = 1
        while True:
            try:
                ret = f(**kwargs)
                break
            except:
                traceback.print_exc()
                time.sleep(.5)
                self.open()
                cnt += 1
                if cnt > LIMIT:
                    raise Exception()
                print('re connect ..')
        return ret

"""
    def success(self, info):
        self.__logging(info, 'success')

    def error(self, info):
        self.__logging(info, 'error')

    def __logging(self, info, status):
        return 0
"""