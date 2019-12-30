# coding=utf-8
# email:  linsijian@datagrand.com
# create: 2018年8月18日14:34:42
# coding=utf-8
import pymysql
import traceback
import datetime
import sys
import os
import time

reload(sys)
sys.setdefaultencoding('utf8')

get_environ_value = os.environ.get

MYSQL_HOST = get_environ_value('MYSQL_HOST')
MYSQL_PORT = get_environ_value('MYSQL_PORT')
MYSQL_PW = get_environ_value('MYSQL_PASSWORD')
MYSQL_DB = get_environ_value('MYSQL_DB_NAME')
MYSQL_USER = get_environ_value('MYSQL_USER')

"""
这是一个pymysql模块操作数据库的通用类，可以捕获异常执行和数据库连接信息，进行
数据库重连和执行操作，建议数据库相关处理都使用此脚本，防止数据库连接出现问题。
"""

class MySQLOperator(object):
    def __init__(self, host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PW, database=MYSQL_DB, port=MYSQL_PORT,
                 charset='utf8'):
        self.conn = None
        self.cursor = None
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self.connect_timeout = 5
        i = 0
        for i in xrange(5):
            if self.cursor is None:
                self.reconnect()
            else:
                break

        if self.cursor is None:
            print u"connect to mysql failed"

    def do_commit(self):
        try:
            self.conn.commit()
            boole = 'True'
        except Exception as e:
            time.sleep(3)
            try:
                self.reconnect()
                self.conn.commit()
                boole = 'True'
            except Exception, e:
                self.reconnect()
                boole = 'False'
        return boole


    def reconnect(self):
        self.disconnect()
        try:
            self.conn = pymysql.connect(host=self.host,
                                        user=self.user,
                                        password=self.password,
                                        database=self.database,
                                        port=self.port,
                                        charset=self.charset,
                                        connect_timeout=self.connect_timeout)
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        except Exception as e:
            print u"close connect failed: %s" % str(e) + "\t" + traceback.format_exc()

    def disconnect(self):
        try:
            if self.conn and self.conn.open:
                self.conn.commit()
        except Exception as e:
            print u"close connect failed: %s" % str(e) + "\t" + traceback.format_exc()
        try:
            if self.cursor:
                self.cursor.close()
        except Exception as e:
            print u"close connect failed: %s" % str(e) + "\t" + traceback.format_exc()
        try:
            if self.conn and self.conn.open:
                self.conn.close()
        except Exception as e:
            print u"close connect failed: %s" % str(e) + "\t" + traceback.format_exc()

    def execute(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            boole = 'True'
            print u'mysql operator sql is %s' % sql
        except Exception as e:
            time.sleep(3)
            try:
                self.reconnect()
                self.cursor.execute(sql)
                self.conn.commit()
                boole = 'True'
            except Exception, e:
                self.reconnect()
                print u'mysql operator sql is %s' % sql
                print u"close connect failed: %s" % str(e) + "\t" + traceback.format_exc()
                boole = 'False'
        return boole

    def update(self, update_dict, where_in_list, table_name):
        update_list = []
        for key, value in update_dict.items():
            if type(value) == unicode:
                value = str(value)
            if type(value) == str or type(value) == datetime.datetime:
                value = '\'' + self.escape_string(str(value)) + '\''
            update_list.append(key + ' = ' + str(value))
        where_list = []
        for key, value in where_in_list:
            if type(value) == str or type(value) == datetime.datetime:
                value = '\'' + self.escape_string(str(value)) + '\''
            where_list.append(key + ' = ' + str(value))
        sql = 'update ' + table_name + ' SET ' + \
              ' , '.join(update_list) + ' where ' + ' and '.join(where_list)
        return self.execute(sql)

    def update_content(self, update_dict, where_in_list, table_name):
        where_list = []
        for key, value in where_in_list:
            if type(value) == str or type(value) == datetime.datetime:
                value = '\'' + self.escape_string(str(value)) + '\''
            where_list.append(key + ' = ' + str(value))
        sql = 'update ' + table_name + ' SET content = %s , status = %s where ' + ' and '.join(where_list)
        content = self.escape_string(str(update_dict.get('content', '')))
        status = self.escape_string(str(update_dict.get('status', '')))
        try:
            self.cursor.execute(sql, (content, status))
            self.conn.commit()
            boole = 'True'
            print u'update content sql is %s' % sql
        except Exception as e:
            time.sleep(3)
            try:
                self.reconnect()
                self.cursor.execute(sql, (content, status))
                self.conn.commit()
                boole = 'True'
            except Exception, e:
                self.reconnect()
                print u'mysql operator sql is %s' % sql
                print u"close connect failed: %s" % str(e) + "\t" + traceback.format_exc()
                boole = 'False'
        return boole

    def insert(self, insert_dict, table_name):
        keys = insert_dict.keys()
        values = []
        for key, value in insert_dict.items():
            if type(value) == unicode:
                value = str(value)
            if type(value) == str or type(value) == datetime.datetime:
                value = '\'' + self.escape_string(str(value)) + '\''
            values.append(str(value))
        sql = 'insert into ' + table_name + \
              ' (' + ','.join(keys) + ') values (' + ','.join(values) + ');'
        return self.execute(sql)

    def do_select(self, select_list, where_in_list, table_name):
        values = []
        for value in select_list:
            values.append(str(value))

        where_list = []
        for key, value in where_in_list:
            where_list.append(key + ' = ' + str(value))

        sql = 'select ' + ','.join(values) + ' from ' + table_name + ' where ' + ' and '.join(where_list)
        ret_list = []
        try:
            self.cursor.execute(sql)
            for r in self.cursor.fetchall():
                ret_list.append(r)
            return ret_list
        except Exception as e:
            try:
                self.reconnect()
                self.cursor.execute(sql)
                for r in self.cursor.fetchall():
                    ret_list.append(r)
                return ret_list
            except Exception, e:
                self.reconnect()
                print u"do select failed: %s" % str(e) + "\t" + traceback.format_exc()
                return []

    def do_select_return(self, sql):
        ret_list = []
        try:
            self.cursor.execute(sql)
            for r in self.cursor.fetchall():
                ret_list.append(r)
            return ret_list
        except Exception as e:
            try:
                self.reconnect()
                self.cursor.execute(sql)
                for r in self.cursor.fetchall():
                    ret_list.append(r)
                return ret_list
            except Exception, e:
                self.reconnect()
                print u"do select failed: %s" % str(e) + "\t" + traceback.format_exc()
                return []

    @staticmethod
    def escape_string(value):
        return value.decode('string-escape')


if __name__ == '__main__':
    '''
    this is test code
    '''
    MYSQL_HOST = '127.0.0.1'
    MYSQL_PORT = 9302
    MYSQL_PW = 'root'
    MYSQL_DB = 'idps'
    MYSQL_USER = 'root'
    mySQLOperator = MySQLOperator(MYSQL_HOST, MYSQL_USER, MYSQL_PW, MYSQL_DB, MYSQL_PORT)
    tag_id = '4'

    # 定位标题位置
    title_sql = '''select extended from tags where id = %s''' % tag_id
    title_field = mySQLOperator.do_select_return(title_sql)
    print title_field
    title_field = u'' if not title_field else title_field[0][u'extended']
    print title_field
