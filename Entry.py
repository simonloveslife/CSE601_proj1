import sqlite3
import glob
from itertools import islice

class Dbop(object):
    def __init__(self, db):
        self.cursor = db.cursor()

    def put(self, tableName, docLoc):
        self.cursor.execute('DROP TABLE IF EXISTS ' + tableName)
        attlist, i, SQL = [], 0, ''
        with open(docLoc) as f:
            for line in f.readlines():
                att = line
                attlist = att.split()
                break
            for i in range(len(attlist)):
                if i == 0:
                    SQL = 'CREATE TABLE %s(%s VARCHAR (30))' % (tableName, attlist[i])
                else:
                    SQL = 'ALTER TABLE %s ADD COLUMN %s VARCHAR (30)' % (tableName, attlist[i])
                self.cursor.execute(SQL)
        with open(docLoc) as f:
            for line in islice(f, 1, None):
                vals = line.rstrip('\n').split('\t')
                SQL = 'INSERT INTO %s VALUES %r' % (tableName, tuple(vals))
                self.cursor.execute(SQL)

    def query(self, sqlstr):
        return self.cursor.execute(sqlstr)




dw = Dbop(sqlite3.connect('datawarehouse.db'))
path = 'Data_For_Project1/*.txt'
files = glob.glob(path)
#下面的是导入数据库,运行一遍以后注释掉就好了
for file in files:
    tableName = file.title().lstrip('Data_For_Project1').lstrip('/').rstrip('Txt').rstrip('.')
    # print(tableName)
    dw.put(tableName, file.title())

# 下面的是两种query实例
print (dw.query('select * from Drug').fetchall())
for row in dw.query('select * from Drug'):
    print(row)




