import sqlite3
import glob
from itertools import islice
from scipy import stats

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

                line = line.rstrip('\n')
                if tableName == 'Test_Samples':
                    line = line.rstrip('\t')
                vals = line.split('\t')
                SQL = 'INSERT INTO %s VALUES %r' % (tableName, tuple(vals))
                self.cursor.execute(SQL)

    def query(self, sqlstr):
        return self.cursor.execute(sqlstr)


conn = sqlite3.connect('datawarehouse.db')
dw = Dbop(conn)
path = 'Data_For_Project1/*.txt'
files = glob.glob(path)
# 下面的是导入数据库,运行一遍以后注释掉就好了
# for file in files:
#     tableName = file.title().lstrip('Data_For_Project1').lstrip('/').rstrip('Txt').rstrip('.')
#     dw.put(tableName, file.title())
#     conn.commit()

# PART III.1
sql1 = 'select DISTINCT  Patient.p_id, Gene_Fact.UID, Microarray_Fact.exp from Patient JOIN Clinical_Fact JOIN Microarray_Fact JOIN Probe JOIN Gene_Fact on Patient.p_id = Clinical_Fact.p_id and Microarray_Fact.s_id = Clinical_Fact.s_id and Microarray_Fact.pb_id = Probe.pb_id and Probe.UID = Gene_Fact.UID Where Clinical_Fact.p_id in (SELECT Clinical_Fact.p_id FROM Patient JOIN Disease JOIN Clinical_fact ON Patient.p_id = Clinical_fact.p_id and Disease.ds_id = Clinical_fact.ds_id WHERE Disease.name = "ALL" ) order by Gene_Fact.UID'
sql2 = 'select DISTINCT  Patient.p_id, Gene_Fact.UID, Microarray_Fact.exp from Patient JOIN Clinical_Fact JOIN Microarray_Fact JOIN Probe JOIN Gene_Fact on Patient.p_id = Clinical_Fact.p_id and Microarray_Fact.s_id = Clinical_Fact.s_id and Microarray_Fact.pb_id = Probe.pb_id and Probe.UID = Gene_Fact.UID Where Clinical_Fact.p_id in (SELECT Clinical_Fact.p_id FROM Patient JOIN Disease JOIN Clinical_fact ON Patient.p_id = Clinical_fact.p_id and Disease.ds_id = Clinical_fact.ds_id WHERE Disease.name <> "ALL" ) order by Gene_Fact.UID'

alld, otherd = {}, {}
all = dw.query(sql1)
for record in all:
    if record[1] in alld:
        alld[record[1]].append(int(record[2]))
    else:
        alld[record[1]] = [int(record[2])]
conn.commit()
other = dw.query(sql2)
for record in other:
    if record[1] in otherd:
        otherd[record[1]].append(int(record[2]))
    else:
        otherd[record[1]] = [int(record[2])]
conn.commit()
inforgene = []
for uid in alld:
    # print(alld[uid])
    res = stats.ttest_ind(alld[uid], otherd[uid])
    if res[1] < 0.01:
        inforgene.append(uid)

inforgene.sort()

# 总共100个基因,38个相关基因 13个有all的病人,40个没有all的病人,
# PART III.2
allp, otherp = {}, {}
all = dw.query(sql1)  # already sorted by uid

for record in all:
    if record[1] in inforgene:

        if record[0] in allp:  # 13个病人
            allp[record[0]].append(int(record[2]))
        else:
            allp[record[0]] = [int(record[2])]
conn.commit()
other = dw.query(sql2)

for record in other:
    if record[1] in inforgene:

        if record[0] in otherp:
            otherp[record[0]].append(int(record[2]))
        else:
            otherp[record[0]] = [int(record[2])]

conn.commit()

newp = {}

# print(dw.query('select test1, test2, test3, test4, test5 from Test_Samples WHERE UID = "0000198293"').fetchall())

for uid in inforgene:
    sql = 'select test1, test2, test3, test4, test5 from Test_Samples where Test_Samples.UID = %r' % (uid,)
    res = dw.query(sql)

    for record in res:
        for num in range(1, 6):
            if 'test' + str(num) in newp:
                newp['test' + str(num)].append(int(record[num - 1]))
            else:
                newp['test' + str(num)] = [int(record[num - 1])]

    conn.commit()

# print(len(newp['test5']),newp)

rel1, rel2 = [], []
for i in range(1, 6):
    rel1, rel2 = [], []
    for p in allp:
        a = stats.stats.pearsonr(allp[p], newp['test' + str(i)])
        # print(a[0])
        rel1.append(a[0])

    for p in otherp:
        a = stats.stats.pearsonr(otherp[p], newp['test' + str(i)])
        # print('1',a[0])
        rel2.append(a[0])
    t = stats.ttest_ind(rel1, rel2)
    if t[1] < 0.01:
        print('test%s has ALL' % i)
    else:
        print('test%s does not have ALL' % i)
    print(stats.ttest_ind(rel1, rel2))
