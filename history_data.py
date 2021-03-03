import pandas as pd
import numpy as np
import pymysql
import time
import datetime
from dbutils.pooled_db import PooledDB

# mysql连接
mysqlInfo = {

    # "host": '172.22.85.142',
    # 'port': 3308,
    # 'user': 'root',
    # 'passwd': 'sitch#7890',
    # 'db': 'my_sitech',
    # 'charset': 'utf8mb4'

    "host": '172.22.144.168',
    'port': 3310,
    'user': 'sitech',
    'passwd': 'Huawei_123',
    'db': 'my_sitech',
    'charset': 'utf8mb4'
}

# 数据库连接池
__pool = PooledDB(
    creator=pymysql,
    mincached=1,
    maxcached=5,
    maxconnections=6,
    maxshared=3,
    blocking=True,
    maxusage=None,
    setsession=[],
    ping=2,
    host=mysqlInfo['host'],
    user=mysqlInfo['user'],
    passwd=mysqlInfo['passwd'],
    db=mysqlInfo['db'],
    port=mysqlInfo['port'],
    charset=mysqlInfo['charset'])

db = __pool.connection()
cursor = db.cursor(cursor=pymysql.cursors.DictCursor)

for i in range(50):
    ticks_begin = time.time()
    today = (datetime.date.today() + datetime.timedelta(days=-i)).strftime('%Y%m%d')
    today = '\''+today+'\''
    print(today+'存储过程开始运行')
    #
    sql = 'call Proc_dm_pm_index_support_m({0},@s)'.format(today)
    cursor.execute(sql)
    print(today+"存储过程运行成功")
    ticks_finish = time.time()
    print('耗费时间为：' + str(ticks_finish - ticks_begin) + '秒\n')

cursor.close()
db.commit()
db.close()



