import pandas as pd
import numpy as np
import pymysql
import time
import datetime
from dbutils.pooled_db import PooledDB

def mysql_leading(source_excel, target_table, sheet_name):
    # 读取文件
    file_name = source_excel
    table_name = target_table
    data = pd.read_excel(file_name, sheet_name=sheet_name)
    data = data.fillna('*')
    data = data.astype(object)

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

    # 创建数据库连接、游标
    db = __pool.connection()
    cursor = db.cursor(cursor=pymysql.cursors.DictCursor)

    # 判断表是否存在（存在则删除）
    sql = 'DROP TABLE IF EXISTS {};'.format(table_name)
    cursor.execute(sql)

    # 创建表格(读取表格的列名，然后以此创建表格--暂定所有字段都设定为varchar（200）的格式)
    sql = 'create table {} ('.format(table_name)
    for i, j in zip(data.columns, range(0, len(data.columns))):
        if j != len(data.columns) - 1:
            if i == 'order_id' or i == 'ID' or i == 'ORDER_ID':
                sql += i + ' ' + 'int(20)' + ','
            else:
                sql += i + ' ' + 'varchar(200)' + ','
        else:
            if i == 'order_id' or i == 'ID' or i == 'ORDER_ID':
                sql += i + ' ' + 'int(20)' + ')'
            else:
                sql += i + ' ' + 'varchar(200)' + ')'
    cursor.execute(sql)

    # 导入数据
    # 格式：insert into （列名）values （值）
    query = 'insert into {} ('.format(table_name)
    for i, j in zip(data.columns, range(0, len(data.columns))):
        if j != len(data.columns) - 1:
            query += i + ','
        else:
            query += i + ')'

    for i in range(0, len(data)):
        try:
            values = '('
            for j in range(0, len(data.columns)):
                if j != len(data.columns) - 1:
                    if data.iloc[i, j] == '*':
                        values += 'NULL' + ','
                    else:
                        if str(data.iloc[i, j]).count("'") > 0:
                            values += '"' + str(data.iloc[i, j]) + '"' + ','
                        elif str(data.iloc[i, j]).count('"') > 0:
                            values += "'" + str(data.iloc[i, j]) + "'" + ','
                        else:
                            values += '"' + str(data.iloc[i, j]) + '"' + ','
                else:
                    if data.iloc[i, j] == '*':
                        values += 'NULL' + ')'
                    else:
                        if str(data.iloc[i, j]).count("'") > 0:
                            values += '"' + str(data.iloc[i, j]) + '"' + ')'
                        elif str(data.iloc[i, j]).count('"') > 0:
                            values += "'" + str(data.iloc[i, j]) + "'" + ')'
                        else:
                            values += '"' + str(data.iloc[i, j]) + '"' + ')'
            sql = query + ' ' + 'values' + ' ' + values
            cursor.execute(sql)
        except Exception as ex:
            print(sql)
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print('\n' + message)
        continue

    cursor.close()
    db.commit()
    print(target_table+"导入成功")
    db.close()

mysql_leading('D:\\思特奇\\工作文件\\最小指标库\\指标模型\\关系表\\导入版\\编码规则对应表单 导入版.xlsx','dim_index_label_info','dim_index_label_info')