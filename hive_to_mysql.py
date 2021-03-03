#! /home/sitech/python3/bin/python3.8
# -*-coding:utf-8 -*-
# --------------------------------
# Created by chenjunbin  on 2020/09/28
# ---------------------------------
from os.path import getsize
from utils.db_util import MySQLDb, OracleDb
from utils.prpcrypt import Pycryptor
from utils.date import CALDATE
from utils.myLogger import logger
from utils.file_fix import delete_file
from utils.readconfig import ReadConfig
import subprocess
import sys
import os
import re
import pandas as pd


class TableETL(object):
    def __init__(self, cf=ReadConfig('/'.join([os.path.dirname(os.path.abspath(__file__)), 'config.ini']))):
        """
          1.获取数据库连接
          2.初始化文件目录信息
        """

        # 密码解密
        self.pc = Pycryptor()
        # 文件位置
        self.file_dir = cf.get_value('dir_info', 'file_dir')
        # 日志位置
        self.log_dir = cf.get_value('dir_info', 'log_dir')
        # mysql连接
        self.mysqlClient = MySQLDb(host=cf.get_value('mysql_idd', 'host'),
                                   port=int(cf.get_value('mysql_idd', 'port')),
                                   user=cf.get_value('mysql_idd', 'user'),
                                   password=self.pc.decrypt(cf.get_value('mysql_idd', 'password')),
                                   dbname=cf.get_value('mysql_idd', 'dbname'),
                                   local_infile=1)
        # orcale连接
        self.oracleClient = OracleDb(host=cf.get_value('oracle_iboss', 'host'),
                                     port=cf.get_value('oracle_iboss', 'port'),
                                     user=cf.get_value('oracle_iboss', 'user'),
                                     password=Pycryptor().decrypt(cf.get_value('oracle_iboss', 'password')),
                                     dbname=cf.get_value('oracle_iboss', 'dbname'))
        # hive连接
        self.hdfs = cf.get_value('hive_info', 'hdfs')
        self.env = cf.get_value('hive_info', 'env')
        self.keytab = cf.get_value('hive_info', 'keytab')
        self.username = cf.get_value('hive_info', 'user')
        self.database = cf.get_value('hive_info', 'database')
        self.file_path = ''
        self.fresh_table_info = ''

    def init_db(self):
        self.mysqlClient.connect()
        self.oracleClient.connect()



    def hive_table_down(self, table_name,date, partiton_name=None, partition_value=None):

        down_sql = os.sep.join([self.file_dir, '.'.join(table_name, 'sql'])])
        filename = os.sep.join([self.file_dir, '.'.join([dict_paramter['SOURCE_TABLE'].split(' ')[0], 'csv'])])

        with open(down_sql, 'w') as sql:
            if partiton_name:
                down_script = '''use {db_name};\nselect {column_list} from {table_name} where {condition} partition({partiton_name}={partition_value});
                '''.format(db_name=idd_database,
                           column_list='*',
                           table_name=table_name,
                           condition='stat_date =' + date,
                           partiton_name=partiton_name,
                           partition_value=partition_value)
            else:
                down_script = '''use {db_name};\nselect {column_list} from {table_name} where {condition} ;
                '''.format(column_list='*',
                           table_name=table_name,
                           condition='stat_date =' + date,
                           db_name=idd_database)
            sql.write(down_script)

        down_script = '''set hive.resultset.use.unique.column.names = false;source {env};
                          kinit -kt {keytab} {username};
                          beeline beeline --outputformat=dsv --showHeader=true --delimiterForDSV=$'\x01' --hiveconf hive.server2.logging.operation.level=NONE --silent=true -f {down_sql} | sed -e '/^$/d' > {filename};'''.format(
            env=self.env,
            keytab=self.keytab,
            username=self.username,
            down_sql=down_sql,
            filename=filename)

        logger.info(down_script)
        extract_num = 0
        # if subprocess.call(down_script,shell=True):
        #   sys.exit("{}:执行异常，请重试".format(down_script))
        #  extract_num = int(subprocess.getstatusoutput("cat {}|wc -l".format(filename))[1])
        p = subprocess.Popen(down_script, shell=True)
        while True:
            stdout, stderr = p.communicate()  # 记录错误日志文件
            returncode = p.returncode
            #    print returncode
            if returncode == 0:
                print('执行结果：执行成功！\r\n')
                extract_num = int(subprocess.getstatusoutput("cat {}|wc -l".format(filename))[1])
                #   fh.write('执行结果：执行成功！\r\n')
                if extract_num > 0:
                    logger.debug("抽取{}的条数为:{}".format(table_name, extract_num))
                    return filename
                else:
                    sys.exit("抽取文件文件为空")
                    break
            if returncode != 0:
                sys.exit("{}:执行异常，请重试".format(down_script))
                break

    def file_column_del(self,file_dir,table_name):
        #1.读取文件
        data = pd.read_csv(file_dir,sep='\x01')
        #2.删除指定列
        column = table_name + '.' + 'stat_date'
        data = data.drop(['stat_date'],axis=1)
        #3.将文件保存回去
        data.to_csv(file_dir)

    def mysql_table_load(self, target_table, file_dir):
        """
        :param dict_paramter: 加载的表配置信息
        :param file_dir: 加载文件所在目录
        :return:
        """
        load_script = '''load data local infile "{file}" replace into table {table_name} fields terminated by '\x01' enclosed by '"' IGNORE 1 ROWS;
                '''.format(
            file=file_dir,
            table_name=target_table)
        logger.debug(load_script)
        self.mysqlClient.execute_sql('use idd;')
        self.mysqlClient.execute('truncate table {table_name};'.format(table_name= target_table))
        self.mysqlClient.execute(load_script)


def main():
    if len(sys.argv) < 4:
        print('''{script}需要输入表名,时间和mysql上传表名，
        eg: python3 {script} table_name 20200713/202007 target_table 
        '''.format(script=sys.argv[0]))
        exit('程序异常退出')
    table_name = sys.argv[1]
    date_in = sys.argv[2]
    target_table = sys.argv[3]
    if len(sys.argv[2]) == 6:
        date = date_in + '01'
    else:
        date = date_in
    try:
        # 第1步：抽hive的表形成文件
        table_elt = TableETL()
        table_elt.init_db()
        file_name = table_elt.hive_table_down(table_name,date)
        #第2步：处理stat_date字段
        table_elt.file_column_del(file_name,table_name)
        # 第2步：将文件加载到mysql中
        table_elt.mysql_table_load(target_table, file_name)
        # 第3步：删除抽取形成的文件

    except ValueError as error:
        sys.exit(error)


if __name__ == '__main__':
    YYYYMM = ''
    YYYYMMDD = ''
    main()
