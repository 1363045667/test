import pandas as pd
import numpy as np
import pymysql
import time
import datetime
from dbutils.pooled_db import PooledDB


# 封装数据导入mysql函数
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


# 封装指标详细信息整理功能
def index_info_process(source_excel, source_sheet, target_excel, target_excel_lead):
    data1 = pd.read_excel(source_excel, usecols=range(3, 53, 1), sheet_name=source_sheet)

    data = data1
    data = data.reset_index(drop=True)

    # 加入最小指标范围和数据序号
    data3 = pd.read_excel(source_excel, usecols=[0, 1, 4], sheet_name=source_sheet)
    data_2 = data3
    data_2 = data_2.reset_index(drop=True)
    data = data.merge(data_2, on='指标编码', how='left')

    # 加入需要呈现的数值和可用维度
    data5 = pd.read_excel(source_excel, usecols=[4, 63, 64], sheet_name=source_sheet)
    data_3 = data5
    data_3 = data_3.reset_index(drop=True)
    data = data.merge(data_3, on='指标编码', how='left')

    data.to_excel(target_excel, index=False)

    # 读取导入版的列名
    info_name = pd.read_excel('D:\\思特奇\\工作文件\\最小指标库\\指标模型\\指标\\指标模型第八版 导入版.xlsx').columns
    data.columns = info_name
    data.to_excel(target_excel_lead, index=False)

    # 将处理完的信息导入数据库
    mysql_leading(target_excel_lead, 'dim_index_base_info', 'Sheet1')


# 封装指标主题域梳理功能
def index_domain_process(source_excel, source_sheet, index_tree, taget_excel, target_excel_lead):
    domain1 = pd.read_excel(source_excel, sheet_name=source_sheet,
                            usecols=[4, 5, 60, 61, 62]).dropna()
    domain = domain1
    domain = domain.reset_index(drop=True)

    # 获取指标数信息
    domain_id = pd.read_excel(index_tree)

    # 获取指标主题域信息
    domain_id = domain_id[domain_id['当前节点层级'] == 1][['指标树编码', '指标树名称']]
    domain_id.columns = ['主题域编码', '主题域']

    # 处理异常值
    domain = domain.replace('企业运营', '企业运营管理').merge(domain_id, on='主题域', how='left')

    # 增加相关时间信息
    now = datetime.datetime.now()
    domain['操作人'] = 'admin'
    domain['操作时间'] = now.strftime('%Y/%m/%d')
    domain['创建人'] = 'admin'
    domain['创建时间'] = '2020/1/1'
    domain['版本'] = 1

    #
    columns = ['指标编码', '指标中文名称', '主题域', '主题域编码', '主题子域', '主题', '操作人', '操作时间', '创建人', '创建时间', '版本']
    domain = domain[columns]
    domain.to_excel(taget_excel, index=False)

    domain.columns = ['INDEX_ID', 'INDEX_CN_NAME', 'DOMAIN', 'DOMAIN_ID', 'SUB_DOMAIN', 'TOPIC', 'operate_id',
                      'operate_time', 'create_id', 'create_time', 'version']
    domain.to_excel(target_excel_lead, sheet_name='dim_index_domain_info',
                    index=False)

    # 将处理信息导入数据库
    mysql_leading(target_excel_lead, 'dim_index_domain_info', 'dim_index_domain_info')


# 封装指标使用部门汇总及分割功能
def index_dept_process(source_excel, source_sheet, target_excel, target_excel_lead):
    data1 = pd.read_excel(source_excel, usecols=[5, 27], sheet_name=source_sheet)

    data = data1
    data = data.reset_index(drop=True)

    index_id = pd.read_excel(source_excel, usecols=[4, 5], sheet_name=source_sheet)
    index_id.columns = ['指标id', '指标中文名称']

    tmp = pd.DataFrame(columns=['指标中文名称', '指标使用部门'])

    for i in range(len(data)):
        tmp2 = pd.DataFrame(columns=['指标中文名称', '指标使用部门'])
        if data['指标使用部门'].isnull()[i] == True:
            tmp = tmp.append(data.iloc[i, :])
        else:
            tmp2['指标使用部门'] = data.iloc[i, 1].split(',')
            tmp2['指标中文名称'] = data.iloc[i, 0]
            tmp = tmp.append(tmp2)

    final = tmp.reset_index(drop=True).merge(index_id, on=['指标中文名称'], how='left')
    final = final[final['指标使用部门'].notnull()].reset_index(drop=True)

    now_time = datetime.datetime.now()
    final['创建时间'] = '2020/1/1'
    final['创建人'] = 'admin'
    final['更新时间'] = now_time.strftime('%Y/%m/%d')
    final['更新人'] = 'admin'

    final['显示顺序'] = 1
    for i in range(len(final)):
        if final.loc[i, '指标使用部门'] == '管理层':
            final.loc[i, '显示顺序'] = 1
        elif final.loc[i, '指标使用部门'] == '市场部':
            final.loc[i, '显示顺序'] = 2
        else:
            final.loc[i, '显示顺序'] = 3

    final.to_excel(target_excel, index=False)

    final.columns = ['index_name', 'index_use_dept', 'index_id', 'create_time', 'creator', 'update_time', 'updater',
                     'order_id']
    final.to_excel(target_excel_lead, sheet_name='dim_index_use_dept_info',
                   index=False)

    mysql_leading(target_excel_lead, 'dim_index_use_dept_info', 'dim_index_use_dept_info')


# 封装指标标签汇总功能
def index_label_process(source_excel, source_sheet, target_excel, target_excel_lead):
    label1 = pd.read_excel(source_excel, usecols=[5, 53, 54, 55, 56, 57, 58, 59],
                           sheet_name=source_sheet)
    label = label1
    label = label.reset_index(drop=True)

    index = pd.read_excel(source_excel, usecols=[4, 5], sheet_name=source_sheet)

    label = label.replace('-', np.nan).replace('低重要', '不重要').replace('中重要', '一般重要').replace('高重要', '非常重要')

    index.columns = ['指标id', '指标中文名称']

    label_id = pd.read_excel('D:\\思特奇\\工作文件\\最小指标库\\指标模型\\关系表\\编码规则对应表单.xlsx', sheet_name='标签', usecols=[0, 5])
    label_id.columns = ['标签id', '标签名']
    tmp = pd.DataFrame(columns=['指标中文名称', '指标id', '标签名'])

    for i in label.columns[1:]:
        if i == '标签重要性':
            label[i] = label[i].replace('不重要', '数据重要性低').replace('一般重要', '数据重要性中').replace('非常重要', '数据重要性高')
        if i == '标签敏感性':
            label[i] = label[i].replace('低敏感', '数据敏感度低').replace('中敏感', '数据敏感度中').replace('高敏感', '数据敏感度高')
        if i == '标签KPI级别':
            label[i] = label[i].replace('公司级', '集团考核')
        label_data = index.merge(label[['指标中文名称', i]], on='指标中文名称', how='left')
        label_data.columns = ['指标id', '指标中文名称', '标签名']
        tmp = tmp.append(label_data)

    tmp = tmp.dropna().reset_index(drop=True).merge(label_id, on='标签名', how='left')

    tmp['创建时间'] = '2020/1/1'
    tmp['创建人'] = 'admin'
    tmp['更新时间'] = '2020/12/25'
    tmp['更新人'] = 'admin'

    tmp2 = pd.DataFrame(columns=['指标中文名称', '指标id', '标签名'])
    for i in range(len(tmp)):
        if len(tmp.iloc[i, 2].split(',')) > 1:
            for j in tmp.iloc[i, 2].split(','):
                tmp3 = pd.DataFrame(columns=['指标中文名称', '指标id', '标签名'])
                tmp3['指标中文名称'] = [tmp.iloc[i, 1]]
                tmp3['指标id'] = [tmp.iloc[i, 0]]
                tmp3['标签名'] = [j]
                if j == '运营商市场':
                    tmp3['标签id'] = 100203
                elif j == '企业市场':
                    tmp3['标签id'] = 100202
                elif j == '个人市场':
                    tmp3['标签id'] = 100201
                tmp3['创建时间'] = [tmp.iloc[i, 4]]
                tmp3['创建人'] = [tmp.iloc[i, 5]]
                tmp3['更新时间'] = [tmp.iloc[i, 6]]
                tmp3['更新人'] = [tmp.iloc[i, 7]]
                tmp2 = tmp2.append(tmp3)
        else:
            tmp2 = tmp2.append(tmp.iloc[i, :])

    columns = ['指标id', '指标中文名称', '标签名', '标签id', '创建时间', '创建人', '更新时间', '更新人']
    tmp2 = tmp2[columns]

    tmp2.to_excel(target_excel, index=False)

    tmp2.columns = ['index_id', 'index_name', 'label_name', 'label_id', 'create_time', 'create_id', 'operate_time',
                    'operate_id']
    tmp2.to_excel(target_excel_lead,
                  sheet_name='dim_index_label_relation_info', index=False)

    mysql_leading(target_excel_lead, 'dim_index_label_relation_info', 'dim_index_label_relation_info')


# 封装指标维度汇总功能
def index_dimension_process(source_excel, source_sheet, target_excel, target_excel_lead):
    dimension1 = pd.read_excel(source_excel, usecols=[4, 5, 64],
                               sheet_name=source_sheet).dropna()

    dimension = dimension1
    dimension = dimension.reset_index(drop=True)

    dimension_id = pd.read_excel('D:\\思特奇\\工作文件\\最小指标库\\指标模型\\关系表\\编码规则对应表单.xlsx', sheet_name='维度')

    dimension_id = dimension_id[['dimension_id', 'dimension_cn_name']]
    dimension_id.columns = ['维度id', '维度']

    tmp = pd.DataFrame(columns=['指标编码', '指标中文名称', '维度'])
    tmp2 = pd.DataFrame(columns=['指标编码', '指标中文名称', '维度'])

    for i in range(len(dimension)):
        for j in dimension.iloc[i, 2].split(','):
            tmp['维度'] = [j]
            tmp['指标编码'] = [dimension.iloc[i, 0]]
            tmp['指标中文名称'] = [dimension.iloc[i, 1]]

            tmp2 = tmp2.append(tmp)

    tmp2 = tmp2.merge(dimension_id, on='维度', how='left').dropna()

    now_time = datetime.datetime.now()

    tmp2['操作时间'] = now_time.strftime('%Y/%m/%d')
    tmp2['操作人'] = 'admin'
    tmp2['创建时间'] = '2020/1/1'
    tmp2['创建人'] = 'admin'
    tmp2['版本号'] = 1

    tmp2.to_excel(target_excel, index=False)

    del tmp2['维度']
    tmp2.columns = ['index_id', 'index_name', 'dimension_id', 'operate_id', 'operate_time', 'create_id', 'create_time',
                    'version']

    tmp2.to_excel(target_excel_lead, sheet_name='dim_index_dimension_info',
                  index=False)

    mysql_leading(target_excel_lead, 'dim_index_dimension_info', 'dim_index_dimension_info')


# 开始进行处理
ticks_begin = time.time()
print('开始导入数据')

source_excel = 'D:\\思特奇\\工作文件\\最小指标库\\指标模型\\指标信息完整版18.0.xlsx'
target_excel = 'D:\\思特奇\\工作文件\\最小指标库\\指标模型\\关系表\\'
target_excel_lead = 'D:\\思特奇\\工作文件\\最小指标库\\指标模型\\关系表\\导入版\\'
index_tree = 'D:\\思特奇\\工作文件\\最小指标库\\指标模型\\关系表\\指标树模型2.xlsx'
source_sheet = 'Data指标'

index_info_process(source_excel, source_sheet, 'D:\\思特奇\\工作文件\\最小指标库\\指标模型\\指标\\指标模型第十版.xlsx'
                   , 'D:\\思特奇\\工作文件\\最小指标库\\指标模型\\指标\\指标模型第十版 导入版.xlsx')
index_domain_process(source_excel, source_sheet, index_tree,target_excel + '主题域指标关系表.xlsx',target_excel_lead + '主题域指标关系表 导入版.xlsx')
index_dept_process(source_excel, source_sheet, target_excel + '使用部门汇总.xlsx' , target_excel_lead + '使用部门汇总 导入版.xlsx')
index_label_process(source_excel, source_sheet, target_excel + '指标标签关系表.xlsx' , target_excel_lead + '指标标签关系表 导入版.xlsx' )
index_dimension_process(source_excel, source_sheet, target_excel + '维度指标关系表.xlsx' , target_excel_lead + '维度指标关系表 导入版.xlsx' )

ticks_finish = time.time()
print('耗费时间为：' + str(ticks_finish - ticks_begin) + '秒')
