#!/bin/bash -l /home/sitech/python3/bin/python3.8 "$@"
# -*-coding:utf-8 -*-
# ******************************************************************************
# **  文件名称:
# **   描   述:
# **   功   能:
# **  输入参数: 运行日期
# **  执行说明：python3 prc_idd_dm_looping_report_d 20200916
# **  输出参数:
# **  输入资源:
# **  输出资源: DM_LOOPING_REPORT
# **  创建人员: chengming
# **  创建日期: 2020/09/16
# **  修改日志:
# **  修改日期:
# **  修改人员:
# **  修改内容:
# **  执行说明:
# **  公司名称: si-tech
# **  编写规则说明
# **     说明1：程序名以小写命名，所有自定义变量均用小写，并以v_打头；所有字段名均用大写
# **     说明2: hive中用到的模式名、表名、函数名均用大写
# **     说明3: 用4个空格来缩进代码，不要用tab, 也不要tab和空格混用
# **     说明4: SQL语句目标和源要齐整
# **     说明5: 每行最大长度79，换行可以使用反斜杠，最好使用圆括号
# ******************************************************************************
# ===========================================================================================
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.hiveExe import HiveExe, Except
from utils.print_info import start, end
from utils import date
import random

if len(sys.argv) < 2:
    exit("程序需求需要一个日期参数，例如：python3 {} 20200911，请重试".format(sys.argv[0]))
name = sys.argv[0][sys.argv[0].rfind(os.sep) + 1:].rstrip('.py')
v_txdate = sys.argv[1]
v_serial_no = random.randint(1000000, 9999999)
v_step = 0
# **启动文件日志记录
hiveExcute = HiveExe()
hiveExcute.connect()

try:
    # ** 日期变量初始化
    v_tmpnum = date.CALDATE(v_txdate, 'D', 0)  # 20170705
    v_thisyyyymmdd = date.CALDATE(v_tmpnum, 'D', 0)  # 20170705
    v_thisyyyymm = v_thisyyyymmdd[0:6]  # 201707
    v_thisyymmdd = v_thisyyyymmdd[2:6]  # 170705
    v_thisyymm = v_thisyyyymmdd[2:6]  # 1707
    v_thismmdd = v_thisyyyymmdd[4:8]  # 0705
    v_thisyyyy = v_thisyyyymmdd[0:4]  # 2017
    v_thisdd = v_thisyyyymmdd[6:8]  # 05
    v_lastyyyymmdd = date.CALDATE(v_tmpnum, 'D', -1)  # 20170704
    v_lastyyyymm = v_lastyyyymmdd[0:6]  # 201707
    v_lastyymm = v_lastyyyymmdd[2:6]  # 1707
    v_lastdd = v_lastyyyymmdd[6:8]  # 04
    v_nextyyyymmdd = date.CALDATE(v_tmpnum, 'D', 1)  # 20170706
    v_nextdd = v_nextyyyymmdd[6:8]  # 06
    v_thismon = date.CALDATE(v_tmpnum, 'M', 0)  # 201707
    v_nextmon = date.CALDATE(v_tmpnum, 'M', 1)  # 201708
    v_lastmon = date.CALDATE(v_tmpnum, 'M', -1)  # 201706
    v_last2mon = date.CALDATE(v_tmpnum, 'M', -2)  # 201705
    v_last3mon = date.CALDATE(v_tmpnum, 'M', -3)  # 201704
    v_thisyear = date.CALDATE(v_tmpnum, 'Y', 0)  # 2017
    v_last2yyyymm = v_last2mon  # 201705
    v_last3yyyymm = v_last3mon  # 201704
    v_thismon4 = v_thismon[2:6]  # 1707
    v_lastmon4 = v_lastmon[2:6]  # 1706
    v_last2mon4 = v_last2mon[2:6]  # 1705
    v_firstdayofthismon = v_thismon[0:6] + '01'  # 20170701
    v_firstyyyymmdd = v_thismon[0:6] + '01'  # 20170701
    v_firstdayoflastmon = v_lastmon[0:6] + '01'  # 20170601
    v_lastdayoflastmon = date.CALDATE(v_firstdayofthismon, 'D', -1)  # 20170630
    v_lastythism = date.CALDATE(v_tmpnum, 'Y', -1) + v_thismon[4:6]  # 201607
    v_ltmnyyyymmdd = v_lastmon + v_thisdd  # 20170604
    v_dlldyyyymmdd = date.CALDATE(v_nextmon + '01', 'D', -1)  # 20170731
    v_dealyyyymm = date.CALDATE(v_nextyyyymmdd, 'M', -1)  # 201706
    v_dealyymm = v_dealyyyymm[2:6]  # 1706
    v_dealyyyy = v_dealyyyymm[0:4]  # 2017
    v_step = 0
    # ** 固定变量
    v_database = 'sitech_database1'.lower()  # hive pprt库
    v_table_name = ''.upper()  # 目标表名

    ###########################################业务代码区域########################################################
    '''
    1. SQL中的动态变量用{}包起来，例如传入日期的前一天{v_lastyyyymmdd}
    2. 动态SQL前面的F是使用Python3的 f-Strings来实现str.formate()的功能。
    '''


    v_step = 1
    hivesql = [F'''
    CREATE TABLE if not  exists  tmpOutTraffic 
    (
    Call_date string
    ,ProductID int
    ,PRODUCT  string 
    ,ITECHNOLOGY_TRUNK_ID int
    ,TECHNOLOGY_TRUNK_NAME string 
    ,AccountID int 
    ,Account string 
    ,Country string 
    ,Answered int
    ,Seized int)
    ''']
    v_deal_type = 'I'  # 创建表
    v_deal_tab = ''
    start(v_step, hivesql[0])
    hiveExcute.execute(hivesql, v_database, v_thisyyyymmdd, v_serial_no, v_step, v_deal_type, v_deal_tab, name)
    end(v_step)

    v_step = 2
    hivesql = [F'''
    CREATE TABLE if not  exists  tmpInTraffic 
    (
    Call_date string
    ,ProductID int
    ,PRODUCT  string 
    ,ITECHNOLOGY_TRUNK_ID int
    ,TECHNOLOGY_TRUNK_NAME string 
    ,AccountID int 
    ,Account string 
    ,Country string 
    ,Answered int
    ,Seized int)
    ''']
    v_deal_type = 'I'  # 创建表
    v_deal_tab = ''
    start(v_step, hivesql[0])
    hiveExcute.execute(hivesql, v_database, v_thisyyyymmdd, v_serial_no, v_step, v_deal_type, v_deal_tab, name)
    end(v_step)
    

    
    v_step = 3
    hivesql = [F'''
    INSERT overwrite table  tmpInTraffic
        SELECT    
        i.Call_Date,
        i.ROUTING_PRODUCT_ID,
        i.ROUTING_PRODUCT_NAME,
            i.IN_TECHNOLOGY_TRUNK_ID,
            i.IN_TECHNOLOGY_TRUNK_NAME,
            i.IN_ACCOUNT_ID,
            i.IN_ACCOUNT_NAME,
            i.out_COUNTRY_ENG_NAME,
            SUM(i.ANSWERED_CNT),
            sum(i.SEIZED_CNT)
        From     dwb_r_rt_cdr_withrate_report_d_6h i
        where     i.stat_date >= {v_thisyear}0101 and i.stat_date <= {v_thisyyyymmdd}
            AND    COALESCE(i.ROUTING_PRODUCT_ID, 0) <> -1
            AND    COALESCE(i.ROUTE_CLASS_ID, 0) in (1,2,3,4,5,6,7,8,9,12)
        Group    by 
        i.Call_Date,
        i.ROUTING_PRODUCT_ID,
        i.ROUTING_PRODUCT_NAME,
            i.IN_TECHNOLOGY_TRUNK_ID,
            i.IN_TECHNOLOGY_TRUNK_NAME,
            i.IN_ACCOUNT_ID,
            i.IN_ACCOUNT_NAME,
            i.out_COUNTRY_ENG_NAME
    ''']
    v_deal_type = 'I'  # 创建表
    v_deal_tab = ''
    start(v_step, hivesql[0])
    hiveExcute.execute(hivesql, v_database, v_thisyyyymmdd, v_serial_no, v_step, v_deal_type, v_deal_tab, name)
    end(v_step)
    
    
    v_step = 4
    hivesql = [F'''
    INSERT overwrite table  tmpOutTraffic
        SELECT    
        i.Call_Date,
        i.ROUTING_PRODUCT_ID,
        i.ROUTING_PRODUCT_NAME,
            i.OUT_TECHNOLOGY_TRUNK_ID,
            i.OUT_TECHNOLOGY_TRUNK_NAME,
            i.OUT_ACCOUNT_ID,
            i.OUT_ACCOUNT_NAME,
            i.OUT_COUNTRY_ENG_NAME,
            SUM(i.ANSWERED_CNT),
            sum(i.SEIZED_CNT)
        From     dwb_r_rt_cdr_withrate_report_d_6h i
        where     i.stat_date >= {v_thisyear}0101 and i.stat_date <= {v_thisyyyymmdd}
            AND    COALESCE(i.ROUTE_CLASS_ID, 0) in (1,2,3,4,5,6,7,8,9,12)
        Group    by 
        i.Call_Date,
        i.ROUTING_PRODUCT_ID,
        i.ROUTING_PRODUCT_NAME,
            i.OUT_TECHNOLOGY_TRUNK_ID,
            i.OUT_TECHNOLOGY_TRUNK_NAME,
            i.OUT_ACCOUNT_ID,
            i.OUT_ACCOUNT_NAME,
            i.OUT_COUNTRY_ENG_NAME
    ''']
    v_deal_type = 'I'  # 创建表
    v_deal_tab = ''
    start(v_step, hivesql[0])
    hiveExcute.execute(hivesql, v_database, v_thisyyyymmdd, v_serial_no, v_step, v_deal_type, v_deal_tab, name)
    end(v_step)


    v_step = 5
    hivesql = [F'''
    INSERT overwrite table dm_Looping_Report PARTITION (stat_date = {v_thisyyyy})
    Select
            I.Call_Date,
            I.Product,
            I.Account,
            I.TECHNOLOGY_TRUNK_NAME,
            I.Country,
            Case
                When    abs(I.Seized) < abs(O.Seized) Then
                    abs(I.Seized)
                Else
                    abs(O.Seized)
                End    Looping,
            I.Answered,
            I.Seized,
            case when I.Seized = 0 
            then null 
             else I.Answered*100/I.Seized 
        end as InASR,
            O.Answered,    
            O.Seized,
            case when O.Seized = 0 
            then null 
             else O.Answered*100/O.Seized 
        end as OutASR
    FROM tmpInTraffic I 
    INNER JOIN tmpOutTraffic O ON I.call_date = o.call_date and  I.TECHNOLOGY_TRUNK_NAME=O.TECHNOLOGY_TRUNK_NAME
            and    I.AccountID = O.AccountID
            and    I.ProductID = O.ProductID
    ''']
    v_deal_type = 'I'  # 创建表
    v_deal_tab = ''
    start(v_step, hivesql[0])
    hiveExcute.execute(hivesql, v_database, v_thisyyyymmdd, v_serial_no, v_step, v_deal_type, v_deal_tab, name)
    end(v_step)

    # -------------------------------------------------------------------------------------------
    # * 程序结束 停止日志记录
    # -------------------------------------------------------------------------------------------
    # -------------------------------------------------------------------------------------------
    # * 设置脚步返回值.提供给数据集成平台组件捕捉
    # -------------------------------------------------------------------------------------------
    os.system(F'echo "{name} is  finished and succeed";')


    # -------------------------------------------------------------------------------------------
    # 异常处理
    # -------------------------------------------------------------------------------------------
except Exception as e:
    Except(hivesql[0], v_database, v_txdate, v_serial_no, v_step, v_deal_type, '', name, str(e)[0:4000], -1)
    print(F"第{v_step} 发生异常,错误信息日志{str(e)[0:4000]}，请到idd数据库查看etl_job_run_log中的运行日志")
    os.system(F'echo "{name} is failed";')
finally:
    hiveExcute.close()