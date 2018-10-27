# newspaper_wordcoud
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 14:07:52 2018

@author: FHD
"""

import pymysql as mdb
import pandas as pd
import numpy as np
import time
time_start=time.time()

year_1 = ["ALL",2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018]

attitude = input("请输入要查询的情绪")

for year in year_1:
    db='newspaperdatabase2'
    city_list = []
    f = open('E:/city_connection_inndex/newspaper_query_system/城市1.txt',encoding="gbk")
    lines = f.readlines()
    
    for each in lines:
        each=each.strip('\n')
        city_list.append(each)
    
    with open('E:/city_connection_inndex/newspaper_query_system/new_name.txt',encoding="gbk") as f:
            # new_named的txt文件里面就是要查询的城市的名字
        lines = f.readlines()
            # readlines()读取整个文件所有行，保存在一个列表(list)变量中，每行作为一个元素
    lines=[x.strip() for x in lines if x!='\n']
        # \n换行
    keys=[x.split(',')[1] for x in lines]
    result_dict1 = keys
    
    
    def get_table_name(database_name):
        #定义get_table_name函数 括号内部为函数的参数
        conn = mdb.connect(
        # 创建连接
            host='localhost',
            port=3306,
            user='root',
            # user='root',
            passwd='password',
            # passwd='bull',
            db='newspaperdatabase2'
        )
        # INFORMATION_SCHEMA提供了访问数据库元数据的方式
        with conn:
            # # 创建游标
            cur = conn.cursor()
            # 使用execute方法执行SQL语句
            cur.execute('set names utf8')
            cur.execute('set character set utf8')
            cur.execute('set character_set_connection=utf8')
            cur.execute('use newspaperdatabase2')
            sql = """ select table_name from information_schema.tables where
                  TABLE_SCHEMA = '%s' """
                  # Python支持将值格式化为字符串。最基本的用法是将值插入到%s 占位符的字符串中
            cur.execute(sql % database_name)
            # 执行一个查询
            rows = cur.fetchall()
            #使用fetchall函数，将结果集（多维元组）存入rows里面
            #rows=list(list(zip(*rows))[0])
        return  rows
    
    def  query_ratio_report(table_name,city):
        # 定义query_ratio_report函数
        conn = mdb.connect(
        # 创建连接
        host='localhost',
        port=3306,
        user='root',
        # user='root',
        passwd='password',
        # passwd='bull',
        db='newspaperdatabase2'
    )
    # INFORMATION_SCHEMA提供了访问数据库元数据的方式
        with conn:
            cur=conn.cursor()
            # 创建游标
            cur.execute('set names utf8')
            cur.execute('set character set utf8')
            cur.execute('set character_set_connection=utf8')
            cur.execute('use newspaperdatabase2')
            # 使用execute方法值型sql语句
            sql_1="select count(*) from %s "
            sql_2="""select count(*) from %s
                     where WORD like "%%%s%%" AND year = %s """
            sql_3="""select count(*) from %s
                     where WORD like "%%%s%%" """
            sql_4="""select count(*) from %s
                     where WORD like "%%%s%%" AND year = %s AND attitude = 1 """
            sql_5="""select count(*) from %s
                     where WORD like "%%%s%%" AND attitude = 1 """                 
            sql_6="""select count(*) from %s
                     where WORD like "%%%s%%" AND year = %s AND attitude = 0 """
            sql_7="""select count(*) from %s
                     where WORD like "%%%s%%" AND attitude = 0 """  
            if year == "ALL":
                if attitude == "全部":
                    cur.execute(sql_3 % (table_name,city))
                elif attitude == "正面":
                    cur.execute(sql_5 % (table_name,city))
                elif attitude == "负面":
                    cur.execute(sql_7 % (table_name,city))
            else:
                if attitude == "全部":
                    cur.execute(sql_2 % (table_name,city,year))
                elif attitude == "正面":
                    cur.execute(sql_4 % (table_name,city,year))
                elif attitude == "负面":
                    cur.execute(sql_6 % (table_name,city,year))
             # 执行一个查询
            rows_2 = cur.fetchall()
            #使用fetchall函数，将结果集（多维元组）存入rows里面
            cur.execute(sql_1 % table_name)
            rows_1 = cur.fetchall()
            rows_1=list(list(zip(*rows_1))[0])[0]
            rows_2=list(list(zip(*rows_2))[0])[0]
            conn.commit()
            #提交数据
        return rows_1,rows_2
    
    def single_database_to_num(table_name,city):
        #定义single_database_to_num函数
        #table_names=get_table_name("newspaperdatabase")
#        temp_all=0.0
#        temp_a=0.0
        #for table_name in table_names:
        temp_1,temp_2=query_ratio_report(table_name,city)
#        temp_all+=temp_1
#        temp_a+=temp_2
        result=(temp_2/(temp_1))*100
        # return  temp_all,temp_a
        return result
    
    def  read_city_name():
        # 定义read_city_name函数
        with open('E:/city_connection_inndex/newspaper_query_system/new_name.txt',encoding="gbk") as f:
            # new_named的txt文件里面就是要查询的城市的名字
            lines = f.readlines()
            # readlines()读取整个文件所有行，保存在一个列表(list)变量中，每行作为一个元素
        lines=[x.strip() for x in lines if x!='\n']
        # \n换行
        keys=[x.split(',')[1] for x in lines]
        values=[x.split(',')[0] for x in lines]
        result_dict={}
        for i,x in enumerate(keys):
            # enumerate会将数组或列表组成一个索引序列
            result_dict[x]=values[i]
        return result_dict
    
    def main(result_dict):
        temp=np.empty((len(result_dict),len(city_list),))
        temp[:] = np.nan
        df=pd.DataFrame(temp,columns=city_list,index=result_dict.keys())
        
        for x in result_dict1:
            for y in city_list:
                df.loc[x,y]=single_database_to_num(x,y)
                #print(single_database_to_num(x,y))
            print(x,' 完成啦')
        
        #for i,x in enumerate(result_dict1):
            #i为编号位置，0，12345.。。x为具体的值，各报纸名称
        #    for j, y in enumerate(city_list):
                #j为城市编号，y为城市
        #        df.loc[x,y]=single_database_to_num(x,y)#### # 访问多行数据，索引参数为一个列表对象
                #single_database_to_num的第一个参数为哪个报纸，即数据库里面的table，第二个参数为城市
         #       print(j,x)
        #    print(i,x,'is ok')
        df.to_csv('E:/city_connection_inndex/result/result/'+str(year)+'.csv')
        # 写入  df.to_csv
        return df
    if __name__=='__main__':
        # data=single_database_to_num('ZZRB')
        # print data
        data=read_city_name()
        result=main(data)
        print(result)
    print(str(year)+'年结果已完成')
time_end2=time.time()
print('恭喜程序没出错，共耗时',int(time_end2-time_start),"秒")    
    
