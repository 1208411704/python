from  pymysql import *

conn =connect(host='localhost',user='root',password='zhang030806',database='dbm',port=3306)
cursor =conn.cursor()

def query(sql,params,type='no_select'):
    params =tuple(params)
    cursor.execute((sql,params))
    if type != 'no_select':
        data_list =cursor.fetchall()
        conn.commit()
        return data_list
    else:
        conn.commit()
        return '数据库语句执行成功'

