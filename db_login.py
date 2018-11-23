import os
import sys
import psycopg2

def connectPostgreSQL(sql):
    conn = psycopg2.connect(database="", user="postgres", password="new.1234", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print 'id=', row[0], ',name=', row[1], ',pwd=', row[2], ',singal=', row[3], '\n'
    conn.close()