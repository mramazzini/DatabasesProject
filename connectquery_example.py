#!/usr/bin/python

import psycopg2

pg_password= input("password: ")
conn = psycopg2.connect(database = "COSC3380", user = "user_name", password = pg_password)
cursor = conn.cursor()

# example get column names & types
print("Table R")
cursor.execute("SELECT * FROM R LIMIT 1;")  # has 2 columns
onerow=cursor.fetchone()
colname= cursor.description 
print("names: ",colname[0].name," ",colname[1].name )
print("type code: ",colname[0].type_code," ",colname[1].type_code )
print("types: ",type(onerow[0])," ",type(onerow[1]) )


# example: count all rows from small table
cursor.execute("SELECT count(*) FROM t;")
cnt_table = int(cursor.fetchall()[0][0])
print("|t| =",cnt_table)
