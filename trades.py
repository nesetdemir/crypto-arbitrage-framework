import ccxt
import time
import datetime
from baglanti import mysql_baglan
import json
import requests
import sys

db = mysql_baglan("bingo")
cursor = db.cursor()
cursor.execute('SET NAMES utf8;')
cursor.execute('SET CHARACTER SET utf8;')
cursor.execute('SET character_set_connection=utf8;')


sql = "SELECT order_response FROM `botalsat` where botname = 'nesetdemir' ORDER BY id ASC"
cursor.execute(sql)
rows = cursor.fetchall()

for row in rows:
    orders = json.loads(row[0])
    zaman = int(orders['timestamp'])/1000
    zaman = datetime.datetime.fromtimestamp(zaman)
    set_data = [[1, 'nesetdemir', orders['id'], zaman, orders['symbol'],
                 orders['side'], orders['amount'], orders['average'], orders['status'], orders['fee']['currency'], orders['fee']['cost'], orders['cost']]]

    sqlguncelleme = "INSERT INTO trades (user_id, botname, orderid, datetime, symbol, side, amount, price, status,fee_currency,fee_cost,cost) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sqlguncelleme, set_data,)
    db.commit()
