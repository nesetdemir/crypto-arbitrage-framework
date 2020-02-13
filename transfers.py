import ccxt
import json
import time
from baglanti import mysql_baglan
import datetime
import requests
from urllib.parse import urljoin
import sys

db = mysql_baglan("bingo")
cursor = db.cursor()
cursor.execute('SET NAMES utf8;')
cursor.execute('SET CHARACTER SET utf8;')
cursor.execute('SET character_set_connection=utf8;')

sql = "SELECT apikey,secret,id FROM `users` WHERE status = '1' order by id desc"
cursor.execute(sql)
results = cursor.fetchall()
column_names = ['apikey', 'secret', 'id']
for row in results:
    user = dict(zip(column_names, row))
    print(user['id'])
    exchange = ccxt.binance({
        'apiKey': user['apikey'],
        'secret': user['secret'],
        'enableRateLimit': True
    })

    #BTC
    if exchange.has['fetchDeposits']:
        withdrawals = exchange.fetch_withdrawals()
        set_data = []
        for withdraw in withdrawals:
            date_time = int(withdraw['timestamp'])/1000
            date_time = datetime.datetime.fromtimestamp(date_time)
            
            set_data.append([user['id'], withdraw['currency'], withdraw['txid'], withdraw['address'], withdraw['type'], withdraw['amount'], withdraw['status'], withdraw['fee']['cost'],date_time])
        sqlguncelleme = "INSERT INTO transfers (user_id, currency, txid, address, type, amount, status, fee, datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE user_id=(user_id)"
        cursor.executemany(sqlguncelleme, set_data,)
        db.commit()

        withdrawals = exchange.fetch_deposits()
        set_data = []
        for withdraw in withdrawals:
            date_time = int(withdraw['timestamp'])/1000
            date_time = datetime.datetime.fromtimestamp(date_time)
            
            set_data.append([user['id'], withdraw['currency'], withdraw['txid'], withdraw['address'], withdraw['type'], withdraw['amount'], withdraw['status'], '0',date_time])
        sqlguncelleme = "INSERT INTO transfers (user_id, currency, txid, address, type, amount, status, fee, datetime) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE user_id=(user_id)"
        cursor.executemany(sqlguncelleme, set_data,)
        db.commit()

