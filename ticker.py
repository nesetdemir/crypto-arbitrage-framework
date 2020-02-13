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

exchange = ccxt.binance({
    'apiKey': 'DF4J3Ebo2HOWMGfhxFpVXatzfjl3EVFB24qUSLWgiFt1Dst5ygnuQyGctGs9wX5C',
    'secret': 'Ythnld9QpSAXqYvjOtOrwYs0Mq2FIgCF1em2nX4ly02c5lrnuk9CAdHWtMtEDGEs',
    'enableRateLimit': True
})

#BTC
ticker = exchange.fetch_ticker('BTC/USDT')
date_time = int(ticker['timestamp'])/1000
date_time = datetime.datetime.fromtimestamp(date_time)
set_order_data = [[ticker['symbol'], ticker['percentage'], ticker['quoteVolume'], ticker['last'], date_time]]
sqlguncelleme = "INSERT INTO ticker (symbol, priceChange, volume, price, datetime) VALUES(%s, %s, %s, %s, %s)"
cursor.executemany(sqlguncelleme, set_order_data,)
db.commit()

#ETH
ticker = exchange.fetch_ticker('ETH/USDT')
date_time = int(ticker['timestamp'])/1000
date_time = datetime.datetime.fromtimestamp(date_time)
set_order_data = [[ticker['symbol'], ticker['percentage'], ticker['quoteVolume'], ticker['last'], date_time]]
sqlguncelleme = "INSERT INTO ticker (symbol, priceChange, volume, price, datetime) VALUES(%s, %s, %s, %s, %s)"
cursor.executemany(sqlguncelleme, set_order_data,)
db.commit()

#XRP
ticker = exchange.fetch_ticker('XRP/USDT')
date_time = int(ticker['timestamp'])/1000
date_time = datetime.datetime.fromtimestamp(date_time)
set_order_data = [[ticker['symbol'], ticker['percentage'], ticker['quoteVolume'], ticker['last'], date_time]]
sqlguncelleme = "INSERT INTO ticker (symbol, priceChange, volume, price, datetime) VALUES(%s, %s, %s, %s, %s)"
cursor.executemany(sqlguncelleme, set_order_data,)
db.commit()

#BNB
ticker = exchange.fetch_ticker('BNB/USDT')
date_time = int(ticker['timestamp'])/1000
date_time = datetime.datetime.fromtimestamp(date_time)
set_order_data = [[ticker['symbol'], ticker['percentage'], ticker['quoteVolume'], ticker['last'], date_time]]
sqlguncelleme = "INSERT INTO ticker (symbol, priceChange, volume, price, datetime) VALUES(%s, %s, %s, %s, %s)"
cursor.executemany(sqlguncelleme, set_order_data,)
db.commit()
