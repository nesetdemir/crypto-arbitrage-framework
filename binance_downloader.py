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


def download_historical_data(symbol, date, interval):

    def download_12h(start_date, end_date, interval):
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)

        BINANCE_API_KEY = 'DF4J3Ebo2HOWMGfhxFpVXatzfjl3EVFB24qUSLWgiFt1Dst5ygnuQyGctGs9wX5C'
        BINANCE_SECRET_KEY = 'Ythnld9QpSAXqYvjOtOrwYs0Mq2FIgCF1em2nX4ly02c5lrnuk9CAdHWtMtEDGEs'
        BASE_URL = 'https://api.binance.com'

        PATH = '/api/v1/klines'

        params = {
            'symbol': symbol,
            'interval': interval,   # 1m, 3m, 5m, 15m, 30m
            'limit': 1000,       # 1000 is max, but we using 720 = 60 (minutes) * 12 (hours)
            'startTime': start_time,
            'endTime': end_time
        }

        headers = {
            'X-MBX-APIKEY': BINANCE_API_KEY
        }

        url = urljoin(BASE_URL, PATH)
        r = requests.get(url, headers=headers, params=params)

        if r.status_code == 200:
            return r.json()
        else:
            print(r.json())
            

    # start_date = datetime.datetime(2019, 7, 15)
    # 0-12h
    start_date = datetime.datetime.strptime(date, '%Y-%m-%d')
    end_date = start_date + datetime.timedelta(hours=12) - datetime.timedelta(seconds=1)
    data = download_12h(start_date, end_date, interval)

    # 12-24h
    start_date = start_date + datetime.timedelta(hours=12)
    end_date = start_date + datetime.timedelta(hours=12) - datetime.timedelta(seconds=1)
    data.extend(download_12h(start_date, end_date, interval))

    set_data = []
    for row in data:
        zaman = int(row[0])/1000
        zaman = datetime.datetime.fromtimestamp(zaman)
        set_data.append([symbol, zaman, row[1], row[2], row[3], row[4], row[5], interval, date])

    sqlguncelleme = "INSERT INTO ohclv (symbol, opentime, open, high, low, close, volume, mum, date) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(sqlguncelleme, set_data,)
    db.commit()


pair = sys.argv[1]
day = sys.argv[2]
end_day = sys.argv[3]
#day = '2019-01-01'
#end_day = '2019-12-25'
while day != end_day:
    day = str(day)
    if(day == end_day):
        break
    download_historical_data(pair, day, '30m')
    day = datetime.datetime.strptime(day, "%Y-%m-%d")
    day = day + datetime.timedelta(days=1)
    day = datetime.date.fromtimestamp(day.timestamp())
    print(day)
