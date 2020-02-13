import json
import time
from baglanti import mysql_baglan
import datetime
import requests
from urllib.parse import urljoin
import sys
from talib.abstract import *
import talib
import numpy as np

db = mysql_baglan("bingo")
cursor = db.cursor()
cursor.execute('SET NAMES utf8;')
cursor.execute('SET CHARACTER SET utf8;')
cursor.execute('SET character_set_connection=utf8;')

#okex
#da0effcb-e0e0-414d-9922-036a2c1bd2b5
#0A39A37BF7DE4897F7D7523518EAC944

def clearData(mumlar):
  high = []
  open = []
  low = []
  close = []
  volume = []
  opentime = []
  for mum in mumlar:
      open.append(round(mum[1], 8))
      high.append(round(mum[2],8))
      low.append(round(mum[3],8))
      close.append(round(mum[4],8))
      volume.append(mum[5])
      opentime.append(mum[0])

  inputs = {
      'open': np.array(open),
      'high': np.array(high),
      'low': np.array(low),
      'close': np.array(close),
      'volume': np.array(volume),
      'opentime': np.array(opentime),
  }

  return inputs

def simulate_bot(count,pair):
    sql = "SELECT opentime,open,high,low,close,volume FROM `ohclv` WHERE symbol = 'XRPUSDT' AND mum = '30m' AND opentime > '2018-01-01' ORDER BY `ohclv`.`opentime` ASC LIMIT " + \
        str(count)+", 500"
    cursor.execute(sql)
    mumlar = cursor.fetchall()

    inputs = clearData(mumlar)
    kucuk_ortalama = MA(inputs, timeperiod=2, price='close')
    buyuk_ortalama = MA(inputs, timeperiod=25, price='close')
    #adx14 = talib.ADX(inputs['high'], inputs['low'], inputs['close'], timeperiod=14)
    zaman = inputs['opentime'][-1]
    #macd, macdsignal, macdhist = MACD(inputs, fastperiod=14, slowperiod=26, signalperiod=14, price='close')

    son_fiyat = float(inputs['open'][-1])

    if(kucuk_ortalama[-1] > buyuk_ortalama[-1]):
        anlik_islem = "buy"
        sql = "SELECT cost FROM `trades_test` WHERE botname = 'testhesabi' AND side = 'sell' ORDER BY id DESC LIMIT 1"
        cursor.execute(sql)
        son_miktar = cursor.fetchone()
        if(son_miktar != None):
            amount = round(float(son_miktar[0])/son_fiyat, 8)
            cost = round(float(son_miktar[0]), 8)
    else:
        anlik_islem = "sell"
        sql = "SELECT amount FROM `trades_test` WHERE botname = 'testhesabi' AND side = 'buy' ORDER BY id DESC LIMIT 1"
        cursor.execute(sql)
        son_miktar = cursor.fetchone()
        if(son_miktar != None):
            amount = round(float(son_miktar[0]), 8)
            cost = round(float(son_miktar[0])*son_fiyat, 8)

    sql = "SELECT side FROM `trades_test` WHERE botname = 'testhesabi' ORDER BY id DESC LIMIT 1"
    cursor.execute(sql)
    son_islem = cursor.fetchone()

    try:
        if(son_islem[0] != anlik_islem):
            print(" İşlem: " + str(anlik_islem))
            sql = "SELECT mum_time,price FROM `trades_test` WHERE botname = 'testhesabi' AND user_id = '4' ORDER BY id DESC LIMIT 1"
            cursor.execute(sql)
            sonalim = cursor.fetchone()
            
            if(anlik_islem=="sell"):
                    fark = (son_fiyat/float(sonalim[1])-1)*100
                #if(((fark < -0.500)) or (fark > 0)):
                    set_order_data = [['4', 'testhesabi', '123', zaman, 'XRP/USDT',
                                       anlik_islem, amount, son_fiyat, 'closed', 'XRP', '0.01', cost, zaman, fark]]

                    sqlguncelleme = "INSERT INTO trades_test (user_id, botname, orderid, datetime, symbol, side, amount, price, status,fee_currency,fee_cost,cost,mum_time,difference) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.executemany(sqlguncelleme, set_order_data,)
                    db.commit()
            else:
                fark = 0
                set_order_data = [['4', 'testhesabi', '123', zaman, 'XRP/USDT',
                                    anlik_islem, amount, son_fiyat, 'closed', 'XRP', '0.01', cost, zaman, fark]]

                sqlguncelleme = "INSERT INTO trades_test (user_id, botname, orderid, datetime, symbol, side, amount, price, status,fee_currency,fee_cost,cost,mum_time,difference) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.executemany(sqlguncelleme, set_order_data,)
                db.commit()

    except Exception as e:
        print(e)

start = 0
stop = 35000
pair = 'XRPUSDT'
while start <= stop:
    simulate_bot(start, pair)
    start = start+1
