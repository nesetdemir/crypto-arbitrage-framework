import ccxt
import time
import datetime
from baglanti import mysql_baglan
import json
import requests
import sys
import telegram
from talib.abstract import *
import talib
import numpy as np



db = mysql_baglan("bingo")
cursor = db.cursor()
cursor.execute('SET NAMES utf8;')
cursor.execute('SET CHARACTER SET utf8;')
cursor.execute('SET character_set_connection=utf8;')

sql = "SELECT apikey,secret,id,sellcoin,buycoin,botname FROM `users` WHERE status = '1'"
cursor.execute(sql)
column_names = ['apikey', 'secret', 'id', 'sellcoin', 'buycoin', 'botname']
results = cursor.fetchall()


def clearData(mumlar):
  high = []
  open = []
  low = []
  close = []
  volume = []
  opentime = []
  for mum in mumlar:
      open.append(round(mum[1], 2))
      high.append(round(mum[2], 2))
      low.append(round(mum[3], 2))
      close.append(round(mum[4], 2))
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
telegram_bot_sinyal = False
for row in results:
    user = dict(zip(column_names, row))
    if(user['apikey'] != '0'):
        exchange = ccxt.binance({
            'apiKey': user['apikey'],
            'secret': user['secret'],
            'enableRateLimit': True
        })

        botname = user['botname']
        sellcoin = user['sellcoin']
        buycoin = user['buycoin']
        symbol = buycoin+'/'+sellcoin

        sell_balance = exchange.fetch_balance()[sellcoin]['free']
        buy_balance = exchange.fetch_balance()[buycoin]['free']

        son_fiyat = float(exchange.fetch_ticker(symbol)['info']['bidPrice'])
        alis_fiyati = son_fiyat

        orderbook = exchange.fetch_order_book(symbol)["bids"]
        sum_price = 0
        a = 0
        x = 0
        y = 0
        for val_data in orderbook:
            if(sell_balance >= sum_price):
                price = val_data[0]
                amount = val_data[1]
                sum_price = price * amount
                x = x + price
                y = y + amount
                a = a + sum_price

        best_buy = a/y
        total_order_book = y * best_buy

        # Hesabımızdaki miktarın %20'ı ile alış emri verelim
        miktar = (sell_balance/alis_fiyati)

        print('botname: ' + str(botname))
        print('Son Fiyat: ' + str(son_fiyat))
        print('Ortalama Fiyat: ' + str(best_buy))
        print('Alinacak miktar: ' + str(miktar))
        print('Sat Balance: ' + str(sell_balance))
        print('Al Balance: ' + str(buy_balance))
        if exchange.has['fetchOHLCV']:
            mumlar = exchange.fetch_ohlcv(symbol, '30m')

            inputs = clearData(mumlar)
            zaman = inputs['opentime'][-1]
            zaman = int(zaman)/1000
            zaman = datetime.datetime.fromtimestamp(zaman)
            macd, macdsignal, macdhist = MACD(inputs, fastperiod=12, slowperiod=26, signalperiod=13, price='close')
            print("Zaman:", str(zaman), "macd:", str(macd[-1]), "macdsignal:", str(macdsignal[-1]), "macdhist:", str(macdhist[-1]), "son fiyat:", str(son_fiyat))

            if(macd[-1] > macdsignal[-1]) and (macdhist[-1] > 0) and (macd[-1] > 25) and (macdsignal[-1] > 25):
                print("Mum Zamanı: " + str(zaman) + " İşlem: Al")
                anlik_islem = "buy"
                side = 'buy'
                amount = miktar
                bildirim_mesaji = str(symbol) + " için al sinyali - şuanki fiyat: " + str(
                    best_buy) + ""
            elif(macd[-1] < macdsignal[-1]) and (macdhist[-1] < 0) and (macd[-1] < -25) and (macdsignal[-1] < -25):
                print("Mum Zamanı: " + str(zaman) + " İşlem: Sat")
                anlik_islem = "sell"
                side = 'sell'
                amount = buy_balance
                bildirim_mesaji = str(symbol) + " için sat sinyali - şuanki fiyat: " + str(
                    best_buy) + ""
            else:
                anlik_islem = "bekle"

            sql = "SELECT side FROM `trades` WHERE botname = '"+botname + \
                "' AND user_id = '" + \
                str(user['id'])+"' ORDER BY id DESC LIMIT 1"
            cursor.execute(sql)
            son_islem = cursor.fetchone()

            try:
                if(son_islem[0] != anlik_islem) and (anlik_islem != "bekle"):

                    sql = "SELECT mum_time FROM `trades` WHERE botname = '"+botname + \
                        "' AND user_id = '" + \
                        str(user['id'])+"' ORDER BY id DESC LIMIT 1"
                    cursor.execute(sql)
                    sonalim = cursor.fetchone()
                    if(sonalim[0] != zaman):
                        type = 'market'
                        price = None
                        params = {}

                        order = exchange.create_order(
                            symbol, type, side, amount, price, params)
                        orderdata = order
                        order = json.dumps(order)

                        date_time = int(orderdata['timestamp'])/1000
                        date_time = datetime.datetime.fromtimestamp(date_time)
                        set_order_data = [[user['id'], botname, orderdata['id'], date_time, orderdata['symbol'],
                                           orderdata['side'], orderdata['amount'], orderdata['average'], orderdata['status'], orderdata['fee']['currency'], orderdata['fee']['cost'], orderdata['cost'], zaman]]

                        sqlguncelleme = "INSERT INTO trades (user_id, botname, orderid, datetime, symbol, side, amount, price, status,fee_currency,fee_cost,cost,mum_time) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.executemany(sqlguncelleme, set_order_data,)
                        db.commit()

                        telegram_bot_sinyal = True

                        
            except Exception as e:
                print(e)
                
if(telegram_bot_sinyal==True):
    bot = telegram.Bot(token="1072130790:AAGHsSP1wlf6oZJnoq4nB5XKfpo7MGUaqvc")
    bot.send_message(chat_id="@Bingo2Win", text=bildirim_mesaji)
