import ccxt
import time
import datetime
from baglanti import mysql_baglan
import json
import requests
import sys
import telegram



db = mysql_baglan("bingo")
cursor = db.cursor()
cursor.execute('SET NAMES utf8;')
cursor.execute('SET CHARACTER SET utf8;')
cursor.execute('SET character_set_connection=utf8;')


def bildirimAt(mesaj):
    url = "https://bildiren.xyz/send"

    querystring = {
        "secret": "08UMjkavI1QqIwktYuWWRitkIeX2_takim", "message": mesaj}

    payload = ""
    headers = {
        'cache-control': "no-cache",
    }

    response = requests.request(
        "GET", url, data=payload, headers=headers, params=querystring)


sql = "SELECT apikey,secret,id,sellcoin,buycoin,botname FROM `users` WHERE status = '1'"
cursor.execute(sql)
column_names = ['apikey', 'secret', 'id', 'sellcoin', 'buycoin', 'botname']
results = cursor.fetchall()
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

            ma7ler = mumlar[-7:]
            ma7kapanis = 0.0
            for ma7 in ma7ler:
                zaman = int(ma7[0])/1000
                zaman = datetime.datetime.fromtimestamp(zaman)
                ma7kapanis = ma7kapanis + float(ma7[4])

            ma7ortalama = ma7kapanis/7
            print("ma7 ortalama:" + str(ma7ortalama))  # mum kapanışı

            ma25ler = mumlar[-14:]
            ma25kapanis = 0.0
            for ma25 in ma25ler:
                zaman = int(ma25[0])/1000
                zaman = datetime.datetime.fromtimestamp(zaman)

                ma25kapanis = ma25kapanis + float(ma25[4])

            ma25ortalama = ma25kapanis/14
            print("ma31 ortalama:" + str(ma25ortalama))

            if(ma7ortalama > ma25ortalama):
                print("Mum Zamanı: " + str(zaman) + " İşlem: Al")
                anlik_islem = "buy"
                side = 'buy'
                amount = miktar
                bildirim_mesaji = str(symbol) + " için al sinyali - şuanki fiyat: " + str(
                    best_buy) + " otomatik işlem yapmak için https://bingo2w.in"
            else:
                print("Mum Zamanı: " + str(zaman) + " İşlem: Sat")
                anlik_islem = "sell"
                side = 'sell'
                amount = buy_balance
                bildirim_mesaji = str(symbol) + " için sat sinyali - şuanki fiyat: " + str(
                    best_buy) + " otomatik işlem yapmak için https://bingo2w.in"

            sql = "SELECT side FROM `trades` WHERE botname = '"+botname + \
                "' AND user_id = '" + \
                str(user['id'])+"' ORDER BY id DESC LIMIT 1"
            cursor.execute(sql)
            son_islem = cursor.fetchone()

            try:
                if(son_islem[0] != anlik_islem):

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

                        bot = telegram.Bot(token="1072130790:AAGHsSP1wlf6oZJnoq4nB5XKfpo7MGUaqvc")
                        bot.send_message(chat_id="@Bingo2Win", text=bildirim_mesaji)
            except Exception as e:
                print(e)
