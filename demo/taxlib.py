from talib.abstract import *
import talib
import numpy as np
import ccxt
import time
import datetime

exchange = ccxt.binance({
    'apiKey': 'DF4J3Ebo2HOWMGfhxFpVXatzfjl3EVFB24qUSLWgiFt1Dst5ygnuQyGctGs9wX5C',
    'secret': 'Ythnld9QpSAXqYvjOtOrwYs0Mq2FIgCF1em2nX4ly02c5lrnuk9CAdHWtMtEDGEs',
    'enableRateLimit': True
})


def clearData(mumlar):
  high = []
  open = []
  low = []
  close = []
  volume = []
  opentime = []
  for mum in mumlar:
      open.append(mum[1])
      high.append(mum[2])
      low.append(mum[3])
      close.append(mum[4])
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


mumlar = exchange.fetch_ohlcv('BTC/USDT', '30m')

inputs = clearData(mumlar)

output25 = MA(inputs, timeperiod=14, price='close')
output7 = MA(inputs, timeperiod=7, price='close')
real = talib.ADX(inputs['high'], inputs['low'], inputs['close'], timeperiod=14)
zaman = int(inputs['opentime'][-1])/1000
zaman = datetime.datetime.fromtimestamp(zaman)
print(inputs["open"].size)
print(real[-1])
