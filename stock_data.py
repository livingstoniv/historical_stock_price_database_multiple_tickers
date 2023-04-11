import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime, date, timedelta



wiki = 'https://en.wikipedia.org/wiki/'

tickersDOW = pd.read_html(wiki+'Dow_Jones_Industrial_Average')[1].Symbol.to_list()
tickersSP = pd.read_html(wiki+'List_of_S%26P_500_companies')[0].Symbol.to_list()
tickerSPY = 'SPY'



def getdata(tickers):
  data = []
  one_minute_data = []
  for ticker in tickers:
    data.append(yf.download(ticker).reset_index())
  return data
dow, sp, spy = getdata(tickersDOW), getdata(tickersSP), getdata(tickerSPY)

conne = sqlite3.connect('financial_database.db')

def TOSQL(frames, symbols, conne):
  for frame, symbol in zip(frames, symbols):
    frame.to_sql(symbol + "_Price_Data", conne, index=False, if_exists= 'append')
  print('Successfully Imported Data')

TOSQL(dow, tickersDOW, conne)
TOSQL(sp, tickersSP, conne)
TOSQL(spy, tickerSPY, conne)
