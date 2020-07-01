import os 
import sys
import datetime
import pytz
import re
import pprint
import traceback

import sqlite3
import pandas as pd
import numpy as np

import pyfolio as pf

import backtrader as bt
import quantstats as qs

from backtrader_plotting import Bokeh, OptBrowser

import ta
from ta.trend import MACD
from ta.trend import EMAIndicator

import json
from json2html import *

import smtplib
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

#default timeframe
tf = '1Min'
tz = pytz.timezone("America/New_York")

long_asset_list = {} 
short_asset_list = {} 

#file root
froot = os.path.dirname(os.path.abspath(__file__))
#history root
root = "/Users/andy/.finance_bot/gekko/history/latest/gdaxbtcusd"

exchange_id = "Binance"
current_asset = ""

import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

start_date="2020-05-01"

smtp_server = "smtp.gmail.com"
port = 587  # For starttls


class GmailClient():
    def __init__(self, sender_email, password, receiver_email):
        self.sender_email = sender_email       
        self.password = password
        self.receiver_email = receiver_email       


    def prepare_message(self, subject, text, html):
        message = MIMEMultipart("alternative")
        message["From"] = self.sender_email
        message["To"] = self.receiver_email
        
        message["Subject"] = subject 
        
        # Turn these into plain/html MIMEText objects
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")
          
        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        message.attach(part1)
        message.attach(part2)
    
        return(message)
    
    def send_email(self, subject="", text="", html=""):
        try:
            # Create a secure SSL context
            context = ssl.create_default_context()
    
            # Try to log in to server and send email
            server = smtplib.SMTP(smtp_server,port)
            server.ehlo() # Can be omitted
            server.starttls(context=context) # Secure the connection
            server.ehlo() # Can be omitted
            server.login(self.sender_email, self.password)
            print("successfully logged in. \n Now sending email.")
        
            message = self.prepare_message(subject, text, html)
            server.sendmail(self.sender_email, self.receiver_email, message.as_string())
            print("Successfully sent email.")
        
        except Exception as e:
            # Print any error messages to stdout
            print("Error")
            print(e)
        finally:
            server.quit() 
    

def check_table_exists(conn, table_name):
   # create projects table
   c = conn.cursor()
			
   #get the count of tables with the name
   query = "SELECT count(name) FROM sqlite_master WHERE type=\'table\' AND name=\'" + table_name +"\'"
   #print(cmd)
   c.execute(query)

   #if the count is 1, then table exists
   if c.fetchone()[0] == 1 :
      return True
   else:
      return False

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception as e:
        print(e)
 
    return conn

def get_all_tables(conn):
   c = conn.cursor()
			
   query = "SELECT name FROM sqlite_master WHERE type=\'table\'"
   c.execute(query)
   res = c.fetchall()

   tables=[]
   if len(res) > 1:
      for r in res:
          table_name = r[0]
          if "sqlite" not in table_name:
            tables.append(table_name)

   return(tables)

def get_table_as_df(conn, table_name):
    df = pd.read_sql("SELECT * from " + table_name + " order by start asc", con=conn)

    return(df)

def ohlcsum(df):
    print(df)
    return {
       'open': df['open'][0],
       'high': df['high'].max(),
       'low': df['low'].min(),
       'close': df['close'][-1],
       'volume': df['volume'].sum()
      }

def get_df_by_timeframe(df, tf):
    if tf == '1m':
        tfs = '1Min'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == '3m':
        tfs = '3Min'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == '5m':
        tfs = '5Min'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == '15m':
        tfs = '15Min'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == '30m':
        tfs = '30Min'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == '1h':
        tfs = '1H'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == '4h':
        tfs = '4H'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == '6h':
        tfs = '6H'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == 'D'or tf == '1D':
        tfs = '1D'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == 'W' or tf == '1W':
        tfs = '1W'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    elif tf == 'M' or tf == '1M':
        tfs = '1M'
        dfr = df.resample(tfs).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last', 'volume':'sum'}) 
        return(dfr)
    else:
       return(df)

class PandasData(bt.feeds.PandasData):
    params = (
        ('datetime', None),
        ('dtformat', '%Y-%m-%D %H:%M:%S'),
        #('tmformat', '%H:%M:%S'),
        ('open', -1),
        ('high', -1),
        ('low' , -1),
        ('close', -1),
        ('volume', -1)
    )

class SmaCross(bt.SignalStrategy):
    def __init__(self):
        sma1, sma2 = bt.ind.SMA(period=9), bt.ind.SMA(period=26)
        crossover = bt.ind.CrossOver(sma1, sma2)
        self.signal_add(bt.SIGNAL_LONG, crossover)

class TestStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None

        self.sma = bt.indicators.SimpleMovingAverage(self.datas[0], period=15)
        self.rsi = bt.indicators.RelativeStrengthIndex()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        self.log('Close, %.2f' % self.dataclose[0])
        print('rsi:', self.rsi[0])
        if self.order:
            return

        if not self.position:
            if (self.rsi[0] < 30):
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                self.order = self.buy(size=500)

        else:
            if (self.rsi[0] > 70):
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                self.order = self.sell(size=500)


class IchimokuStrategyTmp(bt.Strategy):
    params = (
        ('atrperiod', 14),  # ATR Period (standard)
        ('atrdist_x', 1.5),   # ATR distance for stop price
        ('atrdist_y', 1.35),   # ATR distance for take profit price
        ('tenkan', 9),
        ('kijun', 26),
        ('senkou', 52),
        ('senkou_lead', 26),  # forward push
        ('chikou', 26),  # backwards push
    )

    '''
    def notify_order(self, order):
        if order.status == order.Completed:
            pass
        if not order.alive():
            self.order = None  # indicate no order is pending
    '''    

    def __init__(self):
        self.ichi = bt.indicators.Ichimoku(self.datas[0],
                                           tenkan=self.params.tenkan,
                                           kijun=self.params.kijun,
                                           senkou=self.params.senkou,
                                           senkou_lead=self.params.senkou_lead,
                                           chikou=self.params.chikou)

        # Cross of tenkan and kijun -
        #1.0 if the 1st data crosses the 2nd data upwards - long 
        #-1.0 if the 1st data crosses the 2nd data downwards - short

        self.crossover = bt.indicators.CrossOver(self.ichi.tenkan_sen, self.ichi.kijun_sen)

        # To set the stop price
        #self.atr = bt.indicators.ATR(self.data, period=self.p.atrperiod)

        # Long Short ichimoku logic
        #self.long = bt.And((self.data.close[0] > self.ichi.senkou_span_a[0]), (self.data.close[0] > self.ichi.senkou_span_b[0]), (self.tkcross == 1))
        #self.long = (self.crossover == 1) 
        
        #self.short = bt.And((self.data.close[0] < self.ichi.senkou_span_a[0]), (self.data.close[0] < self.ichi.senkou_span_b[0]), (self.tkcross == -1))
        #self.short = (self.crossover == -1)

    '''
    def start(self):
        self.size = None  # sentinel to avoid operrations on pending order

    def next(self):
        if self.order:
            return  # pending order execution

        if not self.position:  # not in the market
            if self.short:
                self.order = self.sell()
                ldist = self.atr[0] * self.p.atrdist_x
                self.lstop = self.data.close[0] + ldist
                pdist = self.atr[0] * self.p.atrdist_y
                self.take_profit = self.data.close[0] - pdist
            if self.long:
                self.order = self.buy()
                ldist = self.atr[0] * self.p.atrdist_x
                self.lstop = self.data.close[0] - ldist
                pdist = self.atr[0] * self.p.atrdist_y
                self.take_profit = self.data.close[0] + pdist
        else:  # in the market
            pclose = self.data.close[0]
            pstop = self.pstop

            if  ((pstop<pclose<self.take_profit)|(pstop>pclose>self.take_profit)):
                self.close()  # Close position
    '''

    def start(self):
        self.size = None


    def log(self, txt, dt=None):
        """ Logging function for this strategy
        """
        dt = dt or self.datas[0].datetime.date(0)
        time = self.datas[0].datetime.time()
        print('%s - %s, %s' % (dt.isoformat(), time, txt))

    def next(self):
        if self.position.size == 0:
            if self.crossover > 0:
                amount_to_invest = ( 1 * #self.p.order_pct *
                                    self.broker.cash)
                self.size = amount_to_invest / self.data.close
                msg = "*** MKT: {} BUY: {}"
                print(msg)
                #self.log(msg.format(self.p.market, self.size))
                self.buy(size=self.size)

        if self.position.size > 0:
            # we have an open position or made it to the end of backtest
            last_candle = (self.data.close.buflen() == len(self.data.close) + 1)
            if (self.crossover < 0) or last_candle:
                msg = "*** MKT: {} SELL: {}"
                print(msg)
                #self.log(msg.format(self.p.market, self.size))
                self.close()

class IchimokuStrategy(bt.Strategy):
    params = (
        ('atrperiod', 14),  # ATR Period (standard)
        ('atrdist_x', 1.5),   # ATR distance for stop price
        ('atrdist_y', 1.35),   # ATR distance for take profit price
        ('tenkan', 9),
        ('kijun', 26),
        ('senkou', 52),
        ('senkou_lead', 26),  # forward push
        ('chikou', 26),  # backwards push
        ('order_pct', 0.95),
        ('market', 'BTC/USD')
    )

    def __init__(self):
        self.ichi = bt.indicators.Ichimoku(self.datas[0],
                                           tenkan=self.params.tenkan,
                                           kijun=self.params.kijun,
                                           senkou=self.params.senkou,
                                           senkou_lead=self.params.senkou_lead,
                                           chikou=self.params.chikou)

        # Cross of tenkan and kijun -
        #1.0 if the 1st data crosses the 2nd data upwards - long 
        #-1.0 if the 1st data crosses the 2nd data downwards - short

        self.tkcross = bt.indicators.CrossOver(self.ichi.tenkan_sen, self.ichi.kijun_sen)

        if len(self.data.close) > 0 and len(self.ichi.sencou_span_a) > 0 and len(self.ichi.senkou_span_b) > 0:
            self.long = bt.And((self.data.close[0] > self.ichi.senkou_span_a[0]), (self.data.close[0] > self.ichi.senkou_span_b[0]), (self.tkcross == 1))
            self.short = bt.And((self.data.close[0] < self.ichi.senkou_span_a[0]), (self.data.close[0] < self.ichi.senkou_span_b[0]), (self.tkcross == -1))
        else:
            self.long = (self.tkcross == 1)
            self.short = (self.tkcross == -1)

    def start(self):
        self.size = None

    def log(self, txt, dt=None):
        """ Logging function for this strategy
        """
        dt = dt or self.datas[0].datetime.date(0)
        time = self.datas[0].datetime.time()
        print('%s - %s, %s' % (dt.isoformat(), time, txt))

    def next(self):
        if self.position.size == 0:
            if self.long:
                amount_to_invest = (self.p.order_pct *
                                    self.broker.cash)
                self.size = amount_to_invest / self.data.close
                msg = "*** MKT: {} BUY: {}"
                self.log(msg.format(self.p.market, self.size))
                self.buy(size=self.size)

        if self.position.size > 0:
            # we have an open position or made it to the end of backtest
            last_candle = (self.data.close.buflen() == len(self.data.close) + 1)
            if self.short or last_candle:
                msg = "*** MKT: {} SELL: {}"
                self.log(msg.format(self.p.market, self.size))
                self.close()

class CrossOver(bt.Strategy):
    """A simple moving average crossover strategy,
    at SMA 50/200 a.k.a. the "Golden Cross Strategy"
    """
    params = (('fast', 9), #50
              ('slow', 26), #200
              ('order_pct', 0.95),
              ('market', 'BTC/USD')
             )

    def __init__(self):
        sma = bt.indicators.SimpleMovingAverage
        cross = bt.indicators.CrossOver
        self.fastma = sma(self.data.close,
                          period=self.p.fast,
                          plotname='FastMA')
        sma = bt.indicators.SimpleMovingAverage
        self.slowma = sma(self.data.close,
                          period=self.p.slow,
                          plotname='SlowMA')
        self.crossover = cross(self.fastma, self.slowma)

    def start(self):
        self.size = None
        self.dt = None

    def log(self, txt, dt=None):
        """ Logging function for this strategy
        """
        dt = dt or self.datas[0].datetime.date(0)
        time = self.datas[0].datetime.time()
        print('%s - %s, %s' % (dt.isoformat(), time, txt))

    def next(self):
        if self.position.size == 0:
            if self.crossover > 0:
                amount_to_invest = (self.p.order_pct *
                                    self.broker.cash)
                self.size = amount_to_invest / self.data.close
                msg = "*** MKT: {} BUY: {}"
                self.log(msg.format(current_asset, self.size))
                self.buy(size=self.size)

                dt = self.datas[0].datetime.date(0)
                time = self.datas[0].datetime.time()
                print(dt)
                dtm = '%s %s' % (dt, time)
                global long_asset_list
                if dtm not in long_asset_list.keys():
                    l = []
                    l.append(current_asset)
                    long_asset_list[dtm] = l 
                else:
                    l = long_asset_list[dtm]
                    l.append(current_asset)
                    long_asset_list[dtm] = l

        if self.position.size > 0:
            # we have an open position or made it to the end of backtest
            last_candle = (self.data.close.buflen() == len(self.data.close) + 1)
            if (self.crossover < 0) or last_candle:
                msg = "*** MKT: {} SELL: {}"
                self.log(msg.format(current_asset, self.size))
                self.close()

                dt = self.datas[0].datetime.date(0)
                time = self.datas[0].datetime.time()
                print(dt)
                dtm = '%s %s' % (dt, time)
                global short_asset_list
                if dtm not in short_asset_list.keys():
                    l = []
                    l.append(current_asset)
                    short_asset_list[dtm] = l 
                else:
                    l = short_asset_list[dtm]
                    l.append(current_asset)
                    short_asset_list[dtm] = l

class EmaCross(bt.SignalStrategy):
    def __init__(self):
        ema1, ema2 = bt.ind.EMA(period=9), bt.ind.EMA(period=26)
        crossover = bt.ind.CrossOver(ema1, ema2)
        self.signal_add(bt.SIGNAL_LONG, crossover)

class MACD(bt.SignalStrategy):
    def __init__(self):
        dfr['EMA_12'] = EMAIndicator(close=dfr['close'], n=9, fillna=True).ema_indicator()
        dfr['EMA_26'] = EMAIndicator(close=dfr['close'], n=26, fillna=True).ema_indicator()

        MACD = MACD(close=dfr['close'])
        dfr['MACD'] = dfr['EMA_12'] - dfr['EMA_26']
        dfr['MACD2'] = MACD.macd() 
        dfr['MACD_Signal'] = EMAIndicator(close=dfr['MACD'], n=9).ema_indicator() 
        dfr['MACD_Signal2'] = MACD.macd_signal()
        dfr['MACD_HIST'] = dfr['MACD'] - dfr['MACD_Signal']
        dfr['MACD_HIST2'] = MACD.macd_diff() 

        new_col = 'MACD_HIST_TYPE'
        new_col2 = 'MACD_HIST_TYPE2'
        nr = dfr.shape[0]
        dfr[new_col] = np.empty(nr)
        for k in range(1, nr): 
            i1 = dfr.index.values[k-1] 
            i  = dfr.index.values[k]
            if dfr.loc[i,'MACD_HIST'] > 0 and dfr.loc[i1, 'MACD_HIST'] < 0:
                dfr.loc[i, new_col] = 1 # Cross over 
                dfr.loc[i, new_col2] = 1 # Cross over 
                if i not in long_asset_list.keys(): 
                    tlist = []
                    tlist.append(table_name)
                    long_asset_list[i] = tlist 

                elif dfr.loc[i,'MACD_HIST'] > 0 and dfr.loc[i, 'MACD_HIST'] > dfr.loc[i1, 'MACD_HIST']:
                    dfr.loc[i, new_col] = 2 # Col grow above 
                    dfr.loc[i, new_col2] = 2 # Cross over 
                elif dfr.loc[i,'MACD_HIST'] > 0 and dfr.loc[i, 'MACD_HIST'] < dfr.loc[i1, 'MACD_HIST']:
                    dfr.loc[i, new_col] = 3 # Col fall above 
                    dfr.loc[i, new_col2] = 3 # Cross over 
                elif dfr.loc[i,'MACD_HIST'] < 0 and dfr.loc[i1, 'MACD_HIST'] > 0:
                    a = (i, table_name)
                    dfr.loc[i, new_col] = -1 # Cross over 
                    dfr.loc[i, new_col2] = -1 # Cross over 
                    if i not in short_asset_list.keys(): 
                        tlist = []
                        tlist.append(table_name)
                        short_asset_list[i] = tlist 
                    else:
                        short_asset_list[i].append(table_name)
                elif dfr.loc[i,'MACD_HIST'] < 0 and dfr.loc[i, 'MACD_HIST'] < dfr.loc[i1, 'MACD_HIST']:
                    dfr.loc[i, new_col] = -2 # Col fall above 
                    dfr.loc[i, new_col2] = -2 # Cross over 
                elif dfr.loc[i, 'MACD_HIST'] < 0 and dfr.loc[i, 'MACD_HIST'] > dfr.loc[i1, 'MACD_HIST']:
                    dfr.loc[i, new_col] = -3 # Cross under 
                    dfr.loc[i, new_col2] = -3 # Cross over 
                else:
                    dfr.loc[i, new_col] = 0 
                    dfr.loc[i, new_col2] = 0 # Cross over 

def printTradeAnalysis(analyzer):
    '''
    Function to print the Technical Analysis results in a nice format.
    '''
    #Get the results we are interested in
    total_open = analyzer.total.open
    total_closed = analyzer.total.closed
    total_won = analyzer.won.total
    total_lost = analyzer.lost.total
    win_streak = analyzer.streak.won.longest
    lose_streak = analyzer.streak.lost.longest
    pnl_net = round(analyzer.pnl.net.total,2)
    strike_rate = (total_won / total_closed) * 100
    #Designate the rows
    h1 = ['Total Open', 'Total Closed', 'Total Won', 'Total Lost']
    h2 = ['Strike Rate','Win Streak', 'Losing Streak', 'PnL Net']
    r1 = [total_open, total_closed,total_won,total_lost]
    r2 = [strike_rate, win_streak, lose_streak, pnl_net]
    #Check which set of headers is the longest.
    if len(h1) > len(h2):
        header_length = len(h1)
    else:
        header_length = len(h2)
    #Print the rows
    print_list = [h1,r1,h2,r2]
    row_format ="{:<15}" * (header_length + 1)
    print("Trade Analysis Results:")
    for row in print_list:
        print(row_format.format('',*row))

def printSQN(analyzer):
    sqn = round(analyzer.sqn,2)
    print('SQN: {}'.format(sqn))


def send_daily_crypto_via_email():
    # Create the plain-text and HTML version of your message
    subject = "Daily Crypto Screener"

    text = ""

    html = ""
    '''
    html = """\
    <html>
       <body>
          <p>
    """
    '''
    #add long asset list
    jr = {}
    jr['Long Assets'] = long_asset_list
    jr['Short Assets'] = short_asset_list

    jd = json.dumps(jr, indent=4, sort_keys=True)
    jh = json2html.convert(json = jd)

    html = jh 

    '''
    html += """
         </p>
       </body>
    </html>
    """
    '''

    # Sign in using App Passwords
    # https://support.google.com/accounts/answer/185833?hl=en
    sender_email = "brishtiteveja@gmail.com" 
    password = "pxfcexlmrbxhyrnn"#input("Type your password and press enter: ")

    receiver_email = "brishtiteveja@gmail.com"  # Enter receiver address

    try:
       gmail_client = GmailClient(sender_email, password, receiver_email)
       gmail_client.send_email(subject, text, html)
    except Exception as e:
       print(e)


def send_mail(send_from, send_to, subject, message, files=[],
              server="localhost", port=587, username='', password='',
              use_tls=True):
    """Compose and send email with provided info and attachments.

    Args:
        send_from (str): from name
        send_to (list[str]): to name(s)
        subject (str): message title
        message (str): message body
        files (list[str]): list of file paths to be attached to email
        server (str): mail server host name
        port (int): port number
        username (str): server auth username
        password (str): server auth password
        use_tls (bool): use TLS mode
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(message))

    for path in files:
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(Path(path).name))
        msg.attach(part)

    smtp = smtplib.SMTP(server, port)
    if use_tls:
        smtp.starttls()
    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()

def main():
    args = sys.argv
    if len(args) >= 2:
        print("Getting db file from args")
        db_file=args[1]
    else:
        db_file="../binance_0.1.db"
    

    print(db_file)

    global tf
    tf = '15m'
    if len(args) >= 3:
        tf = args[2]

    try:
        conn = create_connection(db_file)
        tables = get_all_tables(conn)


        for i, table_name in enumerate(tables):
            if table_name == "candles_USDT_BTC":
                print("ok")
            else:
                #print(table_name)
                #continue 
                pass

            '''
            if "candles_BTC_ETH" not in table_name:
                continue
            '''

            print("------------------------------------------------------------------------")
            print("Table No. ", i, " = ", table_name)
            print("")

            global current_asset
            current_asset = table_name.split("candles_")[1]
            print(current_asset)

            if "tmp" not in table_name:
                conn = create_connection(db_file)

                try:
                    df = get_table_as_df(conn, table_name)

                    start = df['start']
                    lstart = list(start)
                    df['time'] = [datetime.datetime.fromtimestamp(ls, tz) for ls in lstart]

                    #ts = int(datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%s"))
                    #df = df[df['start'] >= ts]
                    

                    df = df.set_index('time')
                    df = df.rename_axis("Date")
                    ''' 
                    print(tf)
                    tfs = re.split("[^0-9]", tf)
                    tfn = int(tfs[0])
                    print(df.head(n=tfn))
                    ''' 
                    dfr = get_df_by_timeframe(df, tf)

                    # Clean NaN values
                    dfr = ta.utils.dropna(dfr)

                    # Add all ta features
                    #dfr = ta.add_all_ta_features(
                    #    dfr, open="open", high="high", low="low", close="close", volume="volume")
                    #dfr.columns = [exchange_id + ":" + name.lower() for name in df.columns]

                    cerebro = bt.Cerebro()

                    initial_capital = 10000
                    cerebro.broker.setcash(initial_capital)
                    cerebro.broker.setcommission(commission=0.0015)


                    #cerebro.addobserver(bt.observers.Value)

                    #cerebro.addstrategy(SmaCross)
                    #cerebro.addstrategy(EmaCross)
                    cerebro.addstrategy(CrossOver)
                    #cerebro.addstrategy(IchimokuStrategy)
                    #cerebro.addstrategy(IchimokuStrategy)

                    data0 = PandasData(dataname=dfr)
                    cerebro.adddata(data0)


                    # Analyzer
                    #cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
                    #cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
                    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='tradeanalyzer')
                    cerebro.addanalyzer(bt.analyzers.SQN, _name='SQN')



                    print('Starting Portfolio Value: {0:8.2f}'.format(cerebro.broker.getvalue()))

                    results = cerebro.run()
                    strat = results[0]

		    # analyzer
                    tradeanalyzer = strat.analyzers.tradeanalyzer.get_analysis()
                    sqn = strat.analyzers.SQN.get_analysis()
                    #pprint.pprint(tradeanalyzer)

                    printTradeAnalysis(tradeanalyzer)
                    printSQN(sqn)

                    #bo = Bokeh() #style='bar', plot_mode='single')
                    #browser = OptBrowser(bo, results)
                    #browser.start()

                    #cerebro.plot(iplot=False)

                    print('Final Portfolio Value: {0:8.2f}'.format(cerebro.broker.getvalue()))

 

                    '''
		    pyfolio analyzer
                    pyfoliozer = strat.analyzers.getbyname('pyfolio')

                    returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
                    print('-- RETURNS')
                    print('-- POSITIONS')
                    print(positions)
                    print('-- TRANSACTIONS')
                    print(transactions)

                    pf.create_full_tear_sheet(
                          	returns,
            			positions=positions,
            			transactions=transactions, 
                                gross_lev='2020-05-29',
                                round_trips=True
                                )
                    '''

                    '''
                    #quantstrat integration
                    date = list(returns.keys())
                    close = [float(v) for v in returns.values()]
                    returns = pd.DataFrame(data=close, index=date, columns=['Close'])
                    returns = returns.rename_axis("Date")
                    returns.to_csv("ret.csv")
                    print(returns)
                    print(returns.index)

                    #qs.reports.full(returns, display=False)
                    m = qs.reports.metrics(returns, mode="basic")
                    print(m)
                    '''


                    #print(dfr.head(50))
                    #print(dfr.columns)
                    #dfr.to_csv("ta.csv", index=True)
			



                except Exception as e:
                    exc_info = sys.exc_info()
                    traceback.print_exception(*exc_info)
                    print("Error processing table")
                    print(e)
                #break
        print("Assets to Long per day")
        pprint.pprint(long_asset_list)

        print("Assets to Short per day")
        pprint.pprint(short_asset_list)

        send_daily_crypto_via_email()

        #pprint.pprint(portfolio)
    except Exception as e:
        print(e)



main()
