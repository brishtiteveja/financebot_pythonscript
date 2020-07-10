import sys
import os
import argparse
import time
import pprint as pp
import pandas as pd
import numpy as np
import sqlite3
import ccxt
import datetime
import json

#file root
froot = os.path.dirname(os.path.abspath(__file__))
#history root
history = "../../history/"
root = "../../history/latest"

cur_datetime = datetime.datetime.now().strftime("%d-%m-%Y_%H:%M:%S")

# database connection
conn = None

timeframe = '1m'
now = None

from_datetime = '2020-01-01 00:00:00'
to_datetime = None

last_starttime = {} 
last_starttime_df = None 
first_last_starttime_df = None

first_starttime = {} 
first_starttime_df = None 
exchange_id = 'binance' #'coinbasepro'
exchange_list = ['gdax', 'binance', 'bittrex', 'hitbtc', 'bitstamp', 'bitfinex', 'gemini', 'huobi']
exchanges = {}
for i,e in enumerate(exchange_list):
    exchanges[exchange_list[i]] = i

gekko_columns = ['id', 'start', 'open', 'high', 'low', 'close', 'vwp', 'volume', 'trades']
columns = ['start', 'open', 'high', 'low', 'close', 'volume']

markets=None
all_market_pairs=None
market_pairs=None

lock = None

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

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

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

def get_table_name(market_pair):
    market_symbol = market_pair.split("/")[1] 
    asset_symbol = market_pair.split("/")[0] 

    table_name = "candles" + "_" + market_symbol + "_" + asset_symbol 

    return table_name 

def get_first_last_starttime_from_sql(exchange_id, market_pairs):
    if exchange_id == "coinbasepro":
        exchange_id = "gdax_0.1"

    db_file = history + "/" + exchange_id + "_0.1.db"
    try:
        conn = create_connection(db_file)
    except Exception as e:
        print(e)
        print("Error creating connection")
        sys.exit(1)

    global from_datetime
    global first_starttime, last_starttime
    global last_starttime_df, first_starttime_df, first_last_starttime_df

    for i, market_pair in enumerate(all_market_pairs):
        table_name = get_table_name(market_pair)
        first_starttime[market_pair] = from_datetime 
        last_starttime[market_pair] = from_datetime 
        if check_table_exists(conn, table_name):
            query = "SELECT start from " + table_name + " ORDER BY start DESC limit 1"
            startDF= pd.read_sql(query, con=conn )
            try:
                if len(startDF.start) == 0:
                    continue
                start = startDF.start[0] 

                # fix some of the wierd insertion of start time due to msec, sec 
                # ultimately these need to be fixed
                while isinstance(start, float) and (np.ceil(start) != np.floor(start)):
                    start = start * 1000

                start = int(start) 
                while len(str(start)) != 10:
                    start = start * 1000

                starttime = datetime.datetime.utcfromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S")
                last_starttime[market_pair] = starttime
            except Exception as e:
                starttime = datetime.datetime.utcfromtimestamp(start/1000).strftime("%Y-%m-%d %H:%M:%S")

                last_starttime[market_pair] = starttime

            query = "SELECT start from " + table_name + " ORDER BY start ASC limit 1"
            startDF= pd.read_sql(query, con=conn )
            try:
                if len(startDF.start) == 0:
                    continue
                start = startDF.start[0] 

                # fix some of the wierd insertion of start time due to msec, sec 
                # ultimately these need to be fixed
                while isinstance(start, float) and (np.ceil(start) != np.floor(start)):
                    start = start * 1000

                start = int(start) 
                while len(str(start)) != 10:
                    start = start * 1000

                starttime = datetime.datetime.utcfromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S")
                first_starttime[market_pair] = starttime
            except Exception as e:
                starttime = datetime.datetime.utcfromtimestamp(start/1000).strftime("%Y-%m-%d %H:%M:%S")

                first_starttime[market_pair] = starttime

            #print("market pair " + str(i) + ":" + market_pair)
            #print(last_starttime[market_pair])

    last_starttime_df = pd.DataFrame.from_dict(last_starttime, orient='index', columns=["last_starttime"])
    last_starttime_df = last_starttime_df.rename_axis("market_pair")

    first_starttime_df = pd.DataFrame.from_dict(first_starttime, orient='index', columns=["first_starttime"])
    first_starttime_df = first_starttime_df.rename_axis("market_pair")

    ap = []
    for p in all_market_pairs: 
        ap.append(markets[p]['active'])
    
    last_starttime_df["active"] = ap
    first_starttime_df["active"] = ap

    last_starttime_df = last_starttime_df.sort_index()
    first_starttime_df = first_starttime_df.sort_index()
    pd.set_option("display.max_rows", None)


    first_last_starttime_df = pd.DataFrame(first_starttime_df, copy=True)
    first_last_starttime_df["last_starttime"] = last_starttime_df["last_starttime"] 
    reordered_cols = ["first_starttime", "last_starttime", "active"]
    first_last_starttime_df = first_last_starttime_df[reordered_cols]
    first_last_starttime_df = first_last_starttime_df[reordered_cols]
    first_last_starttime_df = first_last_starttime_df.sort_index()

    '''
    print(first_starttime_df)
    print("\n\n ******************** \n \n")
    print(last_starttime_df)
    '''
    print(first_last_starttime_df)
    pd.set_option("display.max_rows", 20)
    #print(last_starttime_df)

def main():
    exchange = getattr(ccxt, exchange_id)({
        'rateLimit': 3000,
        'enableRateLimit': True,
            #'verbose': True,
    })

    print(str(exchange) +  " data to be imported.")

    global now
    if to_datetime == None:
        now = exchange.milliseconds()
    else:
        now_datetime = to_datetime
        now = exchange.parse8601(now_datetime)

        print("User specified import Daterange is as follows:")
        print("From date = ", from_datetime)
        print("To date = ", to_datetime)

    global markets, market_pairs, all_market_pairs
    markets = exchange.load_markets()

    all_market_pairs = list(markets.keys())
    print("Total active/inactive market pairs = ", len(all_market_pairs))
    global market_pairs
    market_pairs = []
    for market_pair in markets:
        market = markets[market_pair]
        if market['active']:
            market_pairs.append(market_pair) 

    print("Total active market pairs = ", len(market_pairs))

    #market_pairs = ['ATOM/BTC', 'KNC/BTC', 'ATOM/USD']
    #market_pairs = ['BTC/USDT']
    #market_pairs = ['KMD/ETH']

    global batch_import_finished
    batch_import_finished = False 
    initial = True
    get_first_last_starttime_from_sql(exchange_id, market_pairs)

    scanned_markets = {}
    scanned_markets["datasets"] = []
    for i, r in first_last_starttime_df.iterrows():
        market = {}

        table = i
        market["asset"] = table.split("/")[0]
        market["currency"] = table.split("/")[1]

        market["exchange"] = exchange_id

        ranges = {}
        s = r["first_starttime"]
        ranges["from"] = int(time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple()))
        s = r["last_starttime"]
        ranges["to"] = int(time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple()))
        market["ranges"] = [ranges]

        scanned_markets["datasets"].append(market)

    scanned_markets["error"] = []


    #pp.pprint(scanned_markets, indent=4)

    jfname = history + "scanned_market.json"

    with open(jfname, "w") as out:
        json.dump(scanned_markets, out)
        


if __name__ == "__main__":
    main()
