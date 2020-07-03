import os
import sys
import argparse
import time
import datetime
import multiprocessing
import sqlite3
import ccxt  # noqa: E402
import pandas as pd
import pprint as pp

#file root
froot = os.path.dirname(os.path.abspath(__file__))
#history root
root = "../../history/latest"

cur_datetime = datetime.datetime.now().strftime("%d-%m-%Y_%H:%M:%S")

# database connection
conn = None

# common constants
batch_import_finished = False
nprocesses = 15 

msec = 1000
minute = 60 * msec
hour = 60 * minute
day = 24 * hour
week = 7 * day
month = 4 * week
year = 12 * month

# move timeframe for next from_timestamp
move = 3 * minute 
move = week 

# simulate a binary like search sqeezing the search range from a day to a minute
timeframe_list = ['year', 'month', 'week', 'day', '12hour', '6hour', '3hour', '90min', '45min', '24min', '12min', '6min', '3min', '2min', 'min']
search_timeframe = {};
# move from_timestamp by
search_timeframe['year'] = year 
search_timeframe['month'] = month 
search_timeframe['week'] = week 
search_timeframe['day'] = day
search_timeframe['12hour'] = 12 * hour 
search_timeframe['6hour'] = 6 * hour 
search_timeframe['3hour'] = 3 * hour 
search_timeframe['90min'] =  90 * minute 
search_timeframe['45min'] =  45 * minute 
search_timeframe['24min'] =  24 * minute 
search_timeframe['12min'] =  12 * minute 
search_timeframe['6min'] =  6 * minute 
search_timeframe['3min'] =  3 * minute 
search_timeframe['2min'] =  2 * minute 
search_timeframe['min'] =   minute 
    

hold = 10 

timeframe = '1m'
now = None

from_datetime = '2020-01-01 00:00:00'
to_datetime = None

last_starttime = {} 
last_starttime_df = None 
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

def create_table_with_name(conn, table_name, database):
    table_schema = """ CREATE TABLE IF NOT EXISTS {0} (
 	                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        start INTEGER UNIQUE,
                                        open REAL NOT NULL,
                                        high REAL NOT NULL,
                                        low REAL NOT NULL,
                                        close REAL NOT NULL,
                                        vwp REAL NOT NULL,
                                        volume REAL NOT NULL,
                                        trades INTEGER NOT NULL
                                   ); """
    table_schema = table_schema.format(table_name)
    if conn is not None:
        if check_table_exists(conn, table_name):
            print('Table', table_name, 'already exists.')
        else:
            #create tables
            create_table(conn, table_schema)
            print('Table', table_name, 'created.')
    else:
       print("Error! cannot create the database connection.")

def drop_table_with_name(conn, table_name, database):
    if conn is not None:
        if check_table_exists(conn, table_name):
            print('Table', table_name, 'already exists.')
            c = conn.cursor()
			
            query = "DROP TABLE " + table_name 
            #get the count of tables with the name
            c.execute(query)
        else:
            #create tables
            print('Table', table_name, 'does not exist to drop.')
    else:
       print("Error! cannot drop table from database.")

def save_data_to_table(df, table_name, database):
    global conn 
    conn = create_connection(database)
    create_table_with_name(conn, table_name, database)
    try:
        #print("Getting last few rows and trying to append in the table")
        existing = pd.read_sql("SELECT * from " + table_name + " order by start desc limit 1", con=conn)
        last_start_in_df = max(existing["start"])
        existing = existing.set_index("start")
        to_insert = df.set_index("start")

        new = existing.append(to_insert)
        new = new[~new.duplicated(keep='first')]
        if "id" in new.columns:
            new = new.drop("id", axis=1)
        new = new.reset_index().rename(columns={"index":"start"})
        new = new.drop_duplicates(subset="start")
        new = new.reset_index().rename(columns={"index":"id"})
        if "id" in new.columns:
            new = new.drop("id", axis=1)
        new["start"] = new["start"]/msec
        new = new[new["start"] > last_start_in_df]

        print(new.shape)
        if new.shape[0] > 0:
            new.to_sql(table_name, con=conn, index=False, if_exists='append') 
            print('Success writing to database for table ', table_name)
            last_starttime_df.to_sql("last_starttime", con=conn, index=True, if_exists="replace")
            print("1. Last starttime for market pair updated") 
        else:
            print("No new data to append to the table yet.")
    except Exception as e:
        print("Initial failure to append: {}\n".format(e))
        print("Attempting to rectify...")
        # this is problematic when existing table is very big
        existing = pd.read_sql("SELECT * from " + table_name, con=conn)
        existing = existing.set_index("start")

        to_insert = df.set_index("start")

        new = existing.append(to_insert)
        new = new[~new.duplicated(keep='first')]
        if "id" in new.columns:
            new = new.drop("id", axis=1)
        new = new.reset_index().rename(columns={"index":"start"})
        new = new.drop_duplicates(subset="start")
        new = new.reset_index().rename(columns={"index":"id"})
        if "id" in new.columns:
            new = new.drop("id", axis=1)
        new["start"] = new["start"]/msec

        try:
            print("drop table")
            c = conn.cursor()
            try:
                c.execute("DROP TABLE " + table_name + ";");
            except Exception as e:
                print("\nError occurred in dropping the table")
                print(e)

            try:
                print("creating table")
                create_table_with_name(conn, table_name, database)
            except Exception as e:
                print("\nError occurred in creating the table after dropping it.")
                print(e)

            print("writing the new table from pandas data frame")

            new.to_sql(table_name, con=conn, index=False, if_exists="append") 
            print("dropped and added updated table")

            last_starttime_df.to_sql("last_starttime", con=conn, index=True, if_exists="replace")
            print("2. Last starttime for market pair updated") 
        except Exception as e2:
            print("Error occurred in saving the new table.")
            print(e2)
        print('Success after dedupe')
    except Exception as e:
        print("Fake exception for testing.")
        print(e)


def get_table_name(market_pair):
    market_symbol = market_pair.split("/")[1] 
    asset_symbol = market_pair.split("/")[0] 

    table_name = "candles" + "_" + market_symbol + "_" + asset_symbol 

    return table_name 
       
       
def save_to_sqlite_database(df, exchange, market_pair, timeframe, from_timestamp, now_timestamp):
    try:
       from_datetime_f = "_".join(from_datetime.split(" "))
       exchange_id = str(exchange)
       exchange_id = "".join(exchange_id.split(" "))
       exchange_id = exchange_id.lower()
       #db_file = root + "/" + exchange_id + "_" + from_datetime_f + "_" + cur_datetime + ".db"
       if exchange_id == "coinbasepro":
           exchange_id = "gdax"

       db_file = root + "/" + exchange_id + "_0.1.db"

       print(market_pair)

       table_name = get_table_name(market_pair)

       print("Appending to the sqlite table", table_name)
       save_data_to_table(df, table_name, db_file)
    except Exception as e:
       print(e)

def find_next_from_timestamp(parallel_func_id, market_pair, exchange, timeframe, search_timeframe_ix, from_timestamp, now): 
    global timeframe_list, search_timeframe

    next_from_timestamp = from_timestamp
    print("Searching for next available starttime starting from timestamp = ", from_timestamp)
    print('Fetching candles starting from', exchange.iso8601(from_timestamp))

    # start with "day" timeframe by making index to be zero

    initial_from_timestamp = from_timestamp

    unknown_error = False 

    while from_timestamp < now :
        cur_timeframe = timeframe_list[search_timeframe_ix]

        print("Search timeframe: ", search_timeframe[cur_timeframe])
        print("(Search Loop) Inside importing id: ", parallel_func_id ," pair:", market_pair)
        print('(Search Loop) Fetching candles starting from', exchange.iso8601(from_timestamp))

        ohlcvs = []
        try:
            print("Fetching")
            ohlcvs = exchange.fetch_ohlcv(market_pair, timeframe, from_timestamp)
            date = exchange.iso8601(exchange.milliseconds()) 
            print( "(Search Loop)", date, ': ',  'Fetched', len(ohlcvs), 'candles')

        except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
            #print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
            print("2. Timeout error. Retry.")
            #print(error)
            time.sleep(hold)
            continue
        except Exception as error:
            print("Error = ", error)
            print('2. Unknown Exception')
            #print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
            unknown_error = True 

        print("length = ", len(ohlcvs))
        print(unknown_error)
        if len(ohlcvs) != 0: #found some candles
            unknown_error = False
            # no more timeframe to search, keep searching
            cur_timeframe = timeframe_list[search_timeframe_ix]
            if cur_timeframe == 'min':
                # break out and return the from timestamp
                print("Found the exact minute to start importing again:", from_timestamp)
                print("Date to start from again = ", exchange.iso8601(from_timestamp))
                break 

            print("Found some candles.")
            print("Old timeframe: ", cur_timeframe)
            # squeeze timeframe by increasing index, thus decreasing timerange for the next search 
            from_timestamp -= search_timeframe[cur_timeframe] 
            search_timeframe_ix += 1 
            cur_timeframe = timeframe_list[search_timeframe_ix]
            from_timestamp += search_timeframe[cur_timeframe] 
            print("Changing timeframe to:", cur_timeframe)
            print("")
        else:
            print("No candles found. Moving to next timestamp.\n")
            # no candle found so expand search 
            cur_timeframe = timeframe_list[search_timeframe_ix]
            from_timestamp += search_timeframe[cur_timeframe] 
            print("Next timestamp to try = ", from_timestamp)
            print("Date to start from again = ", exchange.iso8601(from_timestamp))
            print("")

    return from_timestamp


def get_historical_data_from_timestamp(parallel_func_id, market_pair, exchange, timeframe, from_timestamp, now): 
    dataDF = pd.DataFrame(columns=columns) 
    
    prev_from_timestamp = -1
    same_prev_cnt = 0
    last_id = 0
    no_fetch = 0
    print("First from timestamp = ", from_timestamp)
    print('1.Fetching candles starting from', exchange.iso8601(from_timestamp))

    while from_timestamp < now:
        try:
            #print(from_timestamp)
            #print(now)
            #print('Now = ', exchange.iso8601(now))
            date = exchange.iso8601(exchange.milliseconds()) 
        
            # optimize by getting the last timestamp in the table in sqlite database            
            # read_sql

            ohlcvs = exchange.fetch_ohlcv(market_pair, timeframe, from_timestamp)

            #lock.acquire()

            '''
            orig_from_timestamp = exchange.parse8601(from_datetime)
            if from_timestamp < orig_from_timestamp:
                print("\n Resetting from timestamp. Something is wrong in date determination. \n")
                from_timestamp = orig_from_timestamp
            '''

            print("Inside importing id: ", parallel_func_id ," pair:", market_pair)
            print('2.Fetching candles starting from', exchange.iso8601(from_timestamp))
            print( date, ': ',  'Fetched', len(ohlcvs), 'candles')


            '''
            if no_fetch == 5:
               break

            if len(ohlcvs) == 0:
               no_fetch += 1
               continue 
            '''

            if len(ohlcvs) != 0:
                print(ohlcvs[-1])
                # updating from_timestamp
                # Set timestamp to the next minute by getting the last candle which has the last minute timestamp
                from_timestamp = ohlcvs[-1][0] + minute; 
                global last_starttime_df
                try:
                    tst = ohlcvs[-1][0]
                    last_starttime_df.loc[market_pair, ["last_starttime"]] = datetime.datetime.utcfromtimestamp(tst).strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    last_starttime_df.loc[market_pair, ["last_starttime"]] = datetime.datetime.utcfromtimestamp(tst/msec).strftime("%Y-%m-%d %H:%M:%S")
                    print("Success updating last_starttime_df with ", market_pair, " ", last_starttime_df.loc[market_pair])
            else:
                #'''
                print("Missing data for timestamp = ", from_timestamp)


                # find next from_timestamp to jump the missing data by repeatedly calling fetch ohlcv in different timeframe
                search_timeframe_ix = 2
                try:
                    from_timestamp = find_next_from_timestamp(parallel_func_id, market_pair, exchange, timeframe, search_timeframe_ix, from_timestamp, now)
                    print("**try. Next from timestamp = ", from_timestamp)
                except Exception as error:
                    print("Something is wrong while finding next from timestamp")
                    print("Error = ", error)
                    
                    print("Setting next timeframe")
                    global timeframe_list, search_timeframe
                    cur_timeframe = timeframe_list[search_timeframe_ix]
                    from_timestamp += search_timeframe[cur_timeframe] 
                    print("**except. Next from timestamp = ", from_timestamp)

                continue


            df = pd.DataFrame(ohlcvs, columns = columns)
            print("Update last index")
            df.index = last_id + 1 + df.index
            last_id = df.index[-1]
            print("current last index", last_id)

            # add these columns to match with gekko sqlite database
            df['vwp'] = df['close']
            df['trades'] = 1 

            if from_timestamp == prev_from_timestamp:
                print("Same previous timestmap. Reached end.")
                same_prev_cnt += 1
                print("Updating previous timestamp.")
                from_timestamp += 60 * 1000 
                if same_prev_cnt >= 10:
                    same_prev_cnt = 0
                    break
   
            prev_from_timestamp = from_timestamp

            dataDF = df 
            dataDF.index.rename('id')

            #print(dataDF.describe())
            print("Saving data frame to database.")
            save_to_sqlite_database(dataDF, exchange, market_pair, timeframe, from_timestamp, now)
            print("Saveddatabase.\n")

            #lock.release()
        except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
            #print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
            #print("Timeout error. Retry.")
            time.sleep(hold)
        except Exception as error:
            #print('Unknown Exception')
            #print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
            time.sleep(hold)

def get_historical_data_for_market_pair(parallel_func_id, market_pair, exchange, timeframe, from_datetime, now): 
    from_timestamp = exchange.parse8601(from_datetime)
    data = get_historical_data_from_timestamp(parallel_func_id, market_pair, exchange, timeframe, from_timestamp, now) 

def parallel_history_func(i, exchange):
    if i >= len(market_pairs):
        print("List index out of range for index i: ", i)
        return
        
    print("Start importing for Market pair ", i, " = ", market_pairs[i])
    market_pair = market_pairs[i]

    # check whether already imported, then get last starttime
    #print(last_starttime)
    global from_datetime
    try:
        from_datetime_pair = last_starttime[market_pair]
    except Exception as e:
        print("From datetime not found for market pair. Choosing default")
        from_datetime_pair = from_datetime

    get_historical_data_for_market_pair(i, market_pair, exchange, timeframe, from_datetime_pair, now) 

def run_parallel_market_import(exchange):
    # get markets
    npairs = len(market_pairs)
    print("Number of market pairs = ", npairs)

    '''
    market_pair='BTC/USD'
    asset_symbol = market_pair.split("/")[0]
    market_symbol = market_pair.split("/")[1]
    market_pair = str(asset_symbol) + '/' + str(market_symbol)
    '''

    global lock
    lock = multiprocessing.Lock()
    nproc= nprocesses
    proc_list = []
    loop_n = 0 
    while True:
        ci = loop_n + loop_n % nproc 

        nproc = nprocesses if (npairs - loop_n) >= 10 else (npairs - loop_n)
        for pi in range(nproc):
            proc = multiprocessing.Process(target=parallel_history_func, args = (ci + pi, exchange))
            proc_list.append(proc)
            loop_n += 1
            proc.start()


        # wait for processes to finish
        for proc in proc_list:
            proc.join()

        # terminating the processes
        for proc in proc_list:
            if proc.is_alive():
                proc.terminate()

        # rejoining after terminating if necessary
        for proc in proc_list:
            if proc.is_alive():
                proc.join()

        proc_list = []


        loop_n += 1
        if (loop_n >= npairs):
            print("Finished importing for all market pairs. \n\n")

            global batch_import_finished
            batch_import_finished = True
            break
        else:
            loop_n -= 1
            print("\nPrepare next batch of processes\n")

def find_gaps_in_table(database, table_name):
    return

def get_last_starttime_from_sql(exchange_id, market_pairs):
    if exchange_id == "coinbasepro":
        exchange_id = "gdax_0.1"

    db_file = root + "/" + exchange_id + "_0.1.db"
    try:
        conn = create_connection(db_file)
    except Exception as e:
        print(e)
        print("Error creating connection")
        sys.exit(1)


    for i, market_pair in enumerate(all_market_pairs):
        table_name = get_table_name(market_pair)
        last_starttime[market_pair] = from_datetime 
        if check_table_exists(conn, table_name):
            query = "SELECT start from " + table_name + " ORDER BY start DESC limit 1"
            startDF= pd.read_sql(query, con=conn )
            try:
                if len(startDF.start) == 0:
                    continue
                start = startDF.start[0] 
                starttime = datetime.datetime.utcfromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S")
                last_starttime[market_pair] = starttime
            except Exception as e:
                starttime = datetime.datetime.utcfromtimestamp(start/1000).strftime("%Y-%m-%d %H:%M:%S")

                last_starttime[market_pair] = starttime

            #print("market pair " + str(i) + ":" + market_pair)
            #print(last_starttime[market_pair])

    global last_starttime_df
    last_starttime_df = pd.DataFrame.from_dict(last_starttime, orient='index', columns=["last_starttime"])
    last_starttime_df = last_starttime_df.rename_axis("market_pair")
    ap = []
    for p in all_market_pairs: 
        ap.append(markets[p]['active'])
    
    last_starttime_df["active"] = ap

    pd.set_option("display.max_rows", None)
    print(last_starttime_df)
    pd.set_option("display.max_rows", 20)
    #print(last_starttime_df)

def get_historical_data(exchange_id, from_datetime):
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
    get_last_starttime_from_sql(exchange_id, market_pairs)

    if to_datetime == None:
        now = exchange.milliseconds()
    else:
        now_datetime = to_datetime
        now = exchange.parse8601(now_datetime)

    while True:
        if initial:
            run_parallel_market_import(exchange)
            initial = False

        # run through this loop when current import phase is ongoing
        if batch_import_finished == True:
            # break this loop and start new import 
            #time.sleep(10)
            batch_import_finished = False
            print("Now sleeping.")
            time.sleep(20)
            print("Resuming import of all market pairs.")

            get_last_starttime_from_sql(exchange_id, market_pairs)
            now = exchange.milliseconds()

            initial = True

            break
            

# test importing 1m data from gdax using ccxt 
def main():
    args = sys.argv
    nargs = len(args)

    global exchange_id
    if nargs == 2:
        exchange_id = args[1]

    if nargs >= 3:
        arg_from = args[1]
        arg_to = args[2]
        global from_datetime, to_datetime
        from_datetime = arg_from
        to_datetime = arg_to

        if nargs >= 4:
            arg_exchange_id = args[3]

    get_historical_data(exchange_id, from_datetime)

if __name__ == "__main__":
    main()
