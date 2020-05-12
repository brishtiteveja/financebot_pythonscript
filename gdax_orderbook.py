import asyncio
import os
import sys
import datetime
import time
import pandas as pd
import sqlite3

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import ccxt.async_support as ccxt  # noqa: E402

root = "/home/ubuntu/gekko/history/latest"
market_pair = 'BTC/USD'

exchange = None
conn = None

async def poll():
    global exchange, market_pair
    exchange = ccxt.coinbasepro()
    while True:
        try:
            yield await exchange.fetch_order_book(market_pair)
            await asyncio.sleep(exchange.rateLimit / 1000)

            yield await exchange.close()
            break
        except Exception as e:
            print("Exception Occurred")
            time.sleep(5)
    exchange.close()


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
                                        bid_price REAL NOT NULL,
                                        bid_amount REAL NOT NULL,
                                        ask_price REAL NOT NULL,
                                        ask_amount REAL NOT NULL
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


def save_data_to_table(df, table_name, database):
    try:
        global conn 
        conn = create_connection(database)
        create_table_with_name(conn, table_name, database)
        df.to_sql(table_name, con=conn, index=True, index_label='id', if_exists='append') 
        print('Success writing to database')
    except Exception as e:
        print("Initial failure to append: {}\n".format(e))
        print("Attempting to rectify...")
        # this is problematic when existing table is very big
        existing = pd.read_sql("SELECT * from " + table_name, con=conn)

        to_insert = df.reset_index().rename(columns={'index':'id'})
        mask = ~to_insert.id.isin(existing.id)
        try:
            to_insert.loc[mask].to_sql(table_name, con=conn, index=False, if_exists='append')
            print("Successful deduplication.")
        except Exception as e2:
            "Could not rectify duplicate entries. \n{}".format(e2)
        print('Success after dedupe')

def get_table_name(market_pair):
    market_symbol = market_pair.split("/")[1] 
    asset_symbol = market_pair.split("/")[0] 

    table_name = "orderbook" + "_" + asset_symbol + "_" + market_symbol 

    return table_name 

def save_orderbook_to_sqlite_database(df, exchange, market_pair):
    try:
       exchange_id = str(exchange)
       exchange_id = "".join(exchange_id.split(" "))
       exchange_id = exchange_id.lower()
       #db_file = root + "/" + exchange_id + "_" + from_datetime_f + "_" + cur_datetime + ".db"
       if exchange_id == "coinbasepro":
           exchange_id = "gdax_0.1"
       elif exchange_id == "binance":
           exchange_id = "binance_0.1"

       db_file = root + "/" + exchange_id + "_orderbook.db"

       print(market_pair)

       table_name = get_table_name(market_pair)

       print("Appending to the sqlite table", table_name)
       save_data_to_table(df, table_name, db_file)
    except Exception as e:
       print(e)


async def main():
    async for orderbook in poll():

        nbids = len(orderbook['bids'])
        nasks = len(orderbook['asks'])
        i = 0
        now = exchange.seconds()
        start = exchange.milliseconds()
        time = datetime.datetime.utcfromtimestamp(now).strftime("%Y-%m-%d %H:%M-%S")
        print("Time = ", time)

        print("Bids = ", nbids, ", Asks = ", nasks)
        
        columns = ["start", "bid_price", "bid_amount", "ask_price", "ask_amount"]
        dataDF = pd.DataFrame(columns=columns)
        while True:
            if i < nbids and i < nasks: 
                b = orderbook['bids'][i]
                a = orderbook['asks'][i]

                s = "{0:3}: Bid: [{1:15}, {2:15}], Ask: [{3:15}, {4:15}]".format(i, b[0], b[1], a[0], a[1])
                print(s)
               
                i += 1

                df = pd.DataFrame(columns)  
                df['start'] = start
                df['bid_price'] = b[0]
                df['bid_amount'] = b[1]
                df['ask_price'] = a[0]
                df['ask_amount'] = a[1]

                dataDF = dataDF.append(df)
                print(dataDF.shape)
                 
            else:
                j = i
                while j < nbids: 
                    print(orderbook['bids'][i], "Empty asks")
                    j += 1
                while j < nasks: 
                    print("Empty bids", orderbook['asks'][i])
                    j += 1

                break
        # save orderbook in sqlite database
        dataDF.index.rename('id')
        print(dataDF)
        print("Saving order book to database")
        #save_orderbook_to_sqlite_database(dataDF, exchange, market_pair);
        print("Finished Saving order book to database")

        print("\n \n")
        print("--------------------------------------------")



asyncio.get_event_loop().run_until_complete(main())
