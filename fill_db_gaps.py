import os
import sys
import argparse
import sqlite3
import pandas as pd
import numpy as np
import pytz

conn = None
start_date="2020-01-01 00:00:00"

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

def rename_table_with_name(conn, table_name, new_table_name, database):
    if conn is not None:
        if check_table_exists(conn, table_name):
            print('Table', table_name, 'already exists.')
            c = conn.cursor()
			
            query = "ALTER TABLE " + table_name + " RENAME TO " + new_table_name 
            #get the count of tables with the name
            c.execute(query)
            print("Successfuly altered the table name")
        else:
            #create tables
            print('Table', table_name, 'does not exist to drop.')
    else:
       print("Error! cannot drop table from database.")

def save_temp_data_to_table(conn, df, table_name, database):
    drop_table_with_name(conn, table_name, database)

    try:
        df.to_sql(table_name, con = conn, index = False)
        print("Succeeded writing temp table")
    except Exception as e:
        print(e)

def fill_gaps_in_table(table_name, conn, db_file):
    global start_date
    df_orig = pd.read_sql("SELECT * from " + table_name + " order by start asc", con=conn)
    df = df_orig.copy()

    
    start = df['start']
    ns = len(start)
    start_e = start[0]
    start_s = start[ns-1] 

    step = 60
    start_new = np.arange(start_s, start_e + step, step)
    ns_new = len(start_new)

    import datetime
    lstart = list(start)

    tz = pytz.timezone("America/New_York")
    df['time'] = [datetime.datetime.fromtimestamp(ls, tz) for ls in lstart]

    #del df['start']
    del df['id']
    try:
        df = df.set_index('time')
    except Exception as e:
        print("Error during setting index")
        print(e)
        return(pd.DataFrame())

    try:
        df = df[df.index >= start_date]

        df_interpol = df.resample('T')
        df_interpol = df_interpol.interpolate(method='linear')
        df_interpol['start'] = [int(d.astype('int')/1e9) for d in df_interpol.index.values]
        cols = ['start', 'open', 'high', 'low', 'close', 'volume', 'trades']
        df_interpol = df_interpol[cols]
    except Exception as e:
        print("Error during resampling and interpolating")
        print(e)
        return(pd.DataFrame())

    if df.shape[0] == df_interpol.shape[0]:
        return(pd.DataFrame())

    print(df.tail())
    print(df_interpol.tail())
    print(df.shape)
    print(df_interpol.shape)

    return(df_interpol)

def main():
    args = sys.argv
    db_file=args[1]

    global start_date
    if len(args) == 3:
        start_date = args[2]
        print("Set start date = ", start_date)


    print(db_file)

    try:
        conn = create_connection(db_file)
        tables = get_all_tables(conn)
        print(tables)
        for i, table_name in enumerate(tables):
            print("Table No. ", i, " = ", table_name)
            if "tmp" not in table_name:
                conn = create_connection(db_file)
                # filling the gaps by interpolating
                try:
                    df = fill_gaps_in_table(table_name, conn, db_file)
                except Exception as e:
                    print("Error filling gaps for this table")
                    print(e)
                    

                if df.empty:
                    print("Dataframe is empty. Maybe already processed")
                    next

                try:
                    new_table_name = table_name + "_tmp"
                    print(new_table_name)
                    save_temp_data_to_table(conn, df, new_table_name, db_file)

                    #dropping the old table
                    drop_table_with_name(conn, table_name, db_file)
                    #renaming the tmp table as the table
                    rename_table_with_name(conn, new_table_name, table_name, db_file) 
                except Exception as e:
                    print("Error in saving, dropping or renaming")
                    print(e)

    except Exception as e:
        print(e)
        print("Error creating connection")
        sys.exit(1)

main()
