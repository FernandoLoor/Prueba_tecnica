from ConnectDB import *
import numpy as np
import datetime
import pandas as pd

def datetime64_to_unix_timestamp(dt):
    epoch = np.datetime64('1970-01-01T00:00:00')
    delta = dt - epoch
    return int(delta / np.timedelta64(1, 'ms'))

def getElements(date1,date2):
    database = ConnectDB()
    #date1='2023-05-01T00:00:00'
    #date2='2023-05-02T23:59:59'
    start_datetime = np.datetime64(date1)
    end_datetime = np.datetime64(date2)
    print("getElements start_datetime:")
    print(start_datetime)
    print("getElements end_datetime:")
    print(end_datetime)

    # Convert datetime64 objects to Unix timestamps
    start_timestamp = datetime64_to_unix_timestamp(start_datetime)
    end_timestamp = datetime64_to_unix_timestamp(end_datetime)
    data = database.get_trades(start_timestamp,end_timestamp)
    
    df = pd.DataFrame(data, columns = ['trade_id', 'price', 'quantity', 'time', 'isSelling'])
    
    return df


#if __name__ == "__main__":
#    database = ConnectDB()
#    start_datetime = np.datetime64('2023-05-01T00:00:00')
#    end_datetime = np.datetime64('2023-05-02T23:59:59')
#    # Convert datetime64 objects to Unix timestamps
#    start_timestamp = datetime64_to_unix_timestamp(start_datetime)
#    end_timestamp = datetime64_to_unix_timestamp(end_datetime)
#    data = database.get_trades(start_timestamp,end_timestamp)
#    print(data)
#
#    df = pd.DataFrame(data, columns = ['trade_id', 'price', 'quantity', 'time', 'isSelling'])
#    print(df)
#    print(len(df))
#    #