#pip install jsonrpclib


from jsonrpclib import Server
import sys
import pandas as pd

def main():
    server = Server('http://localhost:5001')
    df = pd.read_orc("trades.orc")
    try:
        for index, row in df.iterrows():
            server.register_api(str(row["trade_id"]), str(row["price"]),str(row["quantity"]),str(row["time"]),str(row["isSelling"]),1,"192.168.0.2")        

            #print(str(row["trade_id"]), str(row["price"]),str(row["quantity"]),str(row["time"]),str(row["isSelling"]),1,"192.168.0.2")
    except:
        print("Error: ", sys.exc_info())


if __name__ == '__main__':
    main()
