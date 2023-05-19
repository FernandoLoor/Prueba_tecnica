from flask import Flask, request
import re
from ConnectDB import *
    
import pandas as pd
import os

from os.path import exists

#from getElements import *

#import sys
#path = os.path.abspath("../Report/")
#sys.path.append(path)

from etl import *

#@method
def validTradeId(id):# -> Result:    
    if id == "":
        return Error(code=123, message="Empty id provided")
    if re.match("[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?",id):
        print("validID")
        #return Success(True)
        return True
    else:
        print("wrongID")
        #return Success(False)
        return False

def appendDf2Orc(df,filename):
    if exists(filename): 
        loadedDf = pd.read_orc(filename)
        loadedDf = pd.concat([loadedDf, df], ignore_index=True)
        loadedDf.to_orc(filename)
    else:
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        df.to_orc(filename)

#@method
#def validPrice(price) -> Result: 
#    if price == "":
#        return InvalidParams("Null value")
#    if re.match("[+-]?\\d*\\.?\\d+", price):
#        result = { "Price": price, "result" : "Valid Price" }
#    else:
#        result = { "Price": price, "result" : "Invalid Price" }
#    return Success(result)
#
#@method
#def validQuantity(quant) -> Result: 
#    if quant == "":
#        return InvalidParams("Null value")
#    if re.match("[+-]?\\d*\\.?\\d+", quant):
#        result = { "Quantity": quant, "result" : "Valid Quantity" }
#    else:
#        result = { "Quantity": quant, "result" : "Invalid Quantity" }
#    return Success(result)
#
#@method
#def validTime(time) -> Result: 
#    if time == "":
#        return InvalidParams("Null value")
#    if re.match("[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?", time):
#        result = { "Time": time, "result" : "Valid time" }
#    else:
#        result = { "Time": time, "result" : "Invalid Time" }
#    return Success(result)
#
#@method
#def validIsSelling(selling) -> Result: 
#    if selling == "":
#        return InvalidParams("Null value")
#    if re.match("(True)|(False)", selling):
#        result = { "Selling": selling, "result" : "Valid Selling" }
#    else:
#        result = { "Selling": selling, "result" : "Invalid Selling" }
#    return Success(result)


#@method
#def register_api(id,price,quant,time,selling,id_bot,ip_bot):
#    validTradeId(id)    
#    t = (id,price,quant,time,selling)
#    b = (id_bot,ip_bot)
#    database.insert(dataTrades=t,dataBot=b)
#    return Success(True)




app = Flask(__name__)

@app.route('/upload', methods=['POST'])
def upload_file():
    
    global database 
    
    database = ConnectDB()
    
    file = request.files['file']
    
    database.deactivate_indexes()
    
    if file.filename.endswith('.orc'):

        # Save the ORC file temporarily
        file.save('temp.orc')
        
        # Read the ORC file into a DataFrame
        df = pd.read_orc('temp.orc')
        
        df.to_csv("df_new.csv",index=False)

        #INSERCION BULK EN LA BD
        filename = os.path.abspath("df_new.csv")
        database.insert_table(filename)
        
        #Aggregate data:
        [aggregatedDataOutcome_H, aggregatedDataOutcome_T, aggregatedDataTrades_H, aggregatedDataTrades_T, top_chains, data] = etl(df)
        
        #Append new data to aggregated dataframes:
        appendDf2Orc(aggregatedDataOutcome_H,"../AggregatedDatasets/OutcomesH.orc")
        appendDf2Orc(aggregatedDataOutcome_T,"../AggregatedDatasets/OutcomesT.orc")
        appendDf2Orc(aggregatedDataTrades_H,"../AggregatedDatasets/TradesH.orc")
        appendDf2Orc(aggregatedDataTrades_T,"../AggregatedDatasets/TradesT.orc")
        appendDf2Orc(top_chains,"../AggregatedDatasets/TopChains.orc")
        dataDf = pd.DataFrame({'lastOutcome':data[0], 'longestSellChainLength':data[1], 
                                'firstPricelongestSellChain':data[2], 'lastPricelongestSellChain':data[3], 'Date':data[4]}, index=[0])
        appendDf2Orc(dataDf,"../AggregatedDatasets/Data.orc")

        if os.path.exists("temp.orc"):
            os.remove("temp.orc")
        else:
            print("The file does not exist") 
        
        database.con.close()

        # Return a response indicating success
        return {'message': 'File uploaded and processed successfully.'}, 200

    else:
        # Return an error response if the file is not in ORC format
        return {'error': 'Invalid file format. Only ORC files are allowed.'}, 400
    
    database.activate_indexes()

if __name__ == '__main__':
    app.run()