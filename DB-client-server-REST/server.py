from flask import Flask, request
import re
from ConnectDB import *
    
import pandas as pd
import os

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

    if file.filename.endswith('.orc'):

        # Save the ORC file temporarily
        file.save('temp.orc')
        
        # Read the ORC file into a DataFrame
        df = pd.read_orc('temp.orc')
        
        df.to_csv("df_new.csv",index=False)
        
        
        ##print(df)

        #INSERCION FILA A FILA
        #row_nbr=0
        #for index, row in df.iterrows():
        #    print(row)
        #    validTradeId(str(row["trade_id"]))
        #    #t = (id,price,quant,time,selling)
        #    #b = (id_bot,ip_bot)
        #    t = (str(row["trade_id"]), str(row["price"]),str(row["quantity"]),str(row["time"]),str(row["isSelling"]))
        #    b = (1,"192.168.0.2") 
        #    database.insert(dataTrades=t,dataBot=b)
        #    row_nbr+=1
        #    print(" ROW "+str(row_nbr)+" INSERTED ---------------------------------------------------- !")
        #database.con.close()
        filename = os.path.abspath("df_new.csv")

        #INSERCION BULK
        database.insert_table(filename)

        if os.path.exists("temp.orc"):
            os.remove("temp.orc")
        else:
            print("The file does not exist") 

        #if os.path.exists("df_new.csv"):
        #    os.remove("df_new.csv")
        #else:
        #    print("The file does not exist") 
        
        # Return a response indicating success
        return {'message': 'File uploaded and processed successfully.'}, 200

    else:
        # Return an error response if the file is not in ORC format
        return {'error': 'Invalid file format. Only ORC files are allowed.'}, 400

if __name__ == '__main__':
    app.run()