#pip install jsonrpcserver

from jsonrpcserver import Success, method, serve, InvalidParams, Result, Error
import re
from ConnectDB import *





@method
def validTradeId(id) -> Result:    
    if id == "":
        return Error(code=123, message="Empty id provided")
    if re.match("[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?",id):
        return Success(True)
    else:
        return Success(False)

@method
def validPrice(price) -> Result: 
    if price == "":
        return InvalidParams("Null value")
    if re.match("[+-]?\\d*\\.?\\d+", price):
        result = { "Price": price, "result" : "Valid Price" }
    else:
        result = { "Price": price, "result" : "Invalid Price" }
    return Success(result)

@method
def validQuantity(quant) -> Result: 
    if quant == "":
        return InvalidParams("Null value")
    if re.match("[+-]?\\d*\\.?\\d+", quant):
        result = { "Quantity": quant, "result" : "Valid Quantity" }
    else:
        result = { "Quantity": quant, "result" : "Invalid Quantity" }
    return Success(result)

@method
def validTime(time) -> Result: 
    if time == "":
        return InvalidParams("Null value")
    if re.match("[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?", time):
        result = { "Time": time, "result" : "Valid time" }
    else:
        result = { "Time": time, "result" : "Invalid Time" }
    return Success(result)

@method
def validIsSelling(selling) -> Result: 
    if selling == "":
        return InvalidParams("Null value")
    if re.match("(True)|(False)", selling):
        result = { "Selling": selling, "result" : "Valid Selling" }
    else:
        result = { "Selling": selling, "result" : "Invalid Selling" }
    return Success(result)


@method
def register_api(id,price,quant,time,selling,id_bot,ip_bot):
    
    validTradeId(id)    
    t = (id,price,quant,time,selling)
    b = (id_bot,ip_bot)
    database.insert(dataTrades=t,dataBot=b)
    return Success(True)

if __name__ == "__main__":
    global database 
    database = ConnectDB()
    serve('localhost', 5001)
    database.con.close()