#pip install jsonrpclib


from jsonrpclib import Server
import sys,random,socket,struct,time

def main():
    server = Server('http://localhost:5001')
    trade_id= 11177330
    
    while(True):
        price=random.uniform(1,2000)
        quantity=random.uniform(0, 0.5)
        obj = time.gmtime(0)
        curr_time = round(time.time()*1000)
        selling=bool(random.getrandbits(1))
        bot_id=random.randrange(40)
        ip_rand=socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
        #print(trade_id,price,quantity,obj,curr_time,selling,bot_id,ip_rand)
        try:       
            server.register_api(str(trade_id),str(price),str(quantity),str(curr_time),str(selling),bot_id,str(ip_rand))
        except:
            print("Error: ", sys.exc_info())
        trade_id+=1



if __name__ == '__main__':
    main()
