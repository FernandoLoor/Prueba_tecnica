#sudo apt install mysql-server
#sudo mysql -u root
# USE mysql;
# UPDATE user SET plugin='mysql_native_password' WHERE User='root';
# FLUSH PRIVILEGES;
#exit;
#sudo service mysql restart
#mysql -u root
# CREATE USER 'oper'@'localhost' IDENTIFIED BY '123456';
# GRANT ALL PRIVILEGES ON * . * TO 'oper'@'localhost';
# FLUSH PRIVILEGES;
#exit;
#mysql -u oper -p
#123456
#CREATE DATABASE TRADER;


#pip install mysql-connector-python
import time
from mysql.connector import connect

class ConnectDB:

    def __init__(self):
        self.con = connect(
            user='oper',
            password='AknF~5041',
            host='localhost',
            port='3306',
            database='TRADER',
            allow_local_infile=True,
        )

        self.cur = self.con.cursor()

        #self.drop_table(table='trades')

        self.create_tables()

    def create_tables(self):
        #Se crea la tabla de transacciones

        query = '''CREATE TABLE IF NOT EXISTS `trades` (
        `trade_id`  BIGINT    NOT NULL, #AUTO_INCREMENT,
        `price`     DOUBLE          NOT NULL,
        `quant`     VARCHAR(100)    NOT NULL,
        `time`      BIGINT    NOT NULL,
        `selling`   VARCHAR(5)      NOT NULL DEFAULT '0',
        PRIMARY KEY (trade_id));'''
        try:
            self.cur.execute(query)
        except Exception as e:
            print(f'\n[x] La tabla trades no se ha creado [x]: {e}')
        else:
            print('\n[!] Se ha creado la tabla de trades[!]')


        ##Se crea la tabla de bots
        #query = '''CREATE TABLE IF NOT EXISTS `bots` (
        #`bot_id`    INT             NOT NULL,
        #`ip`        VARCHAR(45)     NOT NULL,
        #`date`      DATE            DEFAULT (CURRENT_DATE),
        #PRIMARY KEY (bot_id));'''
        #try:
        #    self.cur.execute(query)
        #except Exception as e:
        #    print(f'\n[x] La tabla de bots no se ha creado [x]: {e}')
        #else:
        #    print('\n[!] Se ha creado la tabla de bots[!]')
#
#
        ##Se crea la tabla de que transacciones hizo cada bot
#
        #query = '''CREATE TABLE IF NOT EXISTS `trades_bots` (
        #`r_trade_id`    VARCHAR(100)         NOT NULL,
        #`r_bot_id`      INT            DEFAULT (CURRENT_DATE),
        #PRIMARY KEY (r_trade_id,r_bot_id),
        #KEY `fk_trades_bots_1` (`r_trade_id`),
        #KEY `fk_trades_bots_2` (`r_bot_id`),
        #CONSTRAINT `fk_trades_bots_1` FOREIGN KEY (`r_trade_id`) REFERENCES `trades` (`trade_id`) ON UPDATE CASCADE,
        #CONSTRAINT `fk_trades_bots_2` FOREIGN KEY (`r_bot_id`) REFERENCES `bots` (`bot_id`) ON UPDATE CASCADE
        #);'''
        #try:
        #    self.cur.execute(query)
        #except Exception as e:
        #    print(f'\n[x] La tabla de relacional no se ha creado [x]: {e}')
        #else:
        #    print('\n[!] Se ha creado la tabla relacional[!]')

    def drop_table(self, table):
        query = f'DROP TABLE IF EXISTS {table};'
        try:
            self.cur.execute(query)
        except Exception as e:
            print(f'\n[x] Fallo al quitar la tabla [x]: {e}')
        else:
            # Commit para registrar a operação/transação no banco.
            self.con.commit()
            print('\n[!] Tabla removida con exito [!]')


    def not_exist_trade(self,id_):
        query = '''SELECT * FROM trades WHERE trade_id = %s;'''        
        try:
            self.cur.execute(query, (id_,))
        except Exception as e:
            self.con.rollback()
            print('\n[x] Fallo al checkear trade[x]')
            print(f'[x] (Rollback) [x]: {e}\n')            
        else:
            return self.cur.fetchone() 

    #def not_exist_bot(self,id_):
    #    query = '''SELECT * FROM bots WHERE bot_id = %s;'''        
    #    try:
    #        self.cur.execute(query, (id_,))
    #    except Exception as e:
    #        self.con.rollback()
    #        print('\n[x] Fallo al checkear bot[x]')
    #        print(f'[x] (Rollback) [x]: {e}\n')            
    #    else:
    #        return self.cur.fetchone()
#
    #def not_exist_rel(self,data):
    #    query = '''SELECT * FROM trades_bots WHERE  `r_trade_id` = %s AND`r_bot_id` = %s;'''        
    #    try:
    #        self.cur.execute(query, data)
    #    except Exception as e:
    #        self.con.rollback()
    #        print('\n[x] Fallo al checkear rel Trade-Bot[x]')
    #        print(f'[x] (Rollback) [x]: {e}\n')            
    #    else:
    #        return self.cur.fetchone()
            
    def insert_trade(self, data):
        if(self.not_exist_trade(data[0]) is None):
            print("NO EXISTE")
            query = '''INSERT INTO trades 
            (trade_id,price,quant,time,selling) 
            VALUES (%s, %s, %s, %s, %s);'''
            try:
                self.cur.execute(query, data)
                print(data)
            except Exception as e:
                self.con.rollback()
                print('\n[x] Fallo al insertar transaccion [x]')
                print(f'[x] (Rollback) [x]: {e}\n')
                print(data)
            else:
                self.con.commit()
                print('\n[!] Se agrego una nueva transaccion [!]')

    #def insert_several_trade(self, data):
    #    query = '''INSERT INTO trades 
    #    (trade_id,price,quant,time,selling) 
    #    VALUES (%s, %s, %s, %s, %s);'''
    #    try:
    #        self.cur.executemany(query, data)
    #    except Exception as e:
    #        self.con.rollback()
    #        print('\n[x] Fallo al insertar transacciones [x]')
    #        print(f'[x] (Rollback) [x]: {e}\n')
    #        print(data)
    #    else:
    #        self.con.commit()
    #        print('\n[!] Se agrego una nueva transacciones [!]')

    #ef insert_bot(self, data):
    #   if(self.not_exist_bot(data[0]) is None):
    #       print("NO EXISTE BOT",data[0])
    #       query = '''INSERT INTO bots 
    #       (bot_id,ip) 
    #       VALUES (%s, %s);'''
    #       try:
    #           self.cur.execute(query, data)
    #       except Exception as e:
    #           self.con.rollback()
    #           print('\n[x] Fallo al insertar bot [x]')
    #           print(f'[x] (Rollback) [x]: {e}\n')
    #       else:
    #           self.con.commit()
    #           print('\n[!] Se agrego un nuevo bot [!]')


    def insert(self, dataTrades,dataBot):
        self.insert_trade(dataTrades)
        self.insert_bot(dataBot)
        newRel= (dataTrades[0],dataBot[0])
        if(self.not_exist_rel(newRel) is None):
            query = '''INSERT INTO trades_bots 
            (r_trade_id,r_bot_id) 
            VALUES (%s, %s);'''
            print(newRel)
            try:
                self.cur.execute(query, newRel)
            except Exception as e:
                self.con.rollback()
                print('\n[x] Fallo al insertar [x]')
                print(f'[x] (Rollback) [x]: {e}\n')
                #print(dataTrades)
            else:
                self.con.commit()
                print('\n[!] Se agrego transaccion,bot y relacion [!]')
                #print(dataTrades)

    def find_trade_by_id(self, tradeid):
        query = 'SELECT * FROM trades WHERE trade_id = %s;'
        self.cur.execute(query, (tradeid,))
        return self.cur.fetchone()

    #def find_bot_by_id(self, botid):
    #    query = 'SELECT * FROM bots WHERE bot_id = %s;'
    #    self.cur.execute(query, (botid,))
    #    return self.cur.fetchone()

    def find_trades(self, limit=5):
        query = 'SELECT * FROM trades LIMIT %s;'
        self.cur.execute(query, (limit,))
        return self.cur.fetchall()

    def get_trades(self,uxDate1,uxDate2):
        #obj = time.gmtime(0)
        #curr_time = round(time.time()*1000)
        #yesterday = curr_time - 86400000
        #print("Milliseconds since epoch:",curr_time,yesterday)
        #query ="SELECT * FROM trades LEFT JOIN trades_bots ON trades_bots.r_trade_id = trades.trade_id INNER JOIN bots ON bots.bot_id = trades_bots.r_bot_id WHERE time BETWEEN %s AND %s;"
        #query ="SELECT * FROM trades WHERE time BETWEEN FROM_UNIXTIME(%s) AND FROM_UNIXTIME(%s);"
        query ="SELECT * FROM trades WHERE time BETWEEN %s AND %s;"
        #query ="SELECT * FROM trades WHERE time BETWEEN FROM_UNIXTIME(%s) AND FROM_UNIXTIME(%s);"
        #query ="SELECT * FROM trades WHERE time BETWEEN UNIX_TIMESTAMP(%s) AND UNIX_TIMESTAMP(%s);"
        try:
            self.cur.execute(query, (uxDate1, uxDate2))
        except Exception as e:
            print(f'[x] Falló la consulta [x]: {e}\n')
        else:
            return self.cur.fetchall()

    def change_trade(self, tradeid, price_, quant_, time_,selling_):
        query = '''UPDATE trades 
        SET price= %s ,quant= %s ,time= %s ,selling = %s
        WHERE trade_id = %s;'''
        try:
            self.cur.execute(query, (price_, quant_, time_,selling_, tradeid))
        except Exception as e:
            self.con.rollback()
            print('\n[x] Fallo al modificar la transacción [x]')
            print(f'[x] (Rollback) [x]: {e}\n')
        else:
            self.con.commit()
            print('\n[!] Transaccion modificada con exito [!]')

    def remove_trade(self, tradeid):
        query = 'DELETE FROM trades WHERE trade_id = %s;'
        try:
            self.cur.execute(query, (tradeid,))
        except Exception as e:
            self.con.rollback()
            print('\n[x] Fallo al quitar transaccion [x]')
            print(f'[x] (Rollback) [x]: {e}\n')
        else:
            self.con.commit()
            print('\n[!] Se quito la transaccion [!]')


    def insert_table(self, filename):    
        print("INSERTAR daily update")
        #query = '''INSERT INTO trades 
        #(trade_id,price,quant,time,selling) 
        #VALUES (%s, %s, %s, %s, %s);'''
        #query='''LOAD DATA LOCAL INFILE \
        #'/home/fernando/Documentos/Universidad/Cursos\ varios/SQL/Prueba_BULK_LOAD/prueba2.csv'  \
        #INTO TABLE TRADER.trades  \
        #FIELDS TERMINATED BY ',' \
        #LINES TERMINATED BY '\n';'''
        query='''LOAD DATA LOCAL INFILE \
        %s  \
        INTO TABLE TRADER.trades  \
        FIELDS TERMINATED BY ',' \
        LINES TERMINATED BY '\n';'''
        try:
            #self.cur.execute(query, data)
            self.cur.execute(query,(filename,))
        except Exception as e:
            self.con.rollback()
            print('\n[x] Fallo al insertar daily update [x]')
            print(f'[x] (Rollback) [x]: {e}\n')
        else:
            self.con.commit()
            print('\n[!] Se agrego una nueva daily update [!]')