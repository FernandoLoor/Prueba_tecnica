#sudo apt install mysql-server
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

    def find_trades(self, limit=5):
        query = 'SELECT * FROM trades LIMIT %s;'
        self.cur.execute(query, (limit,))
        return self.cur.fetchall()

    def get_trades(self,uxDate1,uxDate2):
        query ="SELECT * FROM trades WHERE time BETWEEN %s AND %s;"
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


    def activate_indexes(self):
        query='''ALTER TABLE TRADER.trades ENABLE KEYS;'''
        try:
            #self.cur.execute(query, data)
            self.cur.execute(query)
        except Exception as e:
            self.con.rollback()
            print('\n[x] Fallo al reactivar índices [x]')
            print(f'[x] (Rollback) [x]: {e}\n')
        else:
            self.con.commit()
            print('\n[!] Se reactivaron exitosamente los índices [!]')


    def deactivate_indexes(self):
        query='''ALTER TABLE TRADER.trades DISABLE KEYS;'''
        try:
            #self.cur.execute(query, data)
            self.cur.execute(query)
        except Exception as e:
            self.con.rollback()
            print('\n[x] Fallo al desactivar índices [x]')
            print(f'[x] (Rollback) [x]: {e}\n')
        else:
            self.con.commit()
            print('\n[!] Se desactivaron exitosamente los índices [!]')