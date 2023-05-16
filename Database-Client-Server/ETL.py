from ConnectDB import *

if __name__ == "__main__":
    database = ConnectDB()
    print(database.get_bots_daily())