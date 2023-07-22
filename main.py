from transfer_market import Transfer
from database import DataCleaning, CreateTable, AddToTable

transfer_market = Transfer()
transfer_market()
_ = DataCleaning()
creator = CreateTable('TransferMarket', 'fatemeh', '')
add = AddToTable()
add()
