import yaml
import pyotp
import threading
from symbolsearch import SearchScrip
from PyQt5 import uic,  QtWidgets
import sys
from shared import SharedDict, SharedList
from utils import IndexTableDataFetcher, ChainMaker, OptionChainDataFetcher, WebSocketMonitor
from models import IndexTableModel, OptionChainTableModel
from socket_utils import WebSocketHandler, WSSubscriber
from others import ShoonyaApiPy, check_symbols  
from datetime import datetime

Ui_MainWindow, QtBaseClass = uic.loadUiType("MyUi.ui")

class OCStreamer(QtWidgets.QMainWindow, Ui_MainWindow):
    api = ShoonyaApiPy()
    feedJson = SharedDict() 
    orderJson = SharedDict()
    update_freq = 1000

    indices = {'nifty': '26000', 
               'banknifty': '26009', 
               'finnifty': '26037', 
               'vix': '26017',
               }
    
    def __init__(self):
        super(OCStreamer, self).__init__()
        self.setupUi(self)
        #self.setWindowFlags(QtCore.Qt.FramelessWindowHint)
        self.sc = SearchScrip()
        self.tokenlist = SharedList()
        self.subscribed_list = SharedList()

        self.init_symbolsearch()

        self.wss_monitor = WebSocketMonitor("26009", self.feedJson, 60)
        self.wss_monitor.start()

        self.subscriber = WSSubscriber(self.subscribed_list,self.api) #self.tokenlist, 
        self.wbhandler = WebSocketHandler(self.subscribed_list,self.feedJson, self.orderJson,self.subscriber, self.api)
       
        self.BtnLogin.clicked.connect(lambda: self.run_thread(thread_name="login", timeout=10))
        self.BtnLogOut.clicked.connect(lambda: self.run_thread(thread_name="logout", timeout=10))

        self.index_table_model = IndexTableModel([])
        self.IndexTable.setModel(self.index_table_model)

        self.banknifty_option_table_model = OptionChainTableModel([], parent=self.OCTable_Banknifty)
        self.OCTable_Banknifty.setModel(self.banknifty_option_table_model)

        self.nifty_option_table_model = OptionChainTableModel([], parent=self.OCTable_Nifty)
        self.OCTable_Nifty.setModel(self.nifty_option_table_model)

        self.finnifty_option_table_model = OptionChainTableModel([], parent=self.OCTable_Finnifty)
        self.OCTable_Finnifty.setModel(self.finnifty_option_table_model)

        self.update_login_label("Please Press Login to Continue")

        self.index_fetcher = IndexTableDataFetcher(self.feedJson, self.indices, self.update_freq)
        self.index_fetcher.table_model = self.index_table_model 
        self.index_fetcher.table_data_ready.connect(self.update_table_model)
        self.index_fetcher.start()  
        
        self.banknifty_chain = ChainMaker(self.api, self.sc, self.feedJson, self.indices, "BANKNIFTY", 10, self.update_freq)        
        self.banknifty_chain.start()
        self.banknifty_chain.chain_list_ready.connect(self.subscriber.update_newsublist) #banknifty_
        self.banknifty_chain.atmstrike_ready.connect(self.banknifty_option_table_model.set_highlight_value)

        self.banknifty_fetcher = OptionChainDataFetcher(self.feedJson, self.update_freq)
        self.banknifty_fetcher.table_model = self.banknifty_option_table_model
        self.banknifty_chain.tokendict_ready.connect(self.banknifty_fetcher.process_token_dict)
        self.banknifty_fetcher.table_data_ready.connect(lambda table_data: self.update_option_table_model(self.banknifty_option_table_model, table_data))
        self.banknifty_fetcher.start()

        self.nifty_chain = ChainMaker(self.api, self.sc, self.feedJson, self.indices, "NIFTY", 10, self.update_freq)        
        self.nifty_chain.start()
        self.nifty_chain.chain_list_ready.connect(self.subscriber.update_newsublist) # nifty_
        self.nifty_chain.atmstrike_ready.connect(self.nifty_option_table_model.set_highlight_value)               

        self.nifty_fetcher = OptionChainDataFetcher(self.feedJson, self.update_freq)
        self.nifty_fetcher.table_model = self.nifty_option_table_model
        self.nifty_chain.tokendict_ready.connect(self.nifty_fetcher.process_token_dict)
        self.nifty_fetcher.table_data_ready.connect(lambda table_data: self.update_option_table_model(self.nifty_option_table_model, table_data))
        self.nifty_fetcher.start()

        self.finnifty_chain = ChainMaker(self.api, self.sc, self.feedJson, self.indices, "FINNIFTY", 10, self.update_freq)        
        self.finnifty_chain.start()
        self.finnifty_chain.chain_list_ready.connect(self.subscriber.update_newsublist) # finnifty_
        self.finnifty_chain.atmstrike_ready.connect(self.finnifty_option_table_model.set_highlight_value)         

        self.finnifty_fetcher = OptionChainDataFetcher(self.feedJson, self.update_freq)
        self.finnifty_fetcher.table_model = self.finnifty_option_table_model
        self.finnifty_chain.tokendict_ready.connect(self.finnifty_fetcher.process_token_dict)
        self.finnifty_fetcher.table_data_ready.connect(lambda table_data: self.update_option_table_model(self.finnifty_option_table_model, table_data))
        self.finnifty_fetcher.start()

    def init_symbolsearch(self):
        print("Initializing Symbols...")
        exch_list = ["NSE", "NFO"]
        #self.sc.initialize_symbols(exch_list=exch_list)
        today = datetime.now().date()
        isInitialized = check_symbols(sc=self.sc, exch_list=exch_list, current_date=today)
        if not isInitialized:
            QtWidgets.QApplication.quit() 

    def update_table_model(self, table_data):
        #print(self.feedJson.get())
        self.index_table_model.update_data(table_data)
    
    def update_option_table_model(self, model, table_data):
        #print(self.feedJson.get())
        model.update_data(table_data)

    def update_login_label(self, text):
        self.login_label.setText(text)  
           
    def run_thread(self,thread_name, timeout=None):
        if thread_name == 'login':
            t = threading.Thread(target=self.ShoonyaLogin, daemon=True)
            t.start()
            t.join(timeout)
        if thread_name == 'logout':
            t = threading.Thread(target=self.ShoonyaLogout, daemon=True)
            t.start()
            t.join(timeout)

    def static_token_initialize(self):
        try:
            tokens = []
            for symbol, value in self.indices.items():
                token = f"NSE|{value}"
                tokens.append(token)
            self.tokenlist.append(tokens)
        except Exception as e:
            print("Error static_token_initialize :: {}".format(e))
    
    def subscribe(self):
        self.subscriber.update_newsublist(self.tokenlist.get())
        
    def ShoonyaLogin(self):
        self.update_login_label("Please wait.")
        with open('cred.yml') as f:
            cred = yaml.load(f, Loader=yaml.FullLoader)

        ret = self.api.login(userid=cred['user'],
                        password=cred['pwd'],
                        twoFA=pyotp.TOTP(cred['token']).now(),
                        vendor_code=cred['vc'],
                        api_secret=cred['apikey'],
                        imei=cred['imei'])

        if ret is not None:
            #if ret['stat'] == 'Ok' :
            self.update_login_label(f"Welcome {ret['uname'].title()}")
            self.static_token_initialize()
            self.wbhandler.start()
            self.subscribe()
        else:
            self.update_login_label("Error :: Please Check Credentials")    

    def ShoonyaLogout(self): 
        if hasattr(self.api, '_NorenApi__username'):
            self.api.close_websocket()
            ret = self.api.logout()
            if ret is not None:
                self.update_login_label("Logged Out") 
            else:
                self.update_login_label("Log Out Error")
        else:
            self.update_login_label("Please Login First")  

if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    window = OCStreamer()
    window.show()
    app.exec_()