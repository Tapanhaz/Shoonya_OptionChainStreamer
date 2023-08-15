from PyQt5.QtCore import QThread, pyqtSignal
import threading
import time

class IndexTableDataFetcher(QThread):
    table_data_ready = pyqtSignal(object)

    def __init__(self, feedJson, index_keys, update_freq=500):
        super(IndexTableDataFetcher, self).__init__()
        self.feedJson = feedJson
        self.index_keys = index_keys
        self.update_freq = update_freq

    def run(self):
        while True:
            data = self.feedJson.get()
            table_data = self.get_table_data(data)
            self.table_data_ready.emit(table_data)
            self.msleep(self.update_freq) 
    
    def get_table_data(self, data):
        table_data = []
        for key, value in self.index_keys.items():
            index_data = data.get(value)
            if index_data:
                ltp = index_data.get('lp', '')
                close = index_data.get('c', '')
                if ltp and close:
                    try:
                        ltp_value = float(ltp)
                        close_value = float(close)
                        change = round((ltp_value - close_value), 2)
                    except ValueError:
                        change = None
                else:
                    change = None

                table_data.append([
                    index_data['ts'].upper(),
                    index_data.get('o', ''),
                    index_data.get('h', ''),
                    index_data.get('l', ''),
                    close,
                    ltp,
                    change,
                    index_data.get('toi', ''),
                ])
        return table_data

class OptionChainDataFetcher(QThread):
    table_data_ready = pyqtSignal(list)

    def __init__(self, feed_json, update_freq = 500):
        super(OptionChainDataFetcher, self).__init__()
        self.feed_json = feed_json
        self.token_dict = {}
        self.strike_prices = []
        self.update_freq = update_freq

    def run(self):
        while True:
            data = self.feed_json.get()
            table_data = self.get_table_data(data)
            if table_data:
                self.table_data_ready.emit(table_data)
            self.msleep(self.update_freq)

    def get_table_data(self, data):
        table_data = []
        if self.token_dict:
            for strike_price in self.strike_prices:
                option_list = self.token_dict[strike_price]

                ce_token_data = option_list[0]
                pe_token_data = option_list[1]

                ce_token = ce_token_data['token']
                pe_token = pe_token_data['token']

                ce_data = data.get(str(ce_token), {})
                pe_data = data.get(str(pe_token), {})

                ce_ltp = ce_data.get('lp', '')
                ce_close = ce_data.get('c', '')
                ce_open = ce_data.get('o', '')
                ce_high = ce_data.get('h', '')
                ce_low = ce_data.get('l', '')
                ce_change = None

                if ce_ltp and ce_close:
                    try:
                        ce_ltp_value = float(ce_ltp)
                        ce_close_value = float(ce_close)
                        ce_change = round((ce_ltp_value - ce_close_value),2)
                    except ValueError:
                        pass
                                    
                pe_ltp = pe_data.get('lp', '')
                pe_close = pe_data.get('c', '')
                pe_open = pe_data.get('o', '')
                pe_high = pe_data.get('h', '')
                pe_low = pe_data.get('l', '')
                pe_change = None
                

                if pe_ltp and pe_close:
                    try:
                        pe_ltp_value = float(pe_ltp)
                        pe_close_value = float(pe_close)
                        pe_change = round((pe_ltp_value - pe_close_value),2)
                    except ValueError:
                        pass
                
                table_data.append([
                    ce_low,
                    ce_high,
                    ce_open,
                    ce_data.get('v', ''),
                    ce_data.get('oi', ''),
                    ce_data.get('poi', ''),
                    ce_change,
                    ce_ltp,
                    strike_price,
                    pe_ltp,
                    pe_change,
                    pe_data.get('poi', ''),
                    pe_data.get('oi', ''),
                    pe_data.get('v', ''),
                    pe_open,
                    pe_high,
                    pe_low
                ])
            return table_data

    def process_token_dict(self, token_dict):
        self.strike_prices = sorted(token_dict.keys())
        self.token_dict = token_dict
        
    

class ChainMaker(QThread):
    chain_list_ready = pyqtSignal(object)
    tokendict_ready = pyqtSignal(object)
    atmstrike_ready = pyqtSignal(object)

    def __init__(self, api, sc, feedJson, indices, symbol, no_of_strikes, update_freq = 500):
        super(ChainMaker, self).__init__()
        self.symbol = symbol
        self.strikes_count = no_of_strikes
        self.api = api
        self.sc = sc
        self.indices = indices
        self.feedJson = feedJson
        self.sym_token = self.indices[self.symbol.lower()]
        self.strikediff = self.sc.get_strikediff(symbol=symbol)
        self.exchange = self.sc.get_exchange(symbol=self.symbol)
        #if self.exchange == None:
        #    self.exchange = self.sc.get_exchange(tradingsymbol=self.symbol)
        #print(self.exchange)
        self.expiry = self.sc.get_expiry(exch= self.exchange,symbol= symbol)
        #if self.expiry == None:
        #    self.expiry = self.sc.get_expiry(exch= self.exchange,tradingsymbol= symbol)
        self.update_freq = update_freq
        print(f"{self.symbol} :: {self.expiry}")
    
    def get_atm_strike(self, ltp):
        return round(ltp/self.strikediff)*self.strikediff
    
    def get_range(self, atm):
        range = (self.strikediff)/2
        lower_range = atm - range
        upper_range = atm + range
        return lower_range, upper_range
    
    def run(self):
        first_run = True
        while True:
            lp = self.feedJson.read(self.sym_token)
            if lp is not None:
                ltp = float(lp["lp"])
                if first_run:
                    atm_strike = self.get_atm_strike(ltp)
                    self.atmstrike_ready.emit(atm_strike)
                    lr, ur = self.get_range(atm_strike)
                    #print(f"   {self.symbol} :: ATM :: {atm_strike} LR :: {lr} UR :: {ur}")
                    strikes = self.get_strikelist(atm_strike)
                    self.get_tokens(strikes)
                    first_run = False
                elif ur < ltp or  ltp < lr:
                    #print(f"   {self.symbol} :: Range Breached :: LR :: {lr} UR :: {ur}")
                    atm_strike = self.get_atm_strike(ltp)
                    self.atmstrike_ready.emit(atm_strike)
                    lr, ur = self.get_range(atm_strike)
                    #print(f"{self.symbol} :: ATM :: {atm_strike} LR :: {lr} UR :: {ur}")
                    strikes = self.get_strikelist(atm_strike)
                    self.get_tokens(strikes)
            self.msleep(self.update_freq)
    
    def get_strikelist(self, atm_strike):
        options = ["CE", "PE"]

        option_list = []
        try:
            for opt in options:
                strike_prices = [atm_strike + (i * self.strikediff) for i in range(self.strikes_count, 0, -1)]
                strike_prices += [atm_strike]
                strike_prices += [atm_strike - (i * self.strikediff) for i in range(1, self.strikes_count + 1)]

                opt_list = [
                    {"strikeprice": strike, "optiontype": opt, "expiry": self.expiry, "symbol": self.symbol}
                    for strike in strike_prices
                ]

                option_list += opt_list
            #print(option_list)
            return option_list
        except Exception as e:
            print("Error constructing strike list :: {}".format(e))
    
    def get_tokens(self, strikes):
        tokenlist = []
        tokendict = {}

        if strikes is not None:
            for option in strikes:
                _,tkn = self.sc.search_scrip(
                                        symbol = option['symbol'],
                                        strikeprice = option['strikeprice'],
                                         optiontype = option['optiontype'],
                                          expiry = option['expiry'] 
                                          )
                token = f"NFO|{tkn}"  

                if option['strikeprice'] not in tokendict:
                    tokendict[option['strikeprice']] = []

                tokendict[option['strikeprice']].append({"optiontype": option['optiontype'], "token": tkn})
        
                tokenlist.append(token)
            self.chain_list_ready.emit(tokenlist)  
            self.tokendict_ready.emit(tokendict)
            #print(tokendict)

class WebSocketMonitor(threading.Thread):
    def __init__(self, token, feedJson, max_limit):
        super().__init__() 
        self.daemon = True
        '''feedJson is a shared dict here. max_limit is the max allowed freeze time'''
        self.feedJson = feedJson 
        self.token = token
        self.max_limit = max_limit
        self.last_ltp = None
        self.last_error_time = 0
        self.ltp = None

    def run(self):
        while True:
            tokendict = self.feedJson.read(self.token)
            if tokendict:
                self.ltp = float(tokendict['lp'])

            if self.ltp is not None:
                if self.last_ltp is None or self.last_ltp != self.ltp:
                    self.last_ltp = self.ltp
                    self.last_error_time = time.time()
                elif time.time() - self.last_error_time >= self.max_limit:
                    print("Websocket Connection Is Dead..")
                    self.last_error_time = time.time()
            else:
                if time.time() - self.last_error_time >= self.max_limit:
                    print("WARNING :: Check Websocket..")
                    self.last_error_time = time.time()
            time.sleep(2)  