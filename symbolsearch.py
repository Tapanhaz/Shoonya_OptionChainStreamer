import logging
import os
from typing import List, Union, TypedDict
from io import BytesIO
import json
import pandas as pd
import requests
import numpy as np
from typing_extensions import Literal, NewType
#from functools import lru_cache
from datetime import datetime, date
import http.client
from urllib.parse import urlparse
import urllib.request
import httpx

logger = logging.getLogger(__name__)
#logging.basicConfig(level=logging.DEBUG)


DateFormat = NewType(name="%d-%m-%Y", tp=str)
DateFormat_2 = NewType(name="%d-%b-%Y", tp=str)
DateFormat_3 = NewType(name="%d%b%y", tp=str)

class ScripParams(TypedDict):
    symbol: str
    instrument: str
    optiontype: str
    expiry: Union[DateFormat, DateFormat_2, datetime, date]
    strikeprice: Union[int, float]
    tradingsymbol: str

class SymbolParams(TypedDict):
    symbol: str
    instrument: str
    optiontype: str
    expiry: Union[DateFormat, DateFormat_2, datetime, date]
    strikeprice: Union[int, float]

class LotParams(TypedDict):
    symbol: str
    expiry: Union[DateFormat, DateFormat_2, datetime, date]
    tradingsymbol: str

class SearchScrip:   
    def __init__(self):
        self.symbol_cache = {}
        self.l_path = os.path.dirname(__file__)
        self.config_file = os.path.join(self.l_path, 'search_config.json')
        self.current_date = datetime.now().date()
        self.current_date_str = self.current_date.strftime('%d-%m-%Y')
        self.config_data = {}

    def initialize_symbols(self, exch_list: list, hard_refresh: bool=False):
        self.exch_list = exch_list
        if not hard_refresh:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as file:
                    self.config_data = json.load(file)
                for exch in self.exch_list:
                    if exch in self.config_data and self.config_data[exch] != self.current_date_str:
                        self.get_symbols(exch=exch, redownload=True)
                    elif exch not in self.config_data:
                        self.get_symbols(exch=exch, redownload=True)
                    else:
                        self.get_symbols(exch=exch)
                self.save_config()
            else:
                for exch in self.exch_list:
                    self.get_symbols(exch=exch, redownload=True)
                self.save_config()
        else:
            for exch in self.exch_list:
                self.get_symbols(exch=exch, redownload=True)
            self.save_config()
                
    def save_config(self):
        with open(self.config_file, 'w') as file:
            json.dump(self.config_data, file, indent=4)

    def get_symbols(self, 
                    exch: Literal["NSE", "NFO", "MCX", "BSE", "CDS"], 
                    redownload: bool = False) -> pd.DataFrame:
        if not redownload:
            if exch in self.symbol_cache:
                if self.symbol_cache[exch] is not None:
                    return pd.DataFrame(self.symbol_cache[exch])
            else:
                if os.path.exists(os.path.join(self.l_path, f"{exch}_symbols.csv")):
                    df = pd.read_csv(f"{exch}_symbols.csv", index_col=None)
                    self.symbol_cache[exch] = df
                    return df
        headers = {
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
        }
        inst_urls = [
            f"https://api.shoonya.com/{exch}_symbols.txt.zip",
            f"https://shoonya.finvasia.com/{exch}_symbols.txt.zip"
        ]
        for url in inst_urls:
            try:
                #cookies = requests.cookies.RequestsCookieJar()
                #headers = {
                #    "Cache-Control": "no-cache",
                #    "Pragma": "no-cache"
                #    }
                #res = requests.get(url, headers=headers, cookies=cookies,allow_redirects=True)
               
                with httpx.Client(http2=True,headers=headers) as client:
                    res = client.get(url)
                    res.raise_for_status() 
                    df = pd.read_csv(BytesIO(res.content), compression="zip")
                    self.symbol_cache[exch] = df
                    df.to_csv(f"{exch}_symbols.csv", index=None)
                    self.config_data[exch] = self.current_date_str
                    return df
            except (httpx.RequestError, Exception) as e:
            #except (requests.exceptions.RequestException, Exception) as e:
                logger.debug(e)
                continue
        return pd.DataFrame()

    #@lru_cache(maxsize=None)
    def get_expiry(self,
                   exch: Literal['NFO','CDS','MCX']='NFO',
                   instrument: Literal['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK', 'FUTCOM', 'OPTFUT', 'OPTCUR', 'FUTCUR']= 'OPTIDX',
                    symbol: str=None,
                    expiry: Literal['current', 'next', 'far', 'recent_list']= 'current'
                    ) -> Union[datetime, List[datetime]]:

        if exch == 'MCX' and instrument == 'OPTIDX':
            instrument = 'FUTCOM' #'OPTFUT'
        elif exch == 'CDS' and instrument == 'OPTIDX':
            instrument = 'OPTCUR'

        if instrument == 'OPTIDX' or instrument == 'FUTIDX':
            symbol = 'BANKNIFTY' if symbol is None else symbol
        elif instrument == 'OPTSTK' or instrument == 'FUTSTK':
            symbol = 'RELIANCE' if symbol is None else symbol

        try:
            sym_df = self.get_symbols(exch=exch)
            inst_df = (sym_df.query(f"Symbol in {str([symbol])} and Instrument in {str([instrument])}")).reset_index(drop=True)
            expiry_list = (pd.to_datetime(inst_df['Expiry'],format='mixed', dayfirst=True, errors='coerce').dt.date.unique())
            filtered_expiry_list = sorted([date for date in expiry_list if date >= self.current_date])
            

            if expiry == 'current':
                return filtered_expiry_list[0]
            elif expiry == 'next':
                return filtered_expiry_list[1]
            elif expiry == 'far':
                return filtered_expiry_list[2]
            elif expiry == 'recent_list':
                if instrument == 'OPTIDX':
                    return filtered_expiry_list[:4]
                else:
                    return filtered_expiry_list
        except Exception as e:
            logger.debug("Error Fetching Expiry :: {}".format(e))

    def format_date(self,
                    date_obj: Union[DateFormat, DateFormat_2, date, datetime], 
                    output_type: Union[DateFormat_2, DateFormat_3]= DateFormat_2
                    ) -> Union[DateFormat_2, DateFormat_3]:
        if isinstance(date_obj, str):
            if output_type == DateFormat_2:
                try:
                    dt = datetime.strptime(date_obj, "%d-%m-%Y")
                    return dt.strftime('%d-%b-%Y').upper()
                except ValueError:
                    dt = datetime.strptime(date_obj, "%d-%b-%Y")
                    return date_obj.upper()
            elif output_type == DateFormat_3:
                try:
                    dt = datetime.strptime(date_obj, "%d-%m-%Y")
                    return dt.strftime('%d%b%y').upper()
                except ValueError:
                    dt = datetime.strptime(date_obj, "%d-%b-%Y")
                    return dt.strftime('%d%b%y').upper()
        elif isinstance(date_obj, date) or isinstance(date_obj, datetime):
            if output_type == DateFormat_2:
                return date_obj.strftime('%d-%b-%Y').upper()
            elif output_type == DateFormat_3:
                return date_obj.strftime('%d%b%y').upper()
        else:
            logger.info("Invalid Date Format")

    def search_scrip(self,
                     exch: Literal['NSE','NFO','BSE','CDS','MCX']='NFO',
                     **kwargs: ScripParams) -> Union[str, tuple, dict]:
        try:
            if 'expiry' in kwargs:
                expiry = kwargs['expiry']
                exp = self.format_date(date_obj=expiry)
                logger.info(exp)

            exch_df = self.get_symbols(exch=exch)

            instrument = kwargs.get('instrument')
            trading_symbol = kwargs.get('tradingsymbol')
            if trading_symbol is None:
                if instrument is None:
                    if exch == 'NFO':
                        instrument = 'OPTIDX'
                    elif exch == 'NSE':
                        instrument = 'EQ'
                    elif exch == 'MCX':
                        instrument = 'OPTIDX'
                    elif exch == 'BSE':
                        instrument = 'A'
                    elif exch == 'CDS':
                        instrument = 'OPTCUR'

                query_parts = []
                for key, value in kwargs.items():
                    #if key not in ['expiry', 'instrument', 'optiontype', 'strikeprice', 'tradingsymbol']:
                    if key == 'symbol':
                        query_parts.append(f"{key.capitalize()} in ['{value}']")
                    elif key == 'expiry':
                        query_parts.append(f"{key.capitalize()} in ['{exp}']")

                if instrument is not None:
                    query_parts.append(f"Instrument in ['{instrument}']")

                option_type = kwargs.get('optiontype')
                if option_type is not None:
                    query_parts.append(f"OptionType in ['{option_type}']")

                strike_price = kwargs.get('strikeprice')
                if strike_price is not None:
                    query_parts.append(f"StrikePrice in [{strike_price}]")

                query = ' and '.join(query_parts)
            else:
                query = f"TradingSymbol in ['{trading_symbol}']"
                logger.info(query)
                return exch_df.query(query)['Token'].iloc[0]

            logger.info(query)
            result = exch_df.query(query)[['TradingSymbol', 'Token']]
            if len(result) == 1:
                return result.values[0]
            else:
                return dict(zip(result['TradingSymbol'], result['Token']))
        except Exception as e:
            logger.debug("Error :: {}".format(e))

    def get_tradingsymbol(self,
                          exch: Literal['NSE','NFO','BSE','CDS','MCX']='NFO',
                          **kwargs: SymbolParams) -> str:

        symbol = kwargs.get('symbol')
        instrument = kwargs.get('instrument')

        if exch == 'NSE':
            try:
                if instrument.upper() == 'INDEX':
                    sym_df = self.get_symbols(exch='NSE')
                    tsym = sym_df.query(f'Symbol in ["{symbol}"] and Instrument in ["{instrument}"]')["TradingSymbol"].iloc[0]
                    return tsym
                else:
                    return f"{symbol.upper()}-{instrument.upper()}"
            except Exception as e:
                logger.debug("Error in construction of tradingsymbol :: {}".format(e))
        elif exch == 'NFO':
            try:
                expiry = kwargs.get('expiry')
                option_type = kwargs.get('optiontype')
                strike_price = kwargs.get('strikeprice')
                if instrument == 'OPTIDX' or instrument == 'OPTSTK':
                    return f"{symbol.upper()}{self.format_date(date_obj=expiry, output_type=DateFormat_3)}{option_type.upper()[0]}{strike_price}"
                elif instrument == 'FUTIDX' or instrument == 'FUTSTK':
                    return f"{symbol.upper()}{self.format_date(date_obj=expiry, output_type=DateFormat_3)}F"
            except Exception as e:
                logger.debug("Error in construction of tradingsymbol :: {}".format(e))
        elif exch == 'MCX':
            try:
                expiry = kwargs.get('expiry')
                option_type = kwargs.get('optiontype')
                strike_price = kwargs.get('strikeprice')
                if instrument == 'OPTFUT':
                    return f"{symbol.upper()}{self.format_date(date_obj=expiry, output_type=DateFormat_3)}{option_type.upper()[0]}{strike_price}"
                elif instrument == 'FUTCOM' or instrument == 'FUTIDX':
                    return f'{symbol.upper()}{self.format_date(date_obj=expiry, output_type=DateFormat_3)}'
            except Exception as e:
                logger.debug("Error in construction of tradingsymbol :: {}".format(e))
        elif exch == 'CDS':
            try:
                expiry = kwargs.get('expiry')
                option_type = kwargs.get('optiontype')
                strike_price = kwargs.get('strikeprice')
                if instrument == 'UNDCUR':
                    return symbol.upper()
                elif instrument == 'OPTCUR':
                    return f"{symbol.upper()}{self.format_date(date_obj=expiry, output_type=DateFormat_3)}{option_type.upper()[0]}{strike_price}"
                elif instrument == 'FUTCUR':
                    return f'{symbol.upper()}{self.format_date(date_obj=expiry, output_type=DateFormat_3)}F'
            except Exception as e:
                logger.debug("Error in construction of tradingsymbol :: {}".format(e))
        elif exch == 'BSE':
            try:
                return symbol.upper()
            except Exception as e:
                logger.debug("Error in construction of tradingsymbol :: {}".format(e))

    def get_lotsize(self,
                    exch: Literal['NFO','CDS','MCX']='NFO',
                    **kwargs: LotParams) -> int:

        sym_df = self.get_symbols(exch=exch)
        symbol = kwargs.get('symbol')
        trading_symbol = kwargs.get('tradingsymbol')

        try:
            if trading_symbol is not None:
                return sym_df.query(f"TradingSymbol in ['{trading_symbol}']")['LotSize'].iloc[0]
            elif symbol is not None:
                if 'expiry' in kwargs:
                    expiry = kwargs['expiry']
                    exp = self.format_date(date_obj=expiry)
                    return sym_df.query(f"Symbol in ['{symbol}'] and Expiry in ['{exp}']")['LotSize'].iloc[0]
                else:
                    try:
                        expiry = self.get_expiry(exch=exch,symbol=symbol)
                        exp = self.format_date(date_obj=expiry)
                        return sym_df.query(f"Symbol in ['{symbol}'] and Expiry in ['{exp}']")['LotSize'].iloc[0]
                    except:
                        return sym_df.query(f"Symbol in ['{symbol}']")['LotSize'].iloc[0]
            else:
                print("Check parameters.")
        except Exception as e:
            logger.debug("Error fetching lotsize :: {}".format(e))
    
    def get_exchange(self, symbol: str= None,tradingsymbol: str=None) -> str:
        try:
            if tradingsymbol is not None:
                for exch in self.exch_list:
                    sym_df = self.get_symbols(exch=exch)
                    if tradingsymbol in sym_df['TradingSymbol'].values:
                        return exch
            elif symbol is not None:
                for exch in self.exch_list:
                    sym_df = self.get_symbols(exch=exch)
                    if symbol in sym_df['Symbol'].values:
                        return exch
            else:
                print("Provide either tradingsymbol or symbol ")

        except Exception as e:
            logger.debug("Error fetching exchange name :: {}".format(e))
    
    def get_strikediff(self,
                       exch: Literal['NFO','CDS','MCX']='NFO',
                       symbol: str= None) -> float:
        
        try:
            sym_df = self.get_symbols(exch=exch)
            
            if symbol is not None:
                strike_list = list(set(sym_df.query(f"Symbol in ['{symbol}']")['StrikePrice']))
                filtered_strikes = sorted([i for i in strike_list if i > 0])
                min_diff = min(np.diff(filtered_strikes))
                return min_diff
        except Exception as e:
            logger.debug("Error fetching strikediff :: {}".format(e))
    
    def get_tokendict(self,
                    exch: Literal['NFO','CDS','MCX']='NFO',
                    token: Union[int, float]= None,
                    tradingsymbol: str=None) :
        try:
            sym_df = self.get_symbols(exch=exch)
            
            if token is not None:
                values = sym_df.query(f"Token in ['{token}']")[['StrikePrice','OptionType']].iloc[0]
                return {str(token):{'StrikePrice': values['StrikePrice'], 'OptionType': values['OptionType']}}
            if tradingsymbol is not None:
                values = sym_df.query(f"TradingSymbol in ['{tradingsymbol}']")[['Token','StrikePrice','OptionType']].iloc[0]
                return {values['Token']: {'StrikePrice': values['StrikePrice'], 'OptionType': values['OptionType']}}
        except Exception as e:
            logger.debug("Error fetching strikediff :: {}".format(e))

        
