from Noren import NorenApi
from time import sleep

class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')


def isWithinSixDays(input_date,expiryDate):
    diff = expiryDate - input_date
    return 0 <= diff.days <= 6

def check_symbols(sc,exch_list,current_date,hard_refresh=False, retry=1):
    sc.initialize_symbols(exch_list=exch_list, hard_refresh=hard_refresh)
    cur_expiry = sc.get_expiry()
    if not isWithinSixDays(input_date=current_date, expiryDate=cur_expiry):
        if retry <= 10:
            print(f"Retry == {retry}")
            sleep(3)
            check_symbols(exch_list=exch_list,hard_refresh=True, retry= retry+1)
        else:
            print("Error Fetching Correct Symbolmaster.")
            return False
    else:
        print("Symbolmaster Initialized.")
        return True
