from time import sleep
from PyQt5.QtCore import  QThread

class WebSocketHandler: #(OCStreamer):
    feed_opened = False  
    #def __init__(self):
    #    super().__init__()
    def __init__(self,subscribed_list,feedJson, orderJson,subscriber, api): #tokenlist,
        super(WebSocketHandler, self).__init__()
        #self.tokenlist = tokenlist
        self.api = api
        self.subscribedlist = subscribed_list
        self.feedJson = feedJson
        self.orderJson = orderJson
        self.subscriber = subscriber

    def event_handler_feed_update(self, message):
        if 'tk' in message:
            self.feedJson.write(message['tk'], message)

    def event_handler_order_update(self, inmessage):
        if 'norenordno' in inmessage and 'status' in inmessage:
            self.orderJson.write(inmessage['norenordno'], {'status': str(inmessage['status'])})

    def open_callback(self):
        print("Socket Opened")
        self.feed_opened = True
        #self.api.subscribe(self.tokenlist.get())
        sub = self.subscribedlist.get()
        #print(f"Sub List :: {sub}")
        self.subscriber.update_newsublist(sub, force_subscribe=True)

    def setup_websocket(self):
        self.api.start_websocket(
            order_update_callback=self.event_handler_order_update,
            subscribe_callback=self.event_handler_feed_update,
            socket_open_callback=self.open_callback
        )
        sleep(.5)
        while not self.feed_opened:
            pass
    def start(self):
        self.setup_websocket()


class WSSubscriber(QThread):
    def __init__(self,subscribed_list, api): #tokenlist,
        super(WSSubscriber, self).__init__()
        #self.tokenlist = tokenlist
        self.api = api
        self.subscribedlist = subscribed_list
        #self.newsublist = []

    def update_newsublist(self, new_list, force_subscribe= False):
        #print("Getting new tokens")
        if not force_subscribe:
            new_tokens = self.find_new_items(new_list)
        else:
            new_tokens = new_list

        if new_tokens:
            self.subscribe(new_tokens, force_subscribe=force_subscribe)
            
    def find_new_items(self, tokens):
        subscribed_set = set(self.subscribedlist.get())
        new_items = [item for item in tokens if item not in subscribed_set]
        return new_items

    def subscribe(self, tokens, force_subscribe):
        for i in range(0, len(tokens), 30):
            tokens_batch = tokens[i:i+30]
            self.api.subscribe(instrument=tokens_batch)
            if not force_subscribe:
                self.subscribedlist.append(tokens_batch)

