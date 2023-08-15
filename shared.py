import threading

class SharedDict:
    def __init__(self):
        self.feedJson = {}
        self.lock = threading.Lock()

    def read(self, key):
        with self.lock:
            return self.feedJson.get(key, None)

    def write(self, key, value):
        with self.lock:
            if key in self.feedJson:
                self.feedJson[key].update(value)
            else:
                self.feedJson[key] = value
                    
    def get(self):
        with self.lock:
            return self.feedJson

class SharedList:
    def __init__(self):
        self.tokenlist = []
        self.lock = threading.Lock()

    def get(self):
        with self.lock:
            return self.tokenlist

    def append(self, itemList):
        with self.lock:
            self.tokenlist.extend(itemList)
    
    def remove(self, itemList):
        with self.lock:
            itemsSet = set(itemList)
            self.tokenlist = [item for item in self.tokenlist if item not in itemsSet]