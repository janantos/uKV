import os
import ujson
__version__ = '0.0.1'


class PersistentKV:
    def __init__(self, storepath='ukv_data'):
        self.storepath = storepath
        if storepath not in os.listdir():
            try:
                os.mkdir(storepath)
            except:
                raise uKVException('mkdir failed for data store')
        
    def set(self, key, value):
        try:
            f = open(self.storepath + '/' + key, 'w')
            f.write(ujson.dumps(value))
            f.close()
        except:
            raise uKVException('failed to write to data store')

    def get(self, key):
        f = open(self.storepath + '/' + key)
        ret = f.read()
        f.close()
        return ujson.loads(ret)
    
    def dropKV(self):
        for f,_,_ in os.ilistdir(self.storepath):
            os.remove(self.storepath + '/' + f)
        os.rmdir(self.storepath)

    def keys(self):
        keys = set()
        for key,_,_ in os.ilistdir(self.storepath):
            keys.add(key)
        return list(keys)

    def incr(self, key):
        val = self.get(key)
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            val = val + 1
            self.set(key, val)
            return val
        else:
            raise uKVException('INCR error: key value not a number')
    
    def incrby(self, key, value):
        val = self.get(key)
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            val = val + value
            self.set(key, val)
            return val
        else:
            raise uKVException('INCRBY error: key value not a number')

    def decr(self, key):
        val = self.get(key)
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            val = val - 1
            self.set(key, val)
            return val
        else:
            raise uKVException('INCR error: key value not a number')
    
    def decrby(self, key, value):
        val = self.get(key)
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            val = val - value
            self.set(key, val)
            return val
        else:
            raise uKVException('INCRBY error: key value not a number')

   
class uKVException(Exception):
    pass

