import os
import ujson
__version__ = '0.1.0'


class SimpleKV:
    def __init__(self, storepath='ukv_data'):
        self.storepath = storepath
        if storepath not in os.listdir():
            try:
                os.mkdir(storepath)
            except:
                raise uKVException('mkdir failed for data store')
    
    def exist(self, key):
        if key in os.listdir(self.storepath + '/'):
            return True
        else:
            return False
        
    def set(self, key, value):
        try:
            f = open(self.storepath + '/' + key, 'w')
            f.write(ujson.dumps(value))
            f.close()
        except:
            raise uKVException('failed to write to data store')

    def get(self, key):
        if  self.exist(key) is False:
            return None
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
        if self.exist(key) is False:
            self.set(key, 1)
            return 1
        val = self.get(key)
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            val = val + 1
            self.set(key, val)
            return val
        else:
            raise uKVException('INCR error: key value not a number')
    
    def incrby(self, key, value):
        if self.exist(key) is False:
            self.set(key, value)
            return value
        val = self.get(key)
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            val = val + value
            self.set(key, val)
            return val
        else:
            raise uKVException('INCRBY error: key value not a number')

    def decr(self, key):
        if self.exist(key) is False:
            self.set(key, -1)
            return -1
        val = self.get(key)
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            val = val - 1
            self.set(key, val)
            return val
        else:
            raise uKVException('INCR error: key value not a number')
    
    def decrby(self, key, value):
        if self.exist(key) is False:
            self.set(key, -value)
            return -value
        val = self.get(key)
        if isinstance(val, (int, float, complex)) and not isinstance(val, bool):
            val = val - value
            self.set(key, val)
            return val
        else:
            raise uKVException('INCRBY error: key value not a number')

    def type(self, key):
        return type(self.get(key))

   
class uKVException(Exception):
    pass

