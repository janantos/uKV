import os
__version__ = '0.0.1'


class PersistentKV:
    def __init__(self, storepath='./ukv_data'):
        self.storepath = storepath
        if storepath not in os.listdir():
            try:
                os.mkdir(storepath)
            except:
                raise uKVDataStoreException('mkdir failed')
        
    def set(self, key, value):
        f = open(self.storepath + '/' + key, 'w')
        f.write(value)
        f.close()

    def get(self, key):
        f = open(self.storepath + '/' + key)
        ret = f.read()
        f.close()
        return ret
    
    def dropKV(self):
        os.chdir(self.storepath)
        for f,_,_ in os.ilistdir():
            os.remove(f)
        os.chdir("..")
        os.rmdir(self.storepath)

class uKVDataStoreException(Exception):
    pass

