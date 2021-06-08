import logging as lg
import os

class Logger:
    def __init__(self, filename):
        self.filename = filename

    def logger(self, logtype, error):
        if self.filename not in os.listdir():
            with open(os.path.join(os.getcwd(),self.filename), 'a+') as f:
                print(f.read())

        lg.basicConfig(filename=os.path.join(os.getcwd(),self.filename), level=lg.INFO, format='%(name)s - %(asctime)s - %(message)s')
        if logtype == 'INFO':
            lg.info(error)
        elif logtype == 'ERROR':
            lg.info(error)
        lg.shutdown()