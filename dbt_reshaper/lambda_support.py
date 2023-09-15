import multiprocessing.context
import os

### This is a hack to get dbt to work in aws lambda

os.environ['HOME'] = '/tmp'
def Lock(ctx = None):
    import threading
    return threading.Lock()
def RLock(ctx = None):
    import threading
    return threading.RLock()

multiprocessing.synchronize.Lock = Lock
multiprocessing.synchronize.RLock = RLock