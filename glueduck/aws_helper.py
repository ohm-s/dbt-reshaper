import boto3
import threading

_local_aws_store = threading.local()
def get_thread_safe_client(name):
    session = _get_or_set_local_store('client', lambda: boto3.Session())
    return _get_or_set_local_store(name, lambda: session.client(name))

def _get_or_set_local_store(name, value_lambda):
    if not hasattr(_local_aws_store, name):
        setattr(_local_aws_store, name, value_lambda())
    return getattr(_local_aws_store, name)

def retry_api_call(func, tries = 10):
    import time, random
    counter = 0
    while True:
        try:
            return func()
        except Exception as e:
            if counter > tries:
                raise e
            counter += 1
            timer = random.randint(23, 999)
            time.sleep(timer / 1000)