import logging

# create an instance of the logger
logger = logging.getLogger(__name__)

# logging set up 
log_format = logging.Formatter('%(asctime)-15s %(levelname)-2s %(message)s')
sh = logging.StreamHandler()
sh.setFormatter(log_format)

# add the handler
logger.addHandler(sh)
logger.setLevel(logging.INFO)


def do_something():
    return None

def call_do_something():
  # This will obviously throw and exception
    return do_something() + 4

print("*"*100)
print("ERORR 1")
print("*"*100)
# logging exception with logger.error()
try:
    call_do_something()
except Exception as e: 
    logger.error(e)

print("%"*100)
print("ERROR with args")
print("%"*100)
# logging exception with logger.error() with full traceback
try:
    call_do_something()
except Exception as e: 
    logger.error(e, stack_info=True, exc_info=True)

print("X"*100)
print("EXCEPTION")
print("X"*100)
# logging exception with logger.exception() with full traceback
try:
    call_do_something()
except Exception as e: 
    logger.exception(e)
    raise e