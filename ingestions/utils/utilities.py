import logging
import sys
import datetime
import pytz

def init_logger(app):
    logger = logging.getLogger(app)  #1
    logger.setLevel(logging.INFO)  #2
    handler = logging.StreamHandler(sys.stderr)  #3
    handler.setLevel(logging.INFO)  #4
    formatter = logging.Formatter(  
           '%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S') #5
    handler.setFormatter(formatter)  #6
    logger.addHandler(handler)  #7
    return logger

def current_time_sp():
       current_time = datetime.datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y-%m-%d %H:%M:%S")
       return current_time