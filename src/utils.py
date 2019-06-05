import glob
import os
import time

ALLOWED_EXTENSIONS = set(['txt', 'csv', 'xlsx'])

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def repeated_file(filename):
	return filename in glob.glob("*.csv")


def create_rdata(argument):
    os.system(f"Rscript createRData.R {argument}")

def timer(func):
    def timer_wr(*args, **kwargs):

        t0 = time.time()
        result = func(*args, **kwargs)
        t1 = time.time()
        with open("logs/loading_file_time.log", 'a+') as f: f.write(f"function:{func.__name__}|time:{round(t1-t0,2)}segs|args:{args}|kwargs:{kwargs}\n")

        
        return result
    
    return timer_wr