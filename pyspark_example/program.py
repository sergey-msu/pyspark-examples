import time as t
import datetime as dt
#from custom_dag import run
#from rdd import run
from data_frame import run


def main():

    start = t.time()
    print('STARTED ', dt.datetime.now())
    run()
    end = t.time()
    print('DONE in {}s ({}m)'.format(round(end-start, 2), round((end-start)/60, 2)))

    return


if __name__=='__main__':
    main()