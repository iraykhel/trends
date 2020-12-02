import requests
import json
import pprint
from sortedcontainers import *
import traceback
import time
from datetime import datetime
import csv

def get_kline(symbol,start):
    end = start + 9999*60
    url = "https://api-pub.bitfinex.com/v2/candles/trade:1m:t"+symbol+"/hist?start="+str(start*1000)+"&end="+str(end*1000)+"&limit=10000"
    # print(url)
    response = requests.get(url)
    try:
        if response.status_code != 200:
            print("Couldn't download "+url)
            return
        data = json.loads(response.content)
        # print(len(data))

        out = SortedDict()
        for entry in data:
            ts = entry[0]//1000
            open_price = entry[1]
            out[ts] = open_price

        #Fill in holes
        prev = None
        fill_cnt = 0
        for ts in range(start,end+60,60):
            if ts not in out:
                out[ts] = prev
                fill_cnt+= 1
            else:
                prev = out[ts]
        # print("Fills",fill_cnt)
        start_dt = datetime.utcfromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S')
        end_dt = datetime.utcfromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S')
        print("Downloaded batch",start_dt,end_dt,"Holes",fill_cnt)

        return out
    except:
        print("Exception",traceback.format_exc(), response.content)
        exit(1)






def download_history(symbol,start,end):
    history = SortedDict()
    batch = get_kline(symbol, start - 600)
    history.update(batch)
    for ts in history.copy():
        if ts < start:
            del history[ts]
    start = batch.keys()[-1]

    done = False
    while not done:
        batch = get_kline(symbol, start)
        start = batch.keys()[-1]

        # print(len(history))
        # print((batch.keys()[-1]-batch.keys()[0])//60, len(batch) )

        if start >= end:
            done = True
            for ts in batch.copy():
                if ts > end:
                    del batch[ts]

        history.update(batch)

        if len(batch) == 0:
            done = True
        time.sleep(3)
    return history


def write_history(history, filename):
    file = open(filename,'w', newline='')
    writer = csv.writer(file)

    writer.writerow(['UTC timestamp','price'])
    for ts, price in history.items():
        writer.writerow([ts,price])
    file.close()



jan_01_2014 = 1388534400
jan_01_2015 = 1420070400
jan_01_2016 = 1451606400
jan_01_2017 = 1483228800
dec_01_2020 = 1606780800
history = download_history('BTCUSD',jan_01_2014, dec_01_2020)
write_history(history,'BTC_history.csv')



