"""Matrix class."""

from pymongo import MongoClient
from operator import itemgetter
from config import UP_TO_DATE, AV_API_KEY
import json
from datetime import datetime
from time import sleep
from random import shuffle

import requests

client = MongoClient('mongodb://localhost:27017/')
print(client)
db = client.history_db

BARS = db.bars
INDUSTRIES = db.industries
SECTORS = db.sectors
METRICS = db.metrics
STOCKS = db.stocks


class Matrix():
    """The Matrix."""

    def checkup(self):
        """Check up all the data."""
        all_stocks = STOCKS.distinct("symbol")
        print(len(all_stocks), "stocks total")
        has_history = BARS.distinct("symbol")
        print(len(has_history), "stocks has history")
        absent = [s for s in all_stocks if s not in has_history]
        print(len(absent), "histories are absent")
        shuffle(absent)

        self.load_history(absent[:450])


        # for s in has_history:
        #     h = sorted(
        #         list(BARS.find({"symbol": s})),
        #         key=itemgetter("datetime")
        #     )
        #     first_date = h[0]["datetime"]
        #     last_date = h[-1]["datetime"]
        #     print(s, first_date, last_date)
        #     if last_date < UP_TO_DATE:
        #         print('OUTDATED')

    def load_history(self, symbols):
        """Data requests from Alpha Vantage API."""
        url_temp = (
            'https://www.alphavantage.co/query?'
            'function=TIME_SERIES_DAILY&symbol=%s'
            '&outputsize=full&apikey='
        )
        for symbol in symbols:
            print(symbol, "in 11 sec")
            sleep(11)
            url = url_temp % symbol
            print('requesting ' + symbol)
            response = requests.request('GET', url + AV_API_KEY)
            print(response.status_code)
            if response.status_code == requests.codes.ok:
                print('OK')
                res = response.text

                try:
                    hdata = json.loads(res)["Time Series (Daily)"]
                except:
                    print(res)
                    with open('failed.txt', 'a+') as f:
                        f.write(symbol+'\n')
                else:

                    src_data = [{key: hdata[key]} for key in sorted(hdata.keys())]

                    res_data = []
                    for d in src_data:
                        dt = list(d.keys())[0]

                        bar = {
                            "open": float(d[dt]['1. open']),
                            "high": float(d[dt]['2. high']),
                            "low": float(d[dt]['3. low']),
                            "close": float(d[dt]['4. close']),
                            "datetime": datetime.strptime(dt, '%Y-%m-%d'),
                            "volume": int(d[dt]['5. volume']),
                            "symbol": symbol
                        }
                        res_data.append(bar)
                    if BARS.count_documents({"symbol": symbol}) == 0:
                        BARS.insert_many(res_data)
                    else:
                        print("BARS ALREADY EXIST")


            else:
                print('REQUEST ERROR')
                return None
