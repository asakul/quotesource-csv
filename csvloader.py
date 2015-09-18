
import quotes as li
import datetime
import csv

class CsvQuoteLoader:
    def __init__(self):
        self.id = "csv"

    def load(self, source):
        col_ticker = None
        col_per = None
        col_date = None
        col_last = None
        col_open = None
        col_high = None
        col_low = None
        col_close = None
        col_volume = None

        code = None
        per_id = None

        candles = []

        tick_mode = False
        with open(source) as f:
            reader = csv.reader(f)
            header = reader.__next__()
            col_ticker = header.index("<TICKER>")
            try:
                col_per = header.index("<PER>")
            except:
                pass
            col_date = header.index("<DATE>")
            col_time = header.index("<TIME>")
            try:
                col_last = header.index("<LAST>")
                tick_mode = True
            except:
                pass
            try:
                col_open = header.index("<OPEN>")
                col_high = header.index("<HIGH>")
                col_low = header.index("<LOW>")
                col_close = header.index("<CLOSE>")
                if tick_mode:
                    return None
            except:
                pass
            col_volume = header.index("<VOL>")
            for row in reader:
                if code == None:
                    code = row[col_ticker]
                else:
                    if row[col_ticker] != code: raise Exception("Mixed tickers in file: not supported")

                if per_id == None and col_per:
                    per_id = row[col_per]
                else:
                    if col_per and row[col_per] != per_id: raise Exception("Mixed tickers in file: not supported")

                time = self.parse_time(row[col_date], row[col_time])
                volume = float(row[col_volume])
                
                if not tick_mode:
                    price_open = float(row[col_open])
                    price_high = float(row[col_high])
                    price_low = float(row[col_low])
                    price_close = float(row[col_close])
    
                    candle = li.Candle(time, price_low, price_high, price_open, price_close, volume)
                    candles.append((time, candle))
                else:
                    price = float(row[col_last])
                    tick = li.Tick(time, price, volume)
                    candles.append((time, tick))
                    
        q = li.Quotes(code, li.interval_by_short_id(per_id))
        q.tick_mode = tick_mode
        q.candles = candles
        return q

    def probe(self, source):
        ext = source[-3:].lower()
        print("CsvLoader: ext", ext)
        return ext == "csv" or ext == "txt"

    def parse_time(self, date, time):
        if len(date) != 8:
            raise Exception("Invalid date format: should be YYYYMMDD, have: " + date)
        if len(time) != 6:
            raise Exception("Invalid time format: should be HHMMDD, have: " + date)
        y = int(date[0:4])
        m = int(date[4:6])
        d = int(date[6:8])

        hour = int(time[0:2])
        minutes = int(time[2:4])
        sec = int(time[4:6])

        return datetime.datetime(y, m, d, hour, minutes, sec, tzinfo=datetime.timezone.utc)

