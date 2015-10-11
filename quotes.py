from enum import Enum
import datetime


class TickerInterval(Enum):
        week = 1
        day = 2
        hour_8 = 3
        hour_4 = 4
        hour_2 = 5
        hour_1 = 6
        min_30 = 7
        min_15 = 8
        min_5 = 9
        min_1 = 10,
        ticks = 11


class IntervalInfo:
    def __init__(self, value, delta, name, short_id):
        self.value = value
        self.delta = delta
        self.name = name
        self.short_id = short_id


intervals = {TickerInterval.week: IntervalInfo(TickerInterval.week, datetime.timedelta(days=7), "week", "W"),
        TickerInterval.day : IntervalInfo(TickerInterval.day, datetime.timedelta(days=1), "day", "D"),
        TickerInterval.hour_8 : IntervalInfo(TickerInterval.hour_8, datetime.timedelta(hours=8), "8 hours", "8H"),
        TickerInterval.hour_4 : IntervalInfo(TickerInterval.hour_4, datetime.timedelta(hours=4), "4 hours", "4H"),
        TickerInterval.hour_2 : IntervalInfo(TickerInterval.hour_2, datetime.timedelta(hours=2), "2 hours", "2H"),
        TickerInterval.hour_1 : IntervalInfo(TickerInterval.hour_1, datetime.timedelta(hours=1), "1 hour", "H"),
        TickerInterval.min_30 : IntervalInfo(TickerInterval.min_30, datetime.timedelta(minutes=30), "30 min", "30"),
        TickerInterval.min_15 : IntervalInfo(TickerInterval.min_15, datetime.timedelta(minutes=15), "15 min", "15"),
        TickerInterval.min_5 : IntervalInfo(TickerInterval.min_5, datetime.timedelta(minutes=5), "5 min", "5"),
        TickerInterval.min_1 : IntervalInfo(TickerInterval.min_1, datetime.timedelta(minutes=1), "1 min", "1"),
        TickerInterval.ticks : IntervalInfo(TickerInterval.ticks, datetime.timedelta(seconds=0), "tick", "0")}

def interval_by_short_id(short_id):
    for (k, v) in intervals.items():
        if v.short_id == short_id:
            return k

def interval_info(i_id):
    for (k, v) in intervals.items():
        if k == i_id:
            return v

class Candle:
    def __init__(self, time, min_price, max_price, open_price, close_price, volume):
        self.time = time
        self.min_price = min_price
        self.max_price = max_price
        self.open_price = open_price
        self.close_price = close_price
        self.volume = volume

class Tick:
    def __init__(self, time, price, volume):
        self.time = time
        self.price = price
        self.volume = volume

class Quotes:
    def __init__(self, code, interval):
        self.code = code
        self.candles = []
        self.interval = interval
        self.tick_mode = False

    def get_candle(self, index):
        return self.candles[index]

    def get_by_time(self, time):
        for candle in self.candles:
            if candle[0] == time:
                return candle
        return None

    def total_candles(self):
        return len(self.candles)

