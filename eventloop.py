'''
'''

import zmq
import threading
import struct
import random
import csvloader
import datetime
import re
import utils
import quotes as li

class EventloopError(Exception):
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return repr(self.value)

class QuoteStream():
    def __init__(self, filenames, from_time, to_time):
        self.quotes = []
        loader = csvloader.CsvQuoteLoader()
        for file in filenames:
            try:
                self.quotes.append(loader.load(file))
            except FileNotFoundError:
                raise EventloopError("File not found: " + file)
        
        self.indices = [0] * len(filenames)
        self.from_time = from_time
        self.to_time = to_time
            
    def next(self):
        while True:
            item = self._get_next_item()
            if not item:
                return item
            
            (_, _, candle_or_tick, _) = item
            if not candle_or_tick:
                return item
            
            (time, _) = candle_or_tick
            if self.from_time and time < self.from_time:
                continue
            if self.to_time and time > self.to_time:
                continue
            
            return item               
        
    def _get_next_item(self):
        for i in range(0, len(self.indices)):
            if self.indices[i] >= self.quotes[i].total_candles():
                ticker = self.quotes[i].code
                tick_mode = self.quotes[i].tick_mode
                del self.quotes[i]
                del self.indices[i]
                return (tick_mode, ticker, None, 0)
        
        if len(self.quotes) == 0:
            return None
        
        min_dt = self.quotes[0].get_candle(self.indices[0])[0]
        min_index = 0
        for i in range(0, len(self.quotes)):
            this_dt = self.quotes[i].get_candle(self.indices[i])[0]
            if min_dt > this_dt:
                min_dt = this_dt
                min_index = i
        
        i = self.indices[min_index]        
        self.indices[min_index] += 1
        
        return (self.quotes[min_index].tick_mode, self.quotes[min_index].code,
                self.quotes[min_index].get_candle(i), li.interval_info(self.quotes[min_index].interval).delta.total_seconds())
                

class EventLoop():
    '''
    Main event loop
    '''


    def __init__(self, zeromq_context, control_endpoint, stream_endpoint, exchange_id):
        '''
        Constructor
        '''
        
        self.ctx = zeromq_context
        self.control_endpoint_name = control_endpoint
        self.stream_endpoint_name = stream_endpoint
        self.exchange_id = exchange_id
        self.quote_stream = None
        self.stream_delay = 0
        
        self.run = False
        
    def start(self):
        self.thread = threading.Thread(target=self.eventLoop)
        self.thread.start()
        
    def stop(self, timeout=1000):
        if not self.run:
            return
        self.run = False
        self.signal = self.ctx.socket(zmq.REQ)
        self.signal.connect(self.control_endpoint_name)
        self.signal.send_json({"command" : "shutdown"})
        self.thread.join(timeout)
    
    def eventLoop(self):
        self.control = self.ctx.socket(zmq.REP)
        self.control.bind(self.control_endpoint_name)
        
        self.stream = self.ctx.socket(zmq.PUB)
        self.stream.bind(self.stream_endpoint_name)
        
        self.poller = zmq.Poller()
        self.poller.register(self.control)
        self.run = True
        while self.run:
            events = dict(self.poller.poll(self.stream_delay))
            if self.control in events:
                command = self.control.recv_json()
                self.handleCommand(command)
            if self.quote_stream:
                self.processStream()
                
        self.control.close()
        self.stream.close()
        
    def processStream(self):
        next_item = self.quote_stream.next()
        if not next_item:
            self.stopStream()
            return
        (tick_mode, ticker, item, period) = next_item
        
        if not item:
            self.stream.send_multipart([(self.exchange_id + ":" + ticker).encode('utf-8'), self._endOfStreamPacket()])
        else:
            if tick_mode:
                self.stream.send(self._serializeTick(item))
            else:
                self.stream.send_multipart([(self.exchange_id + ":" + ticker).encode('utf-8'), self._serializeCandle(item, period)])
        
    def startStream(self, src, from_time, to_time, delay):
        if to_time and from_time and from_time >= to_time:
            raise EventloopError("'from' should be earlier than 'to'")
        self.quote_stream = QuoteStream(src, from_time, to_time)
        self.stream_delay = delay
        if delay == 0:
            self.poller.register(self.stream)
            
    def stopStream(self):
        if self.stream_delay == 0:
            self.poller.unregister(self.stream)
        self.quote_stream = None
                
    def handleCommand(self, command):
        try:
            if command["command"] == "shutdown":
                self.handleShutdown()
            elif command["command"] == "stream-ping":
                self.handleStreamPing()
            elif command["command"] == "start":
                self.handleStart(command)
        except KeyError:
            pass
        
    def handleShutdown(self):
        self.control.send_json({"result" : "success"})
        self.run = False
        
    def handleStreamPing(self):
        self.control.send_json({"result" : "success"})
        self.stream.send_multipart([self.exchange_id.encode('utf-8'),
                                    struct.pack("<ii", 0x03, 0x02)])
    
        
    def handleStart(self, command):
        from_dt = None
        if "from" in command:
            try:
                from_dt = datetime.datetime.strptime(command["from"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                from_dt = datetime.datetime.strptime(command["from"], "%Y-%m-%d")
            
        to_dt = None
        if "to" in command:
            try:
                to_dt = datetime.datetime.strptime(command["to"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                to_dt = datetime.datetime.strptime(command["to"], "%Y-%m-%d")
            
        delay = 0
        try:
            str_delay = command["delay"]
            m = re.match("(\\d+)(\\w+)", str_delay)
            if m:
                if m.group(2) == "ms":
                    delay = int(m.group(1))
                elif m.group(2) == "s":
                    delay = int(m.group(1)) * 1000
        except KeyError:
            pass # Swallow
        
        try:
            self.startStream(command["src"], from_dt, to_dt, delay)
            self.control.send_json({"result" : "success"})
        except EventloopError as e:
            self.control.send_json({"result" : "error", 
                                    "reason" : str(e)})
        
    def _serializeTick(self, tick):
        pass
    
    def _serializeCandle(self, candle, period):
        timestamp = int((candle[0] - datetime.datetime(1970, 1, 1)).total_seconds())
        (p_open, p_open_frac) = utils.float_to_fixed(candle[1].open_price)
        (p_high, p_high_frac) = utils.float_to_fixed(candle[1].max_price)
        (p_low, p_low_frac) = utils.float_to_fixed(candle[1].min_price)
        (p_close, p_close_frac) = utils.float_to_fixed(candle[1].close_price)
        return struct.pack("<IQIIqiqiqiqiiI", 0x02, timestamp, 0, 0x01, p_open, p_open_frac, p_high, p_high_frac, p_low, p_low_frac, p_close, p_close_frac,
                    int(candle[1].volume), int(period))
        
    def _endOfStreamPacket(self):
        return struct.pack("<II", 0x03, 0x01)
