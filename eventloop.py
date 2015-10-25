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
import json

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
        self.credit = 0
            
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


    def __init__(self, zeromq_context, control_endpoint, exchange_id):
        '''
        Constructor
        '''
        
        self.ctx = zeromq_context
        self.control_endpoint_name = control_endpoint
        self.exchange_id = exchange_id
        self.streams = {}
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
        self.signal.send_multipart([b'\x01', json.dumps({"command" : "shutdown"}).encode('utf-8')])
        self.thread.join(timeout)
        
    def wait(self):
        self.thread.join()
    
    def eventLoop(self):
        self.control = self.ctx.socket(zmq.ROUTER)
        self.control.bind(self.control_endpoint_name)
        
        self.poller = zmq.Poller()
        self.poller.register(self.control, zmq.POLLIN)
        self.run = True
        while self.run:
            events = dict(self.poller.poll(100))
            if self.control in events:
                if events[self.control] & zmq.POLLIN:
                    in_packet = self.control.recv_multipart()
                    peer_id = in_packet[0]
                    self.handlePacket(peer_id, in_packet[2:])
            self.processStreams()
                    
                
        self.control.close()
        
    def handlePacket(self, peer_id, in_packet):
        if in_packet[0] == b'\x01':
            self.processCommand(peer_id, json.loads(in_packet[1].decode('utf-8')))
        elif in_packet[0] == b'\x03':
            self.incrementStreamCredit(peer_id)
            
    def incrementStreamCredit(self, peer_id):
        if not peer_id in self.streams:
            print('Error: requested stream credit increment for non-started stream')
            return
        
        self.streams[peer_id].credit += 1
        
    def processStreams(self):
        for k, v in self.streams.items():
            if v.credit > 0:
                next_item = v.next()
                if not next_item:
                    self.stopStream(k)
                    return
                (tick_mode, ticker, item, period) = next_item
                
                if not item:
                    self.control.send_multipart([k, b'', b'\x02', (self.exchange_id + ":" + ticker).encode('utf-8'), self._endOfStreamPacket()])
                else:
                    if tick_mode:
                        self.control.send_multipart([k, b'', b'\x02', (self.exchange_id + ":" + ticker).encode('utf-8'), self._serializeTick(item)])
                    else:
                        self.control.send_multipart([k, b'', b'\x02', (self.exchange_id + ":" + ticker).encode('utf-8'), self._serializeCandle(item, period)])
                v.credit -= 1
        
    def startStream(self, peer_id, src, from_time, to_time, delay):
        if to_time and from_time and from_time >= to_time:
            raise EventloopError("'from' should be earlier than 'to'")
        self.streams[peer_id] = QuoteStream(src, from_time, to_time)
            
    def stopStream(self, peer_id):
        del self.streams[peer_id]
                
    def processCommand(self, peer_id, command):
        try:
            if command["command"] == "shutdown":
                self.handleShutdown(peer_id)
            elif command["command"] == "start":
                self.handleStart(peer_id, command)
        except KeyError:
            pass
        
    def handleShutdown(self, peer_id):
        self.control.send_multipart([peer_id, b'', b'\x01', json.dumps({"result" : "success"}).encode('utf-8')])
        self.run = False
    
        
    def handleStart(self, peer_id, command):
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
            self.startStream(peer_id, command["src"], from_dt, to_dt, delay)
            self.control.send_multipart([peer_id, b'', b'\x01', json.dumps({"result" : "success"}).encode('utf-8')])
        except EventloopError as e:
            self.control.send_multipart([peer_id, b'', b'\x01', json.dumps({"result" : "error", 
                                    "reason" : str(e)}).encode('utf-8')])
        
    def _serializeTick(self, tick):
        timestamp = int((tick[0] - datetime.datetime(1970, 1, 1)).total_seconds())
        (price_int, price_frac) = utils.float_to_fixed(tick[1].price)
        return struct.pack("<IQIIqii", 0x01, timestamp, 0, 0x01, price_int, price_frac, int(tick[1].volume))
    
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
