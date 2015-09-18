'''
'''

import zmq
import threading
import struct

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
        self.control = self.ctx.socket(zmq.REP)
        self.stream = self.ctx.socket(zmq.PUB)
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
        self.control.bind(self.control_endpoint_name)
        self.stream.bind(self.stream_endpoint_name)
        self.poller = zmq.Poller()
        self.poller.register(self.control)
        self.run = True
        while self.run:
            events = dict(self.poller.poll(100))
            if self.control in events:
                command = self.control.recv_json()
                self.handleCommand(command)
                
        self.control.close()
        self.stream.close()
        
                
    def handleCommand(self, command):
        try:
            if command["command"] == "shutdown":
                self.handleShutdown()
            elif command["command"] == "stream-ping":
                self.handleStreamPing()
        except KeyError:
            pass
        
    def handleShutdown(self):
        self.control.send_json({"result" : "success"})
        self.run = False
        
    def handleStreamPing(self):
        self.control.send_json({"result" : "success"})
        self.stream.send_multipart([self.exchange_id.encode('utf-8'),
                                    struct.pack("<ii", 0x03, 0x02)])
