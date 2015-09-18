'''

'''
import unittest
import zmq
import struct
import csv
from eventloop import EventLoop


class Test(unittest.TestCase):


    def setUp(self):
        self.ctx = zmq.Context.instance()
        self.control = self.ctx.socket(zmq.REQ)
        self.stream = self.ctx.socket(zmq.SUB)
        self.eventloop = EventLoop(self.ctx,
                                   "inproc://eventloop-control",
                                   "inproc://eventloop-stream",
                                   "MOEX")
        
        self.control.connect("inproc://eventloop-control")
        self.stream.connect("inproc://eventloop-stream")
        self.stream.set_string(zmq.SUBSCRIBE, "")
        
        self.eventloop.start()

    def tearDown(self):
        self.eventloop.stop()
        self.ctx.destroy()
        
    def doStreamPing(self):
        self.control.send_json({"command" : "stream-ping"})
        response = self.control.recv_json()
        ping = self.stream.recv_multipart()
        self.assertEqual("success", response["result"])
        self.assertEqual(b'\x03\x00\x00\x00\x02\x00\x00\x00', ping[1])

    def testStreamPing(self):
        self.control.send_json({"command" : "stream-ping"})
        response = self.control.recv_json()
        
        ping = self.stream.recv_multipart()
        
        self.assertEqual("success", response["result"])
        self.assertEqual("MOEX".encode('utf_8'), ping[0])
        self.assertEqual(b'\x03\x00\x00\x00\x02\x00\x00\x00', ping[1])

    def testShutdown(self):
        self.control.send_json({"command" : "shutdown"})
        response = self.control.recv_json()
         
        self.assertEqual("success", response["result"])
        
    def testBarFeed(self):
        self.doStreamPing()
        self.control.send_json({ "command" : "start",
                                "src" : ["data/GAZP_010101_151231.txt"]})

        with open("data/GAZP_010101_151231.txt") as csvfile:
            r = csv.DictReader(csvfile)
            r.__next__() # Skip header
        
            while True:
                packet = self.stream.recv_multipart()
                self.assertEqual("MOEX:GAZP", packet[0])
                data = packet[1]
                (packet_type, timestamp, useconds, open, high, low, close, volume) = struct.unpack("<IQIddddI", data)
                self.assertEqual(0x02, packet_type)
            


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()