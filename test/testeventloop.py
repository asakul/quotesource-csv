'''

'''
import unittest
import zmq
import struct
import csv
from eventloop import EventLoop
import datetime


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
                                "src" : ["test/data/GAZP_010101_151231.txt"]})

        with open("test/data/GAZP_010101_151231.txt") as csvfile:
            r = csv.DictReader(csvfile)
        
            for row in r:
                packet = self.stream.recv_multipart()
                self.assertEqual("MOEX:GAZP", packet[0].decode('utf-8'))
                data = packet[1]
                (packet_type, timestamp, useconds, datatype, p_open, p_open_frac,
                 p_high, p_high_frac, p_low, p_low_frac, p_close, p_close_frac, volume, summary_period_sec) = struct.unpack("<IQIIqiqiqiqiiI", data)
                self.assertEqual(0x02, packet_type)
                self.assertEqual(86400, summary_period_sec)
                self.assertEqual(0x01, datatype)
                dt = int((datetime.datetime.strptime(row["<DATE>"] + "-" + row["<TIME>"], "%Y%m%d-%H%M%S") - datetime.datetime(1970, 1, 1)).total_seconds())
                true_open = float(row["<OPEN>"])
                true_high = float(row["<HIGH>"])
                true_low = float(row["<LOW>"])
                true_close = float(row["<CLOSE>"])
                true_vol = int(row["<VOL>"])
                self.assertEqual(dt, timestamp)
                self.assertEqual(0, useconds)
                self.assertAlmostEqual(true_open, p_open + p_open_frac / 1000000000)
                self.assertAlmostEqual(true_high, p_high + p_high_frac / 1000000000)
                self.assertAlmostEqual(true_low, p_low + p_low_frac / 1000000000)
                self.assertAlmostEqual(true_close, p_close + p_close_frac / 1000000000)
                self.assertEqual(true_vol, volume)
            
            packet = self.stream.recv_multipart()
            self.assertEqual("MOEX:GAZP", packet[0].decode('utf-8'))
            self.assertEqual(struct.pack("<II", 0x03, 0x01), packet[1])
            

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()