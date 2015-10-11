'''

'''
import unittest
import zmq
import struct
import csv
from eventloop import EventLoop
import datetime
import json


class Test(unittest.TestCase):


    def setUp(self):
        self.ctx = zmq.Context.instance()
        self.control = self.ctx.socket(zmq.DEALER)
        self.eventloop = EventLoop(self.ctx,
                                   "inproc://eventloop-control",
                                   "MOEX")
        
        self.control.connect("inproc://eventloop-control")
        
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
        
    def sendControlCommand(self, json_object):
        self.control.send_multipart([b'', b'\x01', json.dumps(json_object).encode('utf-8')])
        
    def recvControlResponse(self):
        response = self.control.recv_multipart()
        self.assertEqual(b'', response[0])
        self.assertEqual(b'\x01', response[1])
        return json.loads(response[2].decode('utf-8'))
    
    def sendStreamCredit(self):
        self.control.send_multipart([b'', b'\x03'])
        
    def recvStreamPacket(self):
        packet = self.control.recv_multipart()
        self.assertEqual(b'', packet[0])
        self.assertEqual(0x02, packet[1][0])
        
        return packet[2:]
        
    def doBarsCheck(self, filename, min_date, max_date, expected_period):
        
        self.sendStreamCredit()
        
        with open(filename) as csvfile:
            r = csv.DictReader(csvfile)
        
            for row in r:
                this_time = datetime.datetime.strptime(row["<DATE>"] + "-" + row["<TIME>"], "%Y%m%d-%H%M%S")
                if min_date and this_time < min_date:
                    continue
                if max_date and this_time >= max_date:
                    continue
                
                dt = int((this_time - datetime.datetime(1970, 1, 1)).total_seconds())
                
                self.sendStreamCredit()
                
                packet = self.recvStreamPacket()
                self.assertEqual("MOEX:GAZP", packet[0].decode('utf-8'))
                data = packet[1]
                (packet_type, timestamp, useconds, datatype, p_open, p_open_frac,
                 p_high, p_high_frac, p_low, p_low_frac, p_close, p_close_frac, volume, summary_period_sec) = struct.unpack("<IQIIqiqiqiqiiI", data)
                self.assertEqual(0x02, packet_type)
                self.assertEqual(expected_period, summary_period_sec)
                self.assertEqual(0x01, datatype)
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
            
            packet = self.recvStreamPacket()
            self.assertEqual("MOEX:GAZP", packet[0].decode('utf-8'))
            self.assertEqual(struct.pack("<II", 0x03, 0x01), packet[1])
            
    def doTicksCheck(self, filename, min_date, max_date):
        
        self.sendStreamCredit()
        
        with open(filename) as csvfile:
            r = csv.DictReader(csvfile)
        
            for row in r:
                this_time = datetime.datetime.strptime(row["<DATE>"] + "-" + row["<TIME>"], "%Y%m%d-%H%M%S")
                if min_date and this_time < min_date:
                    continue
                if max_date and this_time >= max_date:
                    continue
                
                dt = int((this_time - datetime.datetime(1970, 1, 1)).total_seconds())
                
                self.sendStreamCredit()
                
                packet = self.recvStreamPacket()
                self.assertEqual("MOEX:GAZP", packet[0].decode('utf-8'))
                data = packet[1]
                (packet_type, timestamp, useconds, datatype, price_int, price_frac, volume) = struct.unpack("<IQIIqii", data)
                self.assertEqual(0x01, packet_type)
                self.assertEqual(0x01, datatype)
                true_price = float(row["<LAST>"])
                true_vol = int(row["<VOL>"])
                self.assertEqual(dt, timestamp)
                self.assertEqual(0, useconds)
                self.assertAlmostEqual(true_price, price_int + price_frac / 1000000000)
                self.assertEqual(true_vol, volume)
            
            packet = self.recvStreamPacket()
            self.assertEqual("MOEX:GAZP", packet[0].decode('utf-8'))
            self.assertEqual(struct.pack("<II", 0x03, 0x01), packet[1])

    def testShutdown(self):
        self.sendControlCommand({"command" : "shutdown"})
        response = self.recvControlResponse()
        
        self.assertEqual("success", response["result"])
        
    def testBarFeed(self):
        self.sendControlCommand({ "command" : "start",
                                "src" : ["test/data/GAZP_010101_151231.txt"]})
        
        response = self.recvControlResponse()
        self.assertEqual("success", response["result"])

        self.doBarsCheck("test/data/GAZP_010101_151231.txt", None, None, 86400)
            
    def testBarFeed_timeBoundaries(self):
        self.sendControlCommand({ "command" : "start",
                                "src" : ["test/data/GAZP_010101_151231.txt"],
                                "from" : "2010-01-01",
                                "to" : "2011-01-01"})
        
        response = self.recvControlResponse()
        self.assertEqual("success", response["result"])
        
        self.doBarsCheck("test/data/GAZP_010101_151231.txt", datetime.datetime(2010, 1, 1), datetime.datetime(2011, 1, 1), 86400)
        
    def testBarFeed_timeBoundaries_incorrectBoundaries(self):
        self.sendControlCommand({ "command" : "start",
                                "src" : ["test/data/GAZP_010101_151231.txt"],
                                "from" : "2012-01-01",
                                "to" : "2011-01-01"})
        
        response = self.recvControlResponse()
        
        self.assertEqual("error", response["result"])
        
    def testBarFeed_invalidSource(self):
        self.sendControlCommand({ "command" : "start",
                                "src" : ["does-not-exist.txt"]})
        
        response = self.recvControlResponse()
        
        self.assertEqual("error", response["result"])
        
    def testTickFeed(self):
        self.sendControlCommand( {"command" : "start",
                                 "src" : ["test/data/GAZP_ticks.txt"] } )
        
        response = self.recvControlResponse()
        self.assertEqual("success", response["result"])
        
        self.doTicksCheck("test/data/GAZP_ticks.txt", None, None)
        
    def testTickFeed_timeBoundaries(self):
        self.sendControlCommand( {"command" : "start",
                                 "src" : ["test/data/GAZP_ticks.txt"],
                                 "from" : "2015-04-01 10:10:00",
                                 "to" : "2015-04-01 11:00:00" } )
        
        response = self.recvControlResponse()
        self.assertEqual("success", response["result"])
        
        self.doTicksCheck("test/data/GAZP_ticks.txt", datetime.datetime(2015, 4, 1, 10, 10), datetime.datetime(2015, 4, 1, 11, 0))
            

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()