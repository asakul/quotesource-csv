'''

'''
import unittest
import csvloader
import datetime


class Test(unittest.TestCase):


    def setUp(self):
        self.loader = csvloader.CsvQuoteLoader()


    def tearDown(self):
        pass


    def testLoadsDailyData(self):
        quotes = self.loader.load("test/data/GAZP_010101_151231.txt")
        self.assertEqual(2295, quotes.total_candles())
        
        (first_candle_time, first_candle) = quotes.get_candle(0)
        
        self.assertAlmostEqual(first_candle.open_price, 239.0000000)        
        self.assertAlmostEqual(first_candle.max_price, 239.0000000)
        self.assertAlmostEqual(first_candle.min_price, 218.4900000)
        self.assertAlmostEqual(first_candle.close_price, 218.8900000)
        self.assertEqual(5078252, first_candle.volume)
        
        self.assertEqual(datetime.datetime(2006, 1, 23), first_candle_time)
        
    def testLoadsTickData(self):
        quotes = self.loader.load("test/data/GAZP_ticks.txt")
        self.assertEqual(8186, quotes.total_candles())
        
        (first_tick_time, first_tick) = quotes.get_candle(0)
        
        self.assertAlmostEqual(138.45, first_tick.price)
        self.assertEqual(10, first_tick.volume)
        
        self.assertEqual(datetime.datetime(2015, 4, 1, 9, 59, 59), first_tick_time)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()