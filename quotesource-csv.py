#!/usr/bin/env python
'''

'''

import argparse
import zmq
from eventloop import EventLoop

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Quotesource that streams quotes from csv-files')
    parser.add_argument('--exchange-id', help='Specifies exchange ID for all streams', required=True)
    parser.add_argument('--control-ep', help='Specifies control endpoint', required=True)
    
    args = parser.parse_args()
    
    ctx = zmq.Context.instance()
    loop = EventLoop(ctx, args.control_ep, args.exchange_id)
    loop.start()
    loop.wait()