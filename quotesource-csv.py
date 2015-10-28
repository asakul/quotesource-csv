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
    parser.add_argument('--naive-delta', action='store_true', help='Enables naive delta calculation (upticks are buy, downticks are sell)')
    
    args = parser.parse_args()
    
    config = {'naive-delta' : False}
    
    if args.naive_delta:
        config['naive-delta'] = True
    
    ctx = zmq.Context.instance()
    loop = EventLoop(ctx, args.control_ep, args.exchange_id, config)
    loop.start()
    loop.wait()