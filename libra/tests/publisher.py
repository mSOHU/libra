#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: johnxu
@date: 3/15/2016 5:02 PM
"""
import time
import zmq

def main():
    """main method"""

    # Prepare our context and publisher
    context   = zmq.Context()
    publisher = context.socket(zmq.PUB)
    publisher.connect("tcp://10.13.82.165:5555")
    # publisher.bind("tcp://*:5555")

    while True:
        # Write two messages, each with an envelope and content
        publisher.send_multipart([b"A", b"We don't want to see this"])
        publisher.send_multipart([b"B", b"We would like to see this"])
        publisher.send_multipart([b"AB", b"We would like to see AB"])
        publisher.send_multipart([b"BA", b"We would like to see BA"])
        time.sleep(1)

    # We never get here but clean up anyhow
    publisher.close()
    context.term()

if __name__ == "__main__":
    main()
