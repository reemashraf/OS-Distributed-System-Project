# encoding: utf-8
#
#   Reading from multiple sockets
#   This version uses zmq.Poller()
#
#   Author: Jeremy Avnet (brainsik) <spork(dash)zmq(at)theory(dot)org>
#

import zmq

# Prepare our context and sockets
context = zmq.Context()

# Connect to task ventilator
receiver = context.socket(zmq.PULL)
receiver.bind("tcp://192.168.1.12:5557")

# Connect to weather server
subscriber = context.socket(zmq.SUB)
subscriber.bind("tcp://192.168.1.12:5556")
subscriber.setsockopt_string(zmq.SUBSCRIBE, "10001")

# Initialize poll set
poller = zmq.Poller()
# poller.register(receiver, zmq.POLLIN)
poller.register(subscriber, zmq.POLLIN)

# Process messages from both sockets
while True:
    socks = dict(poller.poll())

    
    # if receiver in socks:
    #     message = receiver.recv()
    #     # process task

    if subscriber in socks:
        message = subscriber.recv()
        print(message)