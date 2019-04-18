#
#   Weather update server
#   Binds PUB socket to tcp://*:5556
#   Publishes random weather updates
#

import zmq
from random import randrange
import time

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://192.168.1.12:5556")

while True:
    zipcode = 10001
    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)

    socket.send_string("%i %i %i" % (zipcode, temperature, relhumidity))
    time.sleep(1)