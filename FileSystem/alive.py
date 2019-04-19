from time import sleep
import zmq
import socket
import multiprocessing as mp
import json
#import chunck

my_ip = socket.gethostbyname(socket.gethostname())
alive_port = 5600  #port where thre process that sends alive message is sent
client_server_port = 5556  #port where thre processes that uploads and downloads
topic_alive = "alive"
master_ACK_port = 5555
process_order = "A"

def send_alive():
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://192.168.1.12:%s" % alive_port)
    while True:
        # message = [ 1 , socket.gethostbyname(socket.gethostname()) ]
        message = "%s %s"%(topic_alive , process_order)
        # socket.send_string(topic , zmq.SNDMORE)
        socket.send_string(message)
        print("finished sending alive message")
        sleep(1)#wait for one second before sending the next alive message

send_alive()