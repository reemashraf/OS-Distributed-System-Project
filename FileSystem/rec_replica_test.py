from time import sleep
import zmq
import socket
from multiprocessing import Process
import json
import os
import zlib 
import pickle
import chunk
import threading
import socket as sc

my_ip = sc.gethostbyname(sc.gethostname())
alive_port = 5600  #port where thre process that sends alive message is sent
# client_server_port = 5556  #port where thre processes that uploads and downloads
topic_alive = "alive"
master_ACK_port = 5603
process_order = "A"
replica_port = 5526
number_of_replicas = 2
NUMBER_OF_PROCESSES = 3
start_port = 5555 
MASTER_IP = "192.168.1.12"
machine_name = 'A'



def recieve_replica(offset,replica_port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://192.168.1.7:%s" % replica_port)
    print("finished binding to replicas")
    message = socket.recv()
    print(offset + " received")
    # parsed_json = json.loads()
    #socket.send_string("AY 7aga")
    p = zlib.decompress(message)
    data = pickle.loads(p)
    # directory = "./" + json
    # if not os.path.exists(directory):
    #     os.makedirs(directory)
    with open("./" + "test"+offset +".mp4", 'wb') as f:  
        f.write(data)
    socket.send_string("finished writting file, success")

if __name__ == "__main__":
    process_1 = Process(target= recieve_replica , args=["1",5526])
    process_2 = Process(target= recieve_replica , args=["2",5528])
    process_1.start()
    process_2.start()
    process_1.join()
    process_2.join()
# recieve_replica(replica_port)