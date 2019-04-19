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
    socket.bind("tcp://*:%s" % replica_port)
    print("finished binding to replicas")
    while True:
        #json = socket.recv_json()
        #parsed_json = json.loads()
        #socket.send_string("AY 7aga") #received the json and ACK is sent

        message = socket.recv()#file and json is received
        print("I received")
        message = pickle.loads(message)
        z = message["file"]
        p = zlib.decompress(z)
        sent_file = pickle.loads(p)
        
        #p = zlib.decompress(message)
        #data = pickle.loads(p)
        directory = "./" + message["username"]
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(directory + "/"+ offset+message["filename"], 'wb') as f:  
            f.write(sent_file)
        socket.send_string("finished writting file, success")

if __name__ == "__main__":
    process_1 = Process(target= recieve_replica , args=["1",5526])
    process_2 = Process(target= recieve_replica , args=["2",5528])
    process_3 = Process(target= recieve_replica , args=["3",5530])
    process_4 = Process(target= recieve_replica , args=["4",5532])
    process_1.start()
    process_2.start()
    process_3.start()
    process_4.start()
    process_1.join()
    process_2.join()
    process_3.join()
    process_4.join()
# recieve_replica(replica_port)