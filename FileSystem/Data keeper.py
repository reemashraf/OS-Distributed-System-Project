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
master_ACK_port = 5602
process_order = "A"
replica_port = 5526
number_of_replicas = 2
NUMBER_OF_PROCESSES = 3
start_port = 5555 
MASTER_IP = "192.168.1.12"
print("Kill me please")
##if file uploaded duplicate name notify the client or pad with underscores 3ashn ahmed myz3lish

def send_alive(): #tested and works fine with the master
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    print("before connect")
    socket.connect("tcp://192.168.1.12:%s" % alive_port)
    print("alive process connected")
    while True:
        # message = [ 1 , socket.gethostbyname(socket.gethostname()) ]
        message = "%s %s"%(topic_alive , process_order)
        # socket.send_string(topic , zmq.SNDMORE)
        socket.send_string(message)
        print("finished sending alive message")
        sleep(1)#wait for one second before sending the next alive message
        
        
def download_uplaod(client_server_port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    print("I took port %s"%client_server_port)
    socket.bind("tcp://*:%s" % client_server_port)
    print("finished binding")
    while True:
        message = socket.recv_json()
        parsed_json = json.loads(message)
        print(parsed_json["mode"]) 
        print("recieved header from client")
        socket.send_string("ACK")
        print("sent ACK to client")
        if(parsed_json["mode"] == "upload"):
            message = socket.recv()
            p = zlib.decompress(message)
            data = pickle.loads(p)
            print("finished recieving")
            socket.send_string("finished writting file, success")
            directory = "./" + parsed_json["username"]
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(directory + "/"+ parsed_json["filename"], 'wb') as f:  
                f.write(data)
            ####will slice here#### ###done and tested#######
            number_of_chunks = chunk.slice_file(directory + "./"+ parsed_json["filename"] , 64*1024)
            ########change to connect
            socket.connect("tcp://%s:%s" % (MASTER_IP,master_ACK_port) )
            header_data = {
                "ip": "ID", ##ID instead don't forget to modify
                "username": parsed_json["username"],
                "filename": parsed_json["filename"],
                "numberOfchunks": number_of_chunks
                }
            header_data_sent_to_master = json.dumps(header_data)
            garbage = socket.recv() 
            socket.send_json(header_data_sent_to_master)
            replica_list_json = socket.recv_json()
            replica_list = json.loads(replica_list_json)
            #####TO_do replicate to other machines#######
            '''
            will send file path(comelete file path) from the client, and replica list, parsed json
            '''
            replicate(directory + "/"+ parsed_json["filename"] , replica_list , parsed_json)
        elif(parsed_json["mode"] == "download"):
            message = socket.recv_json()
            parsed_json = message.loads()
            chunk_number = parsed_json["chunknumber"]
            directory = "./" + parsed_json["username"] + "/" +parsed_json["filename"]
            filename = chunk.get_chunk_name_by_number(chunk_number , directory)
            file_path =  "./" + parsed_json["username"] + "/"+ filename
            with open(file_path , 'rb') as f:
                chunk = f.read(CHUNK_SIZE)
            chunk = f.read()
            p = pickle.dumps(filename.read())
            z = zlib.compress(p)
            filename.close()
            socket.send (z)
            
def replicate(file_path , replica_list , parsed_json):
    #here other nodes act as servers the one responsible for sending is the client
    context = zmq.Context()
    print ("Connecting to server (replica dataNodes)...")
    socket = context.socket(zmq.REQ)
    list_values = [ replica_address for replica_address in replica_list.values()]
    for replica_address in list_values:
        socket.connect ("tcp:%s" %replica_address)
    f = open(file_path , 'rb')
    p = pickle.dumps(f.read())
    z = zlib.compress(p)
    f.close()
    socket.send_json(parsed_json)
    ACK = socket.recv_string()
    print("ack after sending header" , ACK)
    socket.send(z)
    ACK = socket.recv_string()
    print( "ack after sending file", ACK)

def recieve_replica(replica_port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % replica_port)
    print("finished binding to replicas")
    json = socket.recv_json()
    parsed_json = json.loads()
    socket.send_string("AY 7aga")
    message = socket.recv()
    p = zlib.decompress(message)
    data = pickle.loads(p)
    directory = "./" + parsed_json["username"]
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + "/"+ parsed_json["filename"], 'wb') as f:  
        f.write(data)
    socket.send_string("finished writting file, success")


def run(id , port):
    t1 = threading.Thread(target=recieve_replica , args=[port])
    t2 = threading.Thread(target=download_uplaod , args=[port+1])
    t1.start()
    t2.start()
    t1.join()
    t1.join()

if __name__ == '__main__':
    print("Why!")
    processes_alive = Process(target=send_alive)
    ports_list = range(start_port , start_port+NUMBER_OF_PROCESSES*2+1 , 2)
    processes_list = []
    for i in range(NUMBER_OF_PROCESSES):
        processes_list.append(Process(target = run ,args=(i , ports_list[i])))
        # print(processes_list[i + 1])

    processes_alive.start()
    for process in processes_list:
        process.start()


    for i in range(NUMBER_OF_PROCESSES):
        processes_list[i].join()

    processes_alive.join()
        


