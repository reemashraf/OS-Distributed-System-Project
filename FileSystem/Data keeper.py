from time import sleep
import zmq
import socket
from multiprocessing import Process
import json
import os
import zlib 
import pickle
from chunk import *
import threading
import socket as sc

my_ip = sc.gethostbyname(sc.gethostname())
alive_port = 5555  #port where thre process that sends alive message is sent
#client_server_port = 5580  #port where thre processes that uploads and downloads
topic_alive = "alive"
master_ACK_port = 5560
process_order = "A"
replica_port = 5526
number_of_replicas = 2
NUMBER_OF_PROCESSES = 3
start_port = 5580 
MASTER_IP = "192.168.1.16"
MACHINE_IP = "192.168.1.17"
machine_name = 'A'
##if file uploaded duplicate name notify the client or pad with underscores 3ashn ahmed myz3lish

def send_alive(): #tested and works fine with the master
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    print("before connect")
    socket.connect("tcp://%s:%s" % (MASTER_IP,alive_port))
    print("alive process connected")
    while True:
        # message = [ 1 , socket.gethostbyname(socket.gethostname()) ]
        message = "%s %s"%(topic_alive , process_order)
        # socket.send_string(topic , zmq.SNDMORE)
        socket.send_string(message)
        #print("finished sending alive message")
        sleep(1)#wait for one second before sending the next alive message
        
        
def download_uplaod(process_id , client_server_port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    print("process_id",process_id," I took port %s"%client_server_port)
    socket.bind("tcp://%s:%s" % (MACHINE_IP,client_server_port))
    print("finished binding")
    while True:
        message = socket.recv_json()
        parsed_json = json.loads(message)
        print(parsed_json["mode"]) 
        print("recieved header from client")
        if(parsed_json["mode"] == "upload"):
            socket.send_string("ACK")
            print("sent ACK to client")
            message = socket.recv()
            p = zlib.decompress(message)
            data = pickle.loads(p)
            print("finished recieving")
            socket.send_string("finished writting file, success")
            extension_index = len(parsed_json["filename"])
            if "." in parsed_json["filename"]:
                extension_index = parsed_json["filename"].rfind(".")
            directory = "./" + parsed_json["username"] + "/"+str(parsed_json["filename"])[: extension_index]
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(directory + "/"+ parsed_json["filename"], 'wb') as f:  
                f.write(data)
            ####will slice here#### ###done and tested#######
            number_of_chunks = slice_file(directory ,  parsed_json["filename"] , 64*1024)
            ########change to connect
            socket_master_ack = context.socket(zmq.REQ)
            socket_master_ack.connect("tcp://%s:%s" % (MASTER_IP,master_ACK_port) )
            header_data = {
                "machine": machine_name, ##ID instead don't forget to modify
                "username": parsed_json["username"],
                "filename": parsed_json["filename"],
                "numberofchunks": number_of_chunks
                }
            header_data_sent_to_master = json.dumps(header_data)
            socket_master_ack.send_json(header_data_sent_to_master)
            replica_list_json = socket_master_ack.recv_json()
            replica_list = json.loads(replica_list_json)
            #####TO_do replicate to other machines#######
            '''
            will send file path(comelete file path) from the client, and replica list, parsed json
            '''
            replicate(directory + "/"+ parsed_json["filename"] , replica_list , parsed_json)
        elif(parsed_json["mode"] == "download"):
            print("inside download")
            print(machine_name)
            chunk_number = parsed_json["chunknumber"]
            extension_index = len(parsed_json["filename"])
            if "." in parsed_json["filename"]:
                extension_index = parsed_json["filename"].rfind(".")
            directory = "./" + parsed_json["username"] + "/"+str(parsed_json["filename"])[: extension_index]
            #print("directory ", directory)
            #print(os.listdir(directory))
            filename = get_chunck_name_by_number(chunk_number , directory , parsed_json["filename"])
            file_path = directory + "/" +filename
            #print("file path" , file_path)
            with open(file_path , 'rb') as f:
                chunk_small = f.read(64*1024)
            # chunk_small = f.read()
            #print(file_path)
            p = pickle.dumps(chunk_small)
            z = zlib.compress(p)
            f.close()
            socket.send(z)
            
def replicate(file_path , replica_list , parsed_json):
    #here other nodes act as servers the one responsible for sending is the client
    context = zmq.Context()
    print ("Connecting to server (replica dataNodes)...")
    socket = context.socket(zmq.REQ)

    #list_values = [ replica_address for replica_address in replica_list.values()]
    for replica_address in replica_list:
        print(replica_address)
        socket.connect ("tcp://%s" %replica_address)
    
    f = open(file_path , 'rb')
    p = pickle.dumps(f.read())
    z = zlib.compress(p)
    f.close()

    parsed_json["file"] = z
    for i in range(len(replica_list)):
        socket.send(pickle.dumps(parsed_json))
        ACK = socket.recv_string()
        print("ack after sending json header" , ACK)
 

def recieve_replica(replica_port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % replica_port)
    print("finished binding to replicas")
    while True:
    #json = socket.recv_json()
    #parsed_json = json.loads()
    #socket.send_string("AY 7aga") #received the json and ACK is sent

        json = socket.recv_json()#file and json is received
        parsed_json = json.loads()
        z = parsed_json["file"]
        p = zlib.decompress(z)
        sent_file = pickle.loads(p)
        
        #p = zlib.decompress(message)
        #data = pickle.loads(p)
        directory = "./" + parsed_json["username"]
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(directory + "/"+ parsed_json["filename"], 'wb') as f:  
            f.write(sent_file)
        socket.send_string("finished writting file, success")

#for testing purpose only
def send_ack_master():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://%s:%s" % (MASTER_IP,master_ACK_port) )
    header_data = {
        "machine": machine_name, ##ID instead don't forget to modify
        "username": "yasmeen",
        "filename": "vid_1.mp4",
        "numberofchunks": 15
        }
    header_data_sent_to_master = json.dumps(header_data)
    socket.send_json(header_data_sent_to_master)
    replica_list = socket.recv_json()
    print(replica_list)
    socket.close()
    return replica_list


def replicate_test(file_path , replica_list):
    #here other nodes act as servers the one responsible for sending is the client
    context = zmq.Context()
    print ("Connecting to server (replica dataNodes)...")
    socket = context.socket(zmq.REQ)
    #list_values = [ replica_address for replica_address in replica_list.values()]
    for replica_address in replica_list:
        print(replica_list)
        socket.connect ("tcp://%s" %replica_address)
    f = open(file_path , 'rb')
    p = pickle.dumps(f.read())
    z = zlib.compress(p)
    f.close()
    socket.send(z)
    ACK = socket.recv_string()
    print("ack after sending header" , ACK)
    socket.send(z)
    ACK = socket.recv_string()
    print("ack after sending header" , ACK)
    # socket.close()
    return replica_list

def run(id , port):
    process_id = id
    print(process_id)
    t1 = threading.Thread(target=recieve_replica , args=[port])
    t2 = threading.Thread(target=download_uplaod , args=[process_id , port+1])
    t1.start()
    t2.start()
    t1.join()
    t1.join()

if __name__ == '__main__':
    processes_alive = Process(target=send_alive)
    ports_list = range(start_port , start_port+NUMBER_OF_PROCESSES*2+1 , 2)
    processes_list = []
    for i in range(NUMBER_OF_PROCESSES):
        processes_list.append(Process(target = run ,args=(i , ports_list[i])))
        # print(processes_list[i + 1])

    processes_alive.start()
    for process in processes_list:
        process.start()

    processes_alive.join()
    for i in range(NUMBER_OF_PROCESSES):
        processes_list[i].join()

    #replica_list = send_ack_master()
    # data = {
    #     "filename" : "vid_1.mp4",
    #     "username" : "reem_ashraf",
    #     "mode"     : "download",
    #     "numberofchunks": 52,
    #     "machine" : "A"
    # }
    # context = zmq.Context()
    # socket = context.socket(zmq.REQ)
    # port = 5558
    # socket.connect("tcp://192.168.1.9:%s" % port)
    # data_dumped = json.dumps(data)
    # socket.send_json(data_dumped)
    # replica_lis = socket.recv_json()
    # print(type(replica_lis))
    # replica_lis = json.loads(replica_lis)
    # print(type(replica_lis))
    # replicate("vid_1.mp4" , replica_lis ,data)
    # download_uplaod(client_server_port)




