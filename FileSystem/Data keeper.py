from time import sleep
import zmq
import socket
import multiprocessing as mp
import json
import os
import zlib 
import pickle
import chunk

my_ip = socket.gethostbyname(socket.gethostname())
alive_port = 5555  #port where thre process that sends alive message is sent
client_server_port = 5556  #port where thre processes that uploads and downloads
topic_alive = "alive"
master_ACK_port = 5555
process_order = "A"

def send_alive(): #tested and works fine with the master
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://192.168.1.12:%s" % alive_port)
    while True:
        # message = [ 1 , socket.gethostbyname(socket.gethostname()) ]
        message = "%s %s"%(alive_port , process_order)
        # socket.send_string(topic , zmq.SNDMORE)
        socket.send_string(message)
        print("finished sending alive message")
        sleep(1)#wait for one second before sending the next alive message
        
        
def download_uplaod():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
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
            socket.send_string("success")
            directory = "./" + parsed_json["username"]
            if not os.path.exists(directory):
                os.makedirs(directory)
            with open(directory + "./"+ parsed_json["filename"], 'wb') as f:  
                f.write(data)
            ####will slice here####
            number_of_chunks = chunk.slice(directory + "./"+ parsed_json["filename"])
            socket.send_string("finished writting file")
            socket.bind("tcp://*:%s" % master_ACK_port)
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
        elif(parsed_json["mode"] == "download"):
            message = socket.recv_json()
            parsed_json = message.loads()
            chunk_number = parsed_json["chunknumber"]
            filename = chunk.get_chunk_name_by_number(chunk_number , parsed_json["filename"])
            p = pickle.dumps(filename.read())
            z = zlib.compress(p)
            f.close()
            socket.send (z)
            

#processes_alive = mp.Process(target=send_alive)
#processes_downlaod_uplaod = mp.Process(target = download_uplaod)
download_uplaod()
#p.start()


    


