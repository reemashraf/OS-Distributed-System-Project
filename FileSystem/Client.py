

import zmq  # Client
import json
import pickle
import zlib

#IP = "tcp://192.168.1.12:5558"
IP = "tcp://127.0.0.1:10000"
socket = zmq.Context().socket(zmq.REQ)
socket2 = zmq.Context().socket(zmq.REQ)

socket.connect(IP) 

#########################3 to be given from application #####################################


mode = input("Enter Mode u want ")
username = "reem"
filename = "trial.mp4"


#########################3 to be given from application #####################################
if mode == "download":
    data = {"mode": mode,
    "username": username,
    "filename": filename}
    data_json = json.dumps(data)
    #socket.send_json(data_json)
    #received_data = json.loads(socket.recv_json())
    #numberOfChunks = received_data["numberofchunks"]
    #ipList = received_data["mirrorlist"]
    numberOfChunks = 5
    ipList = ""
    print(numberOfChunks)
    print(ipList)
######## close socket1 wala ah ############

    for element in ipList:
        socket2.connect("tcp://"+element)

    while(numberOfChunks != 0):
        data.update({'chunknumber':numberOfChunks})
        data_json = json.dumps(data)
        numberOfChunks = numberOfChunks -1
        socket2.send_json(data_json)
        videochunk = socket2.recv()
        video = zlib.decompress(videochunk)
        video = pickle.loads(video)
        with open("./"+ data_json["username"] + "./"+ data_json["filename"], 'wb') as f:
                f.write(video)
    socket2.close()

elif mode =="upload":
    data = {"mode": mode}
    data_json = json.dumps(data)
    
    #socket.send_json(data_json)
    #nodeIP = socket.recv_string()
    #print(nodeIP)

    data.update({"username": username,
    "filename": filename})
    data_json = json.dumps(data)
    nodeIP = "192.168.1.6:5556"
    socket2.connect("tcp://"+nodeIP)
    socket2.send_json(data_json)
    ack = socket2.recv_string()
    print(ack)
    ## from user
    videoPath = "vid_1.mp4"
    f = open(videoPath,"rb")
    p = pickle.dumps(f.read())
    compressed_file  = zlib.compress(p)
    f.close()
    socket2.send(compressed_file)
    ack = socket2.recv_string()
    print(ack)
    socket2.close()
