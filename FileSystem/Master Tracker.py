import time
import sys
import zmq
from multiprocessing import Process, Manager
import json
import random
import string
import threading


filesList = {}
machinesState = {}
replicaIPs = {}
downloadUploadIPs = {}
NUMBER_OF_DOWNLOAD_MIRRORS = 4
NUMBER_OF_NODES = 3
NUMBER_OF_PROCESSES = 3
MY_IP = "192.168.1.16"
NUMBER_OF_REPLICAS = 2
TIME_OUT = 3

def clientHandler(id,port):
    print("Process %s handling client at port %s"%(id,port))
    #Initilize connection
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (MY_IP,port))

    while True:
        message = socket.recv_json()
        print("Server with id %s, Received %s"%(id,message))
        data = json.loads(message)
        mode = data["mode"]
        if mode == "fileslist":
            username = data["username"]
            if username in filesList:
                dataSent = list(filesList[username].keys())
                socket.send_json(json.dumps(dataSent))
                print("Server %s sent %s"%(id,dataSent))
            else:
                #no data found / username not found
                error = {"error":"no data found"}
                socket.send_json(json.dumps(error))
        elif mode == "download":
            username = data["username"]
            filename = data["filename"]    
            if username in filesList:
                userFiles = filesList[username]
                if filename in userFiles:
                    mirrorList = []
                    numberOfChunks = filesList[username][filename]["numberofchunks"]
                    for mirror in filesList[username][filename]["mirrorlist"]:
                        if machinesState[mirror] == True:
                            mirrorList.extend(downloadUploadIPs[mirror])

                    #Get random download links
                    if len(mirrorList) > NUMBER_OF_DOWNLOAD_MIRRORS:
                        secure_random = random.SystemRandom()
                        mirrorList = secure_random.sample(mirrorList,NUMBER_OF_DOWNLOAD_MIRRORS)
                    dataSent = {"numberofchunks": numberOfChunks, "mirrorlist": mirrorList}
                    socket.send_json(json.dumps(dataSent))
                    print("Server %s sent %s"%(id,dataSent))
                else:
                    #File not found
                    error = {"error":"file not found"}
                    socket.send_json(json.dumps(error))
            else:
                #Username not found
                error = {"error":"no data found"}
                socket.send_json(json.dumps(error))
        else: #Upload
            mirrorList = []
            for machine, state in machinesState.items():
                if state == True:
                    mirrorList.extend(downloadUploadIPs[machine])
            
            if not mirrorList:
                error = {"error":"no mirror list found"}
                socket.send_json(json.dumps(error))
            else:
                secure_random = random.SystemRandom()
                uploadNode = secure_random.choice(mirrorList)
                socket.send_string(uploadNode)
                print("Server %s, I redirected client to %s"%(id,uploadNode))
            

###############################################################################
###############################################################################


def aliveSignalReceiver(id,port):
    print("Alive checker process waiting on port %s"%port)
    lastCheck = {}
    for i in range(NUMBER_OF_NODES):
        machine = string.ascii_uppercase[i]
        lastCheck[machine] = time.time()

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind("tcp://%s:%s" % (MY_IP,port))
    socket.setsockopt_string(zmq.SUBSCRIBE, "alive")
    
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    while True:
        socks = dict(poller.poll(0))
        # print("?")
        if socket in socks:
            message = socket.recv_string()
            # print("I received %s"%message)
            topic, machine = message.split()
            if machinesState[machine] == False:
                print("Machine %s is back"%machine)
            machinesState[machine] = True
            lastCheck[machine] = time.time()
           
        for i in range(NUMBER_OF_NODES):
            machine = string.ascii_uppercase[i]
            if machinesState[machine] == True and time.time() - lastCheck[machine] >= TIME_OUT:
                machinesState[machine] = False
                print("Machine %s is dead"%machine)

###############################################################################
###############################################################################


def nodeTrackerHandler(id,port):
    print("Process %s handling replication at port %s"%(id,port))
    #Initilize connection
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (MY_IP,port))

    while True:
        message = json.loads(socket.recv_json())
        print("I'm process %s, received %s"%(id,message))
        machine = message["machine"]
        # print(machine)
        username = message["username"]
        # print(username)
        fileName = message["filename"]
        # print(fileName)
        numberOfChunks = message["numberofchunks"]
        # print(numberOfChunks)
        machinesList = []
        for i in range(NUMBER_OF_NODES):
            replica = string.ascii_uppercase[i] 
            if machinesState[replica] == True and replica != machine:
                machinesList.append(replica)

        if len(machinesList) > NUMBER_OF_REPLICAS:
            secure_random = random.SystemRandom()
            machinesList = secure_random.sample(machinesList,NUMBER_OF_REPLICAS)

        if username not in filesList:
            filesList[username] = {}
            
        temp = filesList[username]
        mirrorsList = [machine]
        mirrorsList.extend(machinesList)
        temp[fileName] = {
            "numberofchunks": numberOfChunks,
            "mirrorlist": mirrorsList    
        }
        

        filesList[username] = temp
        print(filesList)
        # filesList[username][fileName].append(machine)
        # filesList[username][fileName].extend(machinesList)

        ipsList = []
        for replica in machinesList:
            ips = replicaIPs[replica]
            secure_random = random.SystemRandom()
            ip = secure_random.choice(ips)
            ipsList.append(ip)
        print("Sending %s"%ipsList)
        socket.send_json(json.dumps(ipsList))

        

###############################################################################
###############################################################################


def server(id,port):
    initializeConstants()
    if id == 0:
        print("I'm alive tracking process at port %s"%port)
        aliveSignalReceiver(id,port)
    else:
        # print("I'm process %s handling clients at port %s, replication at port %s"%(id,port,port+1))
        clientThread = threading.Thread(target=clientHandler, args=(id,port))
        nodeTrackerThread = threading.Thread(target=nodeTrackerHandler, args=(id,port+1))
        clientThread.start()
        nodeTrackerThread.start()
        clientThread.join()
        nodeTrackerThread.join()
    

###############################################################################
###############################################################################


def initializeConstants():
    machineIPs = [
        "192.168.1.17",
        "192.168.1.22",
        "192.168.1.16",
        "192.168.1.17"
    ]


    downloadUploadPort = 5581
    replicationPort = 5580

    for i in range(NUMBER_OF_NODES):
        replica = string.ascii_uppercase[i]
        replicaIPs[replica] = []
        for j in range(NUMBER_OF_PROCESSES):
            ip = machineIPs[i]
            port = replicationPort+j*2
            replicaIPs[replica].append("%s:%s"%(ip,port))


    for i in range(NUMBER_OF_NODES):
        machine = string.ascii_uppercase[i] 
        downloadUploadIPs[machine] = []
        for j in range(NUMBER_OF_PROCESSES):
            ip = machineIPs[i]
            port = downloadUploadPort+j*2
            # port = downloadUploadPort
            downloadUploadIPs[machine].append("%s:%s"%(ip,port))


    for i in range(NUMBER_OF_NODES):
        machine = string.ascii_uppercase[i] 
        machinesState[machine] = True

###############################################################################
###############################################################################

if __name__ == '__main__':

    #initialize shared variables
    manager = Manager()
    machinesState = manager.dict()
    filesList = manager.dict()
    
    #Initizlize server ports
    startPort = 5555
    if len(sys.argv) > 1:
        startPort = int(sys.argv[1])

    serverPorts = range(startPort,startPort+7,2)

    #Create server processes
    id = 0 #each process has unique id
    processes = [] #list of server processes
    for serverPort in serverPorts:
        processes.append(Process(target=server, args=(id,serverPort)))
        id += 1
    
    for process in processes:
        process.start()
    for process in processes:
        process.join() #wait untill all processes exit

    
