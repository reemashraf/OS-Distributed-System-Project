import time
import sys
import zmq
from multiprocessing import Process, Manager
import json
import random
import string


machineIPs = {}
filesList = {}
machinesState = {}
NUMBER_OF_DOWNLOAD_MIRRORS = 6
NUMBER_OF_NODES = 4
MY_IP = "192.168.1.12"
NUMBER_OF_REPLICAS = 2

def clientHandler(id,port):
    #Initilize connection
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (MY_IP,port))

    while True:
        message = socket.recv_json()
        print("Server with id %s, Received %s"%(id,message))
        data = json.loads(message)
        mode = data["mode"]
        if mode == "download":
            username = data["username"]
            filename = data["filename"]    
            if username in filesList:
                userFiles = filesList[username]
                if filename in userFiles:
                    mirrorList = []
                    numberOfChunks = filesList[username][filename]["numberofchunks"]
                    for mirror in filesList[username][filename]["mirrorlist"]:
                        if machinesState[mirror] == True:
                            mirrorList.extend(machineIPs[mirror])

                    #Get random download links
                    mirrorList = random.sample(mirrorList,NUMBER_OF_DOWNLOAD_MIRRORS)
                    dataSent = {"numberofchunks": numberOfChunks, "mirrorlist": mirrorList}
                    socket.send_json(json.dumps(dataSent))
                else:
                    #File not found
                    pass
            else:
                #Username not found
                pass
        else: #Upload
            mirrorList = []
            for machine, state in machinesState.items():
                if state == True:
                    mirrorList.extend(machineIPs[machine])
            socket.send_string(random.choice(mirrorList))
            

###############################################################################
###############################################################################


def aliveSignalReceiver(id,port):
    print("Alive checker process waiting on port %s"%port)
    lastCheck = {}
    TIME_OUT = 2
    for i in range(NUMBER_OF_NODES):
        lastCheck[string.ascii_uppercase[i]] = time.time()

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
                print("Machine %s is dead"%string.ascii_uppercase[i])

###############################################################################
###############################################################################


def nodeTrackerHandler(id,port):
    #Initilize connection
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (MY_IP,port))

    while True:
        message = socket.recv_json()
        machine = message["machine"]
        username = message["username"]
        fileName = message["filename"]

        machinesList = []
        for i in range(NUMBER_OF_NODES):
            replica = string.ascii_uppercase[i] 
            if machineState[replica] == True and replica != machine:
                machinesList.append(replica)

        
        filesList[username][fileName] = []
        filesList[username][fileName].append(machine)
        filesList[username][fileName].extend(random.sample(machinesList,NUMBER_OF_REPLICAS))

###############################################################################
###############################################################################


def server(id,port):
    print("I'm server with ID = %d, running on port %d"%(id,port))
    if id == 0:
        aliveSignalReceiver(id,port)
    # else:
    #     clientHandler(id,port)
    

###############################################################################
###############################################################################


if __name__ == '__main__':

    #initialize shared variables
    manager = Manager()
    machinesState = manager.dict()
    filesList = manager.dict()

    #set default values for machine states
    for i in range(NUMBER_OF_NODES):
        machinesState[string.ascii_uppercase[i]] = False
    
    #Initizlize server ports
    startPort = 5555
    if len(sys.argv) > 1:
        startPort = int(sys.argv[1])


    serverPorts = range(startPort,startPort+5,1)


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

    
