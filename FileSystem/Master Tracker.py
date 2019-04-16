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
numberOfDownloadLinks = 6
numberOfNodes = 4
MY_IP = "192.168.1.12"

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
                    mirrorList = random.sample(mirrorList,numberOfDownloadLinks)
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
    for i in range(numberOfNodes):
        lastCheck[string.ascii_uppercase[i]] = time.time()

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind("tcp://%s:%s" % (MY_IP,port))
    socket.setsockopt_string(zmq.SUBSCRIBE, "alive")

    # poller = zmq.Poller()
    # poller.register(port, zmq.POLLIN)

    while True:
        while True:
            try:
                message = socket.recv_string(zmq.DONTWAIT)
                print("I received %s"%message)
                topic, machine = message.split()
                machinesState[machine] = True
                lastCheck[machine] = time.time()
            except zmq.Again:
                break
        
        for i in range(numberOfNodes):
            machine = string.ascii_uppercase[i]
            if machinesState[machine] == True and time.time() - lastCheck[machine] >= TIME_OUT:
                machinesState[machine] = False
                print("Machine %s is dead"%string.ascii_uppercase[i])


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
    for i in range(numberOfNodes):
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

    
