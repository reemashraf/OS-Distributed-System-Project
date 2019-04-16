import time
import sys
from multiprocessing import Process, Manager
import json
import random
import string

machineIPs = {}
filesList = {}
machinesState = {}
numberOfDownloadLinks = 6
numberOfNodes = 4

def clientHandler(port):

    #Initilize connection
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)

    while True:
        message = socket.recv_json()
        data = json.load(message)
        mode = data["mode"]
        if mode == "download":
            username = data["username"]
            filename = data["filename"]    
            if username in filesList:
                userFiles = filesList[username]
                if filename in userFiles:
                    mirrorList = []
                    for mirror in filesList[username][filename]:
                        if machinesState[mirror] == True:
                            mirrorList.extend(machineIPs[mirror])

                    #Get random download links
                    mirrorList = random.sample(mirrorList,numberOfDownloadLinks)
                    print(mirrorList)
                    socket.send_json(json.dumps(mirrorList))
                else:
                    #File not found
                    pass
            else:
                #Username not found
                pass

        else: #Upload
            pass
        

###############################################################################
###############################################################################

def server(id,port):
    print("I'm server with ID = %d, running on port %d"%(id,port))
    




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

    serverPorts = range(startPort,startPort+3,1)


    #Create server processes
    id = 1 #each process has unique id
    processes = [] #list of server processes
    for serverPort in serverPorts:
        processes.append(Process(target=server, args=(id,serverPort)))
        id += 1
    
    for process in processes:
        process.start()
    for process in processes:
        process.join() #wait untill all processes exit

    
