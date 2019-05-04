import time
import sys
import zmq
from multiprocessing import Process, Manager
import json
import random
import string
import threading
import sqlite3

machinesState = {}
replicaIPs = {}
notifyReplicaIPs = {}
downloadUploadIPs = {}
NUMBER_OF_DOWNLOAD_MIRRORS = 1
NUMBER_OF_NODES = 2
NUMBER_OF_PROCESSES = 3
MIN_NUMBER_OF_COPYS = 3
MY_IP = "192.168.1.17"
NOTIFY_REPLICA_PORT = "5410"
NUMBER_OF_REPLICAS = 2
TIME_OUT = 3
REPLICATE_CHECK_TIME = 15
INSERT_QUERY = "INSERT INTO files(USERNAME,FILENAME,NUMBEROFCHUNKS,MIRROR) VALUES(?,?,?,?)"
MIRROR_LIST_QUERY = "SELECT NUMBEROFCHUNKS,MIRROR from files where USERNAME = ? AND FILENAME = ?"
FILES_LIST_QUERY = "SELECT DISTINCT FILENAME FROM files WHERE USERNAME = ?"
FILES_NEEDS_REPLICATION = "SELECT USERNAME,FILENAME FROM FILES GROUP BY USERNAME,FILENAME HAVING COUNT(*) < %s"%MIN_NUMBER_OF_COPYS

def clientHandler(id,port):
    
    print("Process %s handling client at port %s"%(id,port))
    #Initilize connection
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (MY_IP,port))

    #Connect to DB
    conn = sqlite3.connect('myDB.db',check_same_thread=False)
    c = conn.cursor()

    while True:
        message = socket.recv_json()
        print("Server with id %s, Received %s"%(id,message))
        data = json.loads(message)
        mode = data["mode"]
        if mode == "fileslist":

            username = data["username"]
            filesList = []
            
            t = (username,)
            for row in c.execute(FILES_LIST_QUERY,t):
                filesList.append(row[0])
            
            socket.send_json(json.dumps(filesList))
            print("Server %s sent %s"%(id,filesList))

        elif mode == "download":
            username = data["username"]
            filename = data["filename"]    
            
        
            mirrorList = []
            numberOfChunks = 0
            t = (username,filename)

            for row in c.execute(MIRROR_LIST_QUERY,t):
                numberOfChunks = row[0]
                mirror = row[1]
                if machinesState[mirror] == True:
                    mirrorList.extend(downloadUploadIPs[mirror])

            #Get random download links
            if len(mirrorList) > NUMBER_OF_DOWNLOAD_MIRRORS:
                secure_random = random.SystemRandom()
                mirrorList = secure_random.sample(mirrorList,NUMBER_OF_DOWNLOAD_MIRRORS)

            dataSent = {"numberofchunks": numberOfChunks, "mirrorlist": mirrorList}
            socket.send_json(json.dumps(dataSent))
            print("Server %s sent %s"%(id,dataSent))

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
                dataSent = {"error": "There exist a file with the same name",
                    "uploadnode": uploadNode
                }
                socket.send_json(uploadNode)
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
        if socket in socks:
            message = socket.recv_string()
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

     #Connect to DB
    conn = sqlite3.connect('myDB.db',check_same_thread=False)
    c = conn.cursor()


    while True:
        message = json.loads(socket.recv_json())
        print("I'm process %s, received %s"%(id,message))
        machine = message["machine"]
        # print(machine)
        username = message["username"]
        # print(username)
        fileName = message["filename"]
        # print(fileName)
        numberOfChunks = int(message["numberofchunks"])
        
        t = (username,fileName,numberOfChunks,machine)
        c.execute(INSERT_QUERY,t)
        conn.commit()

        socket.send_string("ACK")

        

###############################################################################
###############################################################################

def replicate(id,port):
    #Connect to DB
    conn = sqlite3.connect('myDB.db',check_same_thread=False)
    c = conn.cursor()
    

    time.sleep(30)
    while(True):
        print("Starting checking files for replication")
        filesNeedsReplication = []
        for row in c.execute(FILES_NEEDS_REPLICATION):
            print("File (%s) needs replication for user (%s)"%(row[1],row[0]))
            filesNeedsReplication.append(row)
        
        for row1 in filesNeedsReplication:
            print("Handling",row1)
            mirrorList = []
            numberOfChunks = 0
            username = row1[0]
            filename = row1[1]

            t = (username,filename)
            for row in c.execute(MIRROR_LIST_QUERY,t):
                numberOfChunks = row[0]
                mirror = row[1]
                mirrorList.append(mirror)
            
            #NUMBER OF extra REPLICAS NEEDED
            needed = MIN_NUMBER_OF_COPYS - len(mirrorList)

            machinesList = []
            replicaNames = []
            for i in range(NUMBER_OF_NODES):
                replica = string.ascii_uppercase[i]
                if (machinesState[replica] == True and replica not in mirrorList):
                    secure_random = random.SystemRandom()
                    machinesList.append(secure_random.choice(replicaIPs[replica]))
                    replicaNames.append(replica)

            if len(machinesList) == 0: #No machine is alive to replicate, na7s
                print("No alive machine to replicate to")
                continue
            
            sourceMachine = None

            for mirror in mirrorList:
                if machinesState[mirror] == True:
                    sourceMachine = mirror
                    break
            
            if sourceMachine is None: #No source machine is alive to replicate, na7s bardo
                print("No alive source machine")
                continue 

            if len(machinesList) > needed:
                secure_random = random.SystemRandom()
                machinesList = secure_random.sample(machinesList,needed)

            dataSent = {"filename": filename,
                        "username": username,
                        "replicalist": machinesList}

            print("Replicating file (%s) for user (%s)"%(filename,username))
            print("Source machine: %s with ip %s"%(sourceMachine,notifyReplicaIPs[sourceMachine]))
            print("Replicating to",machinesList)
            

            
            
            
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            socket.connect("tcp://%s" % (notifyReplicaIPs[sourceMachine]))
            socket.send_json(json.dumps(dataSent))
            socket.recv_string()

            socket.close()
            
            #Notify database
            for machine in replicaNames:
                t = (username,filename,numberOfChunks,machine)
                c.execute(INSERT_QUERY,t)
                conn.commit()

        time.sleep(REPLICATE_CHECK_TIME)

            
###############################################################################
###############################################################################


def server(id,port):
    initializeConstants()
    if id == 0:
        aliveHandler = threading.Thread(target=aliveSignalReceiver, args=(id,port))
        replicateHandler = threading.Thread(target=replicate, args=(id,port+1))
        aliveHandler.start()
        replicateHandler.start()
        aliveHandler.join()
        replicateHandler.join()
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
        "192.168.1.8",
        "192.168.43.53",
        "192.168.43.53"
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
        replica = string.ascii_uppercase[i]
        ip = machineIPs[i]
        notifyReplicaIPs[replica] = "%s:%s"%(ip,NOTIFY_REPLICA_PORT)
        

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

    
