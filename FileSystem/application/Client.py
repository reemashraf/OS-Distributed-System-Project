import zmq  
import json
import pickle
import zlib
import os
import zmq.ssh

IP = "tcp://192.168.1.16:5557"
DATABASEIP = "tcp://database@server.os:3000"
class Client():
    def __init__(self,username,mode,filename=None,videopath=None):
        self.username = username
        self.mode = mode
        self.filename = filename
        self.signin = False
        self.socket = zmq.Context().socket(zmq.REQ)
        self.databaseSocket = None
        self.socket.connect(IP)
        self.socket2 = None
        self.videopath = videopath
        self.data = None

    def setusername(self,name):
        self.username = name
    def setmode(self,mode):
        self.mode = mode
    def setfilename(self,file):
        self.filename = file
    def setvideopath(self,path):
        self.videopath = path
    def run(self):
        if (self.mode == "upload"):
            return (self.upload())
        elif(self.mode == "download"):
            return (self.download())
        elif(self.mode == "fileslist"):
            return (self.getlist())

    def login(self,name,password):
        self.databaseSocket = zmq.Context().socket(zmq.REQ)
        
       # zmq.ssh.tunnel_connection(self.databaseSocket, "tcp://locahost:3000", "abdo@41.235.188.134:1337")
        self.databaseSocket.connect("tcp://10.5.50.212:3000")
        self.data = {"mode": "signin",
        "username":name,
        "password":password }
        #data_json = json.dumps(self.data)
        self.databaseSocket.send_json(self.data)
        signin = self.databaseSocket.recv_string()
        print(signin)
        print("ana 5rga")
        self.databaseSocket.close()
        return signin

    def signup(self,name,password):
        self.databaseSocket = zmq.Context().socket(zmq.REQ)
        
        #zmq.ssh.tunnel_connection(self.databaseSocket, "tcp://locahost:3000", "abdo@41.235.188.134:1337")
        self.databaseSocket.connect("tcp://10.5.50.212:3000")
        self.data = {"mode": "signup",
        "username":name,
        "password":password }
        #data_json = json.dumps(self.data)
        self.databaseSocket.send_json(self.data)
        signup = self.databaseSocket.recv_string()
        self.databaseSocket.close()
        print(signup)
        print("ana 5rga")
        return signup

    def upload(self):
        
        self.data = {"mode": self.mode}
        data_json = json.dumps(self.data)
        self.socket.send_json(data_json)
        nodeIP = self.socket.recv_string()
        print("NodeIP from Master:"+nodeIP)


        self.data.update({"username": self.username,
                     "filename": self.filename})
        data_json = json.dumps(self.data)
        self.socket2 = zmq.Context().socket(zmq.REQ)
        self.socket2.connect("tcp://" + nodeIP)
        self.socket2.send_json(data_json)
        ack = self.socket2.recv_string()
        print("Ack From datakeeper:"+ack)

        f = open(self.videopath, "rb")
        p = pickle.dumps(f.read())
        compressed_file = zlib.compress(p)
        f.close()
        self.socket2.send(compressed_file)
        ack = self.socket2.recv_string()
        print("Ack2 From datakeeper:" + ack)
        self.socket2.close()
        return ack

    def getlist(self):
        self.data = {"mode": "fileslist",
        "username":self.username}
        data_json = json.dumps(self.data)
        self.socket.send_json(data_json)
        filenames = self.socket.recv_json()
        filenames = json.loads(filenames)
        return filenames

    def download(self):
        self.data.update({'mode':self.mode,'filename' : self.filename})
        data_json = json.dumps(self.data)
        self.socket.send_json(data_json)
        received_data = json.loads(self.socket.recv_json())
        numberOfChunks = received_data["numberofchunks"]
        ipList = received_data["mirrorlist"]
        print(numberOfChunks)
        print(ipList)
        
        self.socket2 = zmq.Context().socket(zmq.REQ)
        for element in ipList:
            print(element)
            self.socket2.connect("tcp://" + element)

        numberofdigits = len(str(numberOfChunks))

        readData = []
        directory = "./" + self.data["username"]
        if not os.path.exists(directory):
                os.makedirs(directory)
        for chunkNumber in range(1,numberOfChunks+1):
            self.data.update({'chunknumber': chunkNumber})
            dataJson = json.dumps(self.data)
            numberOfChunks = numberOfChunks - 1
            self.socket2.send_json(dataJson)
            videochunk = self.socket2.recv()
            print(videochunk)
            video = zlib.decompress(videochunk)
            video = pickle.loads(video)
            readData += video
        

        with open(directory + "/" + self.data["filename"], 'wb') as f:
                f.write(bytes(readData))
        ###TODO Git all files m3 b3d and view
        self.socket2.close()
