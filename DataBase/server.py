import zmq
import sys
import time
import json
import random
import threading
# from parser import *

shards = [
    "a-g",
    "h-m",
    "n-t",
    "u-z"
]

SHARDS = {
    0: {"available": True, "master": 0, "machines": ["sh1m", "sh1r1", "sh1r2"]},
    1: {"available": True, "master": 0, "machines": ["sh2m", "sh2r1", "sh2r2"]},
    2: {"available": True, "master": 0, "machines": ["sh3m", "sh3r1", "sh3r2"]},
    3: {"available": True, "master": 0, "machines": ["sh4m", "sh4r1", "sh4r2"]}
}
TIME_OUT = 3000
DOC = """==========================================================================================================================================
available commands:
====================
1- INSERT: adds new entery to database, args: \{TABLENAME* => STRING, FEILDS => STRING_ARRAY, VALUES => STIRNG_ARRAY\}
2- SELECT: query database, args: \{TABLENAME* => STRING, FEILDS => STRING_ARRAY\}
3- UPDATE: update or modifiy an existing database entry, args: \{TABLENAME* => STRING, FEILDS* => STRING_ARRAY, VALUES* => STRING_ARRAY\}
4- DELETE: remove a database entry, args: \{TABLENAME* => STRING, ID* => INT\}
Notes:
=======
1- input is case sensitive
2- * feilds are required to be entered when using the function
3- optional feilds may lead to instructions not being excuted properly or not excuted at all which it totally your responsibility XD
=========================================================================================================================================="""

def getShard(username):
    userkey = username[0].lower()
    shard = 0
    for key in shards:
        shard = shard + 1
        symbols = key.split("-")
        if userkey >= symbols[0] and userkey <= symbols[1]:
            return shard
    return False

def dropShard(shard_index):
    shard = SHARDS[shard_index]
    shard["available"] = False
    SHARDS[shard_index] = shard


def getMaster(shard_index):
    shard = SHARDS[shard_index]
    if shard["available"]:
        master = shard["master"]
        machines = shard["machines"]
        return machines[master]
    return False


def setMaster(shard_index, new_master_index):
    shard = SHARDS[shard_index]
    machines = shard["machines"]
    if new_master_index >= 0 and new_master_index < len(machines):
        shard["master"] = new_master_index
        SHARDS[shard_index] = shard
        return True
    return False


def getActiveRelicas(shard_index):
    shard = SHARDS[shard_index]
    master = shard["master"]
    machines = shard["machines"]
    if master < len(machines)-1:
        return machines[master+1:]
    return False


def getNextReplica(shard_index):
    shard = SHARDS[shard_index]
    master = shard["master"]
    machines = shard["machines"]
    master = master + 1
    if master >= len(machines):
        return False
    setMaster(shard_index, master)
    return machines[master]


def getMachineIndex(shard_index, machine):
    shard = SHARDS[shard_index]
    machines = shard["machines"]
    if machine in machines:
        return machines.index(machine)
    return -1


class Server:
    def __init__(self, socket=None, poller=None):
        self.socket = socket
        self.poller = zmq.Poller()

    def setSocket(self, socket):
        self.socket = socket
        self.poller.register(socket, zmq.POLLIN)

    def ping(self, machine):
        ack_received = False
        self.socket.send_multipart([bytes(machine, 'utf-8'), b'PING'])
        socks = dict(self.poller.poll(TIME_OUT))
        if socket in socks and socks[self.socket] == zmq.POLLIN:
            message = self.socket.recv_multipart()
            message = message[0]
            message = message.decode()
            if machine in message:
                ack_received = True

        return ack_received

    def signin(self, username, password):
        shard = getShard(username)
        if shard:
            query = "SELECT password FROM users WHERE username='" + username + "'"
            data = self.select(shard, query)
            if data:
                data = data[0]
                user_password = data[0]
                if password == user_password:
                    print("user is logged in!!!")
                    return True
                else:
                    print("user is not logged in!!!")
                    return False
            else:
                print("user doesn't exist!!!")
                return False
        return False

    def signup(self, username, password):
        shard = getShard(username)
        if shard:
            query = "INSERT INTO users (username, password) VALUES ('" + username + "'" + ", '" + password + "')"
            self.insert(shard, query)
            return True
        return False

    def insertInReplicas(self, shard, query):
        replicas = getActiveRelicas(shard)
        for replica in replicas:
            print(replica)
            threading.Thread(target=self.insertInReplica, args=(replica, query,)).start()

    def insertInReplica(self, machine, query):
        if self.ping(machine):
            self.run(machine, query)
        else:
            print("replica: %s is dead" % machine)

    def insert(self, shard, query):
        shard = shard - 1
        machine = getMaster(shard)
        while machine:
            if self.ping(machine):
                if setMaster(shard, getMachineIndex(shard, machine)):
                    self.run(machine, query)
                    self.insertInReplicas(shard, query)

                    break
                else:
                    machine = getNextReplica(shard)
            else:
                machine = getNextReplica(shard)
        if not machine:
            print("WARNING: shard %d is not responding and will be droped!!!" % (shard+1))
            dropShard(shard)

    def select(self, shard, query):
        shard = shard - 1
        machine = getMaster(shard)
        while machine:
            if self.ping(machine):
                if setMaster(shard, getMachineIndex(shard, machine)):
                    data = self.run(machine, query, select=True)
                    print(data)
                    return data
                    break
                else:
                    machine = getNextReplica(shard)
            else:
                machine = getNextReplica(shard)
        if not machine:
            print("WARNING: shard %d is not responding and will be droped!!!" % (shard+1))
            dropShard(shard)
            return False
        return True

    def run(self, machine, query, select=False):
        self.socket.send_multipart([bytes(machine, 'utf-8'), bytes(query, 'utf-8')])
        if select:
            socks = dict(self.poller.poll(TIME_OUT))
            if socket in socks and socks[self.socket] == zmq.POLLIN:
                message = self.socket.recv_multipart()
                receiver = message[1].decode()
                if receiver == "server":
                    message = message[2]
                    message = json.loads(message)
                    return message
                else:
                    print("no response received!!!")
            else:
                print("no response received!!!")
        return True


def http(socket, server):
    result = "0"
    while True:
        # Wait for next request from client
        message = socket.recv_json()
        print("Received request: ", message)
        time.sleep(1)
        keys = message.keys()
        if ("username" in keys) and ("password" in keys) and ("mode" in keys):
            mode = message['mode'].lower()
            if (mode == "signin"):
                if server.signin(message["username"], message["password"]):
                    result = "1"
                else:
                    result = "0"
            elif (mode == "signup"):
                if server.signup(message["username"], message["password"]):
                    result = "1"
                else:
                    result = "0"
            else:
                result = "0"
        else:
            result = "0"
        socket.send_string(result)


# def cli(server):
#     parser = Parser()
#     print(DOC)
#     while True:
#         query = input("> ")
#         if query == "":
#             continue
#         query_arr = query.split()
#         instruction = query_arr[0]

#         if instruction == "INSERT":
#             shard = parser.insert(query)
#             if shard:
#                 threading.Thread(target=server.insert, args=(shard, query,)).start()
#             else:
#                 print("Error: invalid instruction!!!")
#         elif instruction == "SELECT":
#             shard = parser.select(query)
#             if shard:
#                 threading.Thread(target=server.select, args=(shard, query,)).start()
#             else:
#                 print("Error: invalid instruction!!!")
#         else:
#             print("Error: Invalid Instruction!!!")


if __name__ == "__main__":
    port = "5556"
    http_port = "3000"
    if len(sys.argv) > 1:
        port = sys.argv[1]
    if len(sys.argv) > 2:
        http_port = sys.argv[2]

    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.setsockopt(zmq.IDENTITY, b'server')
    socket.bind("tcp://*:%s" % port)
    http_socket = context.socket(zmq.REP)
    http_socket.bind("tcp://*:%s" % http_port)

    server = Server()
    server.setSocket(socket)
    print("server was created successfully ready to receive client requests")

    http(http_socket, server)
    # threading.Thread(target=cli, args=(server,)).start()
    # threading.Thread(target=http, args=(http_socket, server,)).start()
