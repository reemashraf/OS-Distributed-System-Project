import zmq
import sys
import time
import random
import threading
from parser import Parser

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

    def insert(self, shard, query):
        shard = shard - 1
        machine = getMaster(shard)
        while machine:
            if self.ping(machine):
                if setMaster(shard, getMachineIndex(shard, machine)):
                    self.run(machine, query)
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
                    self.run(machine, query)
                    break
                else:
                    machine = getNextReplica(shard)
            else:
                machine = getNextReplica(shard)
        if not machine:
            print("WARNING: shard %d is not responding and will be droped!!!" % (shard+1))
            dropShard(shard)

    def run(self, machine, query):
        self.socket.send_multipart([bytes(machine, 'utf-8'), bytes(query, 'utf-8')])


server = Server()

if __name__ == "__main__":
    port = "5556"
    if len(sys.argv) > 1:
        port = sys.argv[1]

    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.setsockopt(zmq.IDENTITY, b'server')
    socket.bind("tcp://*:%s" % port)

    server.setSocket(socket)
    print("server was created successfully ready to receive client requests")

    parser = Parser()
    print(DOC)
    while True:
        query = input("> ")
        if query == "":
            continue
        query_arr = query.split()
        instruction = query_arr[0]

        if instruction == "INSERT":
            shard = parser.insert(query)
            if shard:
                threading.Thread(target=server.insert, args=(shard, query,)).start()
            else:
                print("Error: invalid instruction!!!")
        elif instruction == "SELECT":
            shard = parser.select(query)
            if shard:
                threading.Thread(target=server.select, args=(shard, query,)).start()
            else:
                print("Error: invalid instruction!!!")
        else:
            print("Error: Invalid Instruction!!!")
