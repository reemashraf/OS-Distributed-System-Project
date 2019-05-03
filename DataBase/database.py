import sys
import zmq
import mysql.connector as MySQLdb
from zmq.eventloop import ioloop, zmqstream


ioloop.install()


class Client:
    def __init__(self,database_ip,database_port, sub_socket=None, push_socket=None, name=None):
        self.name = name
        self.sub_socket = sub_socket
        self.push_socket = push_socket
        self.database_ip = database_ip
        self.database_port = database_port
        self.database = self.getDatabase()

    def setSubSocket(self, sub_socket):
        self.sub_socket = sub_socket

    def setPushSocket(self, push_socket):
        self.push_socket = push_socket

    def setName(self, name):
        self.name = name

    def changeName(self, name):
        self.name = name
        self.sub_socket.setsocketopt_string(zmq.SUBSCRIBE, name)


    def getDatabase(self):
        db = MySQLdb.connect(host=self.database_ip,
                             port=self.database_port,  # your host, usually localhost
                             user="root",  # your username
                             passwd="",  # your password
                             db="test")  # name of the data base
        return db

    def ack(self):
        self.push_socket.send_string(self.name + " ACK")

client = Client("localhost",9898)



def receiveHandler(message):
    message2 = message[0].decode()
    message2 = message2.split()[1]
    print("received the following message from server: " + str(message2))
    if message2 == "EXIT":
        ioloop.IOLoop.instance().stop()
    if message2 == "PING":
        client.ack()
    if message2 == "INSERT":
        Client.getDatabase().cursor().execute(message)
    if message2 == "SELECT":
        cursor = Client.getDatabase().cursor()
        cursor.execute(message)
        return cursor.fetchall()   ##TODO to be the configuration outtool needs



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Error: invalid number arguments, ClientName and Database Port must be provided!!!")
        exit(1)
    name = sys.argv[1]

    port1 = "5556"
    port2 = "5557"
    DatabaseIP = "localhost"
    DatabasePort = sys.argv[2]

    if len(sys.argv) > 3:
        port1 = sys.argv[3]
    if len(sys.argv) > 4:
        port2 = sys.argv[4]

    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect("tcp://localhost:%s" % port1)
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, name)
    stream = zmqstream.ZMQStream(sub_socket)
    stream.on_recv(receiveHandler)

    push_socket = context.socket(zmq.PUSH)
    push_socket.bind("tcp://*:%s" % port2)

    try:
        client.setSubSocket(sub_socket)
        client.setPushSocket(push_socket)
        client.setName(name)

        print("Client was created successfully waiting for server commands")
        print("to turn client off press CTRL+C")
        ioloop.IOLoop.instance().start()
        print("received Exit signal turning off!!!")
        sub_socket.close()
        push_socket.close()
        context.term()
        exit(0)
    except KeyboardInterrupt:
        sub_socket.close()
        push_socket.close()
        context.term()
        print("\nBye")
        exit(2)
