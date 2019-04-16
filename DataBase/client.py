import sys
import zmq
from zmq.eventloop import ioloop, zmqstream
ioloop.install()


class Client:
    def __init__(self, sub_socket=None, push_socket=None, name=None):
        self.name = name
        self.sub_socket = sub_socket
        self.push_socket = push_socket

    def setSubSocket(self, sub_socket):
        self.sub_socket = sub_socket

    def setPushSocket(self, push_socket):
        self.push_socket = push_socket

    def setName(self, name):
        self.name = name

    def chnageName(self, name):
        self.name = name
        self.sub_socket.setsocketopt_string(zmq.SUBSCRIBE, name)

    def ack(self):
        self.push_socket.send_string(self.name + " ACK")


client = Client()


def receiveHandler(message):
    message = message[0].decode()
    message = message.split()[1]
    print("received the following message from server: " + str(message))
    if message == "EXIT":
        ioloop.IOLoop.instance().stop()
    if message == "PING":
        client.ack()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: invalid number arguments, ClientName must be provided!!!")
        exit(1)
    name = sys.argv[1]

    port1 = "5556"
    port2 = "5557"
    if len(sys.argv) > 2:
        port1 = sys.argv[2]
    if len(sys.argv) > 3:
        port2 = sys.argv[3]

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
