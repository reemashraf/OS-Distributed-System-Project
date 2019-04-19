import sys
import zmq
import mysql.connector as MySQLdb


class Client:
    def __init__(self, name, database_ip, database_port, socket):
        self.name = name
        self.database_ip = database_ip
        self.database_port = database_port
        self.database = self.setDatabase()
        self.socket = socket

    def setDatabase(self):
        return MySQLdb.connect(
            host=self.database_ip,
            port=self.database_port,
            user="root",
            passwd="root",
            db="test"
        )

    def setSocket(self, socket):
        self.socket = socket

    def setName(self, name):
        self.name = name

    def chnageName(self, name):
        self.name = name
        self.socket.setsocketopt(zmq.IDENTITY, bytes(name, 'utf-8'))

    def ack(self):
        message = self.name + "ACK"
        self.socket.send_multipart([b'server', bytes(message, 'utf-8')])

    def main(self):
        while True:
            request = self.socket.recv_multipart()
            print(request)
            request = request[0]
            message = request.decode()
            print("received: %s" % message)
            if message == "EXIT":
                break
            elif message == "PING":
                self.ack()
            elif "INSERT" in message:
                self.database.cursor().execute(message)
                # print("")
            elif "SELECT" in message:
                cursor = self.database.cursor()
                cursor.execute(message)
                print(cursor.fetchall())   ##TODO to be the configuration outtool needs
            else:
                pass

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Error: missing arguments -> ClientName, DataBaseIP and DataBasePort!!!")
        exit(1)
    name = sys.argv[1]
    database_ip = sys.argv[2]
    database_port = sys.argv[3]

    port = "5556"
    if len(sys.argv) > 5:
        port = sys.argv[4]

    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, bytes(name, 'utf-8'))
    socket.connect("tcp://localhost:%s" % port)

    client = Client(name, database_ip, database_port, socket)
    print("Client was created successfully waiting for server commands")
    print("to turn client off press CTRL+C")

    try:
        client.main()
    except KeyboardInterrupt:
        socket.close()
        context.term()
        print("\nBye")
        exit(1)

    print("received Exit signal turning off!!!")
    socket.close()
    context.term()
    print("\nBye")
    exit(0)
