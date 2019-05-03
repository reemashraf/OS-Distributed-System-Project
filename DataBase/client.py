import sys
import zmq
import json
from parser import getInsertValues
import mysql.connector as MySQLdb


class Client:
    def __init__(self, name, database_ip, database_port, database_name, socket):
        self.name = name
        self.database_ip = database_ip
        self.database_port = database_port
        self.database_name = database_name
        self.database = self.setDatabase()
        self.socket = socket

    def setDatabase(self):
        return MySQLdb.connect(
            host=self.database_ip,
            port=self.database_port,
            user="root",
            passwd="",
            db=self.database_name
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
        print("sending ack!!!")
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
                values = getInsertValues(message)
                print("INSERT VALUES: ", end=" ")
                print(values)
                if values:
                    sqlFormula = "INSERT INTO users (username, password) VALUES (%s, %s)"
                    try:
                        self.database.cursor().execute(sqlFormula, values)
                    except:
                        print("failed to insert data!!!")
                    self.database.commit()
            elif "SELECT" in message:
                cursor = self.database.cursor()
                cursor.execute(message)
                data = cursor.fetchall()   # TODO to be the configuration outtool needs
                data = json.dumps(data)
                data = bytes(data, 'utf-8')
                self.socket.send_multipart([b'server', data])
            else:
                pass

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Error: missing arguments -> ClientName, DataBaseIP, DataBasePort and DataBaseName!!!")
        exit(1)
    name = sys.argv[1]
    database_ip = sys.argv[2]
    database_port = sys.argv[3]
    database_name = sys.argv[4]

    port = "5556"
    if len(sys.argv) > 5:
        port = sys.argv[5]

    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, bytes(name, 'utf-8'))
    socket.connect("tcp://192.168.43.113:%s" % port)

    client = Client(name, database_ip, database_port, database_name, socket)
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
