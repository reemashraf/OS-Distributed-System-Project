import sys
import zmq
import json
import mysql.connector as MySQLdb


def getInsertValues(query):
        query_arr = query.split()
        if (query_arr[0] != "INSERT") or (query_arr[1] != "INTO") or (query_arr[2] != "users"):
            return False

        params = ""
        for i in range(3, len(query_arr)):
            if i == len(query_arr)-1:
                params = params + query_arr[i]
            else:
                params = params + query_arr[i] + ' '

        values = params
        columns = None
        username = 0
        password = 1
        if "VALUES" in params:
            params = params.split("VALUES")
            columns = params[0]
            values = params[1]

            columns = columns.replace(" ", "")
            columns = columns.replace("(", "")
            columns = columns.replace(")", "")
            columns = columns.split(",")

            if "username" not in columns:
                return False
            if "password" not in columns:
                return False

            for i in range(len(columns)):
                if columns[i] == "username":
                    username = i
                if columns[i] == "password":
                    password = i

        values = values.replace(" ", "")
        values = values.replace("(", "")
        values = values.replace(")", "")
        values = values.split(",")

        for value in values:
            if value[0] != "'" or value[len(value)-1] != "'":
                return False

        temp = []
        for value in values:
            temp.append(value.replace("'", ""))
        values = temp
        auth_params = []
        auth_params.append(values[username])
        auth_params.append(values[password])
        return tuple(auth_params)


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
    socket.connect("tcp://192.168.1.13:%s" % port)

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
