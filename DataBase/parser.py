import sys
import zmq
import time

shards = [
    "a-g",
    "h-m",
    "n-t",
    "u-z"
]

doc = """==========================================================================================================================================
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


class Parser:
    def __init__(self):
        pass

    def insert(self, query):
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
        index = 0
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

            for i in range(len(columns)):
                if columns[i] == "username":
                    index = i

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

        username = values[index]
        userkey = username[0].lower()
        shard = 0
        for key in shards:
            shard = shard + 1
            symbols = key.split("-")
            if userkey >= symbols[0] and userkey <= symbols[1]:
                return shard

        return False

    def select(self, query):
        query_arr = query.split()
        if query_arr[0] != "SELECT":
            return False

        params = ""
        for i in range(1, len(query_arr)):
            if i == len(query_arr)-1:
                params = params + query_arr[i]
            else:
                params = params + query_arr[i] + ' '

        if ("FROM" not in params) or ("WHERE" not in params):
            return False

        params = params.split("FROM")
        params = params[1]
        params = params.split("WHERE")

        if (len(params) != 2) or (params[0].replace(" ", "") != "users"):
            return False

        username = params[1].replace(" ", "")
        if "username=" not in username:
            return False

        username = username.split("=")[1]
        username = username.replace("'", "")
        userkey = username[0].lower()

        shard = 0
        for key in shards:
            shard = shard + 1
            symbols = key.split("-")
            if userkey >= symbols[0] and userkey <= symbols[1]:
                return shard

        return False


if __name__ == "__main__":
    port = "5558"
    if len(sys.argv) > 1:
        port = sys.argv[1]

    # Socket to talk to server
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://*:%s" % port)

    parser = Parser()
    print(doc)
    while True:
        query = input("> ")
        if query == "":
            continue
        query_arr = query.split()
        instruction = query_arr[0]

        if instruction == "INSERT":
            shard = parser.insert(query)
            message = "CLI " + str(shard) + " " + query
            socket.send_string(message)
        elif instruction == "SELECT":
            shard = parser.select(query)
            message = "CLI " + str(shard) + " " + query
            socket.send_string(message)
        else:
            print("Invalid Instruction!!!")
