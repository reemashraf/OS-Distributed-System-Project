from http.server import HTTPServer, BaseHTTPRequestHandler
import json

from Client import Client


def getHtml(path):
    in_file = open(path, "rb")
    return in_file.read()

client = Client(None,None)

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, request, client_address, server):
        super().__init__(request, client_address, server)
        self.username = None
        self.mode = None
        self.filename = None
        self.videopath = None

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        html = b"Hello, World!!!" 
        if self.path == "/":
            html = getHtml("./user.html")
            self.wfile.write(html)

        # if self.path == "/":
        #     html = getHtml("./signin.html")
        #     self.wfile.write(html)
        
        elif (self.path == "/signin"):
            html = getHtml("./signin.html")
            self.wfile.write(html)
        elif (self.path == "/signup"):
            html = getHtml("./signup.html")
            self.wfile.write(html)
        elif (self.path == "/listfiles"):
            client.setmode("fileslist")
            files = client.run()
            print("data recieved")
            print(files)
            file = ''
            for element in files:
                file += element
                file += ','
            
            print("data after conversion")
            print(file)
            json_file = json.dumps(file)
            json_file = bytes(json_file, "utf-8")
            self.wfile.write(json_file)
        else:
            path = getHtml("." + str(self.path))
            self.wfile.write(path)

    def do_POST(self):
         
        if (self.path == "/user2"):
            self.send_response(200)
            self.end_headers()
            print("aywa hena")
            html = getHtml("./user.html")
            self.wfile.write(html)
        else:
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length)
            self.send_response(200)
            self.end_headers()
            if (self.path == "/user"):
                body = body.decode()
                body = body.split("&")
                self.username = body[0]
                self.username = self.username.split("=")[1]
                self.mode = body[1]  #TODO double check it if body[1] or body only 
                self.mode = self.mode.split("=")[1].lower()
                client.setusername(self.username)   
                client.setmode(self.mode)
                if (self.mode == "upload"):
                    html = getHtml("./uploadvideo.html")
                else:
                    html = getHtml("./listfiles.html")
                self.wfile.write(html)

            elif (self.path == "/signin"):
                body = body.decode()
                body = body.split("&")
                self.username = body[0]
                self.username = self.username.split("=")[1]
                password = body[1]
                password =password.split("=")[1]
                if bool(int(client.login(self.username,password))):
                    html = getHtml("./user.html")
                else:
                    html = getHtml("./Error2.html")
                self.wfile.write(html)

            elif (self.path == "/signup"):
                body = body.decode()
                body = body.split("&")
                self.username = body[0]
                self.username = self.username.split("=")[1]
                password = body[1]
                password =password.split("=")[1]
                print(password)
                print(self.username)
                if bool(int(client.signup(self.username,password))):
                    html = getHtml("./user.html")
                else:
                    #Error Exists
                    html = getHtml("./Error1.html")
                self.wfile.write(html)

            elif (self.path == "/videoupload"):
                print(body)
                body = body.decode()
                body = body.split("&")
                self.filename = body[0]
                self.filename = self.filename.split("=")[1]
                self.videopath = body[1]
                self.videopath = self.videopath.split("=")[1]
                #print(self.videopath)
                client.setfilename(self.filename)
                client.setvideopath(self.videopath)  #TODO I have to update to full path
                print(client.username)
                print(client.filename)
                print(client.videopath)
                message = client.run()
                print(message)


            elif (self.path == "/download"):
                body = body.decode()
                self.filename = body = body.split("=")[1]
                client.setfilename(self.filename)
                client.setmode("download")
                
                video = client.run()
                print(video[1:])
                html = getHtml("./videoshow1.html")
                self.wfile.write(html)
                htmlvideo = "<source src='"+video[1:] +"' type='video/mp4'>"
                htmlvideo = bytes(htmlvideo,'utf-8')
                self.wfile.write(htmlvideo)
                html = getHtml("./videoshow2.html")
                self.wfile.write(html)
            
httpd = HTTPServer(('localhost', 8000), SimpleHTTPRequestHandler)
httpd.serve_forever()