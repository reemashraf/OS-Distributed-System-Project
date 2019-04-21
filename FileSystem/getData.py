#!C:\Users\Reem\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Python 3.6.exe
# Import modules for CGI handling
import cgi, cgitb
# Create instance of FieldStorage
form = cgi.FieldStorage()
# Get data from fields
first_name = form.getvalue('first_name')
last_name  = form.getvalue('last_name')
print("Content-type:text/html")
print
print("")
print("")
print("Hello - Second CGI Program")
print("")
print("")
print("Hello %s %s" % (first_name, last_name))
print("")
print("")