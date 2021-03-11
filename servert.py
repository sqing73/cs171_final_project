import socket
import sys
'''
my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

my_socket.bind((socket.gethostname(), 2192))
my_socket.listen(32)
c, add = my_socket.accept()
#c.settimeout(1)
while True:
    m = c.recv(1024)
    print(m)
    sys.exit()

    try:
        m = c.recv(1024)
        if not m:
            print(1)
            c.close()
            my_socket.close()
            break
        print(m)
    except socket.error:
        print("DDD")
        c.close()
        sys.exit()
'''
s = ""
if s == "":
    print(1)