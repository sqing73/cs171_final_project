import socket
import sys

my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

my_socket.bind((socket.gethostname(), 2196))
my_socket.listen(32)
c, add = my_socket.accept()
i = 3

while True:
    c.settimeout(i)
    m = c.recv(1024)
    print(m)
    #i += 3
"""
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
"""