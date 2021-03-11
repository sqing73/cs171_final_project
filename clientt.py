import socket
import sys
import time
server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_sock.settimeout(2)
server_sock.connect((socket.gethostname(), 2192))
a = 0
while a < 10:
    time.sleep(2)
    try:
        server_sock.send("connected".encode())
        a+=1
        if not server_sock:
            print("ddd") 
            server_sock.close()
            sys.exit() 
    except BrokenPipeError:
            print("ddd") 
            server_sock.close()
            sys.exit() 
