import socket
import os
import json
import queue
import sys
import threading
import time
from random import randint

MY_ID = ""
SERVER_SOCKETS = {}
SERVER_IDS = ["1", "2", "3", "4", "5"]
LEADER = "1" # leader hint, 1 at beginning

def receive_message(server_sock):
    while True:
        message = server_sock.recv(1024).decode()
        print("receive from " + message)
    
def send_message(server_id, message):
    global SERVER_SOCKETS
    global MY_ID
    
    if server_id == "all":
        servers = list(SERVER_SOCKETS.values())
        for server in servers:
            server.send(message.encode())
    else:
        server_sock = SERVER_SOCKETS[server_id]
        server_sock.send(message.encode())


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Usage: python {} <client_id>".format(sys.argv[0]))
        sys.exit()

    MY_ID = sys.argv[1]

    with open("config.json", "r") as f:
        config = json.load(f)
        f.close()

    # connect to leader hint
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # if leader crashes, try other servers
    i = int(MY_ID)
    while True:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if i > 5:
            print("All servers down")
            sys.exit()
        try:
            server_sock.connect((socket.gethostname(), config[str(i)]))
            print("Connected with server "+str(i))
            LEADER = str(i)
            server_sock.send("client {}/id".format(MY_ID).encode())
            break
        except ConnectionRefusedError:
            i += 1

    #threading.Thread(target=receive_message, args = (server_sock,)).start()

    while True:
        inp = input("please input: ")
        #from client: client id/operation:put,key,value,id or get,key,id or leader
        operation_id = str(randint(0, 1100))
        inp += "," + operation_id
        to_send = "client " + MY_ID + "/" + inp # server 1/put,key,value
        print("send to server {}/{}".format(LEADER, inp))
        start = time.time()
        time.sleep(4)
        server_sock.send(to_send.encode())
        message = server_sock.recv(1024).decode()
        end = time.time()

        print(message, "time:{}".format(end - start))
        #server_sock.settimeout(6) # 6s to time out
        '''
        try:
            message = server_sock.recv(1024).decode()
            print(message)
        except socket.error:
            print("time out, sending leader to {}".format(i))
            server_sock.settimeout(8) # set longer timeout for leader election and adjust for appropriate delay
            server_sock.send("client {}/leader".format(MY_ID))
            # wait for leader confirm
            try:
                message = server_sock.recv(1024).decode()
                if message == "server {}/new leader":
                    # resend operation to new leader
                    server_sock.send(to_send.encode())
                    
                    message = server_sock.recv(1024).decode()
                    print(message)
            except socket.error:
                print("Netwoking error, quitting..")
                sys.exit()
        '''
    