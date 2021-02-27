import socket
import os
import json
import queue
import sys
import threading

MY_ID = ""
SERVER_SOCKETS = {}

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
    # connect with all servers
    with open("config.json", "r") as f:
        config = json.load(f)
        f.close()

    for i in range(1, 6):
        server_id = str(i)
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_sock.connect((socket.gethostname(), config[server_id]))
        except ConnectionError:
            print("server{} is not extablished".format(server_id))
            sys.exit()
        SERVER_SOCKETS[server_id] = server_sock
        message = "client " + MY_ID + "/id"
        
        server_sock.send(message.encode())
        threading.Thread(target=receive_message, args=(server_sock))

    while True:
        inp = input("Usage : send <server> <id/all>/message\n")
        inp = inp.split("/")
        cmd = inp[0] # send server 1
        message = inp[1]
        cmd = cmd.split()
        receiver_id = cmd[2]
        to_send = "client " + MY_ID + "/" + message # server 1/put s : h
        print("send to server {}/{}".format(cmd[2], message))
        send_message(receiver_id, to_send)

    
        