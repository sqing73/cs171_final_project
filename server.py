import socket
import os
import json
import queue
import sys
import threading
import time

MY_ID = ""
SOCKETS_SEND = {}       # server sockets for send
#SOCKETS_RECEIVE= {}     server sockets for receive
SOCKETS_CLIENTS = {}    # clients sockets for send and receive

def receive_message(server_receive):
    #global SOCKETS_RECEIVE
    global SOCKETS_CLIENTS
    #global MY_ID

    while True:
        message = server_receive.recv(1024).decode()
        print("receive from " + message)
        message = message.split("/")
        # for confirm client id
        # format: client client_id/id
        if message[1] == "id": # client 1, id
            sender = message[0]
            sender_id = sender.split()[1]
            if sender.split()[0] == "client":
                SOCKETS_CLIENTS[sender_id] = server_receive
            print(sender + " connected")

        

def send_message(message, receiver_id = "all", to_client = False):
    global SOCKETS_SEND
    global SOCKETS_CLIENTS
    # send message to servers
    if not to_client:
        if receiver_id == "all":
            servers = list(SOCKETS_SEND.values())
            for server in servers:
                server.send(message.encode())
        else:
            server = SOCKETS_SEND[receiver_id]
            server.send(message.encode())
    else:
        client = SOCKETS_CLIENTS[receiver_id]
        client.send(message.encode())


def handle_input():
    global MY_ID
    while True:
        inp = input("Usage: send <server/client> <id/all>/message\n")
        inp = inp.split("/")
        cmd = inp[0] # send client 1
        message = inp[1]
        cmd = cmd.split()
        receiver_id = cmd[2]
        to_client = False
        if cmd[1] == "client":
            to_client = True
        to_send = "server " + MY_ID + "/" + message # server 1/put s : h
        print("send to {} {}/{}".format(cmd[1], cmd[2], message))
        send_message(to_send, receiver_id, to_client)
        
        


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python {} <server_id>".format(sys.argv[0]))
        sys.exit()

    MY_ID = sys.argv[1]

    with open("config.json", "r") as f:
        config = json.load(f)
        f.close()
    
    others = ["1", "2", "3", "4", "5"]
    others.remove(MY_ID)
    # listen to other servers
    my_port = config[MY_ID]
    my_socket = socket.socket()
    my_socket.bind((socket.gethostname(), my_port))
    my_socket.listen(32)

    # wait for connect command
    while input("please input connect: ") != "connect":
        print("Please establish other servers")
    
    # connect with other servers for send
    for s in others:
        server_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_send.connect((socket.gethostname(), config[s]))
        server_send.send(("server " + MY_ID + "/id").encode())
        SOCKETS_SEND[s] = server_send

    threading.Thread(target=handle_input).start()
    # connect with other servers for receive
    # connect with other clients for receive and send
    while True:
        server_receive, address = my_socket.accept()
        threading.Thread(target=receive_message, args=(server_receive,)).start()

