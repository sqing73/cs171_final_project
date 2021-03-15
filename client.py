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
RECEIVED = True
TIMEOUT = 0
MESSAGE = ""

def receive_message(server_sock):
    global RECEIVED
    global MESSAGE
    while True:
        message = server_sock.recv(1024).decode()
        RECEIVED = True
        MESSAGE = message
        #print("receive from " + message)

# thread for timer of a message
def timer():
    global TIMEOUT
    for i in range(100):
        time.sleep(1)
        if RECEIVED == True:
            return 

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
    LEADER = str(i)
    threading.Thread(target=receive_message, args=(server_sock,)).start()
    #threading.Thread(target=receive_message, args = (server_sock,)).start()

    while True:
        inp = input("please input: ")
        #from client: client id/operation:put,key,value,id or get,key,id or leader
        operation_id = str(randint(1, 1100))
        inp += "," + operation_id
        to_send = "client " + MY_ID + "/" + inp # server 1/put,key,value
        print("send to server {}/{}".format(LEADER, inp))
        start = time.time()
        #time.sleep(4)

        RECEIVED = False
        TIMEOUT = 25
        MESSAGE = ""
        t = threading.Thread(target=timer)
        server_sock.send(to_send.encode())
        t.start()
        t.join(timeout=TIMEOUT)
        end = time.time()
        # time out, ask a server to become a leader
        if RECEIVED == False:
            print("timeout")
            to_send = "client " + MY_ID + "/leader"
            print("send to server {}/leader".format(LEADER))
            RECEIVED = False
            TIMEOUT = 20
            MESSAGE = ""
            t = threading.Thread(target=timer)
            server_sock.send(to_send.encode())
            t.start()
            t.join(timeout=TIMEOUT)

            m = MESSAGE.split("/")
            # I am connecting to a leader, ask another one to become leader, or the server I connect is partitioned
            if m[1] == "other" or RECEIVED == False:
                print("receive from" + MESSAGE)
                server_sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                ids = SERVER_IDS
                ids.remove(LEADER)
                port = config[ids[0]]
                LEADER = ids[0]
                server_sock1.connect((socket.gethostname(), port))
                threading.Thread(target=receive_message,args=(server_sock1,)).start()
                print("Connected with server "+LEADER)
                server_sock1.send("client {}/id".format(MY_ID).encode())
                MESSAGE = ""
                time.sleep(2)
                server_sock1.send("client {}/leader".format(MY_ID).encode())
                print("send to server {}/leader".format(LEADER))

                TIMEOUT = 50
                RECEIVED = False

                t = threading.Thread(target=timer)
                t.start()
                t.join(timeout=TIMEOUT)
                m = MESSAGE.split("/")
                if not RECEIVED:
                    print("still no message, I don't know what's wrong. Quitting...")
                    sys.exit()
                if m[1]=="success":
                    print("receive from " + MESSAGE)
                    print("send to server {}/{}".format(LEADER, inp))
                    to_send = "client " + MY_ID + "/" + inp
                    server_sock1.send(to_send.encode())
                TIMEOUT = 20
                RECEIVED = False

                t = threading.Thread(target=timer)
                t.start()
                t.join(timeout=TIMEOUT)
                

            # leader success, resend request
            elif m[1] == "success":
                print("receive from " + MESSAGE)
                to_send = "client " + MY_ID + "/" + inp # server 1/put,key,value= 
                print("send to server {}/{}".format(LEADER, inp))

                RECEIVED = False
                TIMEOUT = 20
                MESSAGE = ""
                server_sock.send(to_send.encode())
                t = threading.Thread(target=timer)
                t.start()
                t.join(timeout=TIMEOUT)
            end = time.time()
        print("receive from " + MESSAGE)
        print("time used: {}".format(end - start))