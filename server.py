import socket
import os
import json
import sys
import threading
import time
from block import Block
import queue

MY_ID = ""
SOCKETS_SEND = {}       # server sockets for send
SOCKETS_CLIENTS = {}    # clients sockets for send and receive
OPERATIONS = []         # queue for sender/block_info
STORE = {}
BLOCKCHAIN = [Block.get_genesis()]
BALLOT = [0, 0, 0]     # <seq_num, process_id, depth>
                        # fist compare depth then seq_num then id
LINKS = {}              # which links are connected

LEADER = "5"            # current leader, set to 5 at beginning
ACCEPTNUM = []
ACCEPTVAL = ""
COUNTACC = 0            # count number of acceptors
COUNTPRO = 0            # count number of promise
MYVAL = ""              # sender1(_sender2) block_info
DECIDING = False
# commands for communication between servers, no commands between client and server
CMD = ["forward", "accept", "accepted"]
FORMATS = {"forward":"server {}/forward/server {},client {}/{}",# sender id, requestor1 id, requestor2 id, operation
            "accept":"server {}/accept/{}/{}/{}",               # sender id, ballot, (requestors, block_info) val
            "accepted":"server {}/accepted/{}/{}/{}",           # sender id, accpet ballot, (requestors, block_info) val
            "reply":"server {}/reply/{}/{}",                    # sender id, requestors, reply
            "decide":"server {}/decide/{}/{}/{}"                # sender id, ballot, (requestors, block_info) val
        }
def receive_message(server_receive, lock):
    #global SOCKETS_RECEIVE
    global SOCKETS_CLIENTS
    global BLOCKCHAIN
    global OPERATIONS
    global MY_ID
    global LEADER
    global BALLOT
    global ACCEPTNUM
    global ACCEPTVAL
    global COUNTPRO
    global LEADER
    global MYVAL
    global DECIDING
    global FORMATS
    global STORE
    global COUNTACC
    while True:
        # from sender: sender id/cmd/ballot/requestors/val
        # requestors = requestor1 requestor2
        # ballot = seq_num process_id depth
        # val = {"NONCE": , "OPERATION": , "ID": , "HASH": }
        # from client: client id/operation:put,key,value,id or get,key,id or leader 
        m = server_receive.recv(1024).decode()
        print("receive from " + m)
        message = m.split("/")
        # for confirm client id
        # format: client client_id/id
        sender = message[0].split()
        sender_id = sender[1]
        sender_type = sender[0]
        cmd = message[1]
        if message[1] == "id": # client 1, id
            if sender_type == "client":
                SOCKETS_CLIENTS[sender_id] = server_receive
                #send_message("shit", sender_id, True)
            print(message[0] + " connected")
            next
        # a message from or originally from a client to request an operation
        if sender_type == "client" or cmd == "forward":
            lock.acquire()
            operation = message[-1]
            operation = operation.split(",")
            if operation[0] == "put" or operation[0] == "get": # put, key, value, operation_id
                # I am not leader and I will forward operation to actual leader, must be from a client
                # piggy back my id and reply client through me, not leader
                if MY_ID != LEADER:
                    to_send = FORMATS["forward"].format(MY_ID, MY_ID, sender_id, message[-1])
                    send_message(to_send, LEADER)
                else:
                    # I am leader
                    # check if this operation is already in block chain
                    # if in, don't start the accept again 
                    stored = False
                    operation_id = operation[-1]
                    for block in BLOCKCHAIN:
                        if operation_id == block.op_id:
                            stored = True
                            break
                    if not stored:
                       
                        # push operation to queue 
                        # maybe from a client direcly or forwarded by another server
                        requestors = ""
                        if len(message) == 2:
                            requestors = message[0]
                        else:
                            requestors = message[2]
                        op = message[-1]
                        requestors, block_info = generate_value(requestors, op)
                        val = requestors + "/" + block_info
                        OPERATIONS.append(val)
                        if not DECIDING:
                            DECIDING = True
                            MYVAL = val
                            to_send = FORMATS["accept"].format(MY_ID, ballot_toString(BALLOT), requestors, block_info)
                            send_message(to_send)
            lock.release()
        elif cmd == "accept":
            pass
            b = string_toBallot(message[2])
            if compare_ballot(BALLOT, b):
                lock.acquire()
                ACCEPTNUM = b
                v = message[-2] + "/" + message[-1]
                ACCEPTVAL = v
                # create a new block and tag it as tentative
                block_info = json.loads(message[-1])
                block = Block(block_info["HASH"], block_info["OPERATION"], block_info["ID"], block_info["NONCE"])
                BLOCKCHAIN.append(block)
                update_chain_file()
                to_send = FORMATS["accepted"].format(MY_ID, ballot_toString(ACCEPTNUM), message[-2],message[-1])
                send_message(to_send, sender_id)
                lock.release()
        elif cmd == "accepted":
            if DECIDING:
                lock.acquire()
                COUNTACC += 1
                # reach majority, append block to file 
                if COUNTACC >= 3:
                    block_info = json.loads(message[-1])
                    block = Block(block_info["HASH"], block_info["OPERATION"], block_info["ID"], block_info["NONCE"], True)
                    
                    BLOCKCHAIN.append(block)
                    OPERATIONS.pop(0)
                    DECIDING = False
                    COUNTACC = 0
                    MYVAL = ""
                    update_chain_file()
                    # update store
                    operation = block_info["OPERATION"].split(",")
                    # generate reply 
                    reply = ""
                    if operation[0] == "put":
                        key = operation[1]
                        value = operation[2]
                        STORE[key] = value
                        reply = "ack"
                    elif operation[0] == "get":
                        key = operation[1]
                        if key in STORE:
                            reply = "{}'s phone number:{}.".format(key, STORE[key])
                        else:
                            reply = "{}'s phone number cannot be found.".format(key)
                    requestors = message[-2].split("_")
                    requestor1 = requestors[0]
                    # if request is from a server, send reply to a server first then to the client
                    if len(requestors) == 2:
                        to_send = FORMATS["reply"].format(MY_ID, message[-2], reply)
                        send_message(to_send, requestor1.split()[1])
                    # send reply to client directly
                    else:
                        to_send = "server {}/{}".format(MY_ID, reply)
                        send_message(to_send, requestor1.split()[1], True)
                    # send decide to all other servers
                    b = message[2]
                    to_send = FORMATS["decide"].format(MY_ID, b, message[-2], message[-1])
                    send_message(to_send)
                    # if there are other operations in queue, start phase 2
                    if len(OPERATIONS) > 0 and not DECIDING:
                        DECIDING = True
                        val = OPERATIONS[0].split("/")
                        requestors = val[0]
                        block_info = val[1]
                        MYVAL = val
                        to_send = FORMATS["accept"].format(MY_ID, ballot_toString(BALLOT), requestors, block_info)
                        send_message(to_send)
                lock.release()
        elif cmd == "reply":
            # a request is sent through me, I reply to client
            requestors = message[-2].split("_")
            requestor2 = requestors[1]
            client_id = requestor2.split()[1]
            to_send = "server {}/{}".format(MY_ID, message[-1])
            send_message(to_send, client_id, True)
        elif cmd == "decide":
            # set block to decided and uodatae store
            block_info = json.loads(message[-1])
            for i in range(len(BLOCKCHAIN)):
                if BLOCKCHAIN[i].op_id == block_info["ID"]:
                    #print(BLOCKCHAIN[i].op_id, block_info["ID"])
                    lock.acquire()
                    BLOCKCHAIN[i].decide()
                    #print(BLOCKCHAIN[i].decided)
                    op = BLOCKCHAIN[i].operation.split(",")
                    if op[0] == "put":
                        STORE[op[1]] = op[2]
                    lock.release()
            update_chain_file()





            
        # I try to become leader, phase1
        '''
        elif operation[0] == "leader":

            BALLOT[0] += 1
            to_send = "server {}/prepare/{} {} {}".format(MY_ID, BALLOT[0], BALLOT[1], BALLOT[2])
            send_message(to_send.encode())
            COUNTING = True
        elif operation[0] == "prepare":
            b = message[2].split()
            bal = [int(b[0]), b[1], int(b[2])]
            if compare_ballot(BALLOT, bal):
                BALLOT = bal
                to_send = "server {}/promise/{}/{}/{}".format(MY_ID, message[2], ballot_toString(ACCEPTNUM), ACCEPTVAL)
                LEADER =  sender_id
                send_message(to_send, receiver_id=LEADER)
        # collect all promise for 
        #
        #
        #
        #i give up, complete phase 2 first
        elif operation[0] == "promise":
            if COUNTING:
                COUNT += 1
                # if receive from all nodes of a quorum, 
                # leader successful, start phase 2
                if COUNT == 3:
                    COUNTING = False
                    COUNT = 0
                    MYVAL = message[2]
        elif operation[0] == "fail":
            LINKS[sender_id] = False
        elif operation[0] == "fix":
            LINKS[sender_id] = True
        lock.release()
'''
# thread for clearing operations in queue and start from MYVAL
def generate_value(requestors, operation):
    # use _ to partition two requestors
    requestors = requestors.split(",")
    requestors = "_".join(requestors)
    # convert operation to block info
    operation_id = operation.split(',')[-1]
    block = Block(BLOCKCHAIN[-1].after_hash, operation, operation_id)
    block_info = block.toString()
    return requestors, block_info

# ballot2 >= ballot1, true
def compare_ballot(ballot1, ballot2):
    if ballot2[2] >= ballot1[2] or ballot2[0] >= ballot1[0] or ballot2[1] >= ballot2[1]:
        return True
    return False

def ballot_toString(ballot):
    return "{} {} {}".format(ballot[0], ballot[1], ballot[2])

def string_toBallot(ballot_s):
    ballot = ballot_s.split()
    return [int(ballot[0]), int(ballot[1]), int(ballot[2])]

def update_chain_file():
    global BLOCKCHAIN
    global MY_ID
    chain_file = []
    filename = "blockchain{}.json".format(MY_ID)
    with open(filename, "w") as f:
        for block in BLOCKCHAIN:
            if block.operation != "0": # don't put genesis block on disk
                block_info = {"NONCE":block.nonce, "OPERATION":block.operation, "HASH":block.previous_hash, "DECIDED":block.decided, "ID":block.op_id }
                chain_file.append(block_info)
        json.dump(chain_file, f, indent=4)
        f.close()

def send_message(message, receiver_id = "all", to_client = False):
    global SOCKETS_SEND
    global SOCKETS_CLIENTS
    # send message to servers
    time.sleep(4)
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
        '''
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
        '''
        global OPERATIONS
        global LINKS
        inp = input("please input: cmd,id")
        inp = inp.split(",")
        if inp[0] == "queue":
            print(OPERATIONS)
        elif inp[0] == "failLink":
            LINKS[inp[1]] = False
            # ask dest to fail
            message = "server {}/fail".format(MY_ID)
            id = inp[1]
            send_message(message, id)
        elif inp[0] == "fixLink":
            LINKS[inp[1]] = True
            # ask dest to fix
            message = "server {}/fix".format(MY_ID)
            id = inp[1]
            send_message(message, id)
        elif inp[0] == "failProcess":
            pass
        else:
            print("wrong input")
        
        


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python {} <server_id>".format(sys.argv[0]))
        sys.exit()

    MY_ID = sys.argv[1]
    BALLOT[1] = int(MY_ID)

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

    # reconstruct blockchain from disk
    chain_file = "blockchain{}.json".format(MY_ID)
    with open(chain_file, "r") as f:
        stored = json.load(f)
        f.close()
    i = 1
    for block_info in stored:
        # check valid
        #print(block_info)
        prev_hash = BLOCKCHAIN[i-1].after_hash
        stored_hash = block_info["HASH"]
        if prev_hash == stored_hash:
            #print(1)
            block = Block(prev_hash, block_info["OPERATION"], block_info["ID"], block_info["NONCE"], block_info["DECIDED"])
            BLOCKCHAIN.append(block)
        i += 1
    # reconstruct kv store based on filed blockchain
    for block in BLOCKCHAIN:
        op = block.operation
        #print(op)
        op = op.split(",")
        if op[0] == "put" and block.decided:
            STORE[op[1]] = op[2]
    print(STORE)
    
    BALLOT[2] = i-1 # record current depth of blockchain


    # wait for connect command
    while input("please input connect: ") != "connect":
        print("Please establish other servers")
    
    # connect with other servers for send
    for s in others:
        server_send = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_send.connect((socket.gethostname(), config[s]))
        server_send.send(("server " + MY_ID + "/id").encode())
        SOCKETS_SEND[s] = server_send
        LINKS[s] = True


    threading.Thread(target=handle_input).start()
    # connect with other servers for receive
    # connect with other clients for receive and send
    lock = threading.Lock()
    while True:
        server_receive, address = my_socket.accept()
        threading.Thread(target=receive_message, args=(server_receive, lock,)).start()

