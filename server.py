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
ACCEPTNUM = [0 ,0, 0]
ACCEPTVAL = ""
COUNTACC = 0            # count number of acceptors
COUNTPRO = 0            # count number of promise
MYVAL = ""              # sender1(_sender2)/block_info
DECIDING = None         # in deciding next val
DECIDINGL = False       # in deciding next leader
DEPTHS = {}             # store depth of each server in phase 1
CLIENT = ""             # client who starts leader phase
# commands for communication between servers, no commands between client and server
FORMATS = {"forward":"server {}/forward/server {}_client {}/{}",# sender id, requestor1 id, requestor2 id, operation
            "accept":"server {}/accept/{}/{}/{}",               # sender id, ballot, (requestors, block_info) val
            "accepted":"server {}/accepted/{}/{}/{}",           # sender id, accpet ballot, (requestors, block_info) val
            "reply":"server {}/reply/{}/{}",                    # sender id, requestors, reply
            "decide":"server {}/decide/{}/{}/{}",               # sender id, ballot, (requestors, block_info) val
            "prepare":"server {}/prepare/{}",                   # sender id, ballot
            "promise":"server {}/promise/{}/{}/{}/{}",          # sender id, ballot, acceptnum, (requestors, block_info) acceptval 
            "chain":"server {}/chain",                          # sender id
            "rechain":"server {}/rechain/{}",                   # sender id, all block info
            "update":"server {}/update/{}"                      # sender id, all block info
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
    global COUNTPRO
    global DECIDINGL
    global DEPTHS
    global CLIENT
    global LINKS
    while True:
        # from sender: sender id/cmd/ballot/requestors/val
        # requestors = requestor1 requestor2
        # ballot = seq_num process_id depth
        # val = {"NONCE": , "OPERATION": , "ID": , "HASH": }
        # from client: client id/operation:put,key,value,id or get,key,id or leader 
        m = server_receive.recv(1024).decode()
        #print("receive from " + m + "\n")
        message = m.split("/")
        # for confirm client id
        # format: client client_id/id
        sender = message[0].split()
        sender_id = sender[1]
        sender_type = sender[0]
        cmd = message[1]
        if sender_type == "server" and LINKS[sender_id] == False:
            m = ""
            message = ["", ""]
            sender_type = ""
            sender_id = ""
            cmd = ""
        if m != "":
            print("receive from " + m + "\n")
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
                    # if in, don't append to the queue and start phase2 directly 
                    
                    stored = False
                    operation_id = operation[-1]
                    storeblock = None
                    for block in BLOCKCHAIN:
                        if operation_id == block.op_id:
                            stored = True
                            storeblock = block
                            break
                    
                    requestors = ""
                    block_info = ""
                    val = ""
                    if not stored:   
                        # push operation to queue 
                        # maybe from a client direcly or forwarded by another server
                        if len(message) == 2:
                            requestors = "server {}_".format(MY_ID) + message[0]
                        else:
                            requestors = message[-2]
                        op = message[-1]
                        # convert operation to block info
                        # if there are operation in queue ahead of this one, its hash should be previous one's
                        # if no, its hash should be last one in blockchain
                        prev_hash = ""
                        if len(OPERATIONS)>0:
                            prev_block_info = json.loads(OPERATIONS[-1].split("/")[1])
                            prev_block = Block(prev_block_info["HASH"], prev_block_info["OPERATION"], prev_block_info["ID"], prev_block_info["NONCE"])
                            prev_hash = prev_block.after_hash
                        else:
                            prev_hash = BLOCKCHAIN[-1].after_hash
                        block = Block(prev_hash, op, operation_id)
                        block_info = block.toString()
                        val = requestors + "/" + block_info
                        OPERATIONS.append(val)
                    else:
                        # if it is already stored, then its val is in myval
                        requestors = MYVAL.split("/")[0]
                        block_info = MYVAL.split("/")[1]
                        val = MYVAL
                    if not DECIDING:
                        DECIDING = operation_id
                        MYVAL = val
                        #print(111)
                        to_send = FORMATS["accept"].format(MY_ID, ballot_toString(BALLOT), requestors, block_info)
                        send_message(to_send)
            # client time out, ask me to become leader
            elif operation[0] == "leader":
                # I am lader, no need to start phase1, either I am partitioned or too many operations in queue
                # ask client to request another one to become leader
                if LEADER == MY_ID:
                    to_send = "server {}/other".format(MY_ID)
                    send_message(to_send, sender_id, True)
                else:
                    # I am not leader, start phase1, try to become leader
                    # update ballot
                    BALLOT[2] = len(BLOCKCHAIN)-1
                    BALLOT[1] = MY_ID
                    BALLOT[0] += 1
                    CLIENT = sender_id
                    to_send = FORMATS["prepare"].format(MY_ID, ballot_toString(BALLOT))
                    COUNTPRO = 0
                    DECIDINGL = True
                    DEPTHS.clear()
                    send_message(to_send)
            lock.release()            
        elif  cmd == "prepare":
            lock.acquire()
            bal = string_toBallot(message[2])
            if compare_ballot(BALLOT, bal):
                # if I am in phase 1 to compete for leader, stop it
                # if I am processng some message, stop it
                DECIDINGL = False
                BALLOT = bal
                LEADER = sender_id
                DECIDING = False
                accept_v= ACCEPTVAL.split("/")
                accept_r = ""
                accept_b = ""
                if ACCEPTVAL != "":
                    accept_r = accept_v[0]
                    accept_b = accept_v[1]
                to_send = FORMATS["promise"].format(MY_ID, ballot_toString(BALLOT), ballot_toString(ACCEPTNUM), accept_r, accept_b)
                send_message(to_send, sender_id)
            lock.release()
        elif cmd == "promise":
            lock.acquire()
            aval = message[-2] + "/" + message[-1]
            anum = message[-3]
            if  aval != "/":
                MYVAL = aval
            b = string_toBallot(message[2])
            DEPTHS[sender_id] = b[2]
            if DECIDINGL:
                COUNTPRO += 1
                # reach majority ask the server with most depth to send its chain
                if COUNTPRO >= 2:
                    DECIDINGL = False
                    LEADER = MY_ID
                    d = list(DEPTHS.values())
                    maxd = max(d)
                    max_id = 0
                    for key in DEPTHS:
                        if DEPTHS[key] == maxd:
                            max_id = key
                    to_send = FORMATS["chain"].format(MY_ID)
                    send_message(to_send, max_id)
            lock.release()
        elif cmd == "chain":
            # I have the longest chain, send my chain to leader
            block_infos = ""
            for i in range(len(BLOCKCHAIN)):
                if i != 0:
                    block_infos += "_" + BLOCKCHAIN[i].toString()
                else:
                    block_infos += BLOCKCHAIN[i].toString()
            to_send = FORMATS["rechain"].format(MY_ID, block_infos)
            send_message(to_send, sender_id)
        elif cmd == "rechain":
            lock.acquire()
            block_infos = message[-1].split("_")
            #print(block_infos[0])
            BLOCKCHAIN.clear()
            for b in block_infos:
                #print(b)
                block_info = json.loads(b)
                block = Block(block_info["HASH"], block_info["OPERATION"], block_info["ID"], block_info["NONCE"], block_info["DECIDED"])
                BLOCKCHAIN.append(block)
            update_chain_file()
            # tell all servers to update chain
            to_send = FORMATS["update"].format(MY_ID, message[-1])
            send_message(to_send)
            to_send = "server {}/success".format(MY_ID)
            print(CLIENT)
            send_message(to_send, CLIENT, True)
            # start pahse 2 to accept myval, else wait for
            ''' 
            if MYVAL != "":
                requestors = MYVAL.split("/")[0]
                op = MYVAL.split("/")[1]
                to_send = FORMATS["accept"].format(MY_ID, ballot_toString(BALLOT), requestors, op)
            '''
            lock.release()
        elif cmd == "update":
            # update chain 
            lock.acquire()
            BLOCKCHAIN.clear()
            block_infos = message[-1].split("_")
            for b in block_infos:
                block_info = json.loads(b)
                block = Block(block_info["HASH"], block_info["OPERATION"], block_info["ID"], block_info["NONCE"], block_info["DECIDED"])
                BLOCKCHAIN.append(block)
            update_chain_file()
            lock.release()

        elif cmd == "accept":
            b = string_toBallot(message[2])
            if compare_ballot(BALLOT, b):
                lock.acquire()
                ACCEPTNUM = b
                v = message[-2] + "/" + message[-1]
                ACCEPTVAL = v
                # create a new block and tag it as tentative
                block_info = json.loads(message[-1])
                # if this block is alread in chain, dont store it again
                stored = False
                for block in BLOCKCHAIN:
                    if block_info["ID"] == block.op_id:
                        stored = True
                if not stored:
                    block = Block(block_info["HASH"], block_info["OPERATION"], block_info["ID"], block_info["NONCE"])
                    BLOCKCHAIN.append(block)
                    update_chain_file()
                to_send = FORMATS["accepted"].format(MY_ID, ballot_toString(ACCEPTNUM), message[-2],message[-1])
                send_message(to_send, sender_id)
                lock.release()
        elif cmd == "accepted":
            block_info = json.loads(message[-1])
            if DECIDING == block_info["ID"]:
                lock.acquire()
                COUNTACC += 1
                # reach majority, append block to file 
                if COUNTACC >= 2:
                    # check if the val is already stored
                    stored = False
                    operation_id = block_info["ID"]
                    storeblock = None
                    for block in BLOCKCHAIN:
                        if operation_id == block.op_id:
                            stored = True
                            storeblock = block
                            break
                    if not stored:
                        block = Block(block_info["HASH"], block_info["OPERATION"], block_info["ID"], block_info["NONCE"], True)  
                        BLOCKCHAIN.append(block)
                        OPERATIONS.pop(0)
                    DECIDING = None
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
                    requestor1 = requestors[0].split()[1]
                    requestor2 = requestors[1].split()[1]
                    if requestor1 == MY_ID:
                        # send to client directly
                        to_send = "server {}/{}".format(MY_ID, reply)
                        send_message(to_send, requestor2, True)

                    else:
                        # if request is from a server, send reply to a server first then to the client
                        to_send = FORMATS["reply"].format(MY_ID, message[-2], reply)
                        send_message(to_send, requestor1)

                    # send decide to all other servers
                    b = message[2]
                    to_send = FORMATS["decide"].format(MY_ID, b, message[-2], message[-1])
                    send_message(to_send)
                    # if there are other operations in queue, start phase 2
                    if len(OPERATIONS) > 0:
                        val = OPERATIONS[0].split("/")
                        requestors = val[0]
                        block_info = val[1]
                        b = json.loads(block_info)
                        DECIDING = b["ID"]
                        MYVAL = val
                        #print(2222)
                        to_send = FORMATS["accept"].format(MY_ID, ballot_toString(BALLOT), requestors, block_info)
                        send_message(to_send)
                    else:
                        DECIDING = None
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


# thread for clearing operations in queue and start from MYVAL
def generate_value(requestors, operation):
    global BLOCKCHAIN
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
    global LINKS
    # send message to servers
    time.sleep(2)
    if not to_client:
        if receiver_id == "all":
            servers = list(SOCKETS_SEND.keys())
            for server in servers:
                if LINKS[server]:
                    SOCKETS_SEND[server].send(message.encode())
        else:
            if LINKS[receiver_id]:
                server = SOCKETS_SEND[receiver_id]
                server.send(message.encode())
    else:
        client = SOCKETS_CLIENTS[receiver_id]
        client.send(message.encode())


def handle_input():
    global MY_ID
    global OPERATIONS
    global LINKS
    global STORE
    global LEADER
    while True:
        inp = input("please input: ")
        inp = inp.split(",")
        if inp[0] == "queue":
            if MY_ID != LEADER:
                print("I am not leader, I don't have queue")
            else:
                print(OPERATIONS)
        elif inp[0] == "failLink":
            LINKS[inp[1]] = False
        elif inp[0] == "fixLink":
            LINKS[inp[1]] = True
        elif inp[0] == "failProcess":
            pass
        elif inp[0] == "leader":
            print(LEADER)
        elif inp[0] == "store":
            print(STORE)
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
        #print(i, len(BLOCKCHAIN))
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

