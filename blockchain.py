from block import Block
import sys

import hashlib
import json
'''
blockchain = [Block.get_genesis()]
store = {}

if sys.argv[1] == "write":
    chain_file = []

    for i in range(1, 3):
        op = "put,{},{}".format(i, i+3)
        block = Block(blockchain[i-1].get_hash(), op)
        blockchain.append(block)
        print("OP: {}, NONCE: {}, PRE_HASH: {}".format(block.operation, block.nonce, block.previous_hash))
        key_value = op[4:].split(',')
        key = key_value[0]
        value = key_value[1]
        store[key] = value
        print("key:{}, value:{}".format(key, value))

    with open("blockchain.json", 'w') as f:
        for block in blockchain:
            if block.operation != "0":
                block_info = {"NONCE":block.nonce, "OP":block.operation, "PRE_HASH":block.previous_hash }
                chain_file.append(block_info)
        
        json.dump(chain_file, f, indent=4)
        f.close()

    with open("store.json", "w") as f:
        json.dump(store, f, indent=4)
        f.close()


elif sys.argv[1] == "read":
    with open("blockchain.json", "r") as f:
        chain = json.load(f)
        f.close()
    i = 1
    for block_info in chain:
        # check valid
        prev_hash = blockchain[i-1].get_hash()
        stored_hash = block_info["PRE_HASH"]
        if prev_hash == stored_hash:
            block = Block(prev_hash, block_info["OP"])
            blockchain.append(block)
        i+=1
    for block in blockchain:
        print("OP: {}, NONCE: {}, PRE_HASH: {}".format(block.operation, block.nonce, block.previous_hash))

    with open("store.json", "r") as f:
        store = json.load(f)
        f.close()
    for key in store:
        print ("{}:{}".format(key, store[key]))

'''
d = '{"NONCE": "7", "OPERATION": "get,Alice,1012", "ID": "1012", "HASH": "4e972b4a1a2228eae7fac365c9cd5bf2d634a390527db0d60a928e2c6c08f72c", "DECIDED": false}'
h = json.loads(d)
print(h["DECIDED"])
