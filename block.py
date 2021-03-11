import hashlib
import json

class Block:
    def __init__(self, previous_hash, operation, op_id = "0",nonce = None, decided = False):
        self.previous_hash = previous_hash
        self.operation = operation
        self.op_id = op_id
        self.decided = decided
        if nonce != None:
            self.nonce = nonce
        else:
            self.generate_nonce()
        cont =  (str(self.operation) + str(self.nonce) + str(self.previous_hash)).encode('utf-8')
        self.after_hash = hashlib.sha256(cont).hexdigest()
        self.info = {"NONCE":self.nonce, "OPERATION":self.operation, "ID":self.op_id, "HASH":self.previous_hash}

    @staticmethod
    def get_genesis():
        return Block("0", "0", nonce = 1)

    def get_hash(self):
        return self.after_hash
    
    def generate_nonce(self):
        temp = 1
        while True:
            h = hashlib.sha256((str(self.operation) + str(temp)).encode()).hexdigest()
            if h[-1] == "0" or h[-1] == "1" or h[-1] == "2":
                #print(h[-1])
                self.nonce = str(temp)
                break
            else:
                temp += 1
    
    def decide(self):
        self.decided = True
    
    def toString(self):
        return json.dumps(self.info)