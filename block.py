import hashlib

class Block:
    def __init__(self, previous_hash, operation, nonce = None):
        self.previous_hash = previous_hash
        self.operation = operation
        self.nonce = "1"
        content =  str(self.operation) + str(self.nonce) + str(self.previous_hash)
        self.after_hash = hashlib.sha256(content).hexdigest()

    @staticmethod
    def get_genesis():
        return Block("0", "0")
    def get_hash(self):
        return self.after_hash