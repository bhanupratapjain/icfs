import os


class Chunk:
    def __init__(self, checksum, mpt, name, flags, accounts):
        self.checksum = checksum
        self.mpt = mpt
        self.name = name
        self.flags = flags
        self.accounts = accounts

    def create(self):
        open(os.path.join(self.mpt, self.name), 'w').close()
