class Chunk:
    def __init__(self, checksum, mpt, name, p_account, s_account, flags):
        self.checksum = checksum
        self.mpt = mpt
        self.name = name
        self.p_account = p_account
        self.s_account = s_account
        self.flags = flags

    def create(self):
        open(self.mpt+self.name, 'w').close()
