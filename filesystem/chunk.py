import json
class Chunk:
    def __init__(self,checksum,name,p_account, s_account, flags):
        self.checksum = checksum
        self.name = name
        self.p_account = p_account
        self.s_account = s_account
        self.flags = flags
    def  create(self):
        open(self.name,'w').close()
