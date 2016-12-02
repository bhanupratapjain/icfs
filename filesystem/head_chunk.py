import json
import os
import uuid

import constants
import file_handler
from chunk_meta import ChunkMeta


class HeadChunk:
    def __init__(self, mpt, name, p_account, s_account):
        self.mpt = mpt
        self.name = name
        self.p_account = p_account
        self.s_account = s_account
        self.chunk_meta_name = None
        self.chunk_meta = None

    def create(self):
        meta_name = constants.CM_PREFIX + str(uuid.uuid4())
        self.chunk_meta = ChunkMeta(self.mpt, meta_name,
                                    self.p_account, self.s_account)
        self.chunk_meta.create()
        self.write_file()

    # Should be called after fetch()
    def load(self):
        with open(self.mpt+self.name) as hc:
            hc_obj = json.load(hc)
            cm_data = hc_obj["chunk_meta"]
            self.chunk_meta = ChunkMeta(self.mpt, cm_data["name"],
                                        cm_data["p_account"],
                                        cm_data["s_account"])
            self.chunk_meta.fetch()
            self.chunk_meta.load()

    def fetch(self):
        if not os.path.exists(self.mpt + self.name):
            pass # Fetch Head Chunk

    def push(self):
        self.p_account.push(self.mpt + self.name)
        self.s_account.push(self.mpt + self.name)
        self.chunk_meta.push()

    def append_data(self, data):
        self.chunk_meta.append_data(data)

    def write_file(self):
        data = dict()
        # data['name'] = self.name
        # data['p_account'] = self.p_account#used for debug
        # data['s_account'] = self.s_account#used for debug
        data['chunk_meta'] = {
            'name': self.chunk_meta.name,
            'p_account': str(self.chunk_meta.p_account),
            's_account': str(self.chunk_meta.s_account)
        }
        file_handler.json_to_file(self.mpt+self.name, data)
