from chunk_meta import ChunkMeta
import json, uuid

import constants
import file_handler


class HeadChunk:
    def __init__(self, name, p_account, s_account):
        self.name = name
        self.p_account = p_account
        self.s_account = s_account
        self.chunk_meta_name = None
        self.chunk_meta = None

    def create(self):
        meta_name = constants.CM_PREFIX + str(uuid.uuid4())
        self.chunk_meta = ChunkMeta(meta_name, self.p_account, self.s_account)
        self.chunk_meta.create()
        self.write_file()

    # 1. Fetch and init Head Chunk
    def __fetch_head_chunk(self):
        self.chunk_meta_name = None

    # Fetch Chunk Meta from Cloud and Create Local Chunk Meta object
    def ___chunk_meta(self):
        self.chunk_meta = ChunkMeta(self.chunk_meta_name, self.p_account, self.s_account)

    def append_data(self, data):
        self.chunk_meta.append_data(data)

    def write_file(self):
        data = dict()
        data['name'] = self.name
        data['p_account'] = self.p_account#used for debug
        data['s_account'] = self.s_account#used for debug
        data['chunk_meta'] = {
            'name':self.chunk_meta.name,
            'p_account':self.chunk_meta.p_account,
            's_account':self.chunk_meta.s_account
        }
        file_handler.json_to_file(self.name, data)

