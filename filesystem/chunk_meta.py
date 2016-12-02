import json
import os
import sys
import uuid

import constants
import file_handler
from chunk import Chunk


class ChunkMeta:
    def __init__(self, mpt, name, p_account, s_account):
        self.mpt = mpt
        self.name = name
        self.p_account = p_account
        self.s_account = s_account
        self.chunks = []

    def create(self):
        chunk_name = constants.CHUNK_PREFIX + str(uuid.uuid4())
        chunk = Chunk(0, self.mpt, chunk_name, self.p_account, self.s_account,
                      None)
        chunk.create()
        self.chunks.append(chunk)
        self.write_file()

    def load(self):
        with open(self.mpt + self.name) as cm:
            cm_data = json.load(cm)
            chunks_list = cm_data["chunks"]
            clist = []
            for chunk in chunks_list:
                name = chunk["name"]
                if not os.path.exists(self.mpt + name):
                    clist.append(chunk)
            self.__fetch_chunks(clist)
            for chunk in chunks_list:
                self.chunks.append(
                    Chunk(chunk["checksum"],self.mpt, chunk["name"], chunk["p_account"],
                          chunk["s_account"], chunk["flags"]))

    def fetch(self):
        if not os.path.exists(self.mpt + self.name):
            pass  # Fetch

    def push(self):
        self.p_account.push(self.mpt + self.name)
        self.s_account.push(self.mpt + self.name)
        clist = self.__rsync_chunks()
        self.__push_chunks(clist)

    def __rsync_chunks(self):
        pass

    def __push_chunks(self, clist):
        pass # Push All Chunks

    def __fetch_chunks(self, clist):
        pass  # Fetch All Chunks in List

    def __fetch_chunk_meta(self):
        pass

    def __load_chunks(self):
        pass

    def append_data(self, data):
        last_chunk_size = sys.getsizeof(self.chunks[-1])
        data_size = sys.getsizeof(data)
        if data_size + last_chunk_size > constants.CHUNK_SIZE:
            self.chunks.append(
                Chunk(None, self.mpt, self.name + len(self.chunks), None, None, None))
        self.chunks[-1].append(data)

    def write_file(self):
        data = dict()
        # data['name'] = self.name#We have it for debugging
        # data['p_account'] = self.p_account#We have it for debugging
        # data['s_account'] = self.s_account#We have it for debugging
        data['chunks'] = []
        for chunk in self.chunks:
            data['chunks'].append({
                'checksum': chunk.checksum,
                'name': chunk.name,
                'p_account': str(chunk.p_account),
                's_account': str(chunk.s_account),
                'flags': chunk.flags
            })
        file_handler.json_to_file(self.mpt+self.name, data)
