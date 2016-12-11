import json
import os
import sys
import uuid

import constants
import file_handler
from chunk import Chunk
from icfs.cloudapi.exceptions import CloudIOError


class ChunkMeta:
    def __init__(self, mpt, name, cloud, accounts):
        self.mpt = mpt
        self.name = name
        self.chunks = []
        self.cloud = cloud
        self.accounts = accounts

    def create(self):
        self.write_file()

    def load(self):
        with open(os.path.join(self.mpt, self.name)) as cm:
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
                    Chunk(chunk["checksum"], self.mpt, chunk["name"],
                          chunk["flags"], chunk["accounts"]))

    def fetch(self):
        if not os.path.exists(os.path.join(self.mpt, self.name)):
            for acc in self.accounts:
                try:
                    self.cloud.pull(self.name, acc)
                    break
                except CloudIOError as cie:
                    print "Except fetching chunk meta from account{},{}".format(
                        acc, cie.message)

    # should return chunk objects
    def rsync_chunks(self):
        return self.chunks

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
                Chunk(None, self.mpt, self.name + len(self.chunks), None))
        self.chunks[-1].append(data)


    def write_file(self):
        data = dict()
        data['chunks'] = []
        for chunk in self.chunks:
            data['chunks'].append({
                'checksum': chunk.checksum,
                'name': chunk.name,
                'flags': chunk.flags,
                'accounts': chunk.accounts
            })
        file_handler.json_to_file(os.path.join(self.mpt, self.name), data)

    def add_chunk(self):
        chunk_name = constants.CHUNK_PREFIX + str(uuid.uuid4())
        chunk = Chunk(0, self.mpt, chunk_name, None, self.accounts)
        chunk.create()
        self.chunks.append(chunk)
        self.write_file()
        return chunk_name
