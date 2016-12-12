import json
import sys
import uuid

import os

import icfs.filesystem.constants
import icfs.filesystem.file_handler
from icfs.cloudapi.exceptions import CloudIOError
from icfs.filesystem.chunk import Chunk
from icfs.logger import class_decorator
from icfs.logger import logger


@class_decorator(logger)
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
            self.chunks = []
            for chunk in chunks_list:
                self.chunks.append(
                    Chunk(self.mpt, chunk["name"], chunk["flags"], chunk["accounts"], chunk["checksum_weak"],
                          chunk["checksum_strong"]))

    def fetch(self):
        if not os.path.exists(os.path.join(self.mpt, self.name)):
            for acc in self.accounts:
                try:
                    self.cloud.pull(self.name, acc)
                    break
                except CloudIOError as cie:
                    print "Except fetching chunk meta from account{},{}".format(
                        acc, cie.message)

    def __fetch_chunks(self, clist):
        for chunk in clist:
            if not os.path.exists(os.path.join(self.mpt, chunk['name'])):
                for acc in self.accounts:
                    try:
                        self.cloud.pull(chunk['name'], acc)
                        break
                    except CloudIOError as cie:
                        print "Except fetching chunk meta from account{},{}".format(
                            acc, cie.message)

    def __fetch_chunk_meta(self):
        pass

    def append_data(self, data):
        last_chunk_size = sys.getsizeof(self.chunks[-1])
        data_size = sys.getsizeof(data)
        if data_size + last_chunk_size > icfs.filesystem.constants.CHUNK_SIZE:
            self.chunks.append(
                Chunk(self.mpt, self.name + len(self.chunks), None, None, None))
        self.chunks[-1].append(data)

    def remove_chunks(self):
        for chunk in self.chunks:
            os.remove(os.path.join(self.mpt, chunk.name))

    def write_file(self):
        data = dict()
        data['chunks'] = []
        for chunk in self.chunks:
            data['chunks'].append({
                'checksum_weak': chunk.checksum_weak,
                'checksum_strong': chunk.checksum_strong,
                'name': chunk.name,
                'flags': chunk.flags,
                'accounts': chunk.accounts
            })
        icfs.filesystem.file_handler.json_to_file(os.path.join(self.mpt, self.name), data)

    def add_chunk(self, data=''):
        chunk_name = icfs.filesystem.constants.CHUNK_PREFIX + str(uuid.uuid4())
        chunk = Chunk(self.mpt, chunk_name, None, self.accounts, None, None)
        chunk.create(data)
        self.chunks.append(chunk)
        self.write_file()
        return chunk_name

    def add_rsync_chunk(self, data):
        chunk_name = icfs.filesystem.constants.CHUNK_PREFIX + str(uuid.uuid4())
        chunk = Chunk(self.mpt, chunk_name, None, self.accounts, None, None)
        chunk.rsync_create(data)
        return chunk
