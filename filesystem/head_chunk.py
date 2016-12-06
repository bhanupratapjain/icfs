import json
import uuid

import os

import constants
import file_handler
from chunk_meta import ChunkMeta
from cloudapi.exceptions import CloudIOError


class HeadChunk:
    def __init__(self, mpt, name, cloud, accounts):
        self.mpt = mpt
        self.name = name
        self.chunk_meta_name = None
        self.chunk_meta = None
        self.cloud = cloud
        self.accounts = accounts

    def create(self):
        meta_name = constants.CM_PREFIX + str(uuid.uuid4())
        self.chunk_meta = ChunkMeta(self.mpt, meta_name, self.cloud, self.accounts)
        self.chunk_meta.create()
        self.write_file()

    # Should be called after fetch()
    def load(self):
        with open(os.path.join(self.mpt, self.name)) as hc:
            hc_obj = json.load(hc)
            cm_data = hc_obj["chunk_meta"]
            self.chunk_meta = ChunkMeta(self.mpt, cm_data["name"], self.cloud, cm_data["accounts"])
            self.chunk_meta.fetch()
            self.chunk_meta.load()

    def fetch(self):
        if not os.path.exists(self.mpt + self.name):
            for acc in self.accounts:
                try:
                    self.cloud.pull(self.name,acc)
                    break
                except CloudIOError as cie:
                    print "Except fetching head chunk from account{},{}".format(acc,cie.message())

    def append_data(self, data):
        self.chunk_meta.append_data(data)

    def write_file(self):
        data = dict()
        data['chunk_meta'] = {
            'name': self.chunk_meta.name,
            'accounts': self.accounts
        }
        file_handler.json_to_file(os.path.join(self.mpt, self.name), data)
