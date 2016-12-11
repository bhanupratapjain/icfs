import json
import os
import uuid

import constants
import file_handler
from chunk_meta import ChunkMeta
from icfs.cloudapi.exceptions import CloudIOError


class HeadChunk:
    def __init__(self, mpt, name, cloud, accounts):
        self.mpt = mpt
        self.name = name
        self.size = 0
        self.chunk_meta_name = None
        self.chunk_meta = None
        self.cloud = cloud
        self.accounts = accounts

    def create(self):
        meta_name = constants.CM_PREFIX + str(uuid.uuid4())
        self.chunk_meta = ChunkMeta(self.mpt, meta_name, self.cloud,
                                    self.accounts)
        self.chunk_meta.create()
        self.write_file()

    # Should be called after fetch()
    def load(self):
        print "hc load name[{}] mpt[{}] [{}] ".format(self.name, self.mpt, "start")
        print "In load"
        with open(os.path.join(self.mpt, self.name)) as hc:
            hc_obj = json.load(hc)
            self.size = hc_obj['size']
            cm_data = hc_obj["chunk_meta"]
            self.chunk_meta = ChunkMeta(self.mpt, cm_data["name"], self.cloud,
                                        cm_data["accounts"])
            self.chunk_meta.fetch()
            self.chunk_meta.load()
        print "hc load name[{}] mpt[{}] [{}] ".format(self.name, self.mpt, "end")


    def fetch(self):
        print "hc fetch name[{}] mpt[{}] [{}] ".format(self.name, self.mpt, "start")
        print "path in fetch", os.path.join(self.mpt, self.name)
        if not os.path.exists(os.path.join(self.mpt, self.name)):
            print "In if fetch"
            for acc in self.accounts:
                try:
                    self.cloud.pull(self.name, acc)
                    break
                except CloudIOError as cie:
                    print "Except fetching head chunk from account{},{}".format(
                        acc, cie.message)
        print "hc fetch name[{}] mpt[{}] [{}] ".format(self.name, self.mpt, "end")


    def append_data(self, data):
        self.chunk_meta.append_data(data)

    def remove(self):
        self.chunk_meta.remove()
        os.remove(os.path.join(self.mpt, self.name))

    def write_file(self):
        data = dict()
        data['size'] = self.size
        data['chunk_meta'] = {
            'name': self.chunk_meta.name,
            'accounts': self.accounts
        }
        file_handler.json_to_file(os.path.join(self.mpt, self.name), data)
