import json
import sys
import uuid

from chunk import Chunk
import constants
import file_handler


class ChunkMeta:
    def __init__(self, name, p_account, s_account):
        self.name = name
        self.p_account = p_account
        self.s_account = s_account
        self.chunks = []
    def create(self):
        chunk_name = constants.CHUNK_PREFIX + str(uuid.uuid4())
        chunk = Chunk(0,chunk_name,self.p_account, self.s_account, None)
        chunk.create()
        self.chunks.append(chunk)
        self.write_file()

    def __fetch_chunk_meta(self):
        pass

    def __load_chunks(self):
        pass

    def append_data(self,data):
        last_chunk_size = sys.getsizeof(self.chunks[-1])
        data_size = sys.getsizeof(data)
        if data_size + last_chunk_size > constants.CHUNK_SIZE:
            self.chunks.append(Chunk(None,self.name+len(self.chunks),None, None, None))
        self.chunks[-1].append(data)

    def write_file(self):
        data = dict()
        data['name'] = self.name#We have it for debugging
        data['p_account'] = self.p_account#We have it for debugging
        data['s_account'] = self.s_account#We have it for debugging
        data['chunks'] = []
        for chunk in self.chunks:
            data['chunks'].append( {
                'checksum': chunk.checksum,
                'name': chunk.name,
                'p_account': chunk.p_account,
                's_account': chunk.s_account,
                'flags': chunk.flags
             })
        file_handler.json_to_file(self.name,data)