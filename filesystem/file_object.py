import uuid

import os

import constants
from head_chunk import HeadChunk


class FileObject:
    def __init__(self, mpt, file_path, cloud):
        self.mpt = mpt
        self.parent = None  # File Object
        self.file_path = file_path
        self.head_chunk = None
        self.os_fh = None
        self.cloud = cloud

    def create(self, p_account, s_account):
        file_head_chunk_name = constants.HC_PREFIX + str(uuid.uuid4())
        self.head_chunk = HeadChunk(self.mpt, file_head_chunk_name, p_account, s_account, self.cloud)
        self.head_chunk.create()
        # directory, name = os.path.split(self.file_path)
        # self.parent = FileObject(self.mpt, directory, self.cloud)
        # self.parent.open(os.O_APPEND)
        # self.parent.write(name + "  " + self.head_chunk.name, 0)


        # self.parent.close()

    def create_root(self, accounts):
        file_head_chunk_name = constants.ROOT_HC
        self.head_chunk = HeadChunk(self.mpt, file_head_chunk_name, p_account, s_account, self.cloud)
        if not os.path.exists(os.path.join(self.mpt, file_head_chunk_name)):
            self.head_chunk.create()  # Create HeadChunk, ChunkMeta
            self.head_chunk.chunk_meta.add_chunk()  # Add a Chunk and append ChunkMeta
            with open(os.path.join(self.mpt, self.head_chunk.chunk_meta.chunks[0].name), "w") as ch:
                ch.write(".  " + constants.ROOT_HC)
            self.push()
        else:
            self.head_chunk.load()

    def open(self, flags):
        # TODO
        # Figure out Head Chunk
        self.__find_head_chunk()
        self.head_chunk.fetch()
        self.head_chunk.load()
        local_file_name = self.__assemble()
        # open local copy
        self.os_fh = os.open(local_file_name, flags)

    def push(self):
        self.head_chunk.push()

    def __find_head_chunk(self):
        if self.head_chunk is not None:
            pass

    def __get_parent(self):
        pass

    def __assemble(self):
        chunks = self.head_chunk.chunk_meta.chunks
        local_file_name = constants.LOCAL_ASSEMBLED_CHUNK + str(uuid.uuid4())
        with open(local_file_name, "w") as of:
            for chunk in chunks:
                with open(chunk.name, "r") as chf:
                    buf = chf.read(constants.CHUNK_SIZE)
                    of.write(buf)
                os.remove(chunk.name)

        return local_file_name

    def write(self, data, offset):
        os.lseek(self.os_fh, offset, os.SEEK_CUR)
        ret = os.write(self.os_fh, data)
        self.head_chunk.push()
        return ret

    def read(self, length, offset):
        os.lseek(self.os_fh, offset, os.SEEK_CUR)  # SET or CUR ?
        return os.read(self.os_fh, length)
