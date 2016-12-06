import uuid

import os

import constants
from cloudapi.exceptions import CloudIOError
from exceptions import ICFSError
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
        self.head_chunk = HeadChunk(self.mpt, file_head_chunk_name, self.cloud, None)
        self.head_chunk.create()
        # directory, name = os.path.split(self.file_path)
        # self.parent = FileObject(self.mpt, directory, self.cloud)
        # self.parent.open(os.O_APPEND)
        # self.parent.write(name + "  " + self.head_chunk.name, 0)


        # self.parent.close()

    def create_root(self, accounts):
        file_head_chunk_name = constants.ROOT_HC
        self.head_chunk = HeadChunk(self.mpt, file_head_chunk_name, self.cloud, accounts)
        if not os.path.exists(os.path.join(self.mpt, file_head_chunk_name)):#if not in local pull from cloud
            self.__fetch_root_hc(accounts)

        if not os.path.exists(os.path.join(self.mpt, file_head_chunk_name)):#if still not in local create it
            self.head_chunk.create()  # Create HeadChunk, ChunkMeta
            self.head_chunk.chunk_meta.add_chunk()  # Add a Chunk and append ChunkMeta
            with open(os.path.join(self.mpt, self.head_chunk.chunk_meta.chunks[0].name), "w") as ch:
                ch.write(".  " + constants.ROOT_HC)
            self.push_root(accounts)
        else:#else load from local
            self.head_chunk.load()


    def __fetch_root_hc(self, accounts):
        for acc in accounts:
            try:
                self.cloud.pull(constants.ROOT_HC,acc)
                break
            except CloudIOError as cie:
                print "Could not connect to account {}{}".format(acc,cie.message())

    def open(self, flags):
        # TODO
        # Figure out Head Chunk
        self.__find_head_chunk()
        self.head_chunk.fetch()
        self.head_chunk.load()
        local_file_name = self.__assemble()
        # open local copy
        self.os_fh = os.open(local_file_name, flags)


    #throws ICFS error
    def push_root(self,accounts):
        arr = [self.head_chunk,self.head_chunk.chunk_meta]
        arr += self.head_chunk.chunk_meta.rsync_chunks()
        #TODO change the following to push all files  and do that in a separate thread
        for obj in arr:
            for acc in accounts:
                try:
                    self.cloud.push(obj.name,acc)
                except CloudIOError as cie:
                    print "Error pushing root into account {} {}".format(obj.name,cie.message())


    #throws ICFS error
    def push(self):
        arr = [self.head_chunk,self.head_chunk.chunk_meta]
        arr.append(self.head_chunk.chunk_meta.rsync_chunks())
        #TODO change the following to push all files  and do that in a separate thread
        for obj in arr:
            try:
                self.cloud.push(obj.name,obj.p_account)
                self.cloud.push(obj.name,obj.s_account)
            except CloudIOError as cie:
                print "Error pushing into primary {}".format(cie.message())
                try:
                    self.cloud.push(obj.name,obj.s_account)
                except CloudIOError as cie:
                    raise ICFSError("error while push")


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
