import os
import time
import uuid
from stat import S_IFREG, S_IFDIR

import constants
from icfs.cloudapi.exceptions import CloudIOError
from icfs.filesystem.exceptions import ICFSError
from icfs.filesystem.head_chunk import HeadChunk
from icfs.logger import class_decorator, logger


@class_decorator(logger)
class FileObject:
    def __init__(self, mpt, file_path, cloud):
        self.mpt = mpt
        self.parent = None  # ICFS File Object
        self.file_path = file_path
        self.head_chunk = None
        self.a_f_name = None  # Name of the assembled file
        self.a_f_py_obj = None  # Python's File Object, populated after open
        self.cloud = cloud

    def create(self, type, accounts):
        file_head_chunk_name = constants.HC_PREFIX + str(uuid.uuid4())
        self.head_chunk = HeadChunk(self.mpt, file_head_chunk_name, self.cloud,
                                    accounts)
        self.head_chunk.type = type
        self.head_chunk.create()

    def create_root(self, accounts):
        file_head_chunk_name = constants.ROOT_HC
        self.head_chunk = HeadChunk(self.mpt, file_head_chunk_name, self.cloud,
                                    accounts)
        if not os.path.exists(os.path.join(self.mpt,
                                           file_head_chunk_name)):  # if not in local pull from cloud
            self.__fetch_root_hc(accounts)
        if not os.path.exists(os.path.join(self.mpt,
                                           file_head_chunk_name)):  # if still not in local create it
            self.head_chunk.create()  # Create HeadChunk, ChunkMeta
            self.head_chunk.chunk_meta.add_chunk()  # Add a Chunk and append ChunkMeta
            with open(os.path.join(self.mpt,
                                   self.head_chunk.chunk_meta.chunks[0].name),
                      "w") as ch:
                ch.write(".  " + constants.ROOT_HC + "\n")
            # self..head_chunk.load() #To be used when we decide to divide directories data into chunks
            self.push()
        else:  # else load from local
            self.head_chunk.load()

    def __fetch_root_hc(self, accounts):
        if not os.path.exists(constants.ROOT_HC):
            for acc in accounts:
                try:
                    self.cloud.pull(constants.ROOT_HC, acc)
                    break
                except CloudIOError as cie:
                    print "Could not connect to account {}{}".format(acc,
                                                                     cie.message)

    def open(self, flags):
        self.head_chunk.fetch()
        self.head_chunk.load()
        self.a_f_name = self.assemble()
        self.a_f_py_obj = open(os.path.join(self.mpt, self.a_f_name), flags)

    def close(self, delete_local=False):
        if self.a_f_py_obj is not None:
            self.a_f_py_obj.close()
            self.head_chunk.size = os.path.getsize(
                os.path.join(self.mpt, self.a_f_name))
            self.head_chunk.write_file()
            if delete_local:
                if self.file_path != "/":
                    os.remove(os.path.join(self.mpt, self.a_f_name))
                    os.remove(
                        os.path.join(self.mpt, self.head_chunk.chunk_meta.name))
                    os.remove(os.path.join(self.mpt, self.head_chunk.name))
                    self.head_chunk = None
                    self.a_f_py_obj = None
                    self.a_f_name = None

    # throws ICFS error
    def push(self):
        obj_arr = [self.head_chunk, self.head_chunk.chunk_meta]
        obj_arr.extend(self.head_chunk.chunk_meta.rsync_chunks())
        # TODO change the following to push all files  and do that in a separate thread
        for obj in obj_arr:
            acc_push_count = 0
            for acc in obj.accounts:
                try:
                    self.cloud.push(obj.name, acc)
                    acc_push_count += 1
                except CloudIOError as cie:
                    print "Error pushing into primary {} {}".format(obj.name,
                                                                    cie.message)
            if acc_push_count < 1:
                raise ICFSError("error while push")

    def assemble(self):
        chunks = self.head_chunk.chunk_meta.chunks
        local_file_name = constants.LOCAL_ASSEMBLED_CHUNK + self.head_chunk.name
        if not os.path.exists(os.path.join(self.mpt, local_file_name)):
            with open(os.path.join(self.mpt, local_file_name), "w") as of:
                for chunk in chunks:
                    with open(os.path.join(self.mpt, chunk.name), "r") as chf:
                        buf = chf.read(constants.CHUNK_SIZE)
                        of.write(buf)
                    # TODO: Uncomment when Rsync is impl.
                    os.remove(os.path.join(self.mpt, chunk.name))
        return local_file_name

    def write(self, data, offset):
        pos = self.a_f_py_obj.tell()
        self.a_f_py_obj.write(data)
        ret = self.a_f_py_obj.tell() - pos
        self.head_chunk.size += ret
        self.split_chunks()
        try:
            self.push()  # Should be moved to write
        except ICFSError as ie:
            print
            "Error in Pushing at FileSystem Layer. {}".format(ie.message)
        for chunk in self.head_chunk.chunk_meta.chunks:
            os.remove(os.path.join(self.mpt, chunk.name))
        return ret

    def read(self, length, offset):
        self.a_f_py_obj.seek(offset, 1)  # SET or CUR ?
        return self.a_f_py_obj.read(length)

    def getattr(self):
        self.head_chunk.fetch()
        self.head_chunk.load()
        now = time.time()
        if self.head_chunk.type == constants.FILE:
            return dict(st_mode=(S_IFREG | 0o755), st_ctime=now, st_mtime=now,
                        st_atime=now, st_nlink=1, st_size=self.head_chunk.size)
        else:
            return dict(st_mode=(S_IFDIR | 0o755), st_ctime=now, st_mtime=now,
                        st_atime=now, st_nlink=1, st_size=self.head_chunk.size)

    def split_chunks(self):
        f = open(os.path.join(self.mpt, self.a_f_name), 'r')
        count = 0
        for chunk in self.head_chunk.chunk_meta.chunks:
            with open(os.path.join(self.mpt, chunk.name), 'w') as ch:
                buf = f.read(constants.CHUNK_SIZE)
                if buf == "":
                    break
                ch.write(buf)
            count += 1

        while count < len(self.head_chunk.chunk_meta.chunks):
            chunk = self.head_chunk.chunk_meta.chunks[count]
            for client_id in chunk.accounts:
                self.cloud.remove(chunk.name, client_id)
            os.remove(os.path.join(self.mpt, chunk.name))
            self.head_chunk.chunk_meta.chunks.remove(chunk)
            count += 1

        buf = f.read(constants.CHUNK_SIZE)
        while buf != "":
            chunk_name = self.head_chunk.chunk_meta.add_chunk()
            with open(os.path.join(self.mpt, chunk_name), 'w') as ch:
                ch.write(buf)
                ch.flush()
            buf = f.read(constants.CHUNK_SIZE)

    def remove(self):
        self.head_chunk.fetch()
        self.head_chunk.load()

        for chunk in self.head_chunk.chunk_meta.chunks:
            os.remove(os.path.join(self.mpt, chunk.name))
            for client_id in chunk.accounts:
                self.cloud.remove(chunk.name, client_id)

        os.remove(os.path.join(self.mpt, self.head_chunk.chunk_meta.name))
        for client_id in self.head_chunk.chunk_meta.accounts:
            self.cloud.remove(self.head_chunk.chunk_meta.name, client_id)

        os.remove(os.path.join(self.mpt, self.head_chunk.name))
        for client_id in self.head_chunk.accounts:
            self.cloud.remove(self.head_chunk.name, client_id)
