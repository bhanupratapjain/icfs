import time
import uuid

import os
from stat import S_IFREG

import constants
from icfs.cloudapi.exceptions import CloudIOError
from icfs.filesystem.exceptions import ICFSError
from icfs.filesystem.head_chunk import HeadChunk
from icfs.logger import class_decorator, logger
from pyrsync import pyrsync


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

    def create(self, accounts):
        file_head_chunk_name = constants.HC_PREFIX + str(uuid.uuid4())
        self.head_chunk = HeadChunk(self.mpt, file_head_chunk_name, self.cloud,
                                    accounts)
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
            # self.head_chunk.chunk_meta.add_chunk()  # Add a Chunk and append ChunkMeta
            chunk_data = ".  " + constants.ROOT_HC + "\n"
            self.open("w")
            self.write(chunk_data, 0)
            # self.head_chunk.chunk_meta.add_chunk(chunk_data)  # Add a Chunk and append ChunkMeta
            # self.a_f_py_obj = open(os.path.join(self.mpt, self.a_f_name), "w+")
            # with open(os.path.join(self.mpt, self.a_f_name), "w") as f:
            #     f.write(".  " + constants.ROOT_HC + "\n")
            # self.a_f_py_obj.seek(0)
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
        print "Opening File [%s] Flags[%s] Path[%s]" % (self.a_f_name, flags, self.file_path)
        self.a_f_py_obj = open(os.path.join(self.mpt, self.a_f_name), flags)

    def close(self):
        if self.a_f_py_obj is not None:
            print "Closing File [{}] Path[{}]".format(self.a_f_name, self.file_path)
            self.a_f_py_obj.close()
            # self.split_chunks()
            self.head_chunk.size = os.path.getsize(
                os.path.join(self.mpt, self.a_f_name))
            self.head_chunk.write_file()
            os.remove(os.path.join(self.mpt, self.a_f_name))
            self.a_f_name = None
            self.a_f_py_obj = None
            # self.head_chunk.chunk_meta.remove_chunks()

    # throws ICFS error
    def push(self):
        obj_arr = [self.head_chunk, self.head_chunk.chunk_meta]
        push_chunks, remove_chunks = self.rsync_chunks()
        print push_chunks
        print remove_chunks
        obj_arr.extend(push_chunks)
        # TODO change the following to push all files  and do that in a separate thread
        for obj in obj_arr:
            acc_push_count = 0
            for acc in obj.accounts:
                try:
                    print "Pushing obj {} to account {}".format(obj.name, acc)
                    self.cloud.push(obj.name, acc)
                    acc_push_count += 1
                except CloudIOError as cie:
                    print "Error pushing into primary {}".format(cie.message)
            if acc_push_count < 1:
                raise ICFSError("error while push")
        for chunk in remove_chunks:
            for acc in chunk.accounts:
                try:
                    print "Removing obj {} from account {}".format(obj.name, acc)
                    self.cloud.remove(chunk.name, acc)
                except CloudIOError as cie:
                    print "Error removing from account {} {}".format(acc, cie.message)
            os.remove(os.path.join(self.mpt, chunk.name))

    # should return chunk objects
    def rsync_chunks(self):
        assmbl = False
        chck_weak = []
        chck_strong = []
        for chunk in self.head_chunk.chunk_meta.chunks:
            if chunk.checksum_weak is not None:
                chck_weak.append(chunk.checksum_weak)
            if chunk.checksum_strong is not None:
                chck_strong.append(chunk.checksum_strong)

        if self.a_f_name is None:
            assmbl = True
            self.a_f_name = self.assemble()
            print "creating assebled file"
        else:
            print "not creating assebled file"
        print "old hashes,", chck_weak, chck_strong
        print "afn{}, name{}, hc{}".format(self.a_f_name, self.file_path, self.head_chunk.name)
        with open(os.path.join(self.mpt, self.a_f_name), "r") as f:
            delta = pyrsync.rsyncdelta(f, (chck_weak, chck_strong), constants.CHUNK_SIZE)
        print "delta,", delta
        new_chunks = []
        push_chunks = []
        del_chunks = self.head_chunk.chunk_meta.chunks
        print "del chunks ", del_chunks
        for d_val in delta[1:]:
            if isinstance(d_val, int):
                chunk = self.head_chunk.chunk_meta.chunks[d_val]
                new_chunks.append(chunk)
                del_chunks.remove(chunk)
            else:
                chunk = self.head_chunk.chunk_meta.add_rsync_chunk(d_val)
                new_chunks.append(chunk)
                push_chunks.append(chunk)
        self.head_chunk.chunk_meta.chunks = new_chunks
        self.head_chunk.chunk_meta.write_file()
        if assmbl:
            os.remove(os.path.join(self.mpt, self.a_f_name))
            self.a_f_name = None
            self.a_f_py_obj = None
        return push_chunks, del_chunks

    def assemble(self):
        chunks = self.head_chunk.chunk_meta.chunks
        local_file_name = constants.LOCAL_ASSEMBLED_CHUNK + str(uuid.uuid4())
        with open(os.path.join(self.mpt, local_file_name), "w") as of:
            for chunk in chunks:
                with open(os.path.join(self.mpt, chunk.name), "r") as chf:
                    buf = chf.read(constants.CHUNK_SIZE)
                    of.write(buf)
                    # TODO: Uncomment when Rsync is impl.
                    # os.remove(os.path.join(self.mpt, chunk.name))
        return local_file_name

    def write(self, data, offset):
        pos = self.a_f_py_obj.tell()
        self.a_f_py_obj.write(data)
        ret = self.a_f_py_obj.tell() - pos
        self.head_chunk.size += ret
        self.a_f_py_obj.flush()
        return ret

    def read(self, length, offset):
        self.a_f_py_obj.seek(offset, 1)  # SET or CUR ?
        return self.a_f_py_obj.read(length)

    def getattr(self):
        self.head_chunk.fetch()
        self.head_chunk.load()
        now = time.time()
        return dict(st_mode=(S_IFREG | 0o755), st_ctime=now, st_mtime=now,
                    st_atime=now, st_nlink=1, st_size=self.head_chunk.size)

    def split_chunks(self):
        f = open(os.path.join(self.mpt, self.a_f_name), 'r')
        for chunk in self.head_chunk.chunk_meta.chunks:
            with open(os.path.join(self.mpt, chunk.name), 'w') as ch:
                buf = f.read(constants.CHUNK_SIZE)
                if buf == "":
                    break
                ch.write(buf)

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
