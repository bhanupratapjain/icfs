import os
import time
import uuid
from stat import S_IFREG

import constants
from icfs.cloudapi.exceptions import CloudIOError
from icfs.filesystem.exceptions import ICFSError
from icfs.filesystem.head_chunk import HeadChunk


class FileObject:
    def __init__(self, mpt, file_path, cloud):
        self.mpt = mpt
        self.parent = None  # File Object
        self.file_path = file_path
        self.head_chunk = None
        self.ass_fname = None
        self.os_fh = None
        self.cloud = cloud

    def create(self, accounts):
        file_head_chunk_name = constants.HC_PREFIX + str(uuid.uuid4())
        self.head_chunk = HeadChunk(self.mpt, file_head_chunk_name, self.cloud,
                                    accounts)
        self.head_chunk.create()
        directory, name = os.path.split(self.file_path)
        self.parent = FileObject(self.mpt, directory, self.cloud)
        self.parent.open('a')
        self.parent.write(name + "  " + self.head_chunk.name + "\n", 0)
        self.parent.close()

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
        # TODO
        # Figure out Head Chunk
        self.__find_head_chunk()
        self.head_chunk.fetch()
        self.head_chunk.load()
        self.ass_fname = self.assemble()
        # open local copy
        self.os_fh = open(os.path.join(self.mpt, self.ass_fname), flags)
        print "os_fh", self.os_fh

    def close(self):
        if self.os_fh is not None:
            self.os_fh.close()
            self.split_chunks()
            os.remove(os.path.join(self.mpt, self.ass_fname))

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
                    print "Error pushing into primary {}".format(cie.message())
            if acc_push_count < 1:
                raise ICFSError("error while push")

    def __find_head_chunk(self):
        if self.head_chunk is None:
            print "path", self.file_path
            if self.file_path == "/":
                self.head_chunk = HeadChunk(self.mpt, constants.ROOT_HC,
                                            self.cloud, None)
                return
            parent, file = os.path.split(self.file_path)
            self.parent = FileObject(self.mpt, parent, self.cloud)
            self.parent.__find_head_chunk()
            self.parent.open("r")
            for line in self.parent.os_fh:
                if line.startswith(file):
                    hc_name = line.split()[1]
                    self.head_chunk = HeadChunk(self.mpt, hc_name, self.cloud, None)
                    break
            self.parent.close()

    def __get_parent(self):
        pass

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
        # os.lseek(self.os_fh, offset, os.SEEK_CUR)
        print "write", self.os_fh, data
        ret = self.os_fh.write(data)
        # self.head_chunk.push()
        return ret

    def read(self, length, offset):
        os.lseek(self.os_fh, offset, os.SEEK_CUR)  # SET or CUR ?
        return os.read(self.os_fh, length)

    def getattr(self):
        print "In getattr"
        self.__find_head_chunk()
        print "After find hc"
        if self.head_chunk is not None:
            # self.head_chunk.fetch()
            now = time.time()
            return dict(st_mode=(S_IFREG | 0o755), st_ctime=now, st_mtime=now,
                        st_atime=now, st_nlink=1)
        else:
            return {}

    def split_chunks(self):
        f = open(os.path.join(self.mpt, self.ass_fname), 'r')
        for chunk in self.head_chunk.chunk_meta.chunks:
            with open(os.path.join(self.mpt, chunk.name), 'w') as ch:
                buf = f.read(constants.CHUNK_SIZE)
                if buf == "":
                    break
                ch.write(buf)

                # Need to create new chunks