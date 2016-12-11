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
        self.parent = None  # ICFS File Object
        self.file_path = file_path
        self.head_chunk = None
        self.assembled = None  # Name of the assembled file
        self.py_file = None  # Python's File Object, populated after open
        self.cloud = cloud

    def create(self, accounts):
        file_head_chunk_name = constants.HC_PREFIX + str(uuid.uuid4())
        self.head_chunk = HeadChunk(self.mpt, file_head_chunk_name, self.cloud,
                                    accounts)
        self.head_chunk.create()
        print "************ Create FO HC {}, CM{}, CHUNKS{}".format(
            self.head_chunk.name, self.head_chunk.chunk_meta.name,
            len(self.head_chunk.chunk_meta.chunks))
        directory, name = os.path.split(self.file_path)
        self.parent = FileObject(self.mpt, directory, self.cloud)
        self.parent.open('a')
        wr_str = "{} {}".format(name, self.head_chunk.name)
        for acc in self.head_chunk.accounts:
            print "*********Apending Accounts %s" % acc
            wr_str += " {}".format(acc)
        wr_str += "\n"
        print "***********Appending String %s" % wr_str
        self.parent.write(wr_str, 0)
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
        # self.__find_head_chunk()
        self.head_chunk.fetch()
        self.head_chunk.load()
        self.assembled = self.assemble()
        # open local copy
        self.py_file = open(os.path.join(self.mpt, self.assembled), flags)
        print "os_fh", self.py_file

    def close(self):
        if self.py_file is not None:
            self.py_file.close()
            self.split_chunks()
            self.head_chunk.size = os.path.getsize(
                os.path.join(self.mpt, self.ass_fname))
            print "size in close", self.head_chunk.size
            self.head_chunk.write_file()
            os.remove(os.path.join(self.mpt, self.ass_fname))

    # throws ICFS error
    def push(self):
        obj_arr = [self.head_chunk, self.head_chunk.chunk_meta]
        obj_arr.extend(self.head_chunk.chunk_meta.rsync_chunks())
        print "************************ obj array"
        print obj_arr
        # TODO change the following to push all files  and do that in a separate thread
        for obj in obj_arr:
            acc_push_count = 0
            print "Pushing Obj {} with {} accounts".format(obj.name,
                                                           len(obj.accounts))
            for acc in obj.accounts:
                try:
                    self.cloud.push(obj.name, acc)
                    acc_push_count += 1
                except CloudIOError as cie:
                    print "Error pushing into primary {}".format(cie.message)
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
            # self.parent.__find_head_chunk()
            self.parent.open("r")
            for line in self.parent.py_file:
                if line.startswith(file):
                    hc_name = line.split()[1]
                    self.head_chunk = HeadChunk(self.mpt, hc_name, self.cloud,
                                                None)
                    break
            self.parent.close()

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
        print "write", self.py_file, data
        pos = self.py_file.tell()
        self.py_file.write(data)
        ret = self.py_file.tell() - pos
        # self.head_chunk.push()
        self.head_chunk.size += ret
        return ret

    def read(self, length, offset):
        self.py_file.seek(offset, 1)  # SET or CUR ?
        return self.py_file.read(length)

    def getattr(self):
        print "In getattr"
        self.__find_head_chunk()
        print "After find hc"
        if self.head_chunk is not None:
            self.head_chunk.fetch()
            self.head_chunk.load()
            print "chunk_meta", self.head_chunk.chunk_meta.name
            now = time.time()
            return dict(st_mode=(S_IFREG | 0o755), st_ctime=now, st_mtime=now,
                        st_atime=now, st_nlink=1, st_size=self.head_chunk.size)
        else:
            return {}

    def split_chunks(self):
        f = open(os.path.join(self.mpt, self.assembled), 'r')
        for chunk in self.head_chunk.chunk_meta.chunks:
            with open(os.path.join(self.mpt, chunk.name), 'w') as ch:
                buf = f.read(constants.CHUNK_SIZE)
                if buf == "":
                    break
                ch.write(buf)

        buf = f.read(constants.CHUNK_SIZE)
        while buf != "":
            print "buf", buf
            chunk_name = self.head_chunk.chunk_meta.add_chunk()
            with open(os.path.join(self.mpt, chunk_name), 'w') as ch:
                ch.write(buf)
                ch.flush()
            buf = f.read(constants.CHUNK_SIZE)

    def get_parent(self):
        directory, name = os.path.split(self.file_path)
        self.parent = FileObject(self.mpt, directory, self.cloud)

    def remove(self):
        self.head_chunk.fetch()
        self.head_chunk.load()

        for chunk in self.head_chunk.chunk_meta.chunks:
            os.remove(os.path.join(self.mpt, chunk.name))
            for client_id in chunk.accounts:
                self.cloud.remove(chunk.name, client_id)

        os.remove(os.path.join(self.mpt,self.head_chunk.chunk_meta.name))
        for client_id in self.head_chunk.chunk_meta.accounts:
            self.cloud.remove(self.head_chunk.chunk_meta.name, client_id)

        os.remove(os.path.join(self.mpt, self.head_chunk.name))
        for client_id in self.head_chunk.accounts:
            self.cloud.remove(self.head_chunk.name, client_id)
