import os
import random

from fuse import FUSE, Operations, LoggingMixIn

from cloud_mock import MockCloud
from file_object import FileObject


class FileSystem(LoggingMixIn,Operations):
    def __init__(self):
        self.mtpt = ""
        self.root = None
        self.accounts = []
        self.open_files = dict()
        self.fd = 0

    def mount(self, mtpt):
        self.mtpt = mtpt
        p_account = self.__get_random_account()
        s_account = self.__get_random_account(p_account)
        self.root = FileObject(self.mtpt, "/")
        self.root.create(p_account, s_account)
        FUSE(self, self.mtpt, foreground=True)

    def add_account(self, path):
        self.accounts.append(MockCloud(path))

    def __get_random_account(self, primary_account=None):
        if primary_account is None:
            return self.accounts[random.randint(0, len(self.accounts) - 1)]
        else:
            tmp = self.accounts[random.randint(0, len(self.accounts) - 1)]
            while tmp == primary_account:
                tmp = self.accounts[random.randint(0, len(self.accounts) - 1)]
            return tmp

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def create(self, path, mode):  # file_name
        # TODO
        ####
        # Append in Parent Directory
        # if the file_name is a path traverse accordingly and finally append to the data
        # Push these files using apis
        # return success
        p_account = self.__get_random_account()
        s_account = self.__get_random_account(p_account)
        fo = FileObject(self.mtpt, path)
        fo.create(p_account, s_account)
        return self.__open_helper(fo, mode)

    def getattr(self, path, fh=None):
        return {}

    def getxattr(self, path, name, position=0):
        pass

    def listxattr(self, path):
        pass

    def mkdir(self, path, mode):
        pass

    def open(self, path, flags):  # file_name
        fo = FileObject(path)
        # fo.head_chunk =  HeadChunk(path, self.p_account, self.s_account)
        # fo.head_chunk.chunk_meta = ChunkMeta(meta_name, self.p_account, self.s_account)
        return self.__open_helper(fo, flags)

    def __open_helper(self, fo, flags):
        fo.open(flags)
        self.fd += 1
        self.open_files[self.fd] = fo
        return self.fd

    def read(self, path, size, length, offset, fh):
        fo = self.open_files[fh]
        return fo.read(size, length, offset)

    def readdir(self, path, fh):
        pass

    def readlink(self, path):
        pass

    def removexattr(self, path, name):
        pass

    def rename(self, old, new):
        pass

    def rmdir(self, path):
        pass

    def setxattr(self, path, name, value, options, position=0):
        pass

    def statfs(self, path):
        return {}

    def symlink(self, target, source):
        pass

    def truncate(self, path, length, fh=None):
        pass

    def unlink(self, path):
        pass

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        fo = self.open_files[fh]
        bytes = fo.write(data, offset)
        return bytes

if __name__ == "__main__":
    fs = FileSystem()
    fs.accounts = ['s', 'w']
    # directory headchunk
    # dir_hc = HeadChunk( "headchunk_", "p_account", "s_account")
    fd = fs.create("a.txt", os.O_RDWR | os.O_CREAT)
    # fd = fs.open("a.txt","r+");
    fs.write(None, "data", 0, fd)
    fs.write(None, "s", 0, fd)
    print "reading ", fs.read(None, "s", 10, 0, fd)
    fs.add_account("~/mock_cloud1/")
    fs.add_account("~/mock_cloud2/")
    fs.mount("./mpt/")
    # directory headchunk
    # dir_hc = HeadChunk( "headchunk_", "p_account", "s_account")
    # fs.create("a.txt", "r+")
    # fs.open("a.txt","r+");
