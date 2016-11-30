import random

from file_object import FileObject
from head_chunk import HeadChunk


class FileSystem:
    def __init__(self):
        self.accounts = []
        self.open_files = dict()
        self.fd  = 0

    def __get_random_account(self,primary_account = None):
        if primary_account is None:
            return  self.accounts[random.randint(0,len(self.accounts)-1)]
        else:
            tmp = self.accounts[random.randint(0,len(self.accounts)-1)]
            while tmp==primary_account:
                tmp = self.accounts[random.randint(0,len(self.accounts)-1)]
            return tmp

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def create(self, path, flags):#file_name
        #TODO
        ####if the file_name is a path traverse accordingly and finally append to the data
        #Push these files using apis
        #return success

        #create head chunk,meta_chunk and one chunk for the file
        p_account = self.__get_random_account()
        s_account = self.__get_random_account(p_account)
        fo = FileObject(path)
        fo.create(p_account ,s_account)
        return self.__open_helper(fo,flags)

    def getattr(self, path, fh=None):
        pass

    def getxattr(self, path, name, position=0):
        pass

    def listxattr(self, path):
        pass

    def mkdir(self, path, mode):
        pass

    def open(self, path, flags):#file_name
        fo = FileObject(path)
        #fo.head_chunk =  HeadChunk(path, self.p_account, self.s_account)
        #fo.head_chunk.chunk_meta = ChunkMeta(meta_name, self.p_account, self.s_account)
        return self.__open_helper(fo,flags)

    def __open_helper(self,fo,flags):
        fo.open(flags)
        self.fd += 1
        self.open_files[self.fd] = fo
        return self.fd
        #iterate through cur dir head chunk data and find headchunk-number for the file (fetch head chunk and fetch chunk meta)
        #####if the file_name is a path traverse accordingly and then fetch corresponding headchunk
        #use the head chunk to assemble file and save a local copy(hidden) with the file-name provided
        #return file_name

    def read(self, path, size, offset, fh):
        pass

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
        pass

    def symlink(self, target, source):
        pass

    def truncate(self, path, length, fh=None):
        pass

    def unlink(self, path):
        pass

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        pass


if __name__ == "__main__":
    fs = FileSystem()
    fs.accounts = ['s','w']
    #directory headchunk
    #dir_hc = HeadChunk( "headchunk_", "p_account", "s_account")
    fs.create("a.txt","r+");
    #fs.open("a.txt","r+");
