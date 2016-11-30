import random

from file_object import FileObject
from head_chunk import HeadChunk


class FileSystem:
    def _init_(self):
        self.accounts = []

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

    def create(self, path, mode):#file_name
        #iterate through cur dir head chunk data and append the data with given file name
        ####if the file_name is a path traverse accordingly and finally append to the data
        #########if at any point of traverasal the folder wasnt found append to the headchunk data of that iteration, the name of the folder and continue
        #after appending to head chunk creat a inode for the file(with random number), create chunk meta (with random number), then create one chunk (with chunk name)
        #Push these files using apis
        #return success

        #create head chunk,meta_chunk and one chunk for the file
        p_account = self.__get_random_account()
        s_account = self.__get_random_account(p_account)
        fo = FileObject(path)
        fo.create(p_account ,s_account)

        pass

    def getattr(self, path, fh=None):
        pass

    def getxattr(self, path, name, position=0):
        pass

    def listxattr(self, path):
        pass

    def mkdir(self, path, mode):
        pass

    def open(self, path, flags):#file_name
        pass
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
    fs.create("a.txt",0);
    #fs.open("a.txt");
