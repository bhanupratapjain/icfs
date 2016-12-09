import json
import os
import random
import time
from errno import ENOENT
from stat import S_IFDIR

from fuse import LoggingMixIn, Operations, FUSE, FuseOSError

import icfs.filesystem.constants as constants
from icfs.cloudapi.cloud import Cloud
from icfs.filesystem.constants import CLOUD_ACCOUNTS_FILE_NAME
from icfs.filesystem.file_object import FileObject
from icfs.global_constants import PROJECT_ROOT


# TODO::
# Removed Fuse Inheritance to work on Windows
class FileSystem(LoggingMixIn, Operations):
    # class FileSystem():
    def __init__(self, mpt):
        self.mnt = mpt
        self.mpt = constants.MOUNT_ROOT
        self.root = None  # FileObject
        self.cwd = None  # FileObject
        self.accounts = []
        self.open_files = dict()
        self.fd = 0
        self.__create_cloud()

    def start(self):
        self.__create_root()
        # self.__create_cwd()
        FUSE(self, self.mnt, foreground=True)

    def __create_root(self):
        self.root = FileObject(self.mpt, "/", self.cloud)
        self.root.create_root(self.accounts)

    def __create_cwd(self):
        self.cwd = self.root

    # 1. Create Cloud Object
    # 2. Check if accounts already registered.
    def __create_cloud(self):
        self.cloud = Cloud(os.path.join(PROJECT_ROOT, "gdirve_settings.yaml"))
        if os.path.exists(os.path.join(self.mpt, CLOUD_ACCOUNTS_FILE_NAME)):
            self.__load_accounts()
        else:
            with open(os.path.join(self.mpt, CLOUD_ACCOUNTS_FILE_NAME),
                      'a+') as fp:
                data = {
                    "accounts": []
                }
                json.dump(data, fp)

    def __load_accounts(self):
        data = {}
        with open(os.path.join(self.mpt, CLOUD_ACCOUNTS_FILE_NAME), "r") as af:
            data = json.load(af)
        for account_id in data['accounts']:
            self.cloud.restore_gdrive(account_id)
            self.accounts.append(account_id)

    def add_account(self):
        account_id = self.cloud.add_gdrive()
        self.accounts.append(account_id)
        with open(os.path.join(self.mpt, CLOUD_ACCOUNTS_FILE_NAME), "r+") as af:
            data = json.load(af)
            data["accounts"].append(account_id)
            af.seek(0)
            json.dump(data, af)

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

    # Should Push After Successful create
    def create(self, path, mode, fip=None):  # file_name
        print "create", path
        try:
            return self.open(path, os.O_WRONLY | os.O_CREAT)
        except Exception:
            import traceback
            traceback.print_exc()
            return 10

    def getattr(self, path, fh=None):
        print "getattr", path
        now = time.time()
        parent, file = os.path.split(path)
        print "Hello"
        if parent == "/" and file == "":
            return dict(st_mode=(S_IFDIR | 0o755), st_ctime=now, st_mtime=now,
                        st_atime=now, st_nlink=2)

        elif file.startswith("_") or file == ".DS_Store":
            raise FuseOSError(ENOENT)
        else:
            print "Before getattr"
            fo = FileObject(self.mpt, path, self.cloud)
            d = fo.getattr()
            print "After getattr", d
            if d == {}:
                raise FuseOSError(ENOENT)
            return d

    def getxattr(self, path, name, position=0):
        print "getxattr", path
        return ''

    def listxattr(self, path):
        print "listxattr", path

    def mkdir(self, path, mode):
        print("mkdir", path)

    def __create(self, path):
        p_account = self.__get_random_account()
        s_account = self.__get_random_account(p_account)
        fo = FileObject(self.mpt, path, self.cloud)
        fo.create([p_account, s_account])
        # try:
        #     fo.push()
        #     self.__update_cwd(fo.head_chunk.name, fo.head_chunk.accounts)
        # except ICFSError as ie:
        #     print
        #     "Error in Pushing at FileSystem Layer. {}".format(ie.message)
        return fo

    # 1.assembles chunk file for self.cwd
    # 2.appends the newly created file to the assembled file
    # 3.pushes self.cwd
    def __update_cwd(self, hc_name, hc_accounts):
        # TODO: Remove assemble as assemble combines constants.CHUNK_SIZE
        cwd_chunk_file_name = self.cwd.assemble()
        with open(os.path.join(self.mpt, cwd_chunk_file_name), "a") as f:
            wr_str = hc_name
            for acc in hc_accounts:
                wr_str += " {}".format(acc)
            f.write(wr_str)
        self.cwd.push()  # does rsync and pushes
        # self.cwd.head_chunk.load() #To be used when we decide to divide directories data into chunks

    def open(self, path, flags):  # file_name
        # TODO
        ####
        # Append in Parent Directory during path
        # if the file_name is a path traverse accordingly and finally append to the data
        # Push these files using cloudapi
        # return success
        print "open", path
        if flags == os.O_WRONLY | os.O_CREAT:
            fo = self.__create(path)
        else:
            fo = FileObject(self.mpt, path, self.cloud)

        if flags == os.O_APPEND:
            fo.open('a')
        elif flags == os.O_RDONLY:
            fo.open('r')
        elif flags == os.O_WRONLY:
            fo.open('w')
        elif flags == os.O_RDWR:
            fo.open("r+")
        print "after open"
        self.fd += 1
        print "Test"
        self.open_files[self.fd] = fo
        print "fd", self.fd
        return self.fd

    def read(self, path, length, offset, fh):
        fo = self.open_files[fh]
        return fo.read(length, offset)

    def readdir(self, path, fh):
        print "readdir", path
        try:
            fo = FileObject(self.mpt, path, self.cloud)
            fo.open('r')
            files = []
            for line in fo.os_fh:
                files.append(line.split()[0])
            fo.close()
            print "direct", files
            # return files
            return ["hello"]
        except Exception:
            import traceback
            traceback.print_exc()
            return []

    def release(self, path, fh):
        print "release", path, fh
        fo = self.open_files[fh]
        fo.close()

    def flush(self, path, fh):
        print "flush", path, fh

    def readlink(self, path):
        print "readlink", path

    def removexattr(self, path, name):
        print "removexattr", path

    def rename(self, old, new):
        print "rename", old

    def rmdir(self, path):
        print "rmdir", path

    def setxattr(self, path, name, value, options, position=0):
        print "setxattr", path

    def statfs(self, path):
        print "statfs", path
        return dict(f_bsize=constants.CHUNK_SIZE, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        print "symlink", target

    def truncate(self, path, length, fh=None):
        print "open", path

    def unlink(self, path):
        pass

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        fo = self.open_files[fh]
        bytes = fo.write(data, offset)
        return bytes


if __name__ == "__main__":
    fs = FileSystem("./data/mnt/")
    # fs.add_account()
    # fs.add_account()
    fs.start()
    # fs.getattr("/hello")
    # fs.create("/test", None)
    # fs.accounts = ['s', 'w']
    # directory headchunk
    # dir_hc = HeadChunk( "headchunk_", "p_account", "s_account")
    # fd = fs.create("a.txt", os.O_RDWR | os.O_CREAT)
    # # fd = fs.open("a.txt","r+");
    # fs.write(None, "data", 0, fd)
    # fs.write(None, "s", 0, fd)
    # print "reading ", fs.read(None, "s", 10, 0, fd)
    # fs.add_account("~/mock_cloud1/")
    # fs.add_account("~/mock_cloud2/")
    # fs.init("/")
    # directory headchunk
    # dir_hc = HeadChunk( "headchunk_", "p_account", "s_account")
    # fs.create("a.txt", "r+")
    # fs.open("a.txt","r+");
