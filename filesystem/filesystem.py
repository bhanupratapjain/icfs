from __future__ import print_function

import json
import random

import os

from cloudapi.cloud import Cloud
from constants import CLOUD_ACCOUNTS_FILE_NAME
from exceptions import ICFSError
from file_object import FileObject
from global_constants import PROJECT_ROOT


# TODO::
# Removed Fuse Inheritance to work on Windows
# class FileSystem(LoggingMixIn, Operations):
class FileSystem():
    def __init__(self, mpt):
        self.mpt = mpt
        self.root = None  # FileObject
        self.cwd = None  # FileObject
        self.accounts = []
        self.open_files = dict()
        self.fd = 0
        self.__create_cloud()

    def start(self):
        self.__create_root()
        self.__create_cwd()
        # FUSE(self, self.mtpt, foreground=True)

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
            with open(os.path.join(self.mpt, CLOUD_ACCOUNTS_FILE_NAME), 'a+') as fp:
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
        self.open(path, os.O_WRONLY | os.O_CREAT)

    def getattr(self, path, fh=None):
        return {}

    def getxattr(self, path, name, position=0):
        pass

    def listxattr(self, path):
        pass

    def mkdir(self, path, mode):
        pass

    def __create(self, path):
        p_account = self.__get_random_account()
        s_account = self.__get_random_account(p_account)
        fo = FileObject(self.mpt, path, self.cloud)
        fo.create([p_account, s_account])
        try:
            fo.push()
            self.__update_cwd(fo.head_chunk.name, fo.head_chunk.accounts)
        except ICFSError as ie:
            print
            "Error in Pushing at FileSystem Layer. {}".format(ie.message)
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
        if flags == os.O_WRONLY | os.O_CREAT:
            fo = self.__create(path)
        else:
            fo = FileObject(self.mpt, path, None)
        # fo.open(flags)
        self.fd += 1
        self.open_files[self.fd] = fo
        return self.fd

    def read(self, path, length, offset, fh):
        fo = self.open_files[fh]
        return fo.read(length, offset)

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
    fs = FileSystem("./mpt/")
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
