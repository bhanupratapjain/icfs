import json
import os
import random
import time
from errno import ENOENT
from stat import S_IFDIR

from fuse import Operations, FUSE, FuseOSError

import icfs.filesystem.constants as constants
from icfs.cloudapi.cloud import Cloud
from icfs.filesystem.constants import CLOUD_ACCOUNTS_FILE_NAME
from icfs.filesystem.exceptions import ICFSError
from icfs.filesystem.file_object import FileObject
from icfs.filesystem.head_chunk import HeadChunk
from icfs.global_constants import PROJECT_ROOT, DATA_ROOT
from icfs.logger import logger, class_decorator


@class_decorator(logger)
class FileSystem(Operations):
    def __init__(self, mpt):
        self.mnt = mpt
        self.meta = constants.MOUNT_ROOT
        self.root = None  # FileObject
        self.cwd = None  # FileObject
        self.accounts = []
        # Might need to mutex protect
        self.open_files = dict()  # {k-fd, v- icfs fo}
        self.open_file_names = dict()  # {k-name, v- number_of_open}
        self.fd = 0
        self.__create_cloud()

    def start(self):
        print "Creating Root..."
        self.__create_root()
        print "Starting Fuse..."
        FUSE(self, self.mnt, foreground=True)

    def __create_root(self):
        self.root = FileObject(self.meta, "/", self.cloud)
        self.root.create_root(self.accounts)

    def __create_cwd(self):
        self.cwd = self.root

    # 1. Create Cloud Object
    # 2. Check if accounts already registered.
    def __create_cloud(self):
        self.cloud = Cloud(os.path.join(PROJECT_ROOT, "gdirve_settings.yaml"))
        if os.path.exists(os.path.join(self.meta, CLOUD_ACCOUNTS_FILE_NAME)):
            self.__load_accounts()
        else:
            with open(os.path.join(self.meta, CLOUD_ACCOUNTS_FILE_NAME),
                      'a+') as fp:
                data = {
                    "accounts": []
                }
                json.dump(data, fp)

    def __load_accounts(self):
        data = {}
        with open(os.path.join(self.meta, CLOUD_ACCOUNTS_FILE_NAME), "r") as af:
            data = json.load(af)
        for account_id in data['accounts']:
            self.cloud.restore_gdrive(account_id)
            self.accounts.append(account_id)

    def add_account(self):
        account_id = self.cloud.add_gdrive()
        self.accounts.append(account_id)
        with open(os.path.join(self.meta, CLOUD_ACCOUNTS_FILE_NAME),
                  "r+") as af:
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
        try:
            return self.open(path, os.O_WRONLY | os.O_CREAT)
        except Exception:
            import traceback
            traceback.print_exc()
            return 10

    def getattr(self, path, fh=None):
        now = time.time()
        parent, file = os.path.split(path)
        if parent == "/" and file == "":
            return dict(st_mode=(S_IFDIR | 0o755), st_ctime=now, st_mtime=now,
                        st_atime=now, st_nlink=2)

        elif file.startswith("_") or file == ".DS_Store":
            raise FuseOSError(ENOENT)
        else:
            fo = FileObject(self.meta, path, self.cloud)
            try:
                self.__find_head_chunk(fo)
                d = fo.getattr()
            except ICFSError as err:
                print err.message
                raise FuseOSError(ENOENT)
            return d

    def getxattr(self, path, name, position=0):
        print "getxattr", name, path
        return ''

    def listxattr(self, path):
        print "listxattr", path

    def mkdir(self, path, mode):
        print("mkdir", path)
        p_account = self.__get_random_account()
        s_account = self.__get_random_account(p_account)
        fo = FileObject(self.meta, path, self.cloud)
        fo.create(constants.DIRECTORY, [p_account, s_account])
        fo.parent = self.__update_parent(fo)
        fo.open('w')
        self.__increment_link(fo.file_path)
        fo.a_f_py_obj.write(".  " + fo.head_chunk.name + "  " + (
            "    ".join(fo.head_chunk.accounts) + "\n"))
        fo.a_f_py_obj.write("..  " + fo.parent.head_chunk.name + "  " + (
            "    ".join(fo.parent.head_chunk.accounts) + "\n"))
        self.__close(fo)

    def __create(self, path):
        p_account = self.__get_random_account()
        s_account = self.__get_random_account(p_account)
        fo = FileObject(self.meta, path, self.cloud)
        fo.create(constants.FILE, [p_account, s_account])
        fo.parent = self.__update_parent(fo)
        try:
            fo.push()
            fo.parent.push()
        except ICFSError as ie:
            print
            "Error in Pushing at FileSystem Layer. {}".format(ie.message)
        return fo

    def __update_parent(self, fo):
        directory, name = os.path.split(fo.file_path)
        parent_fo = FileObject(self.meta, directory, self.cloud)
        self.__find_and_open(parent_fo, 'a')
        wr_str = "{} {}".format(name, fo.head_chunk.name)
        for acc in fo.head_chunk.accounts:
            wr_str += " {}".format(acc)
        wr_str += "\n"
        parent_fo.write(wr_str, 0)
        self.__close(parent_fo)
        return parent_fo

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
            fo = FileObject(self.meta, path, self.cloud)
        return self.__open(fo, flags)

    def __get_py_flags(self, flags):
        if flags == os.O_APPEND:
            return 'a'
        elif flags == os.O_RDONLY:
            return 'r'
        elif flags == os.O_WRONLY or flags == os.O_WRONLY | os.O_CREAT:
            return 'w'
        elif flags == os.O_RDWR:
            return "r+"

    # Does not return the root
    def __get_parent_list(self, path):
        parents = path.split(os.sep)
        return filter(lambda x: x != '', parents)

    # Throws ICFSIOError
    def __search_hc(self, a_f_py_obj, file_name):
        for line in a_f_py_obj:
            if line.startswith(file_name):
                hc_data = line.split()
                return hc_data
        raise ICFSError("Head Chunk Not Found")

    # Throws ICFSIOError
    def __find_head_chunk(self, fo):
        if fo.head_chunk is None:

            # If path is root
            if fo.file_path == os.sep:
                fo.head_chunk = self.root.head_chunk
                return

            # Get Split File Paths
            parents = self.__get_parent_list(
                fo.file_path)  # Everything after the root

            # If Path is in root.
            # 1. Search in root only for 1st parent
            # 2. Open returned parend
            # 3. Search again in opened parent
            # 4. Continue 2-3 till the last parent.
            # 5. Search for file_name in last parent
            # 6. Update HC in FO
            parent_fo = self.root
            parent_file_path = "/"
            for i, p in enumerate(parents):
                parent_file_path = os.path.join(parent_file_path, p)
                self.__increment_link(parent_fo.file_path)
                parent_fo.open("r")
                hc_data = self.__search_hc(parent_fo.a_f_py_obj, p)
                self.__close(parent_fo)
                parent_fo = FileObject(self.meta, parent_file_path, self.cloud)
                parent_fo.head_chunk = HeadChunk(self.meta, hc_data[1],
                                                 self.cloud, hc_data[2:])

            fo.head_chunk = parent_fo.head_chunk
        return

    def __find_and_open(self, fo, flags):
        self.__find_head_chunk(fo)
        fo.open(flags)
        self.__increment_link(fo.file_path)

    def __increment_link(self, path):
        if path in self.open_file_names:
            self.open_file_names[path] += 1
        else:
            self.open_file_names[path] = 1

    def __open(self, fo, flags):
        py_flags = self.__get_py_flags(flags)
        self.__find_and_open(fo, py_flags)
        self.fd += 1
        self.open_files[self.fd] = fo
        return self.fd

    def read(self, path, length, offset, fh):
        fo = self.open_files[fh]
        return fo.read(length, offset)

    def readdir(self, path, fh):
        try:
            fo = FileObject(self.meta, path, self.cloud)
            self.__find_and_open(fo, 'r')
            files = []
            for line in fo.a_f_py_obj:
                files.append(line.split()[0])
            self.__close(fo)
            return files
            # return ["hello"]
        except Exception:
            import traceback
            traceback.print_exc()
            return []

    def release(self, path, fh):
        fo = self.open_files[fh]
        self.__close(fo)
        self.open_files.pop(fh)

    def __close(self, fo):
        path = fo.file_path
        links = self.open_file_names[path]
        if links - 1 == 0:
            fo.close(True)
            self.open_file_names.pop(path)
        else:
            fo.close()
            self.open_file_names[path] -= 1

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
        fo = FileObject(self.meta, path, self.cloud)
        self.__find_head_chunk(fo)
        directory, file = os.path.split(path)
        parent = FileObject(self.meta, directory, self.cloud)
        self.__find_and_open(parent, 'r')
        files = []
        for line in parent.a_f_py_obj:
            if not line.startswith(file):
                files.append(tuple(line.split()))

        parent.a_f_py_obj.close()
        parent.a_f_py_obj = open(os.path.join(self.meta, parent.a_f_name), 'w')

        for line in files:
            parent.write(line[0] + " " + line[1] + "\n", 0)

        parent.a_f_py_obj.flush()
        self.__close(parent)

        parent.push()

        fo.remove()

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        fo = self.open_files[fh]
        bytes = fo.write(data, offset)
        return bytes


if __name__ == "__main__":
    fs = FileSystem(os.path.join(DATA_ROOT, "mnt"))
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
