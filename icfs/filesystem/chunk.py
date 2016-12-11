import os

from icfs.filesystem import constants
from icfs.logger import class_decorator, logger
from pyrsync import pyrsync


@class_decorator(logger)
class Chunk:
    def __init__(self, mpt, name, flags, accounts, checksum_weak=None, checksum_strong=None):
        self.checksum_weak = checksum_weak
        self.checksum_strong = checksum_strong
        self.mpt = mpt
        self.name = name
        self.flags = flags
        self.accounts = accounts

    def create(self, data=""):
        f = open(os.path.join(self.mpt, self.name), 'w+')
        f.write(data)
        f.seek(0)
        f.close()

    def rsync_create(self , data=""):
        f = open(os.path.join(self.mpt, self.name), 'w+')
        f.write(data)
        f.seek(0)
        checksums = pyrsync.blockchecksums(f, constants.CHUNK_SIZE)
        print "checksums", checksums
        self.checksum_weak, self.checksum_strong = checksums[0][0] if len(checksums[0]) > 0 else None, checksums[1][
            0] if len(checksums[1]) > 0 else None
        print "checksums2", self.checksum_weak, self.checksum_strong
        f.close()
