import os
from shutil import copyfile

from cloud_driver import CloudDriver


class MockCloud(CloudDriver):
    def __init__(self, path):
        self.dir = path
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)

    def __str__(self):
        return self.dir

    def __dict__(self):
        return {"path": self.dir}

    def pull(self, name):
        copyfile(self.dir + name, "./" + name)

    def push(self, name):
        copyfile("./" + name, self.dir + name)

    def delete(self, name):
        os.remove(self.dir + name)
