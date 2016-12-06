from filesystem.constants import MOUNT_ROOT
from filesystem.filesystem import FileSystem

if __name__ == "__main__":
    fs = FileSystem(MOUNT_ROOT)
    # fs.add_account()
    # fs.add_account()
    fs.start()
    fs.create("/test.txt", None)
    # fs.create("/test.txt", "r+")
