import os
from icfs.filesystem.filesystem import FileSystem
from icfs.global_constants import DATA_ROOT


def remove_files(fs):

    #  Delete All Files From Account
    for acc in fs.accounts:
        fs.cloud.remove_all(acc)

    # Delete All Files From Local
    for path in os.listdir(os.path.join(DATA_ROOT, "meta")):
        if path != "_fs_cloud_accounts.json":
            os.remove(os.path.join(os.path.join(DATA_ROOT, "meta"), path))


if __name__ == "__main__":
    fs = FileSystem(os.path.join(DATA_ROOT, "mnt"))

    remove_files(fs)

    # fs.add_account()
    # fs.add_account()
    # fs.add_account()

    fs.start()
    # fs.create("/test.txt", None)
    # fs.create("/test.txt", "r+")
