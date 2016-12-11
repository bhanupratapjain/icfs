import os

from icfs.cloudapi.constants import CLOUD_TEMP_DIR_NAME
from icfs.cloudapi.google import GDrive
from icfs.global_constants import DATA_ROOT
from icfs.logger import class_decorator, logger


# @class_decorator(logger)
class Cloud:
    def __init__(self, gdrive_settings):
        self.gdrive_settings = gdrive_settings
        self.clients = {}
        self.tmp = self.__init_tmp_dir()

    def __init_tmp_dir(self):
        loc = os.path.join(DATA_ROOT, CLOUD_TEMP_DIR_NAME)
        if not os.path.isdir(loc):
            os.mkdir(loc)
        return loc

    def restore_gdrive(self, client_id):
        g_drive = GDrive(self.tmp, self.gdrive_settings)
        g_drive.restore(client_id)
        self.clients[client_id] = g_drive

    def add_gdrive(self):
        g_drive = GDrive(self.tmp, self.gdrive_settings)
        client_id = g_drive.init_auth()
        self.clients[client_id] = g_drive
        return client_id

    # Raises CloudIOError
    def pull(self, filename, client_id):
        self.clients[client_id].pull(filename)

    # Raises CloudIOError
    def push(self, filename, client_id):
        self.clients[client_id].push(filename)

    # Raises CloudIOError
    def remove(self, filename, client_id):
        self.clients[client_id].remove(filename)

    # Removes everything from the cloud
    def remove_all(self, client_id):
        self.clients[client_id].remove_all()

    def about(self, client_id):
        about = self.clients[client_id].about()
        return {
            "current_user_name": about['name'],
            "root_folder_id": about['rootFolderId'],
            "total_quota": about['quotaBytesTotal'],
            "used_quota": about['quotaBytesUsed']
        }
