import os

from cloudapi.constants import CLOUD_TEMP_DIR_NAME
from cloudapi.google import GDrive
from global_constants import DATA_ROOT


class Cloud:
    def __init__(self, gdrive_settings):
        self.gdrive_settings = gdrive_settings
        self.clients = {}
        self.tmp = self.__init_tmp_dir()

    @staticmethod
    def __init_tmp_dir():
        loc = os.path.join(DATA_ROOT, CLOUD_TEMP_DIR_NAME)
        if not os.path.isdir(loc):
            os.mkdir(loc)
        return loc

    def add_gdrive(self, client_id):
        self.clients[client_id] = GDrive(client_id, self.tmp, self.gdrive_settings)
        return client_id

    def pull(self, filename, client_id):
        self.clients[client_id].pull(filename)

    def push(self, filename, client_id):
        self.clients[client_id].push(filename)

    def remove(self, filename, client_id):
        self.clients[client_id].remove(filename)

    def about(self, client_id):
        about = self.clients[client_id].about()
        return {
            "current_user_name": about['name'],
            "root_folder_id": about['rootFolderId'],
            "total_quota": about['quotaBytesTotal'],
            "used_quota": about['quotaBytesUsed']
        }
