from __future__ import print_function

import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pydrive.files import ApiRequestError, FileNotUploadedError, FileNotDownloadableError

from cloudapi.constants import CLOUD_CREDENTIAL_DIR_NAME
from cloudapi.exceptions import CloudIOError
from global_constants import DATA_ROOT


class GDrive:
    def __init__(self, client_id, tmp_location, settings_file):
        self.client_id = client_id
        self.cred_dir = self.__init_cred_dir()
        self.gauth = None
        self.drive = None
        self.tmp = tmp_location
        self.__init_auth(settings_file)
        self.__init_drive()

    @staticmethod
    def __init_cred_dir():
        loc = os.path.join(DATA_ROOT, CLOUD_CREDENTIAL_DIR_NAME)
        if not os.path.isdir(loc):
            os.mkdir(loc)
        return loc

    def __init_auth(self, settings_file):
        gauth = GoogleAuth(settings_file=settings_file)
        gauth.LoadCredentialsFile(os.path.join(self.cred_dir, self.client_id + ".json"))
        if gauth.credentials is None:
            gauth.CommandLineAuth()
        elif gauth.access_token_expired:
            # Refresh them if expired
            gauth.Refresh()
        else:
            # Initialize the saved creds
            gauth.Authorize()
        gauth.SaveCredentialsFile(os.path.join(self.cred_dir, self.client_id + ".json"))
        self.gauth = gauth

    def __init_drive(self):
        self.drive = GoogleDrive(self.gauth)

    def __get_file(self, filename):
        files_array = self.list_file(filename)
        if len(files_array) != 1:
            raise CloudIOError("Unable to Fetch correct File Info from Cloud")
        return files_array[0]

    def list_file(self, filename):
        query = "title = '" + filename + "'"
        return self.drive.ListFile({'q': query}).GetList()

    def list_all(self):
        return self.drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()

    def about(self):
        return self.drive.GetAbout()

    def push(self, filename):
        try:
            c_file = self.__get_file(filename)
        except CloudIOError:
            # Create New File
            c_file = self.drive.CreateFile({'title': filename})
        try:
            c_file.SetContentFile(os.path.join(self.tmp, filename))
            c_file.Upload()
        except ApiRequestError as e:
            raise CloudIOError(e.message)

    def remove(self, filename):
        try:
            c_file = self.__get_file(filename)
            c_file.Delete()
        except ApiRequestError as e:
            raise CloudIOError(e.message)

    def pull(self, filename):
        try:
            c_file = self.__get_file(filename)
            c_file.GetContentFile(os.path.join(self.tmp, filename))
        except ApiRequestError as e:
            raise CloudIOError(e.message)
        except FileNotUploadedError as e:
            raise CloudIOError(e.message)
        except FileNotDownloadableError as e:
            raise CloudIOError(e.message)
