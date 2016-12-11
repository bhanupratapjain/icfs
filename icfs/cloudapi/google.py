from __future__ import print_function

import threading

import os
from googleapiclient.errors import HttpError
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pydrive.files import ApiRequestError, FileNotUploadedError, FileNotDownloadableError

from icfs.cloudapi.constants import CLOUD_CREDENTIAL_DIR_NAME
from icfs.cloudapi.exceptions import CloudIOError
from icfs.global_constants import DATA_ROOT


class GDrive:
    def __init__(self, tmp_location, settings_file):
        self.client_id = None
        self.cred_dir = self.__init_cred_dir()
        self.gauth = None
        self.drive = None
        self.tmp = tmp_location
        self.settings = settings_file

    def __init_cred_dir(self):
        loc = os.path.join(DATA_ROOT, CLOUD_CREDENTIAL_DIR_NAME)
        if not os.path.isdir(loc):
            os.mkdir(loc)
        return loc

    def restore(self, client_id):
        self.client_id = client_id
        gauth = GoogleAuth(settings_file=self.settings)
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
        self.__init_drive()

    def init_auth(self):
        gauth = GoogleAuth(settings_file=self.settings)
        gauth.CommandLineAuth()
        self.gauth = gauth
        self.__init_drive()
        self.__set_client_id()
        if not os.path.exists(os.path.join(self.cred_dir, self.client_id + ".json")):
            gauth.SaveCredentialsFile(os.path.join(self.cred_dir, self.client_id + ".json"))
        return self.client_id

    def __set_client_id(self):
        about = self.about()
        client_id = about['rootFolderId']
        self.client_id = client_id

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
        return self.drive.ListFile({'q': "'root' in parents or trashed=true"}).GetList()

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
        except HttpError as e:
            raise CloudIOError(e.message)

    def remove(self, filename):
        try:
            c_file = self.__get_file(filename)
            c_file.Delete()
        except ApiRequestError as e:
            raise CloudIOError(e.message)

    def remove_all(self):
        try:
            threads = []
            c_file_list = self.list_all()
            for c_file in c_file_list:
                th = threading.Thread(target=c_file.Delete())
                th.start()
                threads.append(th)
            for th in threads:
                th.join()
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
