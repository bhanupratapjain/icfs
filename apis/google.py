from __future__ import print_function

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


class GDrive:
    def __init__(self):
        self.gauth = None
        self.drive = None
        self.__init_auth()
        self.__init_drive()

    def __init_auth(self):
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile("icfs_credentials.json")
        if gauth.credentials is None:
            gauth.CommandLineAuth()
        elif gauth.access_token_expired:
            # Refresh them if expired
            gauth.Refresh()
        else:
            # Initialize the saved creds
            gauth.Authorize()
        gauth.SaveCredentialsFile("icfs_credentials.json")
        self.gauth = gauth

    def __init_drive(self):
        self.drive = GoogleDrive(self.gauth)

    def pull(self):
        return self.drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()

    def push(self):
        pass


if __name__ == '__main__':
    pass
