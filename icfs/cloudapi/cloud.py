from icfs.cloudapi.google import GDrive


# @class_decorator(logger)
class Cloud:
    def __init__(self, gdrive_settings, tmp, creds):
        self.gdrive_settings = gdrive_settings
        self.clients = {}
        self.creds = creds
        self.tmp = tmp

    def restore_gdrive(self, client_id):
        g_drive = GDrive(self.tmp, self.creds, self.gdrive_settings)
        g_drive.restore(client_id)
        self.clients[client_id] = g_drive

    def add_gdrive(self):
        g_drive = GDrive(self.tmp, self.creds, self.gdrive_settings)
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
    def push_all(self, file_list, client_id):
        self.clients[client_id].push_all(file_list)

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
