from apis.google import GDrive


class Cloud:
    def __init__(self, ):
        self.gdrive = None
        self.dropbox = None

    def init_gdrive(self):
        self.gdrive = GDrive()


if __name__ == "__main__":
    cloud = Cloud()
    cloud.init_gdrive()
    files = cloud.gdrive.list_all()
    for f in files:
        print('title: %s, id: %s' % (f['title'], f['id']))
