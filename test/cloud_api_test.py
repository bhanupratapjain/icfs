import pprint

import os

from icfs.cloudapi.cloud import Cloud
from icfs.global_constants import PROJECT_ROOT

if __name__ == "__main__":
    gdrive_settings = os.path.join(PROJECT_ROOT, "gdirve_settings.yaml")
    cloud = Cloud(gdrive_settings)
    c_id1 = cloud.add_gdrive("1")
    c_id2 = cloud.add_gdrive("2")
    files = cloud.clients[c_id1].list_all()
    for f in files:
        print('title: %s, id: %s, name: %s' % (f['title'], f['id'], f['originalFilename']))
    cloud.pull("Getting started", "1")
    cloud.push("push2.txt", "1")
    cloud.pull("push2.txt", "1")
    # cloud.remove("push2.txt", "1")
    # cloud.remove("push2.txt", "2")
    pprint.pprint(cloud.about("1"))
    pprint.pprint(cloud.about("2"))
