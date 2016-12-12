import os

from icfs.global_constants import DATA_ROOT

CHUNK_SIZE = 64
HC_PREFIX = "_hc_"
CM_PREFIX = "_cm_"
CHUNK_PREFIX = "_chunk_"
LOCAL_ASSEMBLED_CHUNK = "_assembled_file_"
ROOT_HC = "_hc_root"
MOUNT_ROOT = os.path.join(DATA_ROOT, "meta")
CLOUD_ACCOUNTS_FILE_NAME = "_fs_cloud_accounts.json"

FILE = 0
DIRECTORY = 1
