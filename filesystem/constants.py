import os

from global_constants import DATA_ROOT

CHUNK_SIZE = 64
HC_PREFIX = "hc_"
CM_PREFIX = "cm_"
CHUNK_PREFIX = "chunk_"
LOCAL_ASSEMBLED_CHUNK = "assembled_file_"
ROOT_HC = "hc_root"
MOUNT_ROOT = os.path.join(DATA_ROOT, "mnt")
CLOUD_ACCOUNTS_FILE_NAME = "fs_cloud_accounts.json"
