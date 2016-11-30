import constants
from head_chunk import HeadChunk
import uuid

class FileObject:
    def __init__(self, file_path):
        self.file_path= file_path
        self.head_chunk = None
        self.local_file = None

    def create(self,p_account,s_account):
        file_head_chunk_name = constants.HC_PREFIX + str(uuid.uuid4())
        file_hc = HeadChunk(file_head_chunk_name, p_account, s_account)
        file_hc.create()

        #TODO push these files