import constants
from head_chunk import HeadChunk
import uuid,os

class FileObject:
    def __init__(self, file_path):
        self.file_path= file_path
        self.head_chunk = None
        self.local_file = None

    def create(self,p_account,s_account):
        file_head_chunk_name = constants.HC_PREFIX + str(uuid.uuid4())
        self.head_chunk = HeadChunk(file_head_chunk_name, p_account, s_account)
        self.head_chunk.create()

    def open(self,flags):
        #TODO
        #download file
        local_file_name = self.__assemble()
        #open local copy
        self.local_file = open(local_file_name,flags)

    def __assemble(self):
        chunks = self.head_chunk.chunk_meta.chunks
        local_file_name = constants.LOCAL_ASSEMBLED_CHUNK + str(uuid.uuid4())
        with open(local_file_name,"w") as of:
            for chunk in chunks:
                with open(chunk.name,"r") as chf:
                    buf = chf.read(constants.CHUNK_SIZE)
                    of.write(buf)
                os.remove(chunk.name)

        return local_file_name
        #TODO push these files