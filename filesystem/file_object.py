from filesystem.head_chunk import HeadChunk


class FileObject:
    def __init__(self, file_path):
        self.file_path= file_path
        self.head_chunk = None
        self.local_file = None