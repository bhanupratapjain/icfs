from filesystem.chunk_meta import ChunkMeta


class HeadChunk:
    def __init__(self, name, p_account, s_account):
        self.name = name
        self.p_account = p_account
        self.s_account = s_account
        self.chunk_meta_name = None
        self.chunk_meta = None

    # 1. Fetch and init Head Chunk
    def __fetch_head_chunk(self):
        self.chunk_meta_name = None

    # Fetch Chunk Meta from Cloud and Create Local Chunk Meta object
    def __load_chunk_meta(self):
        self.chunk_meta = ChunkMeta(self.chunk_meta_name, self.p_account, self.s_account)
