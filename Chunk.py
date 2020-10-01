# slow precise version 
class Chunk:
    def __init__(self, key, **kwargs):
        assert(isinstance(key, tuple) and len(key) == 2 and isinstance(key[0], str) and isinstance(key[1], int))
        self.key = key
        self.bool_map = {}

        #Statistics

        self.reads = 0
        self.block_reads = 0
        self.prefetches = 0
        self.blocks_prefetched = 0

        self.read_prefetch_hits = {0 : 0, 1 : 0}

        self.read_misses = 0
        self.curr_in_cache = 0

    def __hash__(self):
        return hash(self.key)
    
    def read_chunk(self, start, length):
        curr_cache_misses = 0
        for block in range(start, start+length):
            if block in self.bool_map:
                self.read_prefetch_hits[self.bool_map[block]] += 1
            else:
                self.bool_map[block] = 0
                curr_cache_misses += 1

        self.reads += 1
        self.block_reads += length
        self.read_misses += curr_cache_misses
        self.curr_in_cache += curr_cache_misses
        
        return curr_cache_misses

    def prefetch_chunk(self, start, length):
        curr_cache_misses = 0
        for block in range(start, start+length):
            if block not in self.bool_map:
                self.bool_map[block] = 1
                curr_cache_misses += 1

        self.prefetches += 1
        #important question : do we add whole request length to 'blocks_prefetched', or only cache misses ????
        
        self.blocks_prefetched += length  
        # self.blocks_prefetched += curr_cache_misses
        self.curr_in_cache += curr_cache_misses

        return curr_cache_misses

    def get_stats(self):
        tmp = self.__dict__.copy()
        tmp['read_hits'] = tmp['read_prefetch_hits'][0]
        tmp['prefetch_hits'] = tmp['read_prefetch_hits'][1]
        return tmp

# fast inaccurate version
class Fast_Chunk:
    def __init__(self, key, chunk_size, subChunk_size, **kwargs):
        assert(isinstance(key, tuple) and len(key) == 2 and isinstance(key[0], str) and isinstance(key[1], int))
        self.key = key
        self.chunk_size = chunk_size
        self.subChunk_size = subChunk_size

        self.sector_map = [-1]*(self.chunk_size // self.subChunk_size)

        #Statistics
        self.reads = 0
        self.block_reads = 0
        self.prefetches = 0
        self.blocks_prefetched = 0

        self.read_prefetch_hits = {0 : 0, 1 : 0}

        self.read_misses = 0
        self.curr_in_cache = 0

    def __hash__(self):
        return hash(self.key)
    
    def read_chunk(self, start, length):    
        curr_cache_misses = 0
        
        start_ind = int(start / self.subChunk_size)
        end_ind = int((start+length) / self.subChunk_size) + bool((start+length) % self.subChunk_size)
        for i in range(start_ind, end_ind):
            if self.sector_map[i] == -1:
                curr_cache_misses += self.subChunk_size
                self.sector_map[i] = 0
            else:
                self.read_prefetch_hits[self.sector_map[i]] += self.subChunk_size
        
        self.reads += 1
        #important question : do we add lenth of request or (end_ind - start_ind) * self.subChunk_size ????
        
        self.block_reads += (end_ind - start_ind) * self.subChunk_size
        self.read_misses += curr_cache_misses
        self.curr_in_cache += curr_cache_misses
        
        return curr_cache_misses

    def prefetch_chunk(self, start, length):
        curr_cache_misses = 0
        
        start_ind = int(start / self.subChunk_size)
        end_ind = int((start+length) / self.subChunk_size) + bool((start+length) % self.subChunk_size)
        for i in range(start_ind, end_ind):
            if self.sector_map[i] == -1:
                curr_cache_misses += self.subChunk_size
                self.sector_map[i] = 1
            

        self.prefetches += 1
        #important question : do we add whole request length to 'blocks_prefetched', or only cache misses ????
        
        self.blocks_prefetched += (end_ind - start_ind) * self.subChunk_size  
        # self.blocks_prefetched += curr_cache_misses
        self.curr_in_cache += curr_cache_misses

        return curr_cache_misses

    def get_stats(self):
        tmp = self.__dict__.copy()
        tmp['read_hits'] = tmp['read_prefetch_hits'][0]
        tmp['prefetch_hits'] = tmp['read_prefetch_hits'][1]
        return tmp