from Chunk import Chunk, Fast_Chunk
from temperature_detector import temperature_detector
from FPDC import FPDC
from neighborhood_Pref import neighborhood_Pref


#TODO : окрестности, FPDC, description, comments

class cache_with_temperatures_fixed_size:
    
    chunk_class_holder = {'default' : Chunk, 'fast' : Fast_Chunk}

    def __init__(self, chunk_size=512, cache_size=13107*2, chunk_type='default', prefetcher=None, cache_split=[.33, .33, .33],
        evict_to_lower_temp_lists=False, use_FPDC=False, use_neigh_pref=False, cache_struct_upd_freq=10000, **kwargs):
        self.chunk_size = chunk_size
        self.cache_size = cache_size
        self.chunk_type = chunk_type
        self.prefetcher = prefetcher
        self.cache_split = cache_split
        self.cache_struct_upd_freq = cache_struct_upd_freq
        self.evict_to_lower_temp_lists = evict_to_lower_temp_lists
        
        self.FPDC = FPDC if use_FPDC else None
        self.neigh_pref = neighborhood_Pref if use_neigh_pref else None

        self.temperature_detector = temperature_detector(chunk_size=self.chunk_size)

        assert(len(cache_split) == 3 and 0 <= sum(cache_split) <= 1)
        for i in range(cache_split):
            assert(0 <= cache_split[i] <= 1)

        self.restart()

    def restart(self):
        self.inner_counter = 0
        self.global_chunk_table = {}
        self.chunk_temp_correspondence = {}
        self.temperature_tables = [[], [], []]
        self.blocks_in_cache = [0, 0, 0]
        self.stats = {'reads' : 0,'block_reads' : 0,'prefetches' : 0,'blocks_prefetched' : 0,'read_misses' : 0,'read_hits' : 0,'prefetch_hits' : 0}

    def handle_requests(self, row):
        self.inner_counter += 1

        volume_id = row['volumeId']
        block = row['objLba']
        length = row['length']
        access_time = 0

        if self.FPDC:
            self.FPDC.insert(volume_id, block, length)

        req_temp = self.temperature_detector.get_temperature(block)
        blocks_prefetched = self.prefetcher.read(volume_id, block, length, access_time, req_temp) if self.prefetcher else []
        assert(blocks_prefetched != None)

        if not isinstance(blocks_prefetched, list):
            blocks_prefetched = list(blocks_prefetched)
        
        self.put_block_in_cache(volume_id, block, length)

        if self.neigh_pref:
            additional_prefetch = []
            additional_prefetch += self.neigh_pref.read(volume_id, block, length, access_time, req_temp)
            for bp in blocks_prefetched:
                bp_temp = self.temperature_detector.get_temperature(bp.start)
                additional_prefetch += self.neigh_pref.read(volume_id, bp.start, bp.end-bp.start, access_time, bp_temp)
            blocks_prefetched += additional_prefetch
            
        if blocks_prefetched:
            for bp in blocks_prefetched:
                self.put_block_in_cache(volume_id, bp.start, bp.end-bp.start, 1)

        self.check_for_uptades()

    def put_block_in_cache(self, volume_id, block, length, prefetch=0):
        curr_chunk_id = int(block / self.chunk_size)
        curr_offset =  block % self.chunk_size
        remaining_length = length

        while remaining_length > 0:
            curr_length = min(remaining_length, self.chunk_size - curr_offset)
            chunk_key = (volume_id, curr_chunk_id)

            self.temperature_detector.update(volume_id, curr_chunk_id, curr_length)

            if chunk_key not in self.global_chunk_table:
                self.global_chunk_table[chunk_key] = \
                    self.chunk_class_holder[self.chunk_type](key=chunk_key, 
                    chunk_size=self.chunk_size, subChunk_size=self.chunk_size)
                
                chunk_temp = self.temperature_detector.get_temperature(block)
                self.chunk_temp_correspondence[chunk_key] = chunk_temp
                self.temperature_tables[chunk_temp].append(chunk_key)
            
            if prefetch:
                new_blocks = self.global_chunk_table[chunk_key].prefetch_chunk(curr_offset, curr_length)
            else:
                new_blocks = self.global_chunk_table[chunk_key].read_chunk(curr_offset, curr_length)
            
            chunk_temp = self.chunk_temp_correspondence[chunk_key]
            self.temperature_tables[chunk_temp].sort(key=lambda x: x==chunk_key)
            self.blocks_in_cache[chunk_temp] += new_blocks

            self.eviction()

    def eviction(self):
        for temp in reversed(range(len(self.temperature_tables))):
            while self.blocks_in_cache[temp] >= int(self.cache_split[temp]*self.cache_size):
                curr_chunk = self.temperature_tables[temp].pop(0)
                self.blocks_in_cache[temp] -= self.global_chunk_table[curr_chunk].curr_in_cache
                if self.evict_to_lower_temp_lists and temp:
                    self.temperature_tables[temp-1].append(curr_chunk)
                    self.chunk_temp_correspondence[curr_chunk] -= 1
                    self.blocks_in_cache[temp-1] += self.global_chunk_table[curr_chunk].curr_in_cache
                else:
                    self.collect_stat(self.global_chunk_table[curr_chunk])
                    del self.chunk_temp_correspondence[curr_chunk]
                    del self.global_chunk_table[curr_chunk]

    def collect_stat(self, chunk):
        chunk_stat = chunk.get_stats()
        for key in  self.stats.keys():
            self.stats[key] += chunk_stat[key]

    def report(self):
        return self.stats.copy()

    def change_cache_split(self, new_cache_split):
        assert(len(new_cache_split) == 3 and 0 <= sum(new_cache_split) <= 1)
        for val in new_cache_split:
            assert(0 <= val <= 1)
        
        self.cache_split = new_cache_split
        self.eviction()

    def check_for_uptades(self):
        if not (self.inner_counter % self.cache_struct_upd_freq) and self.inner_counter:
            FPDC_res = None
            if self.FPDC:
                FPDC_res = FPDC.count_optimal_values(self.temperature_detector, self.cache_size)
            
            if FPDC_res:
                new_cache_split = [ i/self.cache_size for i in FPDC_res]
                self.change_cache_split(new_cache_split)
                
                if self.neigh_pref:
                    self.neigh_pref.change_neigh(FPDC_res)