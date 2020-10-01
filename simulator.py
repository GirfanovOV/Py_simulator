import yaml
import os

from trace_preprocessing import preprocessor
from Caches import Cache


class simulator:

    def __init__(self, config_file):
        assert(os.path.exists(config_file)) , 'Config file {} not found.'.format(config_file)
        
        with open(config_file, 'r') as f:
            cfg = [i for i in yaml.safe_load_all(f)]
            self.__dict__ = cfg[0]

        # TODO: check if all class fields filled.

        assert(os.path.isdir(self.trace_folder) and os.listdir(self.trace_folder)), \
            "Can't find trace folder ({}), or trace folder is empty".format(self.trace_folder)
        

        self.cache = cache_class_holder[self.cache_type](chunk_size=self.chunk_size, cache_size=self.cache_size)
        

    def run_all_traces(self):

        list_of_traces = os.listdir(self.trace_folder)

        for trace in list_of_traces:
            curr_trace = preprocessor(trace).process_trace()
            self.run_on_single_trace(curr_trace)
            self.report(trace)


    def run_on_single_trace(self, trace):
        self.cache.restart()
        trace.apply(self.cache.handle_requests, axis=1)

    def report(self, trace_name):
        report = self.cache.report()
        


        
         
        
