from simulator import simulator


config_file = 'config file path here'
trace_directory = 'C:/traces/'
log_file = 'C:/Python_simulator_log.txt'

sim = simulator(config_file)

sim.run_all_traces()