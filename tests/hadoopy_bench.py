import hadoopy
import time


def latency_test(launcher):
    output_path = '_hadoopy_bench/%f' % time.time()
    v = 'blah'

    kv = (v, {'client_time': time.time(),
              'value_len': len(v),
              'count': 0})
    hadoopy.writetb(output_path + '/input', [kv])
    launcher(output_path + '/input', output_path + '/output', 'time_job.py')
    v = hadoopy.readtb(output_path + '/output').next()[1]
    v['server_time'] = time.time()
    t0 = v['worker_time'] - v['client_time']
    t1 = v['server_time'] - v['worker_time']
    t2 = v['server_time'] - v['client_time']
    print((t0, t1, t2))
    hadoopy.rmr(output_path)


def throughput_test(launcher):
    output_path = '_hadoopy_bench/%f' % time.time()
    v = 'blah'
    kv = (v, {'client_time': time.time(),
              'value_len': len(v),
              'count': 0})
    num_files = 3
    num_kvs = 10000000
    hadoopy.writetb(output_path + '/input/0', (kv for x in xrange(num_kvs)))
    for x in range(1, num_files):
        hadoopy.cp(output_path + '/input/0', output_path + '/input/%d' % x)
    hadoopy.freeze_script('time_job.py')  # Factor out Pyinstaller time
    st = time.time()
    launcher(output_path + '/input', output_path + '/output', 'time_job.py')
    print((num_kvs * num_files) / (time.time() - st))
    hadoopy.rmr(output_path)
#latency_test(hadoopy.launch_frozen)
#latency_test(hadoopy.launch)
throughput_test(hadoopy.launch_frozen)
