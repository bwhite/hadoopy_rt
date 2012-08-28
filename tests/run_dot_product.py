import hadoopy_rt
import hadoopy
import random
import time
import zmq
import multiprocessing
import logging
import numpy as np
logging.basicConfig(level=logging.DEBUG)

JOB_ID = str(random.random())
MACHINES = ['127.0.0.1']
PORTS = random.sample(xrange(49152, 65536), 5)
NUM_MAPPERS = 3


def input_worker(job_id, machines, ports):
    print('Worker')
    work_graph = hadoopy_rt.discover(job_id, machines, ports)
    print(work_graph)
    ctx = zmq.Context()

    def connect(n):
        s = ctx.socket(zmq.PUSH)
        s.connect('tcp://%s:%s' % work_graph[n])
        return s
    socks = [connect(x) for x in range(1, NUM_MAPPERS + 1)]
    while True:
        for x in xrange(20):
            for s in socks:
                s.send_pyobj((x, x))
        for s in socks:
            s.send_pyobj((hadoopy_rt.ReduceWorker(), None))
        time.sleep(5)
        for x in xrange(20):
            for s in socks:
                s.send_pyobj((x, x))
        for s in socks:
            s.send_pyobj((hadoopy_rt.StopWorker(), None))
        break


def main():
    p = multiprocessing.Process(target=input_worker, args=(JOB_ID, MACHINES, PORTS))
    p.start()
    map_confs = [{'cmdenvs': {'A': x}} for x in range(NUM_MAPPERS)]
    output_path = 'hadoopy_rt/output/%f' % time.time()
    hadoopy_rt.launch_map_reduce(output_path, hadoopy_rt.__path__[0] + '/dot_product_job.py', map_confs, {}, MACHINES, PORTS, JOB_ID)
    p.join()
    expected = dict((x, np.sum(x * np.arange(3))) for x in np.arange(20))
    for kv in hadoopy.readtb(output_path):
        assert expected[kv[0]] == kv[1]
        print(kv)
    

if __name__ == '__main__':
    main()
