import hadoopy_rt
import hadoopy
import random
import time
import zmq
import multiprocessing
import logging
logging.basicConfig(level=logging.DEBUG)

JOB_ID = str(random.random())
MACHINES = ['127.0.0.1']
PORTS = random.sample(xrange(49152, 65536), 5)


def input_worker(job_id, machines, ports):
    print('Worker')
    work_graph = hadoopy_rt.discover(job_id, machines, ports)
    print(work_graph)
    ctx = zmq.Context()

    def connect(n):
        s = ctx.socket(zmq.PUSH)
        s.connect('tcp://%s:%s' % work_graph[n])
        return s
    socks = [connect(x) for x in range(1, 3)]
    while True:
        for x in range(20):
            for s in socks:
                s.send_pyobj((x % 10, 1))
        for s in socks:
            s.send_pyobj((hadoopy_rt.StopWorker(), None))
        time.sleep(5)
        break


def main():
    p = multiprocessing.Process(target=input_worker, args=(JOB_ID, MACHINES, PORTS))
    p.start()
    hadoopy_rt.launch_tree_same('hadoopy_rt/output/%f' % time.time(), hadoopy_rt.__path__[0] + '/sum_job.py', 2, MACHINES, PORTS, JOB_ID)
    p.join()

if __name__ == '__main__':
    main()
