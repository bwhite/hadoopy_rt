import hadoopy_rt
import hadoopy
import random
import time
import zmq
import multiprocessing

JOB_ID = random.random()
MACHINES = ['127.0.0.1']
PORTS = [44000, 44001, 44002]


def input_worker(job_id, machines, ports):
    work_graph = hadoopy_rt.discover(job_id, machines, ports)
    ctx = zmq.Context()

    def connect(n):
        s = ctx.Socket(zmq.PUSH)
        s.connect('tcp://%s:%s' % work_graph[n])
        return s

    socks = [connect(x) for x in range(1, 3)]
    for x in range(1000):
        for s in socks:
            s.send_pyobj((x % 10, 1))


def main():
    p = multiprocessing.Process(target=input_worker, args=(JOB_ID, MACHINES, PORTS))
    p.start()
    hadoopy_rt.launch_tree_same('hadoopy_rt/output/%f' % time.time(), hadoopy_rt.__path__[0] + 'sum_job.py', 2, JOB_ID)
    p.join()

main()
