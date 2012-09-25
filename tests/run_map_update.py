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
    s = connect(0)
    while True:
        for x in xrange(20000):
            s.send_pyobj((x % 10, 1))
        time.sleep(5)
        break


def main():
    p = multiprocessing.Process(target=input_worker, args=(JOB_ID, MACHINES, PORTS))
    p.start()
    sum_job_script = hadoopy_rt.__path__[0] + '/sum_job.py'
    hadoopy_rt.launch_map_update([sum_job_script], MACHINES, PORTS, JOB_ID)
    p.join()

if __name__ == '__main__':
    main()
