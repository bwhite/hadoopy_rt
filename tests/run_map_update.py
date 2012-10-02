import hadoopy_rt
import hadoopy
import random
import time
import zmq
import multiprocessing
import logging
from twitter import twitter
logging.basicConfig(level=logging.DEBUG)

JOB_ID = str(random.random())
MACHINES = ['127.0.0.1']
PORTS = random.sample(xrange(49152, 65536), 5)
fruits = ['apple','banana','carrot']

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
        for x in twitter(fruits):
            s.send_pyobj(x)


def main():
    p = multiprocessing.Process(target=input_worker, args=(JOB_ID, MACHINES, PORTS))
    p.start()
    sum_job_script = hadoopy_rt.__path__[0] + '/sum_job.py'
    tokenize_script = hadoopy_rt.__path__[0] + '/twitter_tokenize_job.py'
    nodes = [{'script_path': y, 'name': x} for x, y in enumerate([tokenize_script, sum_job_script])]
    hadoopy_rt.launch_map_update(nodes, MACHINES, PORTS, JOB_ID)
    p.join()

if __name__ == '__main__':
    main()
