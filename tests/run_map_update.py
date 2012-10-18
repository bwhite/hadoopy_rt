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
REDIS_HOST = hadoopy_rt._get_ip()
fruits = ['apple','banana','carrot']

def input_worker(job_id, redis_host):
    print('Worker')
    flow_controller = hadoopy_rt.FlowController(job_id, redis_host)
    while True:
        for x in twitter(fruits):
            flow_controller.send(0, x)

def main():
    p = multiprocessing.Process(target=input_worker, args=(JOB_ID, REDIS_HOST))
    p.start()
    sum_job_script = hadoopy_rt.__path__[0] + '/sum_job.py'
    tokenize_script = hadoopy_rt.__path__[0] + '/twitter_tokenize_job.py'
    nodes = [{'script_path': y, 'name': x} for x, y in enumerate([tokenize_script, sum_job_script])]
    hadoopy_rt.launch_map_update(nodes, JOB_ID, REDIS_HOST)
    p.join()

if __name__ == '__main__':
    main()
