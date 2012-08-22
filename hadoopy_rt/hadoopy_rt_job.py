import hadoopy
import hadoopy_rt
import zmq
import time
import os
import json
import random


class Mapper(object):

    def __init__(self):
        self.machines = json.loads(os.environ['machines'])
        self.job_id = os.environ['job_id']
        self.discover_ports = json.loads(os.environ['discover_ports'])
        self.work_graph = {}  # [node_num] = (host, port)
        self.setup_deadline = float(os.environ('setup_timeout', '30')) + time.time()
        self.zmq_ctx = zmq.Context()

    def map(self, node_num, data):
        num_nodes = data['num_nodes']
        ctx = zmq.Context()
        in_sock = ctx.socket(zmq.PULL)
        # Randomly select an input port
        worker_port = hadoopy_rt._bind_first_port(in_sock, xrange(49152, 65536))
        if node_num == 0:
            work_graph = hadoopy_rt.discover_master(self.job_id, self.machines, self.ports, self.setup_deadline, num_nodes,
                                                    node_num, worker_port)
        else:
            work_graph = hadoopy_rt.discover_slave(self.job_id, self.machines, self.ports, self.setup_deadline, num_nodes,
                                                   node_num, worker_port)
        open(data['script_name'], 'w').write(data['script_data'])


if __name__ == '__main__':
    hadoopy.run(Mapper)
