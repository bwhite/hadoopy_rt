import hadoopy
import hadoopy_rt
import zmq
import time
import os
import json
import random
import multiprocessing
import base64
import sys
import os


class Mapper(object):

    def __init__(self):
        self.machines = json.loads(base64.b64decode(os.environ['machines']))
        self.job_id = os.environ['job_id']
        self.ports = json.loads(base64.b64decode(os.environ['ports']))
        self.setup_deadline = float(os.environ.get('setup_timeout', '30')) + time.time()
        self.zmq_ctx = zmq.Context()

    def map(self, node_num, data):
        num_nodes = data['num_nodes']
        out_nodes = data['out_nodes']
        num_stops = max(1, sum(1 for x, y in out_nodes.items() if node_num == y))
        sys.stderr.write('HadoopyRT: NodeNum[%d] NumStops[%d]\n' % (node_num, num_stops))
        ctx = zmq.Context()
        in_sock = ctx.socket(zmq.PULL)
        # Randomly select an input port
        worker_port = hadoopy_rt._bind_first_port(in_sock, xrange(49152, 65536))
        cleanup_func = None
        if not node_num:
            self.discover_server = multiprocessing.Process(target=hadoopy_rt.discover_server, args=(self.job_id, num_nodes,
                                                                                                    self.machines, self.ports))
            self.discover_server.start()
            cleanup_func = self.discover_server.terminate
                
        work_graph = hadoopy_rt.discover(self.job_id, self.machines, self.ports,
                                         node_num, worker_port)  # [node_num] = (host, port)
        open(data['script_name'], 'w').write(data['script_data'])
        if node_num in out_nodes:
            out_sock = ctx.socket(zmq.PUSH)
            out_sock.connect('tcp://%s:%d' % work_graph[out_nodes[node_num]])
            hadoopy_rt.launch_zmq(in_sock, out_sock, data['script_name'], cleanup_func=cleanup_func, num_stops=num_stops)
        else:
            out_data = []
            hadoopy_rt.launch_zmq(in_sock, out_data.append, data['script_name'], output_func=True, cleanup_func=cleanup_func,
                                  num_stops=num_stops)
            for x in out_data:
                if not isinstance(x[0], hadoopy_rt.FlushWorker):
                    yield x


if __name__ == '__main__':
    hadoopy.run(Mapper)
