import hadoopy
import hadoopy_rt
import zmq
import time
import os
import json
import multiprocessing
import base64
import sys


class Mapper(object):

    def __init__(self):
        self.machines = json.loads(base64.b64decode(os.environ['machines']))
        self.job_id = os.environ['job_id']
        self.ports = json.loads(base64.b64decode(os.environ['ports']))
        self.setup_deadline = float(os.environ.get('setup_timeout', '30')) + time.time()
        self.zmq_ctx = zmq.Context()

    def map(self, node_num, data):
        num_nodes = data['num_nodes']
        sys.stderr.write('HadoopyRT: NodeNum[%d]\n' % (node_num,))
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
        node_host_ports = hadoopy_rt.discover(self.job_id, self.machines, self.ports,
                                              node_num, worker_port)  # [node_num] = (host, port)
        launch_kw_args = dict((x, data[x]) for x in ['files', 'cmdenvs'] if x in data)
        sys.stderr.write('Extras[%s]\n' % str(launch_kw_args))
        sys.stderr.write('Data[%s]\n' % str(data))
        open(data['script_name'], 'w').write(data['script_data'])
        output_sockets = {}
        for node_num, (host, port) in node_host_ports.items():
            output_socket = ctx.socket(zmq.PUSH)
            output_socket.connect('tcp://%s:%d' % (host, port))
            output_sockets[node_num] = output_socket
        hadoopy_rt.launch_zmq(in_sock, output_sockets, data['script_name'], cleanup_func=cleanup_func, **launch_kw_args)

if __name__ == '__main__':
    hadoopy.run(Mapper)
