import hadoopy
import hadoopy_rt
import zmq
import time
import os
import json
import multiprocessing
import base64
import sys
import redis


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
        outputs = data.get('outputs')
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
        if 'files' in data:
            for f, d in data['files'].items():
                open(f, 'w').write(d)
            data['files'] = list(data['files'])  # Convert to list, removes memory burden
        launch_kw_args = dict((x, data[x]) for x in ['files', 'cmdenvs'] if x in data)
        try:
            launch_kw_args['cmdenvs'] = hadoopy._runner._listeq_to_dict(launch_kw_args['cmdenvs'])
        except KeyError:
            launch_kw_args['cmdenvs'] = {}
        launch_kw_args['cmdenvs']['hadoopy_rt_stream'] = str(node_num)
        sys.stderr.write('Extras[%s]\n' % str(launch_kw_args))
        sys.stderr.write('Data[%s]\n' % str(data))
        open(data['script_name'], 'w').write(data['script_data'])
        output_sockets = {}
        for node_num, (host, port) in node_host_ports.items():
            output_socket = ctx.socket(zmq.PUSH)
            output_socket.connect('tcp://%s:%d' % (host, port))
            output_sockets[node_num] = output_socket
        sys.stderr.write('Outputs[%s]' % str(outputs))
        while True:
            try:
                hadoopy_rt.launch_zmq(in_sock, output_sockets, data['script_name'], cleanup_func=cleanup_func, outputs=outputs,
                                      **launch_kw_args)
            except Exception, e:
                sys.stderr.write('%s\n' % str(e))
            sys.stderr.write('Done with zmq\n')
            sys.stderr.write('Waiting for update[%s]\n' % data['script_name'])
            ps = redis.StrictRedis().pubsub()
            ps.subscribe(data['script_name'])
            for x in ps.listen():
                if x['type'] == 'message':
                    sys.stderr.write('Writing update[%s]\n' % data['script_name'])
                    open(data['script_name'], 'w').write(x['data'])
                    break
                        

if __name__ == '__main__':
    hadoopy.run(Mapper)
