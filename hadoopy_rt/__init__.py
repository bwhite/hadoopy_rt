import hadoopy
import functools
import socket
import zmq
import time
import os
import random
import json
import hadoopy_helper
import base64
import sys
import hadoopy_rt


class DiscoverTimeout(Exception):
    """There was a problem discovering the participating nodes"""


class FlushWorker(object):
    """Flush worker message"""


class StopWorker(FlushWorker):
    """Stop worker message"""


def _lf(fn):
    from . import __path__
    return os.path.join(__path__[0], fn)


def launch_zmq(input_socket, output_socket, script_path, output_func=False, cleanup_func=None, num_stops=1):
    poll = lambda : len(stopped) >= num_stops or input_socket.poll(100)
    stopped = []
    def _kvs():
        kv = (None, None)
        while len(stopped) < num_stops:
            kv = input_socket.recv_pyobj()
            if isinstance(kv[0], hadoopy_rt.StopWorker):
                stopped.append(True)
                if len(stopped) >= num_stops:
                    if cleanup_func:
                        cleanup_func()
                else:
                    continue
            yield kv

    for kv in hadoopy.launch_local(_kvs(), None, script_path, poll=poll)['output']:
        if output_func:
            output_socket(kv)
        else:
            output_socket.send_pyobj(kv)


def launch_tree_same(output_path, script_path, height, machines, ports, job_id):
    num_nodes = 2 ** (height) - 1
    out_nodes = dict((x, (x - 1) / 2) for x in range(1, 2 ** (height) - 1))
    print(out_nodes)
    v = {'script_name': os.path.basename(script_path),
         'script_data': open(script_path).read(),
         'out_nodes': out_nodes,
         'num_nodes': num_nodes}
    cmdenvs = {'machines': base64.b64encode(json.dumps(machines)),
               'job_id': job_id,
               'ports': base64.b64encode(json.dumps(ports))}
    with hadoopy_helper.hdfs_temp() as input_path:
        for node_num in range(num_nodes):
            hadoopy.writetb('%s/%d' % (input_path, node_num), [(node_num, v)])
        hadoopy.launch(input_path, output_path, _lf('hadoopy_rt_job.py'), cmdenvs=cmdenvs,
                       jobconfs={'mapred.map.tasks.speculative.execution': 'false',
                                 'mapred.reduce.tasks.speculative.execution': 'false'})


def _get_ip():
    ips = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1]
    if len(ips) != 1:
        raise ValueError('Could not find local ip: %s' % str(ips))
    return ips[0]


def _bind_first_port(zmq_sock, ports):
    for port in ports:
        try:
            zmq_sock.bind('tcp://*:%s' % (port,))
        except zmq.core.error.ZMQError:
            continue
        else:
            return port


def discover_server(job_id, num_nodes, machines, ports, setup_deadline=30):
    # Setup discover port
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    work_graph = {}  # [node_num] = (host, port)
    _bind_first_port(sock, ports)
    start_time = time.time()
    while True:
        if time.time() - start_time >= setup_deadline:  # TODO(brandyn): Put this on the recv too
            raise DiscoverTimeout()
        slave_job_id, slave_node_num, slave_ip, slave_port = sock.recv_pyobj()
        if job_id != slave_job_id:
            sock.send_pyobj('wrongid')  # They connected to the wrong server
        else:
            if slave_node_num >= 0:  # Negative values are ignored
                try:
                    if work_graph[slave_node_num] != (slave_ip, slave_port):
                        raise ValueError('Conflicting slave ids [%s] != [%s]' % (work_graph[slave_node_num], (slave_ip, slave_port)))
                except KeyError:
                    work_graph[slave_node_num] = slave_ip, slave_port
            if len(work_graph) < num_nodes:
                sock.send_pyobj('trylater')  # They connected to the wrong server
            else:
                sock.send_pyobj(work_graph)


def discover(job_id, machines, ports, node_num=-1, input_port=-1):
    ctx = zmq.Context()
    slave_ip = _get_ip()
    socks = []
    try:
        for machine in machines:
            for port in ports:
                sock = ctx.socket(zmq.REQ)
                sock.connect('tcp://%s:%s' % (machine, port))
                sock.send_pyobj((job_id, node_num, slave_ip, input_port))
                socks.append(sock)
        while True:
            sock = zmq.select(socks, [], [])[0][0]
            data = sock.recv_pyobj()
            if data == 'trylater':
                time.sleep(.1)
                sock.send_pyobj((job_id, node_num, slave_ip, input_port))
            elif isinstance(data, dict):
                return data
    finally:
        # Close sockets
        for sock in socks:
            sock.close(0)


def _output_iter(iter_or_none):
    if iter_or_none is None:
        return ()
    return iter_or_none




def close_on_flush(func):

    def wrap(self, key, value):
        if isinstance(key, hadoopy_rt.FlushWorker):
            for x in _output_iter(self.close()):
                yield x
            yield key, value  # Yield the original FlushWorker
        else:
            for x in _output_iter(func(self, key, value)):
                yield x
    return wrap

