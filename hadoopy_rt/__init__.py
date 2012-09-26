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
import redis


class DiscoverTimeout(Exception):
    """There was a problem discovering the participating nodes"""


def _lf(fn):
    from . import __path__
    return os.path.join(__path__[0], fn)


def launch_zmq(input_socket, output_sockets, script_path, cleanup_func=None, **kw):
    poll = lambda : input_socket.poll(100)

    def _kvs():
        yield input_socket.recv_pyobj()
    while True:
        for k, v in hadoopy.launch_local(_kvs(), None, script_path, poll=poll, **kw)['output']:
            # k is the node number, v is a k/v tuple
            output_sockets[k].send_pyobj(v)


def launch_map_update(script_paths, machines, ports, job_id):
    num_nodes = len(script_paths)
    for node_num, script_path in enumerate(script_paths):
        v = {'script_name': os.path.basename(script_path),
             'script_data': open(script_path).read(),
             'num_nodes': num_nodes}
        cmdenvs = {'machines': base64.b64encode(json.dumps(machines)),
                   'job_id': job_id,
                   'ports': base64.b64encode(json.dumps(ports))}
        with hadoopy_helper.hdfs_temp() as input_path:
            hadoopy.writetb('%s/%d' % (input_path, node_num), [(node_num, v)])
            hadoopy.launch(input_path, input_path + '/output_path_empty', _lf('hadoopy_rt_job.py'), cmdenvs=cmdenvs,
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


class Slate(object):

    def __init__(self, redis, key):
        self._redis = redis
        self._key = key
        self._get = False
        self._set = False
        self._data = None

    def get(self):
        if not self._get:
            self._get = True
            self._data = self._redis.get(self._key)
        return self._data

    def set(self, data):
        if not self._set:
            self._get = self._set = True
            self._data = data

    def _flush(self):
        if self._set:
            self._redis.set(self._key, self._data)
        

class Updater(object):

    def __init__(self):
        self._redis = redis.StrictRedis()  # TODO(Brandyn): Allow setting non default
        self._stream = os.environ['hadoopy_rt_stream']

    def map(self, key, value):
        slate = Slate(self._redis, ':'.join([self._stream, str(key)]))  # TODO(brandyn): Converting to string allows for collisions
        out = self.update(key, value, slate)
        if out is not None:
            for x in out:
                yield x
        slate._flush()
        
