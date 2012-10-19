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


class SendTimeout(Exception):
    """Timed out while sending to a node"""


def _lf(fn):
    from . import __path__
    return os.path.join(__path__[0], fn)


def launch_zmq(flow_controller, script_path, cleanup_func=None, outputs=None, **kw):

    def _kvs():
        while True:
            flow_controller.heartbeat()
            yield flow_controller.recv()

    kvs = hadoopy.launch_local(_kvs(), None, script_path, poll=flow_controller.poll, **kw)['output']
    if outputs is None:
        for k, v in kvs:
            # k is the node number, v is a k/v tuple
            flow_controller.send(k, v)
    else:
        for kv in kvs:
            for s in outputs:
                flow_controller.send(s, kv)


def launch_map_update(nodes, job_id, redis_host):
    num_nodes = len(nodes)
    with hadoopy_helper.hdfs_temp() as input_path:
        for node in nodes:
            print(node)
            v = {'script_name': os.path.basename(node['script_path']),
                 'script_data': open(node['script_path']).read(),
                 'num_nodes': num_nodes}
            if 'cmdenvs' in node and node['cmdenvs'] is not None:
                v['cmdenvs'] = node['cmdenvs']
            if 'files' in node and node['files'] is not None:
                v['files'] = dict((os.path.basename(f), open(f).read()) for f in node['files'])
            cmdenvs = {'job_id': job_id,
                       'hadoopy_rt_redis': redis_host}
            if 'outputs' in node and node['outputs']:
                v['outputs'] = node['outputs']
            hadoopy.writetb('%s/input/%d' % (input_path, node['name']), [(node['name'], v)])
        hadoopy.launch(input_path + '/input', input_path + '/output_path_empty', _lf('hadoopy_rt_job.py'), cmdenvs=cmdenvs,
                       jobconfs={'mapred.map.tasks.speculative.execution': 'false',
                                 'mapred.reduce.tasks.speculative.execution': 'false',
                                 'mapred.task.timeout': '0'})

    
def _get_ip():
    ips = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1]
    if len(ips) != 1:
        raise ValueError('Could not find local ip: %s' % str(ips))
    return ips[0]


class FlowController(object):

    def __init__(self, job_id, redis_host, send_timeout=120):
        super(FlowController, self).__init__()
        self.job_id = job_id
        self.redis = redis.StrictRedis(redis_host, db=1)
        self.send_timeout = max(1, int(send_timeout))

    def send(self, node, kv):
        node_key = self._node_key(node)
        quit_time = time.time() + self.send_timeout
        while 1:
            if quit_time < time.time():
                raise SendTimeout
            try:
                push_socket, expire_time = self.push_sockets[node]
                if time.time() < expire_time:
                    break
            except KeyError:
                pass
            expire_time = time.time()
            ip_port, ttl = self.redis.get(node_key), int(self.redis.ttl(node_key))
            if ip_port is None or ttl == -1:
                time.sleep(1)
                continue
            expire_time += ttl
            push_socket = self.zmq.socket(zmq.PUSH)
            push_socket.connect('tcp://' + ip_port)
            self.push_sockets[node] = push_socket, expire_time
            break
        # At this point self.push_sockets[node] is updated as are push_socket and expire_time
        push_socket.send_pyobj(kv)

    def _node_key(self, node_num):
        return 'nodenum-%s-%d' % (self.job_id, node_num)


class FlowControllerNode(FlowController):

    def __init__(self, job_id, redis_host, node_num, min_port=40000, max_port=65000, worker_timeout=30,
                 heartbeat_timeout=10, send_timeout=120):
        super(FlowControllerNode, self).__init__(job_id=job_id, redis_host=redis_host, send_timeout=send_timeout)
        self.min_port = min_port
        self.max_port = max_port
        self.node_num = node_num
        self.ip = _get_ip()
        self.port = None
        self.ip_port = None
        self.next_heartbeat = 0.
        self.worker_timeout = max(1, int(worker_timeout))
        self.heartbeat_timeout = max(1, min(heartbeat_timeout, worker_timeout))
        self.zmq = zmq.Context()
        self.pull_socket = None
        self.push_sockets = {}  # [node_num] = (socket, time)
        self.node_key = None

    def recv(self):
        if self.pull_socket is None:
            self._pull_socket()
        return self.pull_socket.recv_pyobj()

    def poll(self):
        if self.pull_socket is None:
            self._pull_socket()
        return self.pull_socket.poll(100)

    def _pull_socket(self):
        sys.stderr.write('Pull Socket\n')
        # Get a port for this machine
        if self.node_num is None:
            raise ValueError('Node number is not set!')
        self.node_key = self._node_key(self.node_num)
        self.pull_socket = self.zmq.socket(zmq.PULL)
        self.port = self.pull_socket.bind_to_random_port('tcp://*',
                                                         min_port=self.min_port,
                                                         max_port=self.max_port,
                                                         max_tries=100)
        # See if any other nodes are using this node number
        self.ip_port = '%s:%s' % (self.ip, self.port)
        while 1:
            try:
                out = self.redis.setnx(self.node_key, self.ip_port)
                if not out:
                    raise redis.WatchError
                else:
                    self.redis.expire(self.node_key, self.worker_timeout)
                    self.next_heartbeat = self.heartbeat_timeout + time.time()
                    return
            except redis.WatchError:
                sys.stderr.write('Existing worker, waiting...\n')
                time.sleep(self.heartbeat_timeout * random.random())

    def heartbeat(self):
        if self.pull_socket is None:
            self._pull_socket()        
        if time.time() < self.next_heartbeat:
            return
        while 1:
            try:
                self.redis.setnx(self.node_key, self.ip_port)
                out = self.redis.get(self.node_key)
                if out != self.ip_port:
                    raise redis.WatchError
                else:
                    self.redis.expire(self.node_key, self.worker_timeout)
                    self.next_heartbeat = self.heartbeat_timeout + time.time()
                    break
            except redis.WatchError:
                sys.stderr.write('Existing worker, waiting...\n')
                time.sleep(self.heartbeat_timeout * random.random())


def _output_iter(iter_or_none):
    if iter_or_none is None:
        return ()
    return iter_or_none


class Slate(object):

    def __init__(self, redis, stream, key):
        self._redis = redis
        self._stream = unicode(stream).encode('utf-8')
        self._key = unicode(key).encode('utf-8')
        self._get = False
        self._set = False
        self._data = None

    def get(self):
        if not self._get:
            self._get = True
            self._data = self._redis.hget(self._stream, self._key)
        return self._data

    def set(self, data):
        if not self._set:
            self._get = self._set = True
        self._data = data

    def _flush(self):
        if self._set:
            self._redis.hset(self._stream, self._key, self._data)
            self._redis.publish(self._stream, self._key)
        

class Updater(object):

    def __init__(self):
        self._redis = redis.StrictRedis(os.environ['hadoopy_rt_redis'], db=0)  # TODO(Brandyn): Allow setting non default
        self._stream = os.environ['hadoopy_rt_stream']

    def map(self, key, value):
        slate = Slate(self._redis, self._stream, key)  # TODO(brandyn): Converting to string allows for collisions
        out = self.update(key, value, slate)
        if out is not None:
            for x in out:
                yield x
        slate._flush()
