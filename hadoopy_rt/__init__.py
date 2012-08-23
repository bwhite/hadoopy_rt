import hadoopy
import functools
import socket
import zmq
import time
import os
import random
import json
import hadoopy_helper


class DiscoverTimeout(Exception):
    """There was a problem discovering the participating nodes"""


class FlushWorker(object):
    """Flush worker message"""


class StopWorker(FlushWorker):
    """Stop worker message"""


def _lf(fn):
    from . import __path__
    return os.path.join(__path__[0], fn)


def launch_zmq(input_socket, output_socket, script_path, output_func=False):
    def _kvs():
        kv = None
        while not isinstance(kv, StopWorker):
            kv = input_socket.recv_pyobj()
            yield kv
    poll = functools.partial(input_socket.poll, 0)
    for kv in hadoopy.launch_local(_kvs(), None, script_path, poll=poll)['output']:
        if output_func:
            output_socket(kv)
        else:
            output_socket.send_pyobj(kv)


def launch_tree_same(output_path, script_path, height, machines, job_id):
    num_nodes = 2 ** (height) - 1
    out_nodes = dict((x, x / 2) for x in range(1, 2 ** (height) - 1))
    v = {'script_name': os.path.basename(script_path),
         'script_data': open(script_path).read(),
         'out_nodes': out_nodes,
         'num_nodes': num_nodes}
    cmdenvs = {'machines': json.dumps(machines),
               'job_id': job_id,
               'discover_ports': random.sample(xrange(49152, 65536), 5)}
    with hadoopy_helper.hdfs_temp() as input_path:
        for node_num in range(num_nodes):
            hadoopy.writetb('%s/input/%d' % (input_path, node_num), [(node_num, v)])
        hadoopy.launch(input_path, output_path, _lf('hadoopy_rt_job.py'), cmdenvs=cmdenvs)


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
    # Find master
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
