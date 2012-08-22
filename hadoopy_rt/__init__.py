import hadoopy
import functools
import socket
import zmq
import time


class DiscoverTimeout(Exception):
    """There was a problem discovering the participating nodes"""


def launch_zmq(input_socket, output_socket, script_path):
    def _kvs():
        while True:
            yield input_socket.recv_pyobj()
    poll = functools.partial(input_socket.poll, 0)
    for kv in hadoopy.launch_local(_kvs(), None, script_path, poll=poll)['output']:
        output_socket.send_pyobj(kv)


def launch_tree_same(script_path, height, workers):
    kv = [None, ]
    v = {'script_name': os.path.basename(script_path),
         'script_data': open(script_path).read()}


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


def discover_master(job_id, machines, ports, setup_deadline, num_nodes, node_num, input_port):
    # Setup discover port
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    work_graph = {}  # [node_num] = (host, port)
    work_graph[node_num] = (_get_ip(), input_port)
    _bind_first_port(sock, ports)
    satisfied_nodes = set(work_graph.keys())
    start_time = time.time()
    while len(satisfied_nodes) < num_nodes:
        if time.time() - start_time >= setup_deadline:  # TODO(brandyn): Put this on the recv too
            raise DiscoverTimeout()
        slave_job_id, slave_node_num, slave_ip, slave_port = sock.recv_pyobj()
        if job_id != slave_job_id:
            sock.send_pyobj('wrongid')  # They connected to the wrong server
        else:
            try:
                if work_graph[slave_node_num] != (slave_ip, slave_port):
                    raise ValueError('Conflicting slave ids [%s] != [%s]' % (work_graph[slave_node_num], (slave_ip, slave_port)))
            except KeyError:
                work_graph[slave_node_num] = slave_ip, slave_port
            if len(work_graph) < num_nodes:
                sock.send_pyobj('trylater')  # They connected to the wrong server
            else:
                sock.send_pyobj(work_graph)
                satisfied_nodes.add((slave_ip, slave_port))
    return work_graph


def discover_slave(job_id, machines, ports, setup_deadline, num_nodes, node_num, input_port):
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
