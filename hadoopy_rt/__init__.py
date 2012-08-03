import hadoopy
import functools


def launch_zmq(input_socket, output_socket, script_path):
    def _kvs():
        while True:
            yield input_socket.recv_pyobj()
    poll = functools.partial(input_socket.poll, 0)
    for kv in hadoopy.launch_local(_kvs(), None, script_path, poll=poll)['output']:
        output_socket.send_pyobj(kv)
