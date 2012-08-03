import hadoopy
import os
import fcntl
import functools


class TypedBytesIO(object):

    def __init__(self):
        self.out_fd_write, self.in_fd_write = os.pipe()
        self.out_fd_read, self.in_fd_read = os.pipe()
        fcntl.fcntl(self.out_fd_write, fcntl.F_SETFL, fcntl.fcntl(self.out_fd_write, fcntl.F_GETFL) | os.O_NONBLOCK)
        self.tbf = hadoopy.TypedBytesFile(write_fd=self.in_fd_write,
                                          read_fd=self.out_fd_read,
                                          flush_writes=True)

    def dumps(self, kv):
        self.tbf.write(kv)
        out = []
        while True:
            try:
                out.append(os.read(self.out_fd_write, 1024))
            except OSError:
                break
        return ''.join(out)

    def loads(self, data):
        os.write(self.in_fd_read, data)
        return self.tbf.next()


def launch_zmq(input_socket, output_socket, script_path):
    #tbio = TypedBytesIO()

    def _kvs():
        while True:
            yield input_socket.recv_pyobj()  # tbio.loads(input_socket.recv())
    poll = functools.partial(input_socket.poll, 0)
    for kv in hadoopy.launch_local(_kvs(), None, script_path, poll=poll)['output']:
        output_socket.send_pyobj(kv)  # output_socket.send(tbio.dumps(kv))
