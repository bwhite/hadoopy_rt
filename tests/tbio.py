import os
import hadoopy


class TypedBytesIO(object):

    def __init__(self):
        self.out_fd_write, self.in_fd_write = os.pipe()
        self.out_fd_read, self.in_fd_read = os.pipe()
        self.tbf = hadoopy.TypedBytesFile(write_fd=self.in_fd_write,
                                          read_fd=self.out_fd_read,
                                          flush_writes=True)

    def dumps(self, kv):
        self.tbf.write(kv)
        return os.read(self.out_fd_write, 100000000)

    def loads(self, data):
        os.write(self.in_fd_read, data)
        return self.tbf.next()
