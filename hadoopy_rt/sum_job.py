import hadoopy
import hadoopy_rt


class Mapper(object):

    def __init__(self):
        self.sum = {}

    def map(self, key, value):
        if isinstance(key, hadoopy_rt.FlushWorker):
            self.close()
        try:
            self.sum[key] += value
        except KeyError:
            self.sum[key] = value

    def close(self):
        for kv in self.sum.items():
            yield kv
        self.sum = {}
