#!/usr/bin/env python
import hadoopy
import hadoopy_rt


class Mapper(object):

    def __init__(self):
        self.sum = {}
    
    @hadoopy_rt.close_on_flush
    def map(self, key, value):
        try:
            self.sum[key] += value
        except KeyError:
            self.sum[key] = value

    def close(self):
        for kv in self.sum.items():
            yield kv
        self.sum = {}

if __name__ == '__main__':
    hadoopy.run(Mapper)
