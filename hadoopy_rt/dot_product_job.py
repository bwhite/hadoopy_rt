#!/usr/bin/env python
import hadoopy
import hadoopy_rt
import os


class Mapper(object):

    def __init__(self):
        self.a = float(os.environ['A'])

    @hadoopy_rt.close_on_flush
    def map(self, key, b):
        yield key, self.a * b

class Reducer(object):

    def __init__(self):
        pass

    @hadoopy_rt.close_on_flush
    def reduce(self, key, cs):
        yield key, sum(cs)

if __name__ == '__main__':
    hadoopy.run(Mapper, Reducer)
