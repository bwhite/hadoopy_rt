#!/usr/bin/env python
import hadoopy
import hadoopy_rt


class Mapper(object):

    def __init__(self):
        pass

    def map(self, key, value):
        print((key, value))

if __name__ == '__main__':
    hadoopy.run(Mapper)
