#!/usr/bin/env python
import hadoopy
import hadoopy_rt


class Mapper(object):

    def __init__(self):
        super(Mapper, self).__init__()

    def map(self, key, value):
        for v in value.split():
            yield 1, (v, 1)  # Send all words to 1
            #if v[0] == '#':
            #    yield 2, (v, 1)  # Send all hashtags to 2

if __name__ == '__main__':
    hadoopy.run(Mapper)
