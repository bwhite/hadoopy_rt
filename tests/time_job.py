#!/usr/bin/env python
import hadoopy
import time


def mapper(k, v):
    v['worker_time'] = time.time()
    yield k, v


if __name__ == '__main__':
    hadoopy.run(mapper)
