#!/usr/bin/env python
import hadoopy
import hadoopy_rt


class Updater(hadoopy_rt.Updater):

    def __init__(self):
        super(Updater, self).__init__()

    def update(self, key, value, slate):
        slate.set(value)


if __name__ == '__main__':
    hadoopy.run(Updater)
