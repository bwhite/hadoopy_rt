#!/usr/bin/env python
import hadoopy
import hadoopy_rt


class Updater(hadoopy_rt.Updater):

    def __init__(self):
        super(Updater, self).__init__()

    def update(self, key, value, slate):
        a = slate.get()
        slate.set((0 if a is None else int(a)) + value)


if __name__ == '__main__':
    hadoopy.run(Updater)
