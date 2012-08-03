try:
    import unittest2 as unittest
except ImportError:
    import unittest
import hadoopy


class Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_name(self):
        kv_sizes = [(1, 1), (1024 ** 2, 1024 ** 2), (50 * 1024 ** 2, 50 * 1024 ** 2)]

        a = hadoopy.launch_local((x for x in kv_sizes for y in range(20)), None, 'size_job.py')
        kvs = list(((len(x), len(y)) for x, y in a['output']))
        print(len(kvs))

        a = hadoopy.launch_local(kv_sizes, None, 'size_job.py')
        kvs = list(a['output'])
        print(len(kvs))

        a = hadoopy.launch_local(kvs, None, 'null_job.py')
        kvs = list(a['output'])
        print(len(kvs))


if __name__ == '__main__':
    unittest.main()
