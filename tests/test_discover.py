try:
    import unittest2 as unittest
except ImportError:
    import unittest
import hadoopy_rt
import multiprocessing
import time
import random


def discover(job_id, num_nodes, node_num, out_queue):
    time.sleep(random.random() * 1)  # Randomly sleep so that nodes simulate having different setup orders
    d = hadoopy_rt.discover_slave if node_num else hadoopy_rt.discover_master
    out_queue.put(d(job_id, ['127.0.0.1'], [44000, 44001],
                    30, num_nodes, node_num, node_num))


class Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_name(self):
        for num_nodes in range(10):
            job_id = random.random()
            ps = []
            q = multiprocessing.Queue()
            for node_num in range(num_nodes):
                ps.append(multiprocessing.Process(target=discover, args=(job_id, num_nodes, node_num, q)))
                ps[-1].start()
            for p in ps:
                p.join()
                print q.get()


if __name__ == '__main__':
    unittest.main()
