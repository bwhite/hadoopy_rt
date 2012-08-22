try:
    import unittest2 as unittest
except ImportError:
    import unittest
import hadoopy_rt
import multiprocessing
import time
import random


def discover(job_id, node_num, out_queue):
    time.sleep(random.random() * 1)  # Randomly sleep so that nodes simulate having different setup orders
    out_queue.put(hadoopy_rt.discover(job_id, ['127.0.0.1'], [44000, 44001], node_num, node_num))


def discover_server(job_id, num_nodes):
    time.sleep(random.random() * 1)  # Randomly sleep so that nodes simulate having different setup orders
    hadoopy_rt.discover_server(job_id, num_nodes, ['127.0.0.1'], [44000, 44001])


class Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_name(self):
        for num_nodes in range(1, 10):
            job_id = random.random()
            ps = []
            server = multiprocessing.Process(target=discover_server, args=(job_id, num_nodes))
            server.start()
            q = multiprocessing.Queue()
            for node_num in range(num_nodes):
                ps.append(multiprocessing.Process(target=discover, args=(job_id, node_num, q)))
                ps[-1].start()
            for p in ps:
                p.join()
                print q.get()
            server.terminate()


if __name__ == '__main__':
    unittest.main()
