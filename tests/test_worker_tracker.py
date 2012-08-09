try:
    import unittest2 as unittest
except ImportError:
    import unittest
import redis
import time
import zmq

class Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_name(self):
        import hadoopy_rt
        ctx = zmq.Context()
        pub_sock = ctx.socket(zmq.PUB)
        pub_sock.bind("tcp://127.0.0.1:3000")
        sub_sock = ctx.socket(zmq.SUB)
        sub_sock.connect("tcp://127.0.0.1:3000")
        wt = hadoopy_rt.WorkerTracker(0, 0, 0,
                                      redis_db=redis.StrictRedis(port=6381), admin_socket=sub_sock)
        for x in iter(wt):
            time.sleep(30)
            pub_sock.send_pyobj({'type': 'shutdown'})

if __name__ == '__main__':
    unittest.main()
