import zmq
import time
import os
import hadoopy_rt
print('PID[%s]' % os.getpid())

ctx = zmq.Context()

in_sock = ctx.socket(zmq.PULL)
in_sock.bind("tcp://127.0.0.1:3001")
tbio = hadoopy_rt.TypedBytesIO()

prev_count = -1
st = time.time()
tcount = 0
while True:
    #k, v = tbio.loads(in_sock.recv())
    k, v = in_sock.recv_pyobj()
    tcount += 1
    v['server_time'] = time.time()
    t0 = v['worker_time'] - v['client_time']
    t1 = v['server_time'] - v['worker_time']
    t2 = v['server_time'] - v['client_time']
    if v['count'] - 1 != prev_count:
        print('Mismatch count!')
    prev_count = v['count']
    if time.time() - st >= 5:
        print('Throughput [%f]' % (tcount / (time.time() - st)))
        print((t0, t1, t2))
        st = time.time()
        tcount = 0
