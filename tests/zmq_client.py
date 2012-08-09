import zmq
import time
import os
import hadoopy_rt
print('PID[%s]' % os.getpid())

ctx = zmq.Context()

in_req_sock = ctx.socket(zmq.PUSH)
in_req_sock.connect("tcp://127.0.0.1:3000")

count = 0
latency_check = False
while True:
    v = 'blah'
    kv = (v, {'client_time': time.time(),
              'value_len': len(v),
              'count': count})
    in_req_sock.send_pyobj(kv)
    count += 1
    if latency_check:
        time.sleep(.01)
