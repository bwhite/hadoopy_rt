import zmq
import hadoopy_rt
import os
print('PID[%s]' % os.getpid())

ctx = zmq.Context()

in_sock = ctx.socket(zmq.PULL)
in_sock.bind("tcp://127.0.0.1:3000")
out_sock = ctx.socket(zmq.PUSH)
out_sock.connect("tcp://127.0.0.1:3001")

hadoopy_rt.launch_zmq(in_sock, out_sock, 'time_job.py')
