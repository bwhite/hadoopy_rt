import time


class WorkerTracker(object):

    def __init__(self, worker_address, port, job_name, redis_db=None, admin_socket=None):
        self.worker_address = worker_address
        self.port = port
        self.worker_name = '%s:%s' % (worker_address, port)
        self.job_name = job_name
        self.admin_socket = admin_socket
        self.redis_db = redis_db
        self.heartbeat_period = 5.
        self.heartbeat_expire = int(self.heartbeat_period * 4)
        self.admin_period = .1
        self.start_time = time.time()
        self.last_heartbeat = 0.
        self.last_admin = 0.

    def heartbeat(self):
        "Checkin to the database and update timestamp"
        if time.time() - self.last_heartbeat > self.heartbeat_period and self.redis_db:
            print('Heartbeat')
            self.last_heartbeat = time.time()
            self.redis_db.hmset(self.worker_name, {'last_heartbeat': self.last_heartbeat,
                                                   'job_name': self.job_name,
                                                   'start_time': self.start_time})
            self.redis_db.expire(self.worker_name, self.heartbeat_expire)

    def admin_listen(self):
        "Check if any admin messages have been provided"
        # Check admin socket to see if we need to 1.) Shutdown, 2.) Restart, 3.) Update local install, 4.) Report stats
        if time.time() - self.last_admin > self.admin_period and self.admin_socket:
            admin_cmd = self.admin_socket.recv_pyobj()
            if admin_cmd['type'] == 'shutdown':
                self.shutdown()
            self.last_admin = time.time()

    def shutdown(self):
        "Unregister with the database"
        print('Shutting down')
        if self.redis_db:
            self.redis_db.delete(self.worker_name)
        raise StopIteration

    def __iter__(self):
        return self

    def next(self):
        self.heartbeat()
        self.admin_listen()
