import hadoopy
import hadoopy_rt
import zmq
import time
import os
import json
import multiprocessing
import base64
import sys
import redis


class Mapper(object):

    def __init__(self):
        self.job_id = os.environ['job_id']
        self.redis_server = os.environ['hadoopy_rt_redis']

    def map(self, node_num, data):
        sys.stderr.write('HadoopyRT: NodeNum[%d]\n' % (node_num,))
        flow_controller = hadoopy_rt.FlowControllerNode(self.job_id, self.redis_server, node_num)
        if 'files' in data:
            for f, d in data['files'].items():
                open(f, 'w').write(d)
            data['files'] = list(data['files'])  # Convert to list, removes memory burden
        launch_kw_args = dict((x, data[x]) for x in ['files', 'cmdenvs'] if x in data)
        try:
            launch_kw_args['cmdenvs'] = hadoopy._runner._listeq_to_dict(launch_kw_args['cmdenvs'])
        except KeyError:
            launch_kw_args['cmdenvs'] = {}
        launch_kw_args['cmdenvs']['hadoopy_rt_stream'] = str(node_num)
        launch_kw_args['cmdenvs']['hadoopy_rt_redis'] = self.redis_server
        open(data['script_name'], 'w').write(data['script_data'])
        while True:
            try:
                hadoopy_rt.launch_zmq(flow_controller, data['script_name'], outputs=data.get('outputs'), **launch_kw_args)
            except Exception, e:
                sys.stderr.write('%s\n' % str(e))
            ps = redis.StrictRedis().pubsub()
            ps.subscribe(data['script_name'])
            for x in ps.listen():
                if x['type'] == 'message':
                    open(data['script_name'], 'w').write(x['data'])
                    break

if __name__ == '__main__':
    hadoopy.run(Mapper, required_cmdenvs=['hadoopy_rt_redis', 'job_id'])
