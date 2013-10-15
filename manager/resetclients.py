import zmq
import redis_ops
import rpc

zmq_context = zmq.Context()
db = redis_ops.RedisOps()

nodes = db.node_get_all()

for id, node in nodes.iteritems():
	if node['type'] != 'client':
		continue
	if node.get('public-addr'):
		print 'resetting %s' % id
		if 'count' in node:
			del node['count']
		if 'count-cur' in node:
			del node['count-cur']
		if 'edge' in node:
			del node['edge']
		client = rpc.RpcClient(['tcp://%s:10100' % node['public-addr']], context=zmq_context)
		client.sock.linger = 0
		args = dict()
		args['base-uri'] = ''
		args['count'] = 0
		args['connect-host'] = ''
		client.call('setup-clients', args)
		client.close()
		db.node_update(id, node)
