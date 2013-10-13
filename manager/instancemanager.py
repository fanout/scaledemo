import time
import urllib
import urllib2
import zmq
from boto import ec2
import app
import redis_ops
import rpc

app.init('/etc/scaledemo.conf')

logger = app.logger
zmq_context = zmq.Context()
rpc_server = None
db = redis_ops.RedisOps()

rpc_in_spec = app.config.get('instancemanager', 'rpc_in_spec')
edge_capacity = int(app.config.get('instancemanager', 'edge_capacity'))
client_capacity = int(app.config.get('instancemanager', 'client_capacity'))
default_region = app.config.get('instancemanager', 'region')
aws_access_key = app.config.get('instancemanager', 'aws_access_key')
aws_secret_key = app.config.get('instancemanager', 'aws_secret_key')
origin_host = app.config.get('instancemanager', 'origin_host')

def get_boto_ec2_connection(region):
	return ec2.connect_to_region(region, aws_access_key_id=aws_access_key,
		aws_secret_access_key=aws_secret_key)

# return boto ec2 instance
def create_node(ntype, region):
	conn = get_boto_ec2_connection(region)
	ami_id = app.config.get('instancemanager', 'ami_%s_%s' % (region, ntype))
	key_name = app.config.get('instancemanager', 'key_name')
	sec_group = app.config.get('instancemanager', 'sec_group')
	itype = app.config.get('instancemanager', 'itype_%s' % ntype)
	reservation = conn.run_instances(ami_id, key_name=key_name,
		security_groups=[sec_group], instance_type=itype)
	return reservation.instances[0]

# return boto ec2 instance
def get_node(region, instance_id):
	conn = get_boto_ec2_connection(region)
	reservation = conn.get_all_instances(instance_ids=[instance_id])
	for i in reservation[0].instances:
		if i.id == instance_id:
			return i
	raise ValueError("no such instance")

def terminate_node(region, instance_id):
	conn = get_boto_ec2_connection(region)
	conn.terminate_instances(instance_ids=[instance_id])

def method_handler(method, args, data):
	if len(args) > 0:
		logger.info('call: %s(%s)' % (method, args))
	else:
		logger.info('call: %s()' % method)
	if method == 'set-capacity':
		db.config_set(args['base-uri'], args['count'])
	elif method == 'send':
		uri = db.config_get_base_uri() + '/value/'
		content_raw = urllib.urlencode({ 'body': args['message'] })
		try:
			urllib2.urlopen(urllib2.Request(uri, content_raw))
		except:
			raise rpc.CallError('send-failed')
	else:
		raise rpc.CallError('method-not-found')

def rpc_server_worker(c):
	global rpc_server
	rpc_server = rpc.RpcServer([rpc_in_spec], context=zmq_context)
	c.ready()
	rpc_server.run(method_handler, None)

def nodemanage_worker(c):
	while True:
		count = db.config_get_count()
		base_uri = db.config_get_base_uri()
		if not base_uri:
			count = 0

		nodes = db.node_get_all()
		edge_running = 0
		edge_have = 0
		client_running = 0
		client_have = 0
		for id, node in nodes.iteritems():
			if node['type'] == 'edge':
				edge_have += 1
				if node.get('private-addr'):
					edge_running += 1
			elif node['type'] == 'client':
				client_have += 1
				if node.get('private-addr'):
					client_running += 1

		if count > 0:
			edge_need = ((count - 1) / edge_capacity) + 1
			client_need = ((count - 1) / edge_capacity) + 1
		else:
			edge_need = 0
			client_need = 0

		logger.info('edge: %d/%d/%d, client: %d/%d/%d' % (edge_running, edge_have, edge_need, client_running, client_have, client_need))

		remove_id = None
		if edge_have > edge_need:
			for id, node in nodes.iteritems():
				if node['type'] == 'edge':
					remove_id = id
					break
		elif client_have > client_need:
			for id, node in nodes.iteritems():
				if node['type'] == 'client':
					remove_id = id
					break
		#else:
		#	# see if there's an old node
		#	now = int(time.time() * 1000)
		#	for id, node in nodes.iteritems():
		#		if now >= node['created'] + (5 * 60 * 1000):
		#			remove_id = id
		#			logger.info('%s has expired' % remove_id)
		#			break
		if remove_id:
			node = nodes[remove_id]
			logger.info('removing %s' % remove_id)
			terminate_node(node['region'], node['instance-id'])
			db.node_remove(remove_id)
			# if removing an edge node, remove any client binding
			if node['type'] == 'edge':
				for cid, cnode in nodes.iteritems():
					if cnode['type'] != 'client':
						continue
					if cnode.get('edge') == remove_id:
						del cnode['edge']
						db.node_update(cid, cnode)
			continue

		if edge_have < edge_need:
			logger.info('creating edge node...')
			instance = create_node('edge', default_region)
			node = dict()
			node['type'] = 'edge'
			node['region'] = default_region
			node['instance-id'] = instance.id
			node['created'] = int(time.time() * 1000)
			id = db.node_add('edge-', node)
			logger.info('done: id=%s, ec2-instance=%s' % (id, instance.id))
			continue

		if client_have < client_need:
			logger.info('creating client node...')
			instance = create_node('client', default_region)
			node = dict()
			node['type'] = 'client'
			node['region'] = default_region
			node['instance-id'] = instance.id
			node['created'] = int(time.time() * 1000)
			id = db.node_add('client-', node)
			logger.info('done: id=%s, ec2-instance=%s' % (id, instance.id))
			continue

		updated = False
		for id, node in nodes.iteritems():
			if not node.get('private-addr'):
				instance = get_node(node['region'], node['instance-id'])
				if instance.state == 'running':
					logger.info('node %s running, %s' % (id, instance.ip_address))
					node['private-addr'] = instance.private_ip_address
					node['public-addr'] = instance.ip_address
					instance.add_tag('Name', id)
					db.node_update(id, node)
					updated = True
					break
		if updated:
			continue

		# set client sizes and assign to edge nodes
		if edge_running == edge_need and client_running == client_need:
			for n, i in enumerate(nodes.iteritems()):
				id, node = i
				if node['type'] != 'client':
					continue
				changed = False
				node_client_count = count / len(nodes)
				if n < count % len(nodes):
					node_client_count += 1
				if node.get('count') != node_client_count:
					node['count'] = node_client_count
					changed = True
				if not node.get('edge'):
					# find a free edge
					found = False
					for eid, enode in nodes.iteritems():
						if enode['type'] != 'edge':
							continue
						in_use = False
						for cid, cnode in nodes.iteritems():
							if cnode['type'] != 'client':
								continue
							if node.get('edge') == eid:
								in_use = True
								break
						if not in_use:
							found = True
							break
					if found:
						node['edge'] = eid
						changed = True
				if changed:
					db.node_update(id, node)

		# apply client settings
		changed = False
		for id, node in nodes.iteritems():
			if node['type'] != 'client':
				continue
			if not node.get('public-addr') or not node.get('count') or not node.get('edge'):
				continue
			node_client_count = node['count']
			if node.get('count-cur') != node_client_count:
				enode = nodes[node['edge']]
				client = rpc.RpcClient(['tcp://%s:10100' % node['public-addr']], context=zmq_context)
				client.sock.linger = 0
				args = dict()
				args['base-uri'] = base_uri
				args['count'] = node_client_count
				args['connect-host'] = enode['public-addr']
				client.call('setup-clients', args)
				client.close()
				node['count-cur'] = node_client_count
				db.node_update(id, node)
				changed = True
				break
		if changed:
			continue

		time.sleep(5)

def setproxies_worker(c):
	last_specs = list()
	while True:
		specs = list()	
		nodes = db.node_get_all()
		for id, node in nodes.iteritems():
			if node['type'] == 'edge':
				addr = node.get('public-addr')
				if addr:
					specs.append('tcp://%s:5560' % addr)
		if last_specs != specs:
			logger.info('sending proxy list to origin')
			client = rpc.RpcClient(['tcp://%s:10100' % origin_host], context=zmq_context)
			client.sock.linger = 0
			args = dict()
			args['specs'] = specs
			client.call('set-grip-proxies', args)
			client.close()
			last_specs = specs

		time.sleep(2)

def pinger_worker(c):
	pass

app.spawn(rpc_server_worker, wait=True)
app.spawn(nodemanage_worker, daemon=True)
app.spawn(setproxies_worker, daemon=True)

app.wait_for_quit()

rpc_server.stop()
zmq_context.term()
