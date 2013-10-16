import time
import copy
import urllib
import urllib2
import zmq
import json
import tnetstring
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
stats_out_spec = app.config.get('instancemanager', 'stats_out_spec')
edge_capacity = int(app.config.get('instancemanager', 'edge_capacity'))
client_capacity = int(app.config.get('instancemanager', 'client_capacity'))
regions = app.config.get('instancemanager', 'regions').split(',')
aws_access_key = app.config.get('instancemanager', 'aws_access_key')
aws_secret_key = app.config.get('instancemanager', 'aws_secret_key')
origin_host = app.config.get('instancemanager', 'origin_host')
headline_host = app.config.get('instancemanager', 'headline_host')
client_base_uri = app.config.get('instancemanager', 'client_base_uri')
if app.config.has_option('instancemanager', 'region'):
	default_region = app.config.get('instancemanager', 'region')
else:
	default_region = None
if app.config.has_option('instancemanager', 'az'):
	default_az = app.config.get('instancemanager', 'az')
else:
	default_az = None

update_spec = 'inproc://statsupdate'

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
		security_groups=[sec_group], instance_type=itype, placement=default_az)
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
	out_sock = data['out_sock']
	if len(args) > 0:
		logger.info('call: %s(%s)' % (method, args))
	else:
		logger.info('call: %s()' % method)
	if method == 'set-capacity':
		db.config_set_count(args['count'])
		out_sock.send('update capacity')
	elif method == 'send':
		now = int(time.time() * 1000)
		db.send_set_start(now)
		uri = 'http://%s/value/' % origin_host
		headers = dict()
		headers['Host'] = headline_host
		content_raw = urllib.urlencode({ 'body': args['message'] })
		try:
			logger.info('POST %s %s' % (uri, content_raw))
			urllib2.urlopen(urllib2.Request(uri, content_raw, headers))
		except Exception as e:
			logger.info('send failed: %s' % e.read())
			raise rpc.CallError('send-failed')
	else:
		raise rpc.CallError('method-not-found')

def rpc_server_worker(c):
	global rpc_server
	out_sock = zmq_context.socket(zmq.PUB)
	out_sock.linger = 0
	out_sock.connect(update_spec)
	data = dict()
	data['out_sock'] = out_sock
	rpc_server = rpc.RpcServer([rpc_in_spec], context=zmq_context)
	c.ready()
	rpc_server.run(method_handler, data)

# sort type (edge first), then number
def compare_key(a, b):
	at = a.find('-')
	atype = a[:at]
	anum = int(a[at + 1:])
	at = b.find('-')
	btype = b[:at]
	bnum = int(b[at + 1:])
	if atype == 'edge' and btype != 'edge':
		return -1
	elif btype == 'edge' and atype != 'edge':
		return 1
	elif anum < bnum:
		return -1
	elif anum > bnum:
		return 1
	else:
		return 0

def order_keys(keys):
	tmp = copy.copy(keys)
	out = list()
	while len(tmp) > 0:
		lowest = None
		for n, k in enumerate(tmp):
			if lowest is None or compare_key(k, lowest) < 0:
				lowest = k
				lowest_at = n
		del tmp[lowest_at]
		out.append(lowest)
	return out

def pick_region(ntype, nodes):
	counts = dict()
	for region in regions:
		counts[region] = 0
	for id, node in nodes.iteritems():
		if node['type'] != ntype:
			continue
		region = node['region']
		counts[region] += 1
	r = None
	rmin = None
	assert(len(regions) > 0)
	for region in regions:
		if rmin is None or counts[region] < rmin:
			r = region
			rmin = counts[region]
	return r

def pick_edge(cregion, nodes):
	# get counts of all edge regions we point to from this region
	counts = dict()
	for region in regions:
		if region != cregion:
			counts[region] = 0
	for id, node in nodes.iteritems():
		if node['type'] != 'client' or node['region'] != cregion or not node.get('edge'):
			continue
		eregion = nodes[node['edge']]['region']
		counts[eregion] += 1

	while len(counts) > 0:
		# pick the least-used region
		r = None
		rmin = None
		assert(len(regions) > 0)
		# check regions starting with the next from current
		for n, region in enumerate(regions):
			if region == cregion:
				pos = (n + 1) % len(regions)
				break
		lregions = regions[pos:]
		lregions.extend(regions[:pos])
		for region in lregions:
			if region not in counts:
				continue
			if rmin is None or counts[region] < rmin:
				r = region
				rmin = counts[region]

		# are there any free edges in that region?
		for id in order_keys(nodes.keys()):
			node = nodes[id]
			if node['type'] != 'edge' or node['region'] != r:
				continue
			in_use = False
			for cid, cnode in nodes.iteritems():
				if cnode['type'] != 'client':
					continue
				if cnode.get('edge') == id:
					in_use = True
					break
			if not in_use:
				return id

		# if not, then try next best region
		del counts[r]

	return None

def nodemanage_worker(c):
	out_sock = zmq_context.socket(zmq.PUB)
	out_sock.linger = 0
	out_sock.connect(update_spec)

	need_status = True
	while True:
		count = db.config_get_count()

		nodes = db.node_get_all()
		edge_up = 0
		edge_running = 0
		edge_have = 0
		client_up = 0
		client_running = 0
		client_have = 0
		for id, node in nodes.iteritems():
			if node['type'] == 'edge':
				edge_have += 1
				if node.get('private-addr'):
					edge_running += 1
				if 'ping' in node:
					edge_up += 1
			elif node['type'] == 'client':
				client_have += 1
				if node.get('private-addr'):
					client_running += 1
				if 'ping' in node:
					client_up += 1

		if count > 0:
			edge_need = ((count - 1) / edge_capacity) + 1
			client_need = ((count - 1) / client_capacity) + 1
		else:
			edge_need = 0
			client_need = 0

		if need_status:
			out_sock.send('update nodes')
			logger.info('edge: %d/%d/%d/%d, client: %d/%d/%d/%d'
				% (edge_up, edge_running, edge_have, edge_need,
				client_up, client_running, client_have, client_need))
			need_status = False

		# FIXME: balance removal
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
			need_status = True
			continue

		if edge_have < edge_need:
			logger.info('creating edge node...')
			if default_region:
				region = default_region
			else:
				region = pick_region('edge', nodes)
			instance = create_node('edge', region)
			node = dict()
			node['type'] = 'edge'
			node['region'] = region
			node['instance-id'] = instance.id
			node['created'] = int(time.time() * 1000)
			id = db.node_add('edge-', node)
			logger.info('done: id=%s, region=%s, ec2-instance=%s' % (id, region, instance.id))
			need_status = True
			continue

		if client_have < client_need:
			logger.info('creating client node...')
			if default_region:
				region = default_region
			else:
				region = pick_region('client', nodes)
			instance = create_node('client', region)
			node = dict()
			node['type'] = 'client'
			node['region'] = region
			node['instance-id'] = instance.id
			node['created'] = int(time.time() * 1000)
			id = db.node_add('client-', node)
			logger.info('done: id=%s, region=%s, ec2-instance=%s' % (id, region, instance.id))
			need_status = True
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
			need_status = True
			continue

		# set client sizes and assign to edge nodes
		if edge_running == edge_need and client_running == client_need:
			num_client_nodes = 0
			for id, node in nodes.iteritems():
				if node['type'] == 'client':
					num_client_nodes += 1
			for n, id in enumerate(order_keys(nodes.keys())):
				node = nodes[id]
				if node['type'] != 'client':
					continue
				changed = False
				node_client_count = count / num_client_nodes
				if n < count % num_client_nodes:
					node_client_count += 1
				if node.get('count') != node_client_count:
					node['count'] = node_client_count
					changed = True
				if not node.get('edge'):
					# find a free edge
					eid = pick_edge(node['region'], nodes)
					if eid:
						node['edge'] = eid
						logger.info('mapping %s (%s) -> %s (%s)' % (id, node['region'], eid, nodes[eid]['region']))
						changed = True
				if changed:
					db.node_update(id, node)

		# apply client settings
		changed = False
		for id, node in nodes.iteritems():
			if node['type'] != 'client':
				continue
			if 'ping' not in node or not node.get('count') or not node.get('edge'):
				continue
			node_client_count = node['count']
			if node.get('count-cur') != node_client_count:
				logger.info('applying settings to %s' % id)
				enode = nodes[node['edge']]
				client = rpc.RpcClient(['tcp://%s:10100' % node['public-addr']], context=zmq_context)
				client.sock.linger = 0
				args = dict()
				args['base-uri'] = client_base_uri
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

		# any node need to be pinged?
		origin_info = db.get_origin_info()
		if origin_info is None:
			origin_info = dict()
		now = int(time.time() * 1000)
		oldest_id = None
		oldest_last_ping = None
		for id, node in nodes.iteritems():
			if not node.get('public-addr'):
				continue
			if 'last-ping' not in node:
				oldest_id = id
				oldest_last_ping = -1
				break
			elif oldest_last_ping is None or node['last-ping'] < oldest_last_ping:
				oldest_id = id
				oldest_last_ping = node['last-ping']
		if oldest_last_ping != -1:
			if 'last-ping' not in origin_info:
				oldest_id = 'origin'
				oldest_last_ping = -1
			elif oldest_last_ping is None or origin_info['last-ping'] < oldest_last_ping:
				oldest_id = 'origin'
				oldest_last_ping = origin_info['last-ping']
		if oldest_id == 'origin':
			if 'last-ping' not in origin_info or now >= origin_info['last-ping'] + (5 * 1000):
				logger.debug('pinging %s' % origin_host)
				origin_info['last-ping'] = now
				db.set_origin_info(origin_info)
				changed = True
				client = rpc.RpcClient(['tcp://%s:10100' % origin_host], context=zmq_context)
				client.sock.linger = 0
				success = False
				try:
					client.call('ping')
					start_time = int(time.time() * 1000)
					client.call('ping')
					end_time = int(time.time() * 1000)
					success = True
				except:
					logger.debug('ping failed')
				client.close()
				if success:
					origin_info['ping'] = end_time - start_time
					db.set_origin_info(origin_info)
					logger.debug('ping time: %d' % origin_info['ping'])
		elif oldest_id:
			id = oldest_id
			node = nodes[id]
			if 'last-ping' not in node or now >= node['last-ping'] + (5 * 1000):
				logger.debug('pinging %s' % id)
				node['last-ping'] = now
				db.node_update(id, node)
				changed = True
				client = rpc.RpcClient(['tcp://%s:10100' % node['public-addr']], context=zmq_context)
				client.sock.linger = 0
				success = False
				try:
					client.call('ping')
					start_time = int(time.time() * 1000)
					ret = client.call('ping')
					end_time = int(time.time() * 1000)
					success = True
				except:
					logger.debug('ping failed')
				client.close()
				if success:
					had_ping = 'ping' in node
					node['ping'] = end_time - start_time
					if node['type'] == 'client':
						node['manager-id'] = ret['id']
					db.node_update(id, node)
					logger.debug('ping time: %d' % node['ping'])
					if not had_ping:
						need_status = True
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

def ping_data_worker(c):
	out_sock = zmq_context.socket(zmq.PUB)
	out_sock.linger = 0
	out_sock.connect(update_spec)

	while True:
		ping_total = 0
		ping_count = 0
		ping_min = None
		ping_max = None
		nodes = db.node_get_all()
		for id, node in nodes.iteritems():
			ping = node.get('ping')
			if ping is None:
				continue
			ping_total += ping
			ping_count += 1
			if ping_min is None or ping < ping_min:
				ping_min = ping
			if ping_max is None or ping > ping_max:
				ping_max = ping

		ping_data = dict()
		if ping_min is not None:
			ping_data['min'] = ping_min
		if ping_max is not None:
			ping_data['max'] = ping_max
		if ping_count > 0:
			ping_data['avg'] = ping_total / ping_count
		db.set_ping_data(ping_data)
		out_sock.send('update pings')

		time.sleep(5)

def nodestats_worker(c):
	in_sock = zmq_context.socket(zmq.SUB)
	in_sock.setsockopt(zmq.SUBSCRIBE, 'stats ')
	connected_to = set()

	out_sock = zmq_context.socket(zmq.PUB)
	out_sock.linger = 0
	out_sock.connect(update_spec)

	stats = dict()
	cur_id = -1
	cur_body = ''
	updated = False
	last_send = 0
	send_freq = 100

	poller = zmq.Poller()
	poller.register(in_sock, zmq.POLLIN)
	while True:
		nodes = db.node_get_all()

		# remove any
		for id in connected_to:
			if id not in nodes:
				# lost a connection, start over
				in_sock.close()
				in_sock = zmq_context.socket(zmq.SUB)
				in_sock.setsockopt(zmq.SUBSCRIBE, 'stats ')
				connected_to.clear()
				break

		# add any
		for id, node in nodes.iteritems():
			if node['type'] != 'client':
				continue
			if id not in connected_to and node.get('public-addr'):
				in_sock.connect('tcp://%s:10101' % node['public-addr'])
				connected_to.add(id)
				logger.info('stats connected to %s' % id)

		to_remove = list()
		for pub_id, s in stats.iteritems():
			found = False
			for id, node in nodes.iteritems():
				if node['type'] != 'client':
					continue
				if node.get('manager-id') == pub_id:
					found = True
					break
			if not found:
				to_remove.append(pub_id)
		for pub_id in to_remove:
			del stats[pub_id]

		timeout = 5000 # max timeout, so we can update stats connections
		if updated:
			now = int(time.time() * 1000)
			if last_send + send_freq > now:
				timeout = send_freq - (now - last_send)
			else:
				timeout = 0
		socks = dict(poller.poll(timeout))
		if socks.get(in_sock) == zmq.POLLIN:
			m_raw = in_sock.recv()
			at = m_raw.find(' ')
			m = tnetstring.loads(m_raw[at + 1:])
			pub_id = m['id']
			s = stats.get(pub_id)
			if s is None:
				s = dict()
				stats[pub_id] = s
				s['received'] = 0
				s['rtimes'] = list()
			if 'cur-id' in m and m['cur-id'] != cur_id:
				cur_id = m['cur-id']
				cur_body = m['cur-body']
				# reset receive info on new id
				for id, st in stats.iteritems():
					st['received'] = 0
					st['rtimes'] = list()

			# who sent this?
			pub_node = None
			for id, node in nodes.iteritems():
				if node['type'] != 'client':
					continue
				if node.get('manager-id') == pub_id:
					pub_node = node
					break

			if 'latency' in m:
				latency = m['latency']
			else:
				latency = 0
			if pub_node is not None and 'ping' in pub_node:
				latency += pub_node['ping'] / 2

			s['total'] = m['total']
			s['started'] = m['started']
			s['errored'] = m['errored']
			had_received = s['received']
			s['received'] = m['received']
			if 'cur-id' in m:
				s['cur-id'] = m['cur-id']
			now = int(time.time() * 1000)
			if m['received'] > had_received:
				s['rtimes'].append((now, m['received'] - had_received, latency))
			elif m['received'] < had_received:
				s['rtimes'] = list()
				s['rtimes'].append((now, m['received'], latency))
			updated = True

		if updated:
			now = int(time.time() * 1000)
			if last_send + send_freq < now:
				last_send = now
				updated = False

				total = 0
				started = 0
				received = 0
				errored = 0
				rtimes = list()
				for id, s in stats.iteritems():
					total += s['total']
					started += s['started']
					if 'cur-id' in s and s['cur-id'] == cur_id:
						received += s['received']
					errored += s['errored']
					if len(s['rtimes']) > 0:
						rtimes.extend(s['rtimes'])
					s['rtimes'] = list()

				if len(rtimes) > 0:
					rcount = 0
					latency_total = 0
					for rtime in rtimes:
						rcount += rtime[1]
						latency_total += (now - rtime[0] + rtime[2]) * rtime[1]
					latency = latency_total / rcount
				else:
					latency = None

				m = dict()
				m['total'] = total
				m['started'] = started
				m['received'] = received
				m['errored'] = errored
				if cur_id >= 0:
					m['cur-id'] = cur_id
					m['cur-body'] = cur_body
				if latency is not None:
					m['latency'] = latency
				else:
					m['latency'] = 0

				out_sock.send('stats ' + tnetstring.dumps(m))

def stats_worker(c):
	in_sock = zmq_context.socket(zmq.SUB)
	in_sock.setsockopt(zmq.SUBSCRIBE, 'stats ')
	in_sock.setsockopt(zmq.SUBSCRIBE, 'update ')
	in_sock.bind(update_spec)
	c.ready()

	out_sock = zmq_context.socket(zmq.PUB)
	out_sock.linger = 0
	out_sock.bind(stats_out_spec)

	received = 0
	rtimes = list()
	receive_min = 0
	receive_max = 0
	receive_avg = 0

	while True:
		m_raw = in_sock.recv()
		at = m_raw.find(' ')
		mtype = m_raw[:at]

		mreceived = None
		if mtype == 'stats':
			m = tnetstring.loads(m_raw[at + 1:])
			logger.debug('stats: %s' % m)
			mreceived = m['received']
			mlatency = m['latency']
		elif mtype == 'update':
			utype = m_raw[at + 1:]
			logger.debug('update: ' + utype)

		count = db.config_get_count()

		nodes = db.node_get_all()
		edge_up = 0
		client_up = 0
		for id, node in nodes.iteritems():
			if node['type'] == 'edge':
				if 'ping' in node:
					edge_up += 1
			elif node['type'] == 'client':
				if 'ping' in node:
					client_up += 1

		if count > 0:
			edge_need = ((count - 1) / edge_capacity) + 1
			client_need = ((count - 1) / client_capacity) + 1
		else:
			edge_need = 0
			client_need = 0

		ping_data = db.get_ping_data()

		stats = dict()

		stats['capacity'] = count

		stats['edge-up'] = edge_up
		stats['edge-total'] = edge_need
		stats['client-up'] = client_up
		stats['client-total'] = client_need

		if 'min' in ping_data:
			stats['ping-min'] = ping_data['min']
		else:
			stats['ping-min'] = 0
		if 'max' in ping_data:
			stats['ping-max'] = ping_data['max']
		else:
			stats['ping-max'] = 0
		if 'avg' in ping_data:
			stats['ping-avg'] = ping_data['avg']
		else:
			stats['ping-avg'] = 0

		origin_info = db.get_origin_info()

		if mreceived is not None:
			latency = mlatency
			if 'ping' in origin_info:
				latency += origin_info['ping'] / 2

			now = int(time.time() * 1000)

			had_received = received
			received = mreceived
			if mreceived > had_received:
				rtimes.append((now, mreceived - had_received, latency))
			else:
				rtimes = list()
				rtimes.append((now, mreceived, latency))

			# calculate delivery times
			start_time = db.send_get_start()

			rmin = None
			rmax = None
			rtotal = 0
			rcount = 0
			for rtime in rtimes:
				t = rtime[0] - start_time - rtime[2]
				if t < 0:
					t = 0 # should not happen
				if rmin is None or t < rmin:
					rmin = t
				if rmax is None or t > rmax:
					rmax = t
				rcount += rtime[1]
				rtotal += t * rtime[1]
			if rmin is not None:
				receive_min = rmin
			else:
				receive_min = 0
			if rmax is not None:
				receive_max = rmax
			else:
				receive_max = 0
			if rtotal > 0:
				receive_avg = rtotal / rcount
			else:
				receive_avg = 0

		stats['received'] = received
		stats['receive-min'] = receive_min
		stats['receive-max'] = receive_max
		stats['receive-avg'] = receive_avg

		curdata = db.get_stats_data()
		if curdata is not None:
			del curdata['id']
			if curdata == stats:
				# stats are the same as before. don't write/send
				continue

		stats['id'] = db.set_stats_data(stats)
		out_sock.send('stats ' + json.dumps(stats))

db.config_set_base_uri(client_base_uri)

app.spawn(stats_worker, wait=True, daemon=True)
app.spawn(rpc_server_worker, wait=True)
app.spawn(nodemanage_worker, daemon=True)
app.spawn(setproxies_worker, daemon=True)
app.spawn(ping_data_worker, daemon=True)
app.spawn(nodestats_worker, daemon=True)

app.wait_for_quit()

rpc_server.stop()
zmq_context.term()
