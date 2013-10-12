import time
import uuid
import json
import random
import socket
import copy
import rbtree
import zmq
import tnetstring
import rpc
import lib

rpc_in_spec = 'tcp://' + lib.controller_host + ':10100'
stats_out_spec = 'tcp://' + lib.controller_host + ':10101'
zhttp_out_specs = ['ipc:///tmp/zurl-in']
zhttp_in_specs = ['ipc:///tmp/zurl-out']

instruct_spec = 'inproc://instruct'
stats_spec = 'inproc://stats'
route_spec = 'inproc://server-rpc'

hostname = socket.gethostname()

logger = lib.logger
zmq_context = lib.zmq_context

WaitingToInitialRequest = 1
WaitingForInitialResponse = 2
WaitingToPoll = 3
WaitingForResponse = 4

class ClientManager(object):
	def __init__(self, instance_id, out_sock):
		self.base_uri = None
		self.instance_id = instance_id
		self.out_sock = out_sock
		self.sessions = list()
		self.session_by_req_id = dict()
		self.req_id_by_session = dict()
		self.session_by_timeout = rbtree.rbtree()
		self.timeout_by_session = dict()
		self.start_count = 0
		self.cur_id = None
		self.cur_body = ''
		self.recv_count = 0
		self.flag_updated = False

		self.update_now()

	def cleanup(self):
		self.out_sock = None

	def update_now(self):
		self.now = int(time.time() * 1000)

	def set_base_uri(self, base_uri):
		if self.base_uri == base_uri:
			return

		self.base_uri = base_uri
		count = len(self.sessions)
		for n in range(0, count):
			self.sessions[n].restart(n / 10)

	def set_count(self, count):
		if len(self.sessions) != count:
			self.flag_updated = True

		# shrink
		while len(self.sessions) > count:
			s = self.sessions.pop()
			req_id = self.req_id_by_session.get(s)
			if req_id:
				del self.req_id_by_session[s]
				del self.session_by_req_id[req_id]
			time_key = self.timeout_by_session.get(s)
			if time_key:
				del self.timeout_by_session[s]
				del self.session_by_timeout[time_key]
			if s.started:
				self.start_count -= 1

		# grow
		if len(self.sessions) < count:
			addcount = count - len(self.sessions)
			for n in range(0, addcount):
				s = ClientSession(self, str(n), n / 10)
				self.sessions.append(s)
				if s.exp is not None:
					time_key = format(s.exp, '014d') + '_' + str(id(s))
					self.session_by_timeout[time_key] = (s.exp, s)
					self.timeout_by_session[s] = time_key

	def get_timeout(self):
		if len(self.session_by_timeout) == 0:
			return None

		self.update_now()
		it = iter(self.session_by_timeout)
		it.next()
		exp = it.value[0]
		timeout = exp - self.now
		if timeout < 0:
			timeout = 0
		return timeout

	def send(self, m):
		self.out_sock.send('T' + tnetstring.dumps(m))

	def process(self, message=None):
		self.update_now()
		m = None
		to_process = list()

		if message is not None:
			if len(message) < 2 or message[0] != 'T':
				return
			try:
				m = tnetstring.loads(message[1:])
			except:
				return
			s = self.session_by_req_id.get(m['id'])
			if s is None:
				return
			to_process.append(s)
		else:
			# if no session has data, then have any timed out?
			for k, v in self.session_by_timeout.iteritems():
				exp, s = v
				timeout = exp - self.now
				if timeout > 0:
					break
				to_process.append(s)

		for s in to_process:
			s.process(m)

			# has the req_id changed? if so, remove the current value
			req_id = self.req_id_by_session.get(s)
			if req_id:
				if s.req_id != req_id:
					del self.req_id_by_session[s]
					del self.session_by_req_id[req_id]
					req_id = None

			# set the new req_id, if any
			if s.req_id and not req_id:
				self.req_id_by_session[s] = s.req_id
				self.session_by_req_id[s.req_id] = s

			# has the exp changed? if so, remove the current value
			time_key = self.timeout_by_session.get(s)
			if time_key:
				cur_exp = self.session_by_timeout[time_key]
				if s.exp != cur_exp:
					del self.timeout_by_session[s]
					del self.session_by_timeout[time_key]
					time_key = None

			# set the new exp, if any
			if s.exp is not None and not time_key:
				time_key = format(s.exp, '014d') + '_' + str(id(s))
				self.session_by_timeout[time_key] = (s.exp, s)
				self.timeout_by_session[s] = time_key

			if s.flag_started:
				s.flag_started = False
				self.start_count += 1
				assert(self.start_count <= len(self.sessions))
				if self.cur_id is None:
					self.cur_id = s.cur_id
					self.cur_body = s.cur_body
				self.flag_updated = True

			if s.flag_updated:
				s.flag_updated = False
				if self.cur_id is None or s.cur_id > self.cur_id:
					self.cur_id = s.cur_id
					self.cur_body = s.cur_body
					self.recv_count = 1
				else:
					assert(s.cur_id == self.cur_id)
					self.recv_count += 1
				self.flag_updated = True

class ClientSession(object):
	def __init__(self, manager, name, startdelay):
		self.manager = manager
		self.name = name
		self.req_id = None
		self.exp = None
		self.flag_started = False
		self.flag_updated = False

		self.state = WaitingToInitialRequest
		self.started = False
		self.cur_id = None
		self.cur_body = ''
		self.tries = 0
		self.retry_time = None
		self.set_timeout(startdelay)

		logger.debug('%s: starting...' % self.name)

	def restart(self, startdelay):
		self.req_id = None
		self.exp = None
		self.updated = False

		self.state = WaitingToInitialRequest
		self.started = False
		self.cur_id = None
		self.cur_body = None
		self.tries = 0
		self.retry_time = None
		self.set_timeout(startdelay)

		logger.debug('%s: restarting...' % self.name)

	def send_request(self, path):
		self.req_id = str(uuid.uuid4())
		m = dict()
		m['from'] = self.manager.instance_id
		m['id'] = self.req_id
		m['method'] = 'GET'
		m['uri'] = self.manager.base_uri + path
		logger.debug('%s OUT: %s' % (self.name, m))
		self.manager.send(m)

	def set_timeout(self, timeout):
		if timeout is not None:
			self.exp = self.manager.now + timeout
		else:
			self.exp = None

	def process(self, m=None):
		timedout = False
		if m is not None:
			logger.debug('%s: received %s' % (self.name, m))
			if m.get('type') == 'keep-alive':
				return
			# any other message means request is finished
			self.req_id = None
		else:
			if self.exp is not None and self.manager.now >= self.exp:
				timedout = True
				self.exp = None

		if m is not None:
			logger.debug('%s IN: %s' % (self.name, m))
			if self.state != WaitingForInitialResponse and self.state != WaitingForResponse:
				logger.info('%s: received unexpected response: %s' % (self.name, m))
				assert(0)
			assert(self.state == WaitingForInitialResponse or self.state == WaitingForResponse)
			if m.get('type') == 'error' or m.get('code') != 200:
				logger.debug('%s: received error or unexpected response: %s' % (self.name, m))
				if self.state == WaitingForInitialResponse:
					self.state = WaitingToInitialRequest
				else: # 3
					self.state = WaitingToPoll
				if self.tries == 1:
					self.retry_time = 1
				elif self.tries < 8:
					logger.info("tries=%d, retry_time=%s, m=%s" % (self.tries, self.retry_time, m))
					self.retry_time *= 2
				t = (self.retry_time * 1000) + random.randint(0, 1000)
				logger.debug('%s: trying again in %dms' % (self.name, t))
				self.set_timeout(t)
				return
			else:
				resp = json.loads(m['body'])
				if 'id' in resp:
					self.cur_id = resp['id']
					self.cur_body = resp['body']
					if isinstance(self.cur_body, unicode):
						self.cur_body = self.cur_body.encode('utf-8')
					logger.debug('%s: received id=%d, body=[%s]' % (self.name, self.cur_id, self.cur_body))
					if self.state == WaitingForInitialResponse:
						self.started = True
						self.flag_started = True
						logger.debug('%s: started' % self.name)
					else:
						self.flag_updated = True
				else:
					logger.debug('%s: received empty response' % self.name)

				# poll again soon
				self.tries = 0
				self.state = WaitingToPoll
				t = random.randint(0, 1000)
				logger.debug('%s: polling in %dms' % (self.name, t))
				self.set_timeout(t)
		else:
			assert(self.state == WaitingToInitialRequest or self.state == WaitingToPoll)
			if self.state == WaitingToInitialRequest:
				if timedout:
					url = '/value/'
					logger.debug('%s: GET %s' % (self.name, url))
					self.send_request(url)
					self.tries += 1
					self.state = WaitingForInitialResponse
			elif self.state == WaitingToPoll:
				if timedout:
					url = '/value/?last_id=' + str(self.cur_id)
					logger.debug('%s: GET %s' % (self.name, url))
					self.send_request(url)
					self.tries += 1
					self.state = WaitingForResponse

def client_worker(c):
	instance_id = str(uuid.uuid4())

	instruct_in_sock = zmq_context.socket(zmq.PULL)
	instruct_in_sock.connect(instruct_spec)

	stats_out_sock = zmq_context.socket(zmq.PUSH)
	stats_out_sock.connect(stats_spec)
	stats_out_sock.linger = 0

	zhttp_out_sock = zmq_context.socket(zmq.PUSH)
	zhttp_out_sock.linger = 0
	for spec in zhttp_out_specs:
		zhttp_out_sock.connect(spec)

	zhttp_in_sock = zmq_context.socket(zmq.SUB)
	zhttp_in_sock.setsockopt(zmq.SUBSCRIBE, instance_id + ' ')
	for spec in zhttp_in_specs:
		zhttp_in_sock.connect(spec)

	# give it a chance to connect to zurl
	time.sleep(1)

	man = ClientManager(instance_id, zhttp_out_sock)
	last_stats = 0
	stats_freq = 20 # report stats no faster than 20ms

	poller = zmq.Poller()
	poller.register(instruct_in_sock, zmq.POLLIN)
	poller.register(zhttp_in_sock, zmq.POLLIN)
	while True:
		try:
			m_raw = None
			timeout = man.get_timeout()
			if man.flag_updated:
				now = int(time.time()) * 1000
				if last_stats + stats_freq > now:
					if timeout is None or stats_freq - (now - last_stats) < timeout:
						timeout = stats_freq - (now - last_stats)
				else:
					timeout = 0
			logger.debug('zmq poll with timeout=%s' % timeout)
			socks = dict(poller.poll(timeout))
			if socks.get(instruct_in_sock) == zmq.POLLIN:
				m_raw = instruct_in_sock.recv()
				m = tnetstring.loads(m_raw)
				if m['command'] == 'setup-clients':
					args = m['args']
					man.set_base_uri(args['base-uri'])
					man.set_count(args['count'])
			if socks.get(zhttp_in_sock) == zmq.POLLIN:
				m_raw = zhttp_in_sock.recv()
				at = m_raw.find(' ')
				m_raw = m_raw[at + 1:]
		except zmq.ZMQError as e:
			if e.errno == zmq.ETERM:
				break
			else:
				raise

		man.process(m_raw)

		if man.flag_updated:
			now = int(time.time() * 1000)
			if last_stats + stats_freq < now:
				last_stats = now
				man.flag_updated = False

				m = dict()
				m['total'] = len(man.sessions)
				m['started'] = man.start_count
				m['received'] = man.recv_count
				if man.cur_id is not None:
					m['cur-id'] = man.cur_id
					m['cur-body'] = man.cur_body
				stats_out_sock.send(tnetstring.dumps(m))

	# don't clean up sockets as that has already been done by term()
	man.cleanup()

def stats_worker(c):
	stats_in_sock = zmq_context.socket(zmq.PULL)
	stats_in_sock.bind(stats_spec)
	c.ready()

	stats_out_sock = zmq_context.socket(zmq.PUB)
	stats_out_sock.connect(stats_out_spec)
	stats_out_sock.linger = 0

	stats = None
	updated = False
	last_print = 0
	print_freq = 100

	poller = zmq.Poller()
	poller.register(stats_in_sock, zmq.POLLIN)
	while True:
		timeout = None
		if updated:
			now = int(time.time()) * 1000
			if last_print + print_freq > now:
				timeout = print_freq - (now - last_print)
			else:
				timeout = 0
		socks = dict(poller.poll(timeout))
		if socks.get(stats_in_sock) == zmq.POLLIN:
			m_raw = stats_in_sock.recv()
			m = tnetstring.loads(m_raw)

			stats = copy.deepcopy(m)
			updated = True

			m['id'] = hostname
			stats_out_sock.send('stats ' + tnetstring.dumps(m))

		if updated:
			now = int(time.time()) * 1000
			if last_print + print_freq < now:
				last_print = now
				updated = False
				if 'cur-id' in stats:
					cur_id = stats['cur-id']
				else:
					cur_id = -1
				logger.info('stats: T=%d/S=%d/R=%d cur-id=%d' % (stats['total'], stats['started'], stats['received'], cur_id))

def method_handler(method, args, data):
	global control
	logger.debug('call: %s(%s)' % (method, args))
	if method == 'ping':
		return True
	elif method == 'setup-clients':
		m = dict()
		m['command'] = 'setup-clients'
		m['args'] = args
		logger.info('setup-clients: %s %d' % (args['base-uri'], args['count']))
		data['instruct_out_sock'].send(tnetstring.dumps(m))
	else:
		raise rpc.CallError('method-not-found')

server = None

def server_worker(c):
	global server
	instruct_out_sock = zmq_context.socket(zmq.PUSH)
	instruct_out_sock.bind(instruct_spec)
	instruct_out_sock.linger = 0
	server = rpc.RpcServer([route_spec], context=zmq_context)
	c.ready()
	data = dict()
	data['instruct_out_sock'] = instruct_out_sock
	server.run(method_handler, data)

lib.spawn(server_worker, wait=True)
lib.spawn(lib.server_route_worker, args=(rpc_in_spec, route_spec, lib.instance_id))
lib.spawn(stats_worker, wait=True)
lib.spawn(client_worker)

lib.wait_for_quit()

server.stop()
lib.zmq_context.term()
