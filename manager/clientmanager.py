import time
import uuid
import rbtree
import zmq
import tnetstring
import rpc
import lib

in_cmd_spec = 'tcp://' + lib.controller_host + ':10100'
out_report_spec = 'tcp://' + lib.controller_host + ':10101'
zurl_out_specs = ['ipc:///tmp/zurl-in']
zurl_in_specs = ['ipc:///tmp/zurl-out']

logger = lib.logger

class ClientSession(object):
	def __init__(self):
		self.req_id = None
		self.exp = None
		self.sock = None
		self.instance_id = None
		self.now = 0

		self.state = 0
		self.set_timeout(0)

	def send_request(self, path):
		self.req_id = str(uuid.uuid4())
		m = dict()
		m['from'] = self.instance_id
		m['id'] = self.req_id
		m['method'] = 'GET'
		m['uri'] = 'http://localhost:8080' + path
		self.sock.send('T' + tnetstring.dumps(m))

	def set_timeout(self, timeout):
		if timeout is not None:
			self.exp = self.now + timeout
		else:
			self.exp = None

	def process(self, sock, instance_id, now, m=None):
		self.sock = sock
		self.instance_id = instance_id
		self.now = now
		if m is not None:
			self.req_id = None
		timedout = False
		if self.exp is not None and now >= self.exp:
			timedout = True
			self.exp = None
		self._process(m, timedout)

	def _process(m=None, timedout=False):
		if m is not None:
			assert(self.state == 1)
			logger.info('got response')
			self.state = 2
			self.set_timeout(5000)
		else:
			if self.state == 0:
				logger.info('sending request')
				self.send_request('/')
				self.state = 1
			elif self.state == 2:
				if timedout:
					self.state = 0
					self._process()

def client_worker(c, count):
	instance_id = 'clientmanager'
	out_sock = lib.zmq_context.socket(zmq.PUSH)
	for spec in zurl_out_specs:
		out_sock.connect(spec)
	in_sock = lib.zmq_context.socket(zmq.SUB)
	in_sock.setsockopt(zmq.SUBSCRIBE, instance_id + ' ')
	for spec in zurl_in_specs:
		in_sock.connect(spec)
	time.sleep(1)
	c.ready()

	sessions = list()
	session_by_req_id = dict()
	req_id_by_session = dict()
	session_by_timeout = rbtree.rbtree()
	timeout_by_session = dict()

	for n in range(0, count):
		s = ClientSession()
		sessions.append(s)
		if s.exp is not None:
			time_key = format(s.exp, '014d') + '_' + str(id(s))
			session_by_timeout[time_key] = (s.exp, s)
			timeout_by_session[s] = time_key

	poller = zmq.Poller()
	poller.register(in_sock, zmq.POLLIN)
	while True:
		to_process = list()
		m = None

		# any data for a session?
		timeout = None
		if len(session_by_timeout) > 0:
			it = iter(session_by_timeout)
			it.next()
			exp = it.value[0]
			now = int(time.time() * 1000)
			timeout = exp - now
			if timeout < 0:
				timeout = 0
		logger.debug('polling with timeout=%s' % timeout)
		socks = dict(poller.poll(timeout))
		if socks.get(in_sock) == zmq.POLLIN:
			m_raw = in_sock.recv()
			at = m_raw.find(' T')
			m = tnetstring.loads(m_raw[at + 2:])
			s = session_by_req_id.get(m['id'])
			if s:
				to_process.append(s)

		now = int(time.time() * 1000)

		# if no session has data, then have any timed out?
		if len(to_process) == 0:
			for k, v in session_by_timeout.iteritems():
				exp, s = v
				timeout = exp - now
				if timeout > 0:
					break
				to_process.append(s)

		for s in to_process:
			s.process(out_sock, instance_id, now, m)

			# has the req_id changed? if so, remove the current value
			req_id = req_id_by_session.get(s)
			if req_id:
				if s.req_id != req_id:
					del req_id_by_session[s]
					del session_by_req_id[req_id]
					req_id = None

			# set the new req_id, if any
			if s.req_id and not req_id:
				req_id_by_session[s] = s.req_id
				session_by_req_id[s.req_id] = s

			# has the exp changed? if so, remove the current value
			time_key = timeout_by_session.get(s)
			if time_key:
				cur_exp = session_by_timeout[time_key]
				if s.exp != cur_exp:
					del timeout_by_session[s]
					del session_by_timeout[time_key]
					time_key = None

			# set the new exp, if any
			if s.exp is not None and not time_key:
				time_key = format(s.exp, '014d') + '_' + str(id(s))
				session_by_timeout[time_key] = (s.exp, s)
				timeout_by_session[s] = time_key

control = None

def method_handler(method, args, data):
	global control
	if len(args) > 0:
		logger.info('call: %s(%s)' % (method, args))
	else:
		logger.info('call: %s()' % method)
	if method == 'ping':
		return True
	elif method == 'start':
		count = args['count']
		if control is not None:
			control.stop()
			control = None
		if count > 0:
			control = lib.spawn(client_worker, args=(count,), wait=True)
	else:
		raise rpc.CallError('method-not-found')

server = None
route_spec = 'inproc://server-rpc'

def server_worker(c):
	global server
	server = rpc.RpcServer([route_spec], context=lib.zmq_context)
	c.ready()
	server.run(method_handler, None)

lib.spawn(server_worker, wait=True)
lib.spawn(lib.server_route_worker, args=(in_cmd_spec, route_spec, lib.instance_id))

lib.wait_for_quit()

server.stop()
lib.zmq_context.term()
