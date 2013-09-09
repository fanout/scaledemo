import time
import uuid
import json
import random
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

# state:
#   0 = init, do initial request
#   1 = made initial request, waiting for response
#   2 = got response, waiting to poll
#   3 = polling, waiting for response
#   4 = got error on initial request, waiting to try again
#   5 = got error on poll, waiting to try again
class ClientSession(object):
	def __init__(self, _id, now, startdelay):
		self.id = id
		self.req_id = None
		self.exp = None
		self.updated = False
		self.sock = None
		self.instance_id = None
		self.now = now

		self.state = 0
		self.cur_id = None
		self.cur_body = None
		self.tries = 0
		self.retry_time = None
		self.set_timeout(startdelay)
		logger.debug('%s: starting...' % id(self))

	def send_request(self, path):
		self.req_id = str(uuid.uuid4())
		m = dict()
		m['from'] = self.instance_id
		m['id'] = self.req_id
		m['method'] = 'GET'
		m['uri'] = 'http://localhost:7999' + path
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
		timedout = False
		if m is not None:
			logger.debug('%s: received %s' % (id(self), m))
			if m.get('type') == 'keep-alive':
				return
			self.req_id = None
		else:
			if self.exp is not None and now >= self.exp:
				timedout = True
				self.exp = None
		self._process(m, timedout)

	def _process(self, m=None, timedout=False):
		if m is not None:
			if self.state != 1 and self.state != 3:
				logger.info('%s: received unexpected response: %s' % (id(self), m))
				assert(0)
			assert(self.state == 1 or self.state == 3)
			if m.get('type') == 'error' or m.get('code') != 200:
				logger.debug('%s: received error or unexpected response: %s' % (id(self), m))
				if self.state == 1:
					self.state = 4
				else: # 3
					self.state = 5
				if self.tries == 1:
					self.retry_time = 1
				elif self.tries < 8:
					logger.info("tries=%d, retry_time=%s, m=%s" % (self.tries, self.retry_time, m))
					self.retry_time *= 2
				t = (self.retry_time * 1000) + random.randint(0, 1000)
				logger.debug('%s: trying again in %dms' % (id(self), t))
				self.set_timeout(t)
				return
			else:
				resp = json.loads(m['body'])
				if 'id' in resp:
					self.cur_id = resp['id']
					self.cur_body = resp['body']
					logger.debug('%s: received id=%d, body=[%s]' % (id(self), self.cur_id, self.cur_body))
					if self.state == 1:
						logger.debug('%s: started' % id(self))
					self.updated = True
				else:
					logger.debug('%s: received empty response' % id(self))

				# poll again soon
				self.tries = 0
				self.state = 2
				t = random.randint(0, 1000)
				logger.debug('%s: polling in %dms' % (id(self), t))
				self.set_timeout(t)
		else:
			assert(self.state == 0 or self.state == 2 or self.state == 4 or self.state == 5)
			if self.state == 0:
				url = '/headline/value/'
				logger.debug('%s: GET %s' % (id(self), url))
				self.send_request(url)
				self.tries += 1
				self.state = 1
			elif self.state == 2:
				if timedout:
					url = '/headline/value/?last_id=' + str(self.cur_id)
					logger.debug('%s: GET %s' % (id(self), url))
					self.send_request(url)
					self.tries += 1
					self.state = 3
			elif self.state == 4:
				if timedout:
					self.state = 0
					self._process()
			elif self.state == 5:
				if timedout:
					url = '/headline/value/?last_id=' + str(self.cur_id)
					logger.debug('%s: GET %s' % (id(self), url))
					self.send_request(url)
					self.tries += 1
					self.state = 3

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

	all_started = False
	cur_id = None
	cur_count = 0

	now = int(time.time() * 1000)

	for n in range(0, count):
		s = ClientSession(n, now, n/10)
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
		logger.debug('zmq poll with timeout=%s' % timeout)
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

			if s.updated:
				s.updated = False
				if not all_started:
					if s.state >= 2:
						cur_count += 1
						# print every 10% complete
						if cur_count == count or (cur_count % (count / 10) == 0):
							logger.info('started: %d/%d' % (cur_count, count))
						if cur_count == count:
							all_started = True
				else:
					if cur_id is None or s.cur_id > cur_id:
						cur_id = s.cur_id
						cur_count = 1
					else:
						assert(s.cur_id == cur_id)
						cur_count += 1
					# print every 10% complete
					if cur_count == count or (cur_count % (count / 10) == 0):
						logger.info('updated: %d/%d' % (cur_count, count))

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
