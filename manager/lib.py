import sys
import time
import threading
import logging
from logging.handlers import WatchedFileHandler
import ConfigParser
import tnetstring
import zmq
import rpc

config_file = '/etc/scaledemo.conf'
log_file = None
verbose = False

def process_args():
	global config_file
	global log_file
	global verbose
	n = 0
	count = len(sys.argv)
	while n < count:
		arg = sys.argv[n]
		if arg.startswith('--'):
			buf = arg[2:]
			at = buf.find('=')
			if at != -1:
				var = buf[:at]
				val = buf[at + 1:]
			else:
				var = buf
				val = None
			del sys.argv[n]
			count -= 1

			if var == 'config':
				config_file = val
			elif var == 'logfile':
				log_file = val
			elif var == 'verbose':
				verbose = True
		else:
			n += 1

process_args()

logger = logging.getLogger('handler')
if log_file:
	logger_handler = WatchedFileHandler(log_file)
else:
	logger_handler = logging.StreamHandler(stream=sys.stdout)
if verbose:
	logger.setLevel(logging.DEBUG)
	logger_handler.setLevel(logging.DEBUG)
else:
	logger.setLevel(logging.INFO)
	logger_handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s.%(msecs)03d %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger_handler.setFormatter(formatter)
logger.addHandler(logger_handler)

config = ConfigParser.ConfigParser()
config.read([config_file])

instance_id = None
if config.has_option('global', 'instance_id'):
	instance_id = config.get('global', 'instance_id')

controller_host = None
if config.has_option('global', 'controller_host'):
	controller_host = config.get('global', 'controller_host')

zmq_context = zmq.Context()

def wait_for_quit():
	try:
		while True:
			time.sleep(60)
	except KeyboardInterrupt:
		pass

class SpawnCondition(object):
	def __init__(self, c):
		self.c = c

	def ready(self):
		self.c.acquire()
		self.c.notify()
		self.c.release()

def _spawn_run(target, c, args):
	if c is not None:
		sc = SpawnCondition(c)
	else:
		sc = None
	if args is None:
		args = list()
	try:
		target(sc, *args)
	except zmq.ZMQError as e:
		if e.errno != zmq.ETERM:
			raise

def spawn(target, args=None, wait=False):
	if wait:
		c = threading.Condition()
		c.acquire()
	else:
		c = None
	thread = threading.Thread(target=_spawn_run, args=(target, c, args))
	thread.start()
	if wait:
		c.wait()
		c.release()

def client_route_worker(c, rpc_spec, route_spec):
	in_sock = zmq_context.socket(zmq.ROUTER)
	in_sock.bind(route_spec)
	c.ready()
	out_sock = zmq_context.socket(zmq.ROUTER)
	out_sock.bind(rpc_spec)
	poller = zmq.Poller()
	poller.register(in_sock, zmq.POLLIN)
	poller.register(out_sock, zmq.POLLIN)
	while True:
		socks = dict(poller.poll())
		if socks.get(in_sock) == zmq.POLLIN:
			m_raw = in_sock.recv_multipart()
			m = tnetstring.loads(m_raw[-1])
			to = m['args']['to']
			del m['args']['to']
			m_raw[-1] = tnetstring.dumps(m)
			m_raw.insert(0, to)
			logger.debug('client_route: OUT %s' % (m_raw))
			out_sock.send_multipart(m_raw)
		elif socks.get(out_sock) == zmq.POLLIN:
			m_raw = out_sock.recv_multipart()
			logger.debug('client_route: IN %s' % (m_raw))
			m_raw = m_raw[1:]
			in_sock.send_multipart(m_raw)

def server_route_worker(c, rpc_spec, route_spec, instance_id):
	in_sock = zmq_context.socket(zmq.ROUTER)
	in_sock.identity = instance_id
	in_sock.connect(rpc_spec)
	out_sock = zmq_context.socket(zmq.DEALER)
	out_sock.connect(route_spec)
	poller = zmq.Poller()
	poller.register(in_sock, zmq.POLLIN)
	poller.register(out_sock, zmq.POLLIN)
	while True:
		socks = dict(poller.poll())
		if socks.get(in_sock) == zmq.POLLIN:
			m_raw = in_sock.recv_multipart()
			logger.debug('server_route: IN %s' % (m_raw))
			out_sock.send_multipart(m_raw)
		elif socks.get(out_sock) == zmq.POLLIN:
			m_raw = out_sock.recv_multipart()
			logger.debug('server_route: OUT %s' % (m_raw))
			in_sock.send_multipart(m_raw)

client_route_spec = 'inproc://client-rpc'
client = None

def client_init():
	global client
	rpc_spec = 'tcp://*:10100'
	spawn(client_route_worker, args=(rpc_spec, client_route_spec), wait=True)
	client = rpc.RpcClient([client_route_spec], context=zmq_context)

def client_shutdown():
	global client
	client = None
	zmq_context.term()

def ping(instance_id):
	client.call('ping', {'to': instance_id})

def start(instance_id, count):
	client.call('start', {'to': instance_id, 'count': count})
