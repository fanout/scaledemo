import sys
import time
import threading
import logging
from logging.handlers import WatchedFileHandler
import ConfigParser
import zmq

logger = None
config = None

def init(config_file):
	global logger
	global config
	log_file = None
	verbose = False
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

	logger = logging.getLogger('app')
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

def spawn(target, args=None, wait=False, daemon=False):
	if wait:
		c = threading.Condition()
		c.acquire()
	else:
		c = None
	thread = threading.Thread(target=_spawn_run, args=(target, c, args))
	thread.daemon = daemon
	thread.start()
	if wait:
		c.wait()
		c.release()
