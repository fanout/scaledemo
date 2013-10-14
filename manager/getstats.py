import sys
import os
import logging
import zmq
import json

# reopen stdout file descriptor with write mode
# and 0 as the buffer size (unbuffered)
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

logger = logging.getLogger('getstats')
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger.setLevel(logging.DEBUG)
logger_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s.%(msecs)03d %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger_handler.setFormatter(formatter)
logger.addHandler(logger_handler)

context = zmq.Context()
sock = context.socket(zmq.SUB)
sock.setsockopt(zmq.SUBSCRIBE, 'stats ')
sock.connect('tcp://%s:10101' % sys.argv[1])

while True:
	m_raw = sock.recv()
	m = json.loads(m_raw[6:])
	logger.info('en=%d/%d cn=%d/%d ping=%d/%d/%d r=%d rt=%d/%d/%d' %
		(m['edge-up'], m['edge-total'], m['client-up'],
		m['client-total'], m['ping-min'], m['ping-max'], m['ping-avg'],
		m['received'], m['receive-min'], m['receive-max'],
		m['receive-avg']))
