import sys
import os
import logging
import zmq
import tnetstring

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
	m = tnetstring.loads(m_raw[6:])
	latency = m.get('latency', -1)
	cur_id = m.get('cur-id', -1)
	logger.info('stats T=%d/S=%d/R=%d/E=%d/L=%d cur-id=%d' % (m['total'], m['started'], m['received'], m['errored'], latency, cur_id))
