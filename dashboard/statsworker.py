import json
from django.conf import settings
import zmq
import grip
import redis_ops

zmq_context = zmq.Context()
db = redis_ops.RedisOps()
pub = grip.Publisher()

if hasattr(settings, 'REDIS_HOST'):
	db.host = settings.REDIS_HOST

if hasattr(settings, 'REDIS_PORT'):
	db.port = settings.REDIS_PORT

if hasattr(settings, 'REDIS_DB'):
	db.db = settings.REDIS_DB

if hasattr(settings, 'GRIP_PROXIES'):
	grip_proxies = settings.GRIP_PROXIES
else:
	grip_proxies = list()

if hasattr(settings, 'DASHBOARD_REDIS_PREFIX'):
	db.prefix = settings.DASHBOARD_REDIS_PREFIX
else:
	db.prefix = ''

if hasattr(settings, 'DASHBOARD_GRIP_PREFIX'):
	grip_prefix = settings.DASHBOARD_GRIP_PREFIX
else:
	grip_prefix = 'dashboard-'

pub.proxies = grip_proxies

sock = zmq_context.socket(zmq.SUB)
sock.setsockopt(zmq.SUBSCRIBE, 'stats ')
sock.connect('tcp://localhost:10101')

while True:
	m_raw = sock.recv()
	m = json.loads(m_raw[6:])

	id = str(m['id'])
	if m['id'] > 0:
		prev_id = str(m['id'] - 1)
	else:
		prev_id = ''

	hr_headers = dict()
	hr_headers['Content-Type'] = 'application/json'
	hr_body = json.dumps(m) + '\n'
	pub.publish(grip_prefix + 'status', id, prev_id, hr_headers, hr_body)
