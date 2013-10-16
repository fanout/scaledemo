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
	data = json.loads(m_raw[6:])
	print 'stats: %s' % data

	assert(data['id'] > 0)
	prev_id = data['id'] - 1

	out = dict()
	out['id'] = data['id']
	out['capacity'] = data.get('capacity', 0)
	out['edge_up'] = data.get('edge-up', 0)
	out['edge_total'] = data.get('edge-total', 0)
	out['client_up'] = data.get('client-up', 0)
	out['client_total'] = data.get('client-total', 0)
	out['ping_min'] = data.get('ping-min', 0)
	out['ping_max'] = data.get('ping-max', 0)
	out['ping_avg'] = data.get('ping-avg', 0)
	out['received'] = data.get('received', 0)
	out['receive_min'] = data.get('receive-min', 0)
	out['receive_max'] = data.get('receive-max', 0)
	out['receive_avg'] = data.get('receive-avg', 0)
	out['message'] = data.get('message', '')

	hr_headers = dict()
	hr_headers['Content-Type'] = 'application/json'
	hr_body = json.dumps(out) + '\n'
	pub.publish(grip_prefix + 'status', str(id), str(prev_id), hr_headers, hr_body)
