import time
import json
import zmq
import redis

r = redis.Redis()

context = zmq.Context()
sock = context.socket(zmq.PUB)
sock.bind('tcp://*:10101')
sock.linger = 0

time.sleep(1)

id = 0
while True:
	print 'sending stats'
	out = dict()
	out['capacity'] = 100000
	out['edge_up'] = 20
	out['edge_total'] = 20
	out['client_up'] = 20
	out['client_total'] = 20
	out['ping_min'] = 50
	out['ping_max'] = 100
	out['ping_avg'] = 75
	out['received'] = 50000
	out['receive_min'] = 50
	out['receive_max'] = 100
	out['receive_avg'] = 75
	out['message'] = 'hello'
	r.set('stats-data', json.dumps(out))
	r.set('stats-data-version', str(id))
	out['id'] = id
	id += 1
	sock.send('stats ' + json.dumps(out))

	time.sleep(5)

	print 'sending stats'
	out = dict()
	out['capacity'] = 0
	out['edge_up'] = 0
	out['edge_total'] = 0
	out['client_up'] = 0
	out['client_total'] = 0
	out['ping_min'] = 0
	out['ping_max'] = 0
	out['ping_avg'] = 0
	out['received'] = 0
	out['receive_min'] = 0
	out['receive_max'] = 0
	out['receive_avg'] = 0
	out['message'] = ''
	r.set('stats-data', json.dumps(out))
	r.set('stats-data-version', str(id))
	out['id'] = id
	id += 1
	sock.send('stats ' + json.dumps(out))

	time.sleep(10)
