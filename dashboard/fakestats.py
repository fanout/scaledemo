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

id = 1
while True:
	print 'sending stats'
	out = dict()
	out['capacity'] = 100000
	out['edge-up'] = 15
	out['edge-total'] = 20
	out['client-up'] = 15
	out['client-total'] = 20
	out['ping-min'] = 50
	out['ping-max'] = 60
	out['ping-avg'] = 55
	out['received'] = 50000
	out['receive-min'] = 50
	out['receive-max'] = 60
	out['receive-avg'] = 55
	out['message'] = 'hello'
	r.set('stats-data', json.dumps(out))
	r.set('stats-data-version', str(id))
	out['id'] = id
	id += 1
	sock.send('stats ' + json.dumps(out))

	time.sleep(5)

	print 'sending stats'
	out = dict()
	out['capacity'] = 100000
	out['edge-up'] = 20
	out['edge-total'] = 20
	out['client-up'] = 20
	out['client-total'] = 20
	out['ping-min'] = 50
	out['ping-max'] = 100
	out['ping-avg'] = 75
	out['received'] = 77777
	out['receive-min'] = 50
	out['receive-max'] = 100
	out['receive-avg'] = 75
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
	out['edge-up'] = 0
	out['edge-total'] = 0
	out['client-up'] = 0
	out['client-total'] = 0
	out['ping-min'] = 0
	out['ping-max'] = 0
	out['ping-avg'] = 0
	out['received'] = 0
	out['receive-min'] = 0
	out['receive-max'] = 0
	out['receive-avg'] = 0
	out['message'] = ''
	r.set('stats-data', json.dumps(out))
	r.set('stats-data-version', str(id))
	out['id'] = id
	id += 1
	sock.send('stats ' + json.dumps(out))

	time.sleep(10)
