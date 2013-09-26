import sys
import uuid
import zmq
import tnetstring

context = zmq.Context()
sock = context.socket(zmq.REQ)
sock.linger = 0
sock.connect('tcp://127.0.0.1:10100')

base_uri = sys.argv[1]
count = int(sys.argv[2])

m = dict()
m['id'] = str(uuid.uuid4())
m['method'] = 'setup-clients'
m['args'] = { 'base-uri': base_uri, 'count': count }

sock.send(tnetstring.dumps(m))

m = tnetstring.loads(sock.recv())
if m['success']:
	print 'done'
else:
	print 'error'
