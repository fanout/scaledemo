import zmq
import tnetstring

context = zmq.Context()
sock = context.socket(zmq.SUB)
sock.setsockopt(zmq.SUBSCRIBE, 'stats ')
sock.connect('tcp://127.0.0.1:10101')

while True:
	m_raw = sock.recv()
	m = tnetstring.loads(m_raw[6:])
	print m
