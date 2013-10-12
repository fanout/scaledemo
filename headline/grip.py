from base64 import b64decode
import copy
import threading
import zmq
import tnetstring
import pubcontrol
import gripcontrol

zmq_context = zmq.Context()

def ensure_utf8(s):
	if isinstance(s, unicode):
		return s.encode("utf-8")
	else:
		return s # assume it is already utf-8

# convert json-style transport to tnetstring-style
def convert_json_transport(t):
	out = dict()
	if "code" in t:
		out["code"] = t["code"]
	if "reason" in t:
		out["reason"] = ensure_utf8(t["reason"])
	if "headers" in t:
		headers = dict()
		for k, v in t["headers"].iteritems():
			headers[ensure_utf8(k)] = ensure_utf8(v)
		out["headers"] = headers
	if "body-bin" in t:
		out["body"] = ensure_utf8(b64decode(t["body-bin"]))
	elif "body" in t:
		out["body"] = ensure_utf8(t["body"])
	if "action" in t:
		out["action"] = ensure_utf8(t["action"])
	if "content-bin" in t:
		out["content"] = ensure_utf8(b64decode(t["content-bin"]))
	elif "content" in t:
		out["content"] = ensure_utf8(t["content"])
	return out

def is_proxied(request, proxies):
	if len(proxies) < 1:
		return False

	grip_sig = request.META.get('HTTP_GRIP_SIG')
	if not grip_sig:
		return False

	for p in proxies:
		if gripcontrol.validate_sig(grip_sig, p['key']):
			return True

	return False

class Publisher(object):
	def __init__(self):
		self.lock = threading.Lock()
		self.proxies = list()
		self.zmq_specs = list()
		self.pubs = None
		self.sock = zmq_context.socket(zmq.PUSH)
		self.sock.bind('inproc://headline-grip-publish')
		self.zmq_thread = threading.Thread(target=self._zmq_thread_worker)
		self.zmq_thread.daemon = True
		self.zmq_thread.start()

	def set_zmq_specs(self, specs):
		self.lock.acquire()
		self.zmq_specs = specs
		self.lock.release()

	def publish(self, channel, id, prev_id, rheaders=None, rbody=None, sbody=None, code=None, reason=None):
		self.lock.acquire()

		if self.pubs is None:
			self.pubs = list()
			for p in self.proxies:
				pub = gripcontrol.GripPubControl(p['control_uri'])
				if 'control_iss' in p:
					pub.set_auth_jwt({'iss': p['control_iss']}, p['key'])
				self.pubs.append(pub)

		formats = list()
		if rbody is not None:
			formats.append(gripcontrol.HttpResponseFormat(code=code, reason=reason, headers=rheaders, body=rbody))
		if sbody is not None:
			formats.append(gripcontrol.HttpStreamFormat(sbody))

		item = pubcontrol.Item(formats, id, prev_id)

		zitem = item.export()
		zitem['channel'] = channel
		zitem['http-response'] = convert_json_transport(zitem['http-response'])
		self.sock.send(tnetstring.dumps(zitem))

		for pub in self.pubs:
			pub.publish_async(channel, item)

		self.lock.release()

	def _update_socks(self, socks):
		self.lock.acquire()
		specs = copy.deepcopy(self.zmq_specs)
		self.lock.release()

		# remove any
		to_remove = list()
		for spec, sock in socks.iteritems():
			if spec not in specs:
				sock.close()
				to_remove.append(spec)
		for spec in to_remove:
			del socks[spec]

		# add any
		for spec in specs:
			if spec not in socks:
				sock = zmq_context.socket(zmq.PUSH)
				sock.linger = 0
				sock.connect(spec)
				socks[spec] = sock

	def _zmq_thread_worker(self):
		in_sock = zmq_context.socket(zmq.PULL)
		in_sock.connect('inproc://headline-grip-publish')
		out_socks = dict() # hash of spec->sock
		self._update_socks(out_socks)
		while True:
			m_raw = in_sock.recv()
			self._update_socks(out_socks)
			for sock in out_socks.values():
				sock.send(m_raw)
