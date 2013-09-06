import threading
import json
import redis

class ObjectDoesNotExist(Exception):
	pass

class RedisOps(object):
	def __init__(self):
		self.prefix = ''
		self.host = 'localhost'
		self.port = 6379
		self.db = 0
		self.lock = threading.Lock()
		self.redis = None

	def _get_redis(self):
		self.lock.acquire()
		if not self.redis:
			self.redis = redis.Redis(host=self.host, port=self.port, db=self.db)
		self.lock.release()
		return self.redis

	# return (id, body)
	def headline_get(self):
		r = self._get_redis()
		key = self.prefix + 'value'
		val_raw = r.get(key)
		if not val_raw:
			raise ObjectDoesNotExist()
		val = json.loads(val_raw)
		return (val['id'], val['body'])

	# return (new id, prev_id)
	def headline_set(self, body):
		r = self._get_redis()
		key = self.prefix + 'value'
		while True:
			with r.pipeline() as pipe:
				try:
					pipe.watch(key)
					val_raw = pipe.get(key)
					if val_raw:
						val = json.loads(val_raw)
						prev_id = val['id']
						id = prev_id + 1
						val['id'] = id
					else:
						val = dict()
						prev_id = ''
						id = 0
						val['id'] = id
					val['body'] = body
					val_raw = json.dumps(val)
					pipe.multi()
					pipe.set(key, val_raw)
					pipe.execute()
					return (id, prev_id)
				except redis.WatchError:
					continue
