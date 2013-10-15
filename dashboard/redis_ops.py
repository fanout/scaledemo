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

	def get_stats_data(self):
		r = self._get_redis()
		key = self.prefix + 'stats-data'
		version_key = self.prefix + 'stats-data-version'
		while True:
			with r.pipeline() as pipe:
				try:
					pipe.watch(key)
					pipe.watch(version_key)

					ver_str = pipe.get(version_key)
					if not ver_str:
						return None

					ver = int(ver_str)
					data_raw = pipe.get(key)

					pipe.multi()
					pipe.execute()
					data = json.loads(data_raw)
					data['id'] = ver
					return data
				except redis.WatchError:
					continue
