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

	def config_set_base_uri(self, base_uri):
		r = self._get_redis()
		r.set(self.prefix + 'config_base-uri', base_uri)

	def config_get_base_uri(self):
		r = self._get_redis()
		return r.get(self.prefix + 'config_base-uri')

	def config_set_count(self, count):
		r = self._get_redis()
		r.set(self.prefix + 'config_count', count)

	def config_get_count(self):
		r = self._get_redis()
		count_str = r.get(self.prefix + 'config_count')
		if count_str:
			return int(count_str)
		else:
			return 0

	# return node_id
	def node_add(self, node_id_prefix, info):
		r = self._get_redis()
		nodes_key = self.prefix + 'nodes'
		index = 1
		while True:
			with r.pipeline() as pipe:
				try:
					pipe.watch(nodes_key)

					node_id = node_id_prefix + str(index)
					node_key = self.prefix + 'node_' + node_id
					pipe.watch(node_key)
					if r.exists(node_key):
						index += 1
						continue

					info['id'] = node_id

					pipe.multi()
					pipe.sadd(nodes_key, node_id)
					pipe.set(node_key, json.dumps(info))
					pipe.execute()
					return node_id
				except redis.WatchError:
					continue

	def node_update(self, node_id, info):
		r = self._get_redis()
		nodes_key = self.prefix + 'nodes'
		node_key = self.prefix + 'node_' + node_id
		while True:
			with r.pipeline() as pipe:
				try:
					pipe.watch(nodes_key)
					pipe.watch(node_key)

					if not pipe.exists(node_key):
						raise ObjectDoesNotExist()

					pipe.multi()
					pipe.sadd(nodes_key, node_id)
					pipe.set(node_key, json.dumps(info))
					pipe.execute()
					break
				except redis.WatchError:
					continue

	def node_remove(self, node_id):
		r = self._get_redis()
		nodes_key = self.prefix + 'nodes'
		node_key = self.prefix + 'node_' + node_id
		while True:
			with r.pipeline() as pipe:
				try:
					pipe.watch(nodes_key)
					pipe.watch(node_key)

					if not pipe.exists(node_key):
						raise ObjectDoesNotExist()

					pipe.multi()
					pipe.delete(node_key)
					pipe.srem(nodes_key, node_id)
					pipe.execute()
					break
				except redis.WatchError:
					continue

	# return hash of (node_id, info)
	def node_get_all(self):
		r = self._get_redis()
		nodes_key = self.prefix + 'nodes'
		while True:
			with r.pipeline() as pipe:
				try:
					pipe.watch(nodes_key)

					node_ids = pipe.smembers(nodes_key)

					nodes = dict()
					for node_id in node_ids:
						node_key = self.prefix + 'node_' + node_id
						pipe.watch(node_key)
						info_raw = pipe.get(node_key)
						if not info_raw:
							raise redis.WatchError()
						nodes[node_id] = json.loads(info_raw)

					pipe.multi()
					pipe.execute()
					return nodes
				except redis.WatchError:
					continue

	def set_origin_info(self, info):
		r = self._get_redis()
		r.set(self.prefix + 'origin-info', json.dumps(info))

	def get_origin_info(self):
		r = self._get_redis()
		data_raw = r.get(self.prefix + 'origin-info')
		if data_raw:
			return json.loads(data_raw)
		else:
			return None

	def send_set_start(self, ts):
		r = self._get_redis()
		r.set(self.prefix + 'send-start', ts)

	def send_get_start(self):
		r = self._get_redis()
		ts_str = r.get(self.prefix + 'send-start')
		if ts_str:
			return int(ts_str)
		else:
			return None

	def set_ping_data(self, data):
		r = self._get_redis()
		r.set(self.prefix + 'ping-data', json.dumps(data))

	def get_ping_data(self):
		r = self._get_redis()
		data_raw = r.get(self.prefix + 'ping-data')
		if data_raw:
			return json.loads(data_raw)
		else:
			return None

	def set_stats_data(self, data):
		r = self._get_redis()
		key = self.prefix + 'stats-data'
		version_key = self.prefix + 'stats-data-version'
		while True:
			with r.pipeline() as pipe:
				try:
					pipe.watch(key)
					pipe.watch(version_key)

					ver_str = pipe.get(version_key)
					if ver_str:
						ver = int(ver_str) + 1
					else:
						ver = 1

					pipe.multi()
					pipe.set(key, json.dumps(data))
					pipe.set(version_key, str(ver))
					pipe.execute()
					return ver
				except redis.WatchError:
					continue

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
