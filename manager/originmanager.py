import redis
import zmq
import app
import rpc

app.init('/etc/scaledemo.conf')

rpc_in_spec = app.config.get('originmanager', 'rpc_in_spec')
logger = app.logger
zmq_context = zmq.Context()
db = redis.Redis()
rpc_server = None

def method_handler(method, args, data):
	if len(args) > 0:
		logger.info('call: %s(%s)' % (method, args))
	else:
		logger.info('call: %s()' % method)
	if method == 'ping':
		return True
	elif method == 'set-grip-proxies':
		specs = args['specs']
		db.delete('proxies.tmp')
		for spec in specs:
			db.sadd('proxies.tmp', spec)
		db.rename('proxies.tmp', 'proxies')
	else:
		raise rpc.CallError('method-not-found')

def rpc_server_worker(c):
	global rpc_server
	rpc_server = rpc.RpcServer([rpc_in_spec], context=zmq_context)
	c.ready()
	rpc_server.run(method_handler, None)

app.spawn(rpc_server_worker, wait=True)

app.wait_for_quit()

rpc_server.stop()
zmq_context.term()
