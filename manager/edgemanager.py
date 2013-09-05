import rpc
import lib

in_cmd_spec = 'tcp://' + lib.controller_host + ':10100'

logger = lib.logger

def method_handler(method, args, data):
	if len(args) > 0:
		logger.info('call: %s(%s)' % (method, args))
	else:
		logger.info('call: %s()' % method)
	if method == 'ping':
		return True
	else:
		raise rpc.CallError('method-not-found')

server = None
route_spec = 'inproc://server-rpc'

def server_worker(c):
	global server
	server = rpc.RpcServer([route_spec], context=lib.zmq_context)
	c.ready()
	server.run(method_handler, None)

lib.spawn(server_worker, wait=True)
lib.spawn(lib.server_route_worker, args=(in_cmd_spec, route_spec, lib.instance_id))

lib.wait_for_quit()

server.stop()
lib.zmq_context.term()
