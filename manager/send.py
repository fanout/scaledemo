import sys
import rpc

client = rpc.RpcClient(['tcp://%s:10100' % sys.argv[1]])
print client.call('send', {'message': sys.argv[2]})
