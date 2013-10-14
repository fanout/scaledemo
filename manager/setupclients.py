import sys
import rpc

client = rpc.RpcClient(['tcp://%s:10100' % sys.argv[1]])
print client.call('setup-clients', {'base-uri': sys.argv[2], 'count': int(sys.argv[3]), 'connect-host': sys.argv[4]})
