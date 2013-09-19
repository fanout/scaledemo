import sys
import time
import lib

lib.client_init()
time.sleep(0.5)
try:
	#start = int(time.time() * 1000)
	#lib.ping(sys.argv[1])
	#now = int(time.time() * 1000)
	#print '%dms' % (now - start)
	lib.start(sys.argv[1], int(sys.argv[2]))
	print 'started'
except:
	pass
lib.client_shutdown()
