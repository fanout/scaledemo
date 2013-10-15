import json
from django.http import HttpResponse, HttpResponseNotAllowed
from django.template import RequestContext
from django.shortcuts import render_to_response
import redis_ops

db = redis_ops.RedisOps()

def _get_stats():
	data = db.get_stats_data()
	if data is None:
		data = dict()
	out = dict()
	out['capacity'] = data.get('capacity', 0)
	out['edge-up'] = data.get('edge-up', 0)
	out['edge-total'] = data.get('edge-total', 0)
	out['client-up'] = data.get('client-up', 0)
	out['client-total'] = data.get('client-total', 0)
	out['ping-min'] = data.get('ping-min', 0)
	out['ping-max'] = data.get('ping-max', 0)
	out['ping-avg'] = data.get('ping-avg', 0)
	out['received'] = data.get('received', 0)
	out['receive-min'] = data.get('receive-min', 0)
	out['receive-max'] = data.get('receive-max', 0)
	out['receive-avg'] = data.get('receive-avg', 0)
	out['client-up'] = data.get('client-up', 0)
	return out

def home(request):
	if request.method == 'GET':
		return render_to_response('dashboard/home.html', {}, context_instance=RequestContext(request))
	else:
		return HttpResponseNotAllowed(['GET'])

def status(request):
	if request.method == 'GET':
		out = _get_stats()
		return HttpResponse(json.dumps(out) + '\n')
	else:
		return HttpResponseNotAllowed(['GET'])
