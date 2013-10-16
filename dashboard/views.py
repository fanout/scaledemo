import json
from django.http import HttpResponse, HttpResponseBadRequest, HttpResponseNotAllowed
from django.template import RequestContext
from django.shortcuts import render_to_response
from django.conf import settings
import gripcontrol
import grip
import redis_ops

db = redis_ops.RedisOps()
pub = grip.Publisher()

if hasattr(settings, 'REDIS_HOST'):
	db.host = settings.REDIS_HOST

if hasattr(settings, 'REDIS_PORT'):
	db.port = settings.REDIS_PORT

if hasattr(settings, 'REDIS_DB'):
	db.db = settings.REDIS_DB

if hasattr(settings, 'GRIP_PROXIES'):
	grip_proxies = settings.GRIP_PROXIES
else:
	grip_proxies = list()

if hasattr(settings, 'DASHBOARD_REDIS_PREFIX'):
	db.prefix = settings.DASHBOARD_REDIS_PREFIX
else:
	db.prefix = ''

if hasattr(settings, 'DASHBOARD_GRIP_PREFIX'):
	grip_prefix = settings.DASHBOARD_GRIP_PREFIX
else:
	grip_prefix = 'dashboard-'

pub.proxies = grip_proxies

def _get_stats():
	data = db.get_stats_data()
	if data is None:
		data = dict()
	out = dict()
	if 'id' in data:
		out['id'] = data['id']
	out['capacity'] = data.get('capacity', 0)
	out['edge_up'] = data.get('edge-up', 0)
	out['edge_total'] = data.get('edge-total', 0)
	out['client_up'] = data.get('client-up', 0)
	out['client_total'] = data.get('client-total', 0)
	out['ping_min'] = data.get('ping-min', 0)
	out['ping_max'] = data.get('ping-max', 0)
	out['ping_avg'] = data.get('ping-avg', 0)
	out['received'] = data.get('received', 0)
	out['receive_min'] = data.get('receive-min', 0)
	out['receive_max'] = data.get('receive-max', 0)
	out['receive_avg'] = data.get('receive-avg', 0)
	out['message'] = data.get('message', '')
	return out

def home(req):
	if req.method == 'GET':
		return render_to_response('dashboard/home.html', {}, context_instance=RequestContext(req))
	else:
		return HttpResponseNotAllowed(['GET'])

def status(req):
	if req.method == 'GET':
		last_id_str = req.GET.get('last_id')
		if last_id_str is not None:
			try:
				int(last_id_str)
			except:
				return HttpResponseBadRequest('Bad Request: last_id wrong type\n')

		try:
			data = _get_stats()
		except:
			return HttpResponse('Service Unavailable\n', status=503)

		if 'id' in data:
			id_str = str(data['id'])
		else:
			id_str = ''

		if last_id_str is None or last_id_str != id_str:
			return HttpResponse(json.dumps(data) + '\n', content_type='application/json')
		else:
			if not grip.is_proxied(req, grip_proxies):
				return HttpResponse('Not Implemented\n', status=501)

			channel = gripcontrol.Channel(grip_prefix + 'status', None)#id_str)
			theaders = dict()
			theaders['Content-Type'] = 'application/json'
			tbody = dict()
			tbody_raw = json.dumps(tbody) + '\n'
			tresponse = gripcontrol.Response(headers=theaders, body=tbody_raw)
			instruct = gripcontrol.create_hold_response(channel, tresponse)
			return HttpResponse(instruct, content_type='application/grip-instruct')
	else:
		return HttpResponseNotAllowed(['GET'])
