import json
from django.conf import settings
from django.http import HttpResponse, HttpResponseBadRequest, HttpResponseNotFound, HttpResponseNotAllowed
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

if hasattr(settings, 'HEADLINE_REDIS_PREFIX'):
	db.prefix = settings.HEADLINE_REDIS_PREFIX
else:
	db.prefix = 'headline-'

if hasattr(settings, 'HEADLINE_GRIP_PREFIX'):
	grip_prefix = settings.HEADLINE_GRIP_PREFIX
else:
	grip_prefix = 'headline-'

pub.proxies = grip_proxies

def value(req):
	if req.method == 'GET':
		last_id = req.GET.get('last_id')
		if last_id is not None:
			try:
				last_id = int(last_id)
			except:
				return HttpResponseBadRequest('Bad Request: last_id wrong type\n')

		try:
			id, body = db.headline_get()
		except redis_ops.ObjectDoesNotExist:
			return HttpResponseNotFound('Not Found\n')
		except:
			return HttpResponse('Service Unavailable\n', status=503)

		if last_id is None or last_id != id:
			out = dict()
			out['id'] = id
			out['body'] = body
			return HttpResponse(json.dumps(out) + '\n', content_type='application/json')
		else:
			# don't check for grip sig since we're using the dynamic zmq spec list
			#if not grip.is_proxied(req, grip_proxies):
			#	return HttpResponse('Not Implemented\n', status=501)

			channel = gripcontrol.Channel(grip_prefix + 'value', id)
			theaders = dict()
			theaders['Content-Type'] = 'application/json'
			tbody = dict()
			tbody_raw = json.dumps(tbody) + '\n'
			tresponse = gripcontrol.Response(headers=theaders, body=tbody_raw)
			instruct = gripcontrol.create_hold_response(channel, tresponse)
			return HttpResponse(instruct, content_type='application/grip-instruct')
	elif req.method == 'POST':
		body = req.POST.get('body')
		if body is None:
			return HttpResponseBadRequest('Bad Request: Invalid body\n')

		try:
			id, prev_id = db.headline_set(body)
		except:
			return HttpResponse('Service Unavailable\n', status=503)

		out = dict()
		out['id'] = id
		out['body'] = body

		hr_headers = dict()
		hr_headers['Content-Type'] = 'application/json'
		hr_body = json.dumps(out) + '\n'
		pub.set_zmq_specs(db.proxies_get_all())
		pub.publish(grip_prefix + 'value', str(id), str(prev_id), hr_headers, hr_body)

		return HttpResponse(json.dumps(out) + '\n', content_type='application/json')
	else:
		return HttpResponseNotAllowed(['GET', 'POST'])
