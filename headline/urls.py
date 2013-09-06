from django.conf.urls.defaults import patterns, url

urlpatterns = patterns('headline.views',
	url(r'^value/$', 'value'),
)
