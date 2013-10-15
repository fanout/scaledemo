from django.conf.urls.defaults import patterns, url

urlpatterns = patterns('dashboard.views',
	url(r'^$', 'home'),
	url(r'^status/$', 'status'),
)
