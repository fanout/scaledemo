/*
 * Copyright (C) 2013 Fanout, Inc.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "client.h"

#include <QTimer>
#include <QHostAddress>
#include <QHostInfo>
#include <QUrl>
#include <qjson/parser.h>
#include "log.h"
#include "httprequest.h"

class Client::Private : public QObject
{
	Q_OBJECT

public:
	Client *q;
	QString name;
	QUrl baseUri;
	QHostAddress connectAddr;
	HttpRequest *req;
	QTimer *t;
	int tries;
	int retryTime;
	bool started;
	bool errored;
	int curId;
	QString curBody;

	Private(Client *_q) :
		QObject(_q),
		q(_q),
		req(0),
		tries(0),
		started(false),
		errored(false)
	{
		t = new QTimer(this);
		connect(t, SIGNAL(timeout()), SLOT(t_timeout()));
		t->setSingleShot(true);
	}

	~Private()
	{
		t->disconnect(this);
		t->setParent(0);
		t->deleteLater();
	}

	void start()
	{
		doReq();
	}

	void doReq()
	{
		if(connectAddr.isNull())
		{
			QHostInfo info = QHostInfo::fromName(baseUri.host());
			if(info.error() != QHostInfo::NoError)
			{
				req_error();
				return;
			}

			connectAddr = info.addresses().first();
		}

		QUrl uri = baseUri;
		uri.setEncodedPath(uri.encodedPath() + "/value/");
		if(started)
			uri.addQueryItem("last_id", QString::number(curId));
		req = new HttpRequest(this);
		connect(req, SIGNAL(readyRead()), SLOT(req_readyRead()));
		connect(req, SIGNAL(error()), SLOT(req_error()));
		++tries;
		req->start("GET", uri, HttpHeaders(), connectAddr);
		req->endBody();
	}

private slots:
	void req_readyRead()
	{
		if(req->isFinished())
		{
			QByteArray buf = req->readResponseBody();
			delete req;
			req = 0;

			QJson::Parser parser;
			bool ok;
			QVariant vresp = parser.parse(buf, &ok);
			if(!ok)
			{
				req_error();
				return;
			}

			QVariantMap resp = vresp.toMap();
			if(resp.contains("id"))
			{
				curId = resp["id"].toInt();
				curBody = resp["body"].toString();
			}

			errored = false;
			tries = 0;
			int delay = qrand() % 1000;
			log_debug("%s: polling in %dms", qPrintable(name), delay);
			t->start(delay);

			if(!started)
			{
				started = true;
				emit q->started(curId, curBody);
			}
			else
				emit q->received(curId, curBody);
		}
	}

	void req_error()
	{
		if(req)
		{
			delete req;
			req = 0;
		}

		if(tries == 1)
			retryTime = 1;
		else if(tries < 8)
			retryTime *= 2;
		int delay = (retryTime * 1000) + (qrand() % 1000);
		log_debug("%s: trying again in %dms", qPrintable(name), delay);
		t->start(delay);

		if(!errored)
		{
			errored = true;
			emit q->error();
		}
	}

	void t_timeout()
	{
		doReq();
	}
};

Client::Client(const QString &name, QObject *parent) :
	QObject(parent)
{
	d = new Private(this);
	d->name = name;
}

Client::~Client()
{
	delete d;
}

void Client::start(const QUrl &baseUri)
{
	d->baseUri = baseUri;
	d->start();
}

#include "client.moc"
