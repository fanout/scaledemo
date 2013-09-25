/*
 * Copyright (C) 2012-2013 Fanout, Inc.
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

#include "httprequest.h"

#include <assert.h>
#include <QTimer>
#include <QHostAddress>
#include <QUrl>
#include <QTcpSocket>
#include "httpheaders.h"
#include "bufferlist.h"

class HttpRequest::Private : public QObject
{
	Q_OBJECT

public:
	enum State
	{
		Stopped,
		Starting,
		Connecting,
		Requesting,
		ReceivingStatusLine,
		ReceivingHeaderLine,
		ReceivingBody
	};

	HttpRequest *q;
	State state;
	QTcpSocket *sock;
	QHostAddress connectAddr;
	QString method;
	QUrl uri;
	HttpHeaders requestHeaders;
	BufferList requestBody;
	bool requestBodyFinished;
	int responseCode;
	QByteArray responseReason;
	HttpHeaders responseHeaders;
	BufferList responseBody;
	bool pendingUpdate;
	ErrorCondition errorCondition;
	QByteArray in;
	int contentLength;
	int responseBodySize;

	Private(HttpRequest *_q) :
		QObject(_q),
		q(_q),
		state(Stopped),
		sock(0),
		requestBodyFinished(false),
		responseCode(-1),
		pendingUpdate(false),
		errorCondition(ErrorGeneric),
		contentLength(-1),
		responseBodySize(0)
	{
	}

	~Private()
	{
		cleanup();
	}

	void cleanup()
	{
		if(sock)
		{
			sock->disconnect(this);
			if(sock->isOpen())
				sock->disconnectFromHost();
			sock->setParent(0);
			sock->deleteLater();
			sock = 0;
		}
	}

	void update()
	{
		if(!pendingUpdate)
		{
			pendingUpdate = true;
			QMetaObject::invokeMethod(this, "doUpdate", Qt::QueuedConnection);
		}
	}

private slots:
	void doUpdate()
	{
		pendingUpdate = false;

		if(state == Starting)
		{
			state = Connecting;
			sock = new QTcpSocket(this);
			connect(sock, SIGNAL(connected()), SLOT(sock_connected()));
			connect(sock, SIGNAL(disconnected()), SLOT(sock_disconnected()));
			connect(sock, SIGNAL(readyRead()), SLOT(sock_readyRead()));
			connect(sock, SIGNAL(bytesWritten(qint64)), SLOT(sock_bytesWritten(qint64)));
			sock->connectToHost(connectAddr, uri.port(80));
		}
		else if(state == Requesting)
		{
			if(!requestBody.isEmpty())
				sock->write(requestBody.take());

			if(requestBodyFinished)
				state = ReceivingStatusLine;
		}
	}

	void sock_connected()
	{
		assert(state == Connecting);

		if(!requestHeaders.contains("Host"))
			requestHeaders += HttpHeader("Host", uri.host().toUtf8());

		requestHeaders.removeAll("Connection");
		requestHeaders += HttpHeader("Connection", "close");

		QByteArray res = uri.encodedPath();
		QByteArray query = uri.encodedQuery();
		if(!query.isNull())
			res += '?' + query;

		QByteArray buf = method.toUtf8() + ' ' + res + " HTTP/1.0\r\n";
		foreach(const HttpHeader &h, requestHeaders)
			buf += h.first + ": " + h.second + "\r\n";
		buf += "\r\n";

		if(!requestBody.isEmpty())
			buf += requestBody.take();

		sock->write(buf);

		if(requestBodyFinished)
			state = ReceivingStatusLine;
		else
			state = Requesting;
	}

	void sock_disconnected()
	{
		if(contentLength == -1)
		{
			cleanup();
			state = Stopped;
			emit q->readyRead();
		}
		else
		{
			cleanup();
			state = Stopped;
			emit q->error();
		}
	}

	void sock_readyRead()
	{
		QByteArray buf = sock->readAll();

		if(state == ReceivingStatusLine || state == ReceivingHeaderLine)
		{
			in += buf;
			bool needEmit = false;
			while(true)
			{
				int at = in.indexOf("\r\n");
				if(at == -1)
					break;

				QByteArray line = in.mid(0, at);
				in = in.mid(at + 2);

				if(state == ReceivingStatusLine)
				{
					int start = line.indexOf(' ') + 1;
					at = line.indexOf(' ', start);
					responseCode = line.mid(start, at - start).toInt();
					responseReason = line.mid(at + 1);
					state = ReceivingHeaderLine;
				}
				else if(state == ReceivingHeaderLine)
				{
					if(!line.isEmpty())
					{
						at = line.indexOf(": ");
						HttpHeader h(line.mid(0, at), line.mid(at + 2));
						if(qstricmp(h.first.data(), "Content-Length") == 0)
							contentLength = h.second.toInt();
						responseHeaders += h;
					}
					else
					{
						if(contentLength == -1 || contentLength > 0)
						{
							responseBody += in;
							responseBodySize += in.size();
							in.clear();

							if(contentLength != -1 && responseBodySize == contentLength)
							{
								cleanup();
								state = Stopped;
							}
							else
								state = ReceivingBody;
						}
						else
						{
							cleanup();
							state = Stopped;
						}

						needEmit = true;
					}
				}
			}

			if(needEmit)
				emit q->readyRead();
		}
		else if(state == ReceivingBody)
		{
			responseBody += buf;
			responseBodySize += buf.size();

			if(contentLength != -1 && responseBodySize == contentLength)
			{
				cleanup();
				state = Stopped;
			}

			emit q->readyRead();
		}
		else
		{
			assert(0);
		}
	}

	void sock_bytesWritten(qint64 bytes)
	{
		Q_UNUSED(bytes);

		// TODO
	}
};

HttpRequest::HttpRequest(QObject *parent) :
	QObject(parent)
{
	d = new Private(this);
}

HttpRequest::~HttpRequest()
{
	delete d;
}

void HttpRequest::start(const QString &method, const QUrl &uri, const HttpHeaders &headers, const QHostAddress &connectAddr)
{
	assert(d->state == Private::Stopped);
	d->state = Private::Starting;
	d->method = method;
	d->uri = uri;
	d->requestHeaders = headers;
	d->connectAddr = connectAddr;
	d->update();
}

void HttpRequest::writeBody(const QByteArray &body)
{
	d->requestBody += body;
	d->update();
}

void HttpRequest::endBody()
{
	d->requestBodyFinished = true;
	d->update();
}

int HttpRequest::bytesAvailable() const
{
	return d->responseBody.size();
}

bool HttpRequest::isFinished() const
{
	return d->state == Private::Stopped;
}

HttpRequest::ErrorCondition HttpRequest::errorCondition() const
{
	return d->errorCondition;
}

int HttpRequest::responseCode() const
{
	return d->responseCode;
}

QByteArray HttpRequest::responseReason() const
{
	return d->responseReason;
}

HttpHeaders HttpRequest::responseHeaders() const
{
	return d->responseHeaders;
}

QByteArray HttpRequest::readResponseBody(int size)
{
	return d->responseBody.take(size);
}

#include "httprequest.moc"
