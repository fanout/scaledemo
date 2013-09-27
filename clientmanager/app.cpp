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

#include "app.h"

#include <stdio.h>
#include <assert.h>
#include <QHash>
#include <QSettings>
#include <QHostAddress>
#include <QHostInfo>
#include <QThread>
#include <QDateTime>
#include "qzmqsocket.h"
#include "qzmqreprouter.h"
#include "qzmqreqmessage.h"
#include "tnetstring.h"
#include "processquit.h"
#include "log.h"
#include "httpheaders.h"
#include "httprequest.h"
#include "client.h"
#include "clientthread.h"

class App::Private : public QObject
{
	Q_OBJECT

public:
	class ReceivedTime
	{
	public:
		QDateTime dt;
		int count;
		int latency;

		ReceivedTime(const QDateTime &_dt, int _count, int _latency) :
			dt(_dt),
			count(_count),
			latency(_latency)
		{
		}
	};

	App *q;
	QZmq::RepRouter *rpc_in_sock;
	QZmq::Socket *stats_out_sock;
	QByteArray instanceId;
	QList<ClientThread*> threads;
	QHash<ClientThread*, ClientThread::Stats> threadStats;
	QUrl baseUri;
	int clientCount;
	int total;
	int started;
	int received;
	int errored;
	int curId;
	QString curBody;
	int latency;
	QTimer *statsTimer;
	bool statsPending;
	QList<ReceivedTime> receivedTimes;

	Private(App *_q) :
		QObject(_q),
		q(_q),
		rpc_in_sock(0),
		stats_out_sock(0),
		clientCount(0),
		statsPending(false)
	{
		connect(ProcessQuit::instance(), SIGNAL(quit()), SLOT(doQuit()));
		connect(ProcessQuit::instance(), SIGNAL(hup()), SLOT(reload()));

		statsTimer = new QTimer(this);
		connect(statsTimer, SIGNAL(timeout()), SLOT(statsTimer_timeout()));
		statsTimer->setSingleShot(true);
	}

	~Private()
	{
		qDeleteAll(threads);

		statsTimer->disconnect(this);
		statsTimer->setParent(0);
		statsTimer->deleteLater();
		statsTimer = 0;
	}

	void start()
	{
		QStringList args = QCoreApplication::instance()->arguments();
		args.removeFirst();

		// options
		QHash<QString, QString> options;
		for(int n = 0; n < args.count(); ++n)
		{
			if(args[n] == "--")
			{
				break;
			}
			else if(args[n].startsWith("--"))
			{
				QString opt = args[n].mid(2);
				QString var, val;

				int at = opt.indexOf("=");
				if(at != -1)
				{
					var = opt.mid(0, at);
					val = opt.mid(at + 1);
				}
				else
					var = opt;

				options[var] = val;

				args.removeAt(n);
				--n; // adjust position
			}
		}

		if(options.contains("verbose"))
			log_setOutputLevel(LOG_LEVEL_DEBUG);
		else
			log_setOutputLevel(LOG_LEVEL_INFO);

		QString logFile = options.value("logfile");
		if(!logFile.isEmpty())
		{
			if(!log_setFile(logFile))
			{
				log_error("failed to open log file: %s", qPrintable(logFile));
				emit q->quit();
				return;
			}
		}

		log_info("starting...");

		QString configFile = options.value("config");
		if(configFile.isEmpty())
			configFile = "/etc/clientmanager.conf";

		// QSettings doesn't inform us if the config file doesn't exist, so do that ourselves
		{
			QFile file(configFile);
			if(!file.open(QIODevice::ReadOnly))
			{
				log_error("failed to open %s, and --config not passed", qPrintable(configFile));
				emit q->quit();
				return;
			}
		}

		QSettings settings(configFile, QSettings::IniFormat);

		instanceId = QUuid::createUuid().toString().toLatin1();

		QString rpc_in_spec = settings.value("clientmanager/rpc_in_spec").toString();
		QString stats_out_spec = settings.value("clientmanager/stats_out_spec").toString();
		int threadCount = settings.value("clientmanager/threads", QThread::idealThreadCount()).toInt();

		if(rpc_in_spec.isEmpty() || stats_out_spec.isEmpty())
		{
			log_error("must set rpc_in_spec and stats_out_spec");
			emit q->quit();
			return;
		}

		rpc_in_sock = new QZmq::RepRouter(this);
		connect(rpc_in_sock, SIGNAL(readyRead()), SLOT(rpc_in_readyRead()));
		if(!rpc_in_sock->bind(rpc_in_spec))
		{
			log_error("unable to bind to rpc_in_spec: %s", qPrintable(rpc_in_spec));
			emit q->quit();
			return;
		}

		stats_out_sock = new QZmq::Socket(QZmq::Socket::Pub, this);
		stats_out_sock->setShutdownWaitTime(0);
		if(!stats_out_sock->bind(stats_out_spec))
		{
			log_error("unable to bind to stats_out_spec: %s", qPrintable(stats_out_spec));
			emit q->quit();
			return;
		}

		qsrand(time(NULL));

		total = 0;
		started = 0;
		received = 0;
		errored = 0;
		curId = -1;
		latency = -1;

		for(int n = 0; n < threadCount; ++n)
		{
			ClientThread *c = new ClientThread(this);
			threads += c;
			connect(c, SIGNAL(statsChanged(const ClientThread::Stats &)), SLOT(clientThread_statsChanged(const ClientThread::Stats &)));
			c->start();
		}

		log_info("started");
	}

	void setupClients(const QUrl &_baseUri, int count)
	{
		if(baseUri == _baseUri && count == clientCount)
			return; // no change

		baseUri = _baseUri;
		clientCount = count;

		log_info("setupClients: %s %d", baseUri.toEncoded().data(), count);

		for(int n = 0; n < threads.count(); ++n)
		{
			ClientThread *c = threads[n];
			int threadClientCount = clientCount / threads.count();
			if(n < (count % threads.count()))
				++threadClientCount;

			log_info("setting up thread %d with %d clients", n, threadClientCount);
			c->setupClients(baseUri, threadClientCount);
		}
	}

	void tryStats()
	{
		if(!statsTimer->isActive())
		{
			statsPending = false;

			if(!receivedTimes.isEmpty())
			{
				QDateTime now = QDateTime::currentDateTime();
				int receivedCount = 0;
				int latencyTotal = 0;
				foreach(const ReceivedTime &rt, receivedTimes)
				{
					receivedCount += rt.count;
					latencyTotal += ((int)(now.toMSecsSinceEpoch() - rt.dt.toMSecsSinceEpoch()) + rt.latency) * rt.count;
				}
				latency = latencyTotal / receivedCount;
				receivedTimes.clear();
			}
			else
				latency = -1;

			statsTimer->start(100);

			emitStats();
		}
		else
			statsPending = true;
	}

	void emitStats()
	{
		log_info("stats T=%d/S=%d/R=%d/E=%d/L=%d cur-id=%d", total, started, received, errored, latency, curId);

		QVariantHash m;
		m["id"] = instanceId;
		m["total"] = total;
		m["started"] = started;
		m["received"] = received;
		m["errored"] = errored;
		if(curId != -1)
		{
			m["cur-id"] = curId;
			m["cur-body"] = curBody.toUtf8();
		}

		if(latency != -1)
			m["latency"] = latency;

		QByteArray buf = "stats " + TnetString::fromVariant(m);
		stats_out_sock->write(QList<QByteArray>() << buf);
	}

private slots:
	void clientThread_statsChanged(const ClientThread::Stats &stats)
	{
		ClientThread *c = (ClientThread *)sender();
		threadStats[c] = stats;
		if(curId != stats.id)
		{
			curId = stats.id;
			curBody = stats.body;
		}

		int hadReceived = received;
		bool hadReceivedAll = (total > 0 && received == total);

		// compile
		total = 0;
		started = 0;
		received = 0;
		errored = 0;
		QHashIterator<ClientThread*, ClientThread::Stats> it(threadStats);
		while(it.hasNext())
		{
			it.next();
			const ClientThread::Stats &s = it.value();
			total += s.total;
			started += s.started;
			if(s.id == curId)
				received += s.received;
			errored += s.errored;
		}

		QDateTime now = QDateTime::currentDateTime();
		if(received > hadReceived)
		{
			receivedTimes += ReceivedTime(now, received - hadReceived, stats.latency);
		}
		else if(received < hadReceived) // could happen if restarted
		{
			receivedTimes.clear();
			receivedTimes += ReceivedTime(now, received, stats.latency);
		}

		// expedite?
		if(!hadReceivedAll && received == total && statsTimer->isActive())
		{
			statsTimer->stop();
			statsPending = false;
		}

		tryStats();
	}

	void statsTimer_timeout()
	{
		if(statsPending)
			tryStats();
	}

	void rpc_in_readyRead()
	{
		QZmq::ReqMessage reqMessage = rpc_in_sock->read();
		if(reqMessage.content().count() != 1)
		{
			log_warning("received message with parts != 1, skipping");
			return;
		}

		bool ok;
		QVariant data = TnetString::toVariant(reqMessage.content().first(), 0, &ok);
		if(!ok)
		{
			log_warning("received message with invalid format (tnetstring parse failed), skipping");
			return;
		}

		if(data.type() != QVariant::Hash)
		{
			log_warning("received message with invalid format (wrong type), skipping");
			return;
		}

		QVariantHash m = data.toHash();

		QByteArray id = m["id"].toByteArray();
		QByteArray method = m["method"].toByteArray();
		QVariantHash args = m["args"].toHash();

		if(method == "ping")
		{
			QVariantHash resp;
			resp["id"] = id;
			resp["success"] = true;
			QVariantHash ret;
			ret["id"] = instanceId;
			resp["value"] = ret;
			QByteArray buf = TnetString::fromVariant(resp);
			reqMessage = reqMessage.createReply(QList<QByteArray>() << buf);
			rpc_in_sock->write(reqMessage);
		}
		else if(method == "setup-clients")
		{
			setupClients(QUrl::fromEncoded(args["base-uri"].toByteArray(), QUrl::StrictMode), args["count"].toInt());

			QVariantHash resp;
			resp["id"] = id;
			resp["success"] = true;
			resp["value"] = QVariant();
			QByteArray buf = TnetString::fromVariant(resp);
			reqMessage = reqMessage.createReply(QList<QByteArray>() << buf);
			rpc_in_sock->write(reqMessage);
		}
		else
		{
			QVariantHash resp;
			resp["id"] = id;
			resp["success"] = false;
			resp["condition"] = "method-not-found";
			QByteArray buf = TnetString::fromVariant(resp);
			reqMessage = reqMessage.createReply(QList<QByteArray>() << buf);
			rpc_in_sock->write(reqMessage);
		}
	}

	void reload()
	{
		log_info("reloading");
		log_rotate();
	}

	void doQuit()
	{
		log_info("stopping...");

		// remove the handler, so if we get another signal then we crash out
		ProcessQuit::cleanup();

		log_info("stopped");
		emit q->quit();
	}
};

App::App(QObject *parent) :
	QObject(parent)
{
	d = new Private(this);
}

App::~App()
{
	delete d;
}

void App::start()
{
	d->start();
}

#include "app.moc"
