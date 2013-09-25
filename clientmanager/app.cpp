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
//#include <QHash>
//#include <QUuid>
#include <QSettings>
#include <QHostAddress>
#include <QHostInfo>
//#include "qzmqsocket.h"
//#include "qzmqreqmessage.h"
//#include "qzmqvalve.h"
//#include "tnetstring.h"
#include "processquit.h"
#include "log.h"
#include "httpheaders.h"
#include "httprequest.h"
#include "client.h"
#include "clientthread.h"

#include <qthread.h>
     
    class I : public QThread
    {
    public:
    static void sleep(unsigned long secs) {
    QThread::sleep(secs);
    }
    static void msleep(unsigned long msecs) {
    QThread::msleep(msecs);
    }
    static void usleep(unsigned long usecs) {
    QThread::usleep(usecs);
    }
    };

/*static void cleanStringList(QStringList *in)
{
	for(int n = 0; n < in->count(); ++n)
	{
		if(in->at(n).isEmpty())
		{
			in->removeAt(n);
			--n; // adjust position
		}
	}
}*/

class App::Private : public QObject
{
	Q_OBJECT

public:
	App *q;
	/*QZmq::Socket *in_sock;
	QZmq::Socket *in_stream_sock;
	QZmq::Socket *out_sock;
	QZmq::Socket *in_req_sock;
	QZmq::Valve *in_valve;
	QZmq::Valve *in_req_valve;
	QSet<Worker*> workers;
	QHash<QByteArray, Worker*> streamWorkersByRid;
	QHash<Worker*, QList<QByteArray> > reqHeadersByWorker;*/
	QSet<Client*> clients;
	QHash<ClientThread*, ClientThread::Stats> threadStats;
	int total;
	int started;
	int received;
	int errored;
	int curId;
	QTimer *logTimer;
	bool needLog;

	Private(App *_q) :
		QObject(_q),
		q(_q),
		needLog(false)
		/*in_sock(0),
		in_stream_sock(0),
		out_sock(0),
		in_req_sock(0),
		in_valve(0),
		in_req_valve(0)*/
	{
		connect(ProcessQuit::instance(), SIGNAL(quit()), SLOT(doQuit()));
		connect(ProcessQuit::instance(), SIGNAL(hup()), SLOT(reload()));

		logTimer = new QTimer(this);
		connect(logTimer, SIGNAL(timeout()), SLOT(logTimer_timeout()));
		logTimer->setSingleShot(true);
	}

	~Private()
	{
		logTimer->disconnect(this);
		logTimer->setParent(0);
		logTimer->deleteLater();
		logTimer = 0;
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

		/*config.clientId = settings.value("instance_id").toString().toUtf8();
		if(config.clientId.isEmpty())
			config.clientId = QUuid::createUuid().toString().toLatin1();

		QString in_url = settings.value("in_spec").toString();
		QString in_stream_url = settings.value("in_stream_spec").toString();
		QString out_url = settings.value("out_spec").toString();
		QString in_req_url = settings.value("in_req_spec").toString();
		config.maxWorkers = settings.value("max_open_requests", -1).toInt();
		config.sessionBufferSize = settings.value("buffer_size", 200000).toInt();
		config.sessionTimeout = settings.value("timeout", 600).toInt();
		int inHwm = settings.value("in_hwm", 1000).toInt();
		int outHwm = settings.value("out_hwm", 1000).toInt();

		if((!in_url.isEmpty() || !in_stream_url.isEmpty() || !out_url.isEmpty()) && (in_url.isEmpty() || in_stream_url.isEmpty() || out_url.isEmpty()))
		{
			log_error("if any of in_spec, in_stream_spec, or out_spec are set then all of them must be set");
			emit q->quit();
			return;
		}

		if(in_url.isEmpty() && in_req_url.isEmpty())
		{
			log_error("must set at least in_spec+in_stream_spec+out_spec or in_req_spec");
			emit q->quit();
			return;
		}

		config.defaultPolicy = settings.value("defpolicy").toString();
		if(config.defaultPolicy != "allow" && config.defaultPolicy != "deny")
		{
			log_error("must set defpolicy to either \"allow\" or \"deny\"");
			emit q->quit();
			return;
		}

		config.allowExps = settings.value("allow").toStringList();
		config.denyExps = settings.value("deny").toStringList();

		cleanStringList(&config.allowExps);
		cleanStringList(&config.denyExps);

		dns = new JDnsShared(JDnsShared::UnicastInternet, this);
		dns->addInterface(QHostAddress::Any);
		dns->addInterface(QHostAddress::AnyIPv6);

		if(!in_url.isEmpty())
		{
			in_sock = new QZmq::Socket(QZmq::Socket::Pull, this);

			in_sock->setHwm(inHwm);

			if(!in_sock->bind(in_url))
			{
				log_error("unable to bind to in_spec: %s", qPrintable(in_url));
				emit q->quit();
				return;
			}

			in_valve = new QZmq::Valve(in_sock, this);
			connect(in_valve, SIGNAL(readyRead(const QList<QByteArray> &)), SLOT(in_readyRead(const QList<QByteArray> &)));
		}

		if(!in_stream_url.isEmpty())
		{
			in_stream_sock = new QZmq::Socket(QZmq::Socket::Dealer, this);

			in_stream_sock->setIdentity(config.clientId);
			in_stream_sock->setHwm(inHwm);

			connect(in_stream_sock, SIGNAL(readyRead()), SLOT(in_stream_readyRead()));
			if(!in_stream_sock->bind(in_stream_url))
			{
				log_error("unable to bind to in_stream_spec: %s", qPrintable(in_stream_url));
				emit q->quit();
				return;
			}
		}

		if(!out_url.isEmpty())
		{
			out_sock = new QZmq::Socket(QZmq::Socket::Pub, this);

			out_sock->setWriteQueueEnabled(false);
			out_sock->setHwm(outHwm);

			if(!out_sock->bind(out_url))
			{
				log_error("unable to bind to out_spec: %s", qPrintable(out_url));
				emit q->quit();
				return;
			}
		}

		if(!in_req_url.isEmpty())
		{
			in_req_sock = new QZmq::Socket(QZmq::Socket::Router, this);

			in_req_sock->setHwm(inHwm);

			if(!in_req_sock->bind(in_req_url))
			{
				log_error("unable to bind to in_req_spec: %s", qPrintable(in_req_url));
				emit q->quit();
				return;
			}

			in_req_valve = new QZmq::Valve(in_req_sock, this);
			connect(in_req_valve, SIGNAL(readyRead(const QList<QByteArray> &)), SLOT(in_req_readyRead(const QList<QByteArray> &)));
		}

		if(in_valve)
			in_valve->open();
		if(in_req_valve)
			in_req_valve->open();*/

		log_info("started");

		qsrand(time(NULL));

		started = 0;
		received = 0;
		curId = -1;

		int threadCount = QThread::idealThreadCount();
		int clientCount = args[0].toInt() / threadCount;
		for(int n = 0; n < threadCount; ++n)
		{
			int extra = 0;
			if(n < (args[0].toInt() % threadCount))
				++extra;
			log_info("creating thread with client count=%d", clientCount + extra);
			ClientThread *c = new ClientThread(this);
			connect(c, SIGNAL(statsChanged(const ClientThread::Stats &)), SLOT(clientThread_statsChanged(const ClientThread::Stats &)));
			c->start();
			c->setupClients(QUrl("http://headline"), clientCount + extra);
		}
	}

	// normally responses are handled by Workers, but in some routing
	//   cases we need to be able to respond with an error at this layer
	/*void respondCancel(const QByteArray &receiver, const QByteArray &rid)
	{
		ZhttpResponsePacket out;
		out.id = rid;
		out.type = ZhttpResponsePacket::Cancel;
		QByteArray part = QByteArray("T") + TnetString::fromVariant(out.toVariant());
		out_sock->write(QList<QByteArray>() << (receiver + ' ' + part));
	}*/

	void tryLog()
	{
		if(!logTimer->isActive())
		{
			doLog();
			logTimer->start(100);
		}
		else
			needLog = true;
	}

	void doLog()
	{
		log_info("stats T=%d/S=%d/R=%d/E=%d cur-id=%d", total, started, received, errored, curId);
	}

private slots:
	void clientThread_statsChanged(const ClientThread::Stats &stats)
	{
		ClientThread *c = (ClientThread *)sender();
		threadStats[c] = stats;
		curId = stats.id;

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

		tryLog();
	}

	void client_started(int id, const QString &body)
	{
		Q_UNUSED(body);

		++started;
		assert(started <= clients.count());
		if(curId == -1)
			curId = id;

		tryLog();
	}

	void client_received(int id, const QString &body)
	{
		Q_UNUSED(body);

		if(curId == -1 || id > curId)
		{
			curId = id;
			received = 1;
		}
		else
		{
			assert(id == curId);
			++received;
		}

		tryLog();
	}

	void client_error()
	{
		log_info("client error");
	}

	void logTimer_timeout()
	{
		if(needLog)
		{
			needLog = false;
			doLog();
		}
	}

	/*void in_readyRead(const QList<QByteArray> &message)
	{
		if(message.count() != 1)
		{
			log_warning("received message with parts != 1, skipping");
			return;
		}

		if(message[0].length() < 1 || message[0][0] != 'T')
		{
			log_warning("received message with invalid format (wrong type), skipping");
			return;
		}

		bool ok;
		QVariant data = TnetString::toVariant(message[0].mid(1), 0, &ok);
		if(!ok)
		{
			log_warning("received message with invalid format (tnetstring parse failed), skipping");
			return;
		}

		log_debug("recv: %s", qPrintable(TnetString::variantToString(data, -1)));

		QVariantHash vhash = data.toHash();
		QByteArray rid = vhash.value("id").toByteArray();
		if(rid.isEmpty())
		{
			log_warning("received stream message without request id, skipping");
			return;
		}

		if(streamWorkersByRid.contains(rid))
		{
			log_warning("received request for id already in use, skipping");
			return;
		}

		Worker *w = new Worker(dns, &config, this);
		connect(w, SIGNAL(readyRead(const QByteArray &, const QVariant &)), SLOT(worker_readyRead(const QByteArray &, const QVariant &)));
		connect(w, SIGNAL(finished()), SLOT(worker_finished()));

		workers += w;
		streamWorkersByRid[rid] = w;

		if(config.maxWorkers != -1 && workers.count() >= config.maxWorkers)
		{
			in_valve->close();

			// also close in_req_valve, if we have it
			if(in_req_valve)
				in_req_valve->close();
		}

		w->start(data, Worker::Stream);
	}

	void in_stream_readyRead()
	{
		// message from DEALER socket will have two parts, with first part empty
		QList<QByteArray> message = in_stream_sock->read();
		if(message.count() != 2)
		{
			log_warning("received message with parts != 2, skipping");
			return;
		}

		if(!message[0].isEmpty())
		{
			log_warning("received message with non-empty first part, skipping");
			return;
		}

		if(message[1].length() < 1 || message[1][0] != 'T')
		{
			log_warning("received message with invalid format (wrong type), skipping");
			return;
		}

		bool ok;
		QVariant data = TnetString::toVariant(message[1].mid(1), 0, &ok);
		if(!ok)
		{
			log_warning("received message with invalid format (tnetstring parse failed), skipping");
			return;
		}

		log_debug("recv-stream: %s", qPrintable(TnetString::variantToString(data, -1)));

		QVariantHash vhash = data.toHash();
		QByteArray rid = vhash.value("id").toByteArray();
		if(rid.isEmpty())
		{
			log_warning("received stream message without request id, skipping");
			return;
		}

		Worker *w = streamWorkersByRid.value(rid);
		if(!w)
		{
			QByteArray from = vhash.value("from").toByteArray();
			QByteArray type = vhash.value("type").toByteArray();
			if(!from.isEmpty() && type != "error" && type != "cancel")
				respondCancel(from, rid);

			return;
		}

		w->write(data);
	}

	void in_req_readyRead(const QList<QByteArray> &message)
	{
		QZmq::ReqMessage reqMessage(message);
		if(reqMessage.content().count() != 1)
		{
			log_warning("received message with parts != 1, skipping");
			return;
		}

		QByteArray msg = reqMessage.content()[0];
		if(msg.length() < 1 || msg[0] != 'T')
		{
			log_warning("received message with invalid format (wrong type), skipping");
			return;
		}

		bool ok;
		QVariant data = TnetString::toVariant(msg.mid(1), 0, &ok);
		if(!ok)
		{
			log_warning("received message with invalid format (tnetstring parse failed), skipping");
			return;
		}

		log_debug("recv-req: %s", qPrintable(TnetString::variantToString(data, -1)));

		Worker *w = new Worker(dns, &config, this);
		connect(w, SIGNAL(readyRead(const QByteArray &, const QVariant &)), SLOT(worker_readyRead(const QByteArray &, const QVariant &)));
		connect(w, SIGNAL(finished()), SLOT(worker_finished()));

		workers += w;
		reqHeadersByWorker[w] = reqMessage.headers();

		if(config.maxWorkers != -1 && workers.count() >= config.maxWorkers)
		{
			in_req_valve->close();

			// also close in_valve, if we have it
			if(in_valve)
				in_valve->close();
		}

		w->start(data, Worker::Single);
	}

	void worker_readyRead(const QByteArray &receiver, const QVariant &response)
	{
		Worker *w = (Worker *)sender();

		QByteArray part = QByteArray("T") + TnetString::fromVariant(response);
		if(!receiver.isEmpty())
		{
			log_debug("send: %s", qPrintable(TnetString::variantToString(response, -1)));

			out_sock->write(QList<QByteArray>() << (receiver + ' ' + part));
		}
		else
		{
			log_debug("send-req: %s", qPrintable(TnetString::variantToString(response, -1)));

			assert(reqHeadersByWorker.contains(w));
			QList<QByteArray> reqHeaders = reqHeadersByWorker.value(w);
			in_req_sock->write(QZmq::ReqMessage(reqHeaders, QList<QByteArray>() << part).toRawMessage());
		}
	}

	void worker_finished()
	{
		Worker *w = (Worker *)sender();

		streamWorkersByRid.remove(w->rid());
		reqHeadersByWorker.remove(w);
		workers.remove(w);

		delete w;

		// ensure the valves are open
		if(in_valve)
			in_valve->open();
		if(in_req_valve)
			in_req_valve->open();
	}*/

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
