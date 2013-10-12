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

#include "clientthread.h"

#include <assert.h>
#include <QSet>
#include <QTimer>
#include <QUrl>
#include <QMetaType>
#include <QDateTime>
#include "client.h"

class ClientThread::Worker : public QObject
{
	Q_OBJECT

public:
	QUrl baseUri;
	QList<Client*> clients;
	QSet<Client*> clientsErrored;
	Stats stats;
	QTimer *statsTimer;
	bool statsPending;
	QList<QDateTime> receivedTimes;

	Worker(QObject *parent = 0) :
		QObject(parent),
		statsPending(false)
	{
		statsTimer = new QTimer(this);
		connect(statsTimer, SIGNAL(timeout()), SLOT(statsTimer_timeout()));
		statsTimer->setSingleShot(true);
	}

	~Worker()
	{
		qDeleteAll(clients);

		statsTimer->disconnect(this);
		statsTimer->setParent(0);
		statsTimer->deleteLater();
		statsTimer = 0;
	}

public slots:
	void setupClients(const QUrl &_baseUri, int count, const QString &connectHost)
	{
		bool baseUriChanged = false;
		if(baseUri != _baseUri)
		{
			baseUriChanged = true;
			baseUri = _baseUri;
		}

		// no change?
		if(!baseUriChanged && clients.count() == count)
			return;

		if(baseUriChanged)
		{
			// start over
			qDeleteAll(clients);
			clients.clear();
			clientsErrored.clear();

			stats.started = 0;
			stats.received = 0;
			stats.errored = 0;
		}
		else
		{
			// shrink
			while(clients.count() > count)
			{
				Client *c = clients.takeLast();

				if(c->isStarted())
					--stats.started;

				if(clientsErrored.contains(c))
				{
					clientsErrored.remove(c);
					--stats.errored;
				}

				if(c->receivedId() == stats.id)
					--stats.received;

				delete c;
			}
		}

		// grow
		int index = clients.count();
		for(int n = 0; clients.count() < count; ++n)
		{
			Client *c = new Client(QString::number(index++), this);
			clients += c;
			connect(c, SIGNAL(started(int, const QString &)), SLOT(client_started(int, const QString &)));
			connect(c, SIGNAL(received(int, const QString &)), SLOT(client_received(int, const QString &)));
			c->start(baseUri, connectHost, n / 10);
		}

		tryStats();
	}

public:
	void tryStats()
	{
		if(!statsTimer->isActive())
		{
			statsPending = false;

			stats.total = clients.count();
			stats.errored = clientsErrored.count();

			if(!receivedTimes.isEmpty())
			{
				QDateTime now = QDateTime::currentDateTime();
				int latencyTotal = 0;
				foreach(const QDateTime &dt, receivedTimes)
					latencyTotal += (int)(now.toMSecsSinceEpoch() - dt.toMSecsSinceEpoch());
				stats.latency = latencyTotal / receivedTimes.count();
				receivedTimes.clear();
			}
			else
				stats.latency = -1;

			statsTimer->start(20);

			emit statsChanged(stats);
		}
		else
			statsPending = true;
	}

signals:
	void statsChanged(const ClientThread::Stats &);

private slots:
	void client_started(int id, const QString &body)
	{
		Q_UNUSED(body);

		Client *c = (Client *)sender();

		clientsErrored.remove(c);

		++stats.started;
		assert(stats.started <= clients.count());
		if(stats.id == -1)
		{
			stats.id = id;
			stats.body = body;
		}

		tryStats();
	}

	void client_received(int id, const QString &body)
	{
		Q_UNUSED(body);

		Client *c = (Client *)sender();

		clientsErrored.remove(c);

		if(stats.id == -1 || id > stats.id)
		{
			stats.id = id;
			stats.body = body;
			stats.received = 1;
		}
		else
		{
			assert(id == stats.id);
			++stats.received;
		}

		receivedTimes += QDateTime::currentDateTime();

		// expedite stats once the last client has received
		if(clients.count() == stats.received && statsTimer->isActive())
		{
			statsTimer->stop();
			statsPending = false;
		}

		tryStats();
	}

	void client_error()
	{
		Client *c = (Client *)sender();

		clientsErrored += c;

		tryStats();
	}

	void statsTimer_timeout()
	{
		if(statsPending)
			tryStats();
	}
};

ClientThread::ClientThread(QObject *parent) :
	QThread(parent)
{
	qRegisterMetaType<Stats>("ClientThread::Stats");
}

ClientThread::~ClientThread()
{
	quit();
	wait();
}

void ClientThread::start()
{
	worker = new Worker;
	worker->moveToThread(this);
	connect(worker, SIGNAL(statsChanged(const ClientThread::Stats &)), SLOT(worker_statsChanged(const ClientThread::Stats &)));
	connect(this, SIGNAL(finished()), worker, SLOT(deleteLater()));
	QThread::start();
}

void ClientThread::stop()
{
	quit();
}

void ClientThread::setupClients(const QUrl &baseUri, int count, const QString &connectHost)
{
	QMetaObject::invokeMethod(worker, "setupClients", Qt::QueuedConnection, Q_ARG(QUrl, baseUri), Q_ARG(int, count), Q_ARG(QString, connectHost));
}

void ClientThread::worker_statsChanged(const ClientThread::Stats &stats)
{
	emit statsChanged(stats);
}

#include "clientthread.moc"
