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
#include "client.h"

class ClientThread::Worker : public QObject
{
	Q_OBJECT

public:
	QUrl baseUri;
	QSet<Client*> clients;
	QSet<Client*> clientsErrored;
	Stats stats;
	QTimer *statsTimer;
	bool statsPending;

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
	void setupClients(const QUrl &_baseUri, int count)
	{
		baseUri = _baseUri;

		for(int n = 0; n < count; ++n)
		{
			Client *c = new Client(QString::number(n), this);
			clients += c;
			connect(c, SIGNAL(started(int, const QString &)), SLOT(client_started(int, const QString &)));
			connect(c, SIGNAL(received(int, const QString &)), SLOT(client_received(int, const QString &)));
			c->start(baseUri);
			if(n % 10 == 0)
				msleep(1);
		}
	}

public:
	void tryStats()
	{
		if(!statsTimer->isActive())
		{
			statsTimer->start(20);
			emitStats();
		}
		else
			statsPending = true;
	}

	void emitStats()
	{
		stats.total = clients.count();
		stats.errored = clientsErrored.count();
		emit statsChanged(stats);
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
			stats.id = id;

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
			stats.received = 1;
		}
		else
		{
			assert(id == stats.id);
			++stats.received;
		}

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
	}

	void statsTimer_timeout()
	{
		if(statsPending)
		{
			statsPending = false;
			emitStats();
		}
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

void ClientThread::setupClients(const QUrl &baseUri, int count)
{
	QMetaObject::invokeMethod(worker, "setupClients", Qt::QueuedConnection, Q_ARG(QUrl, baseUri), Q_ARG(int, count));
}

void ClientThread::worker_statsChanged(const ClientThread::Stats &stats)
{
	emit statsChanged(stats);
}

#include "clientthread.moc"
