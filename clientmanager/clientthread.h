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

#ifndef CLIENTTHREAD_H
#define CLIENTTHREAD_H

#include <QThread>

class QUrl;
class Client;

class ClientThread : public QThread
{
	Q_OBJECT

public:
	class Stats
	{
	public:
		int total;
		int started;
		int received;
		int errored;
		int id;
		QString body;
		int latency;

		Stats() :
			total(0),
			started(0),
			received(0),
			errored(0),
			id(-1),
			latency(0)
		{
		}
	};

	ClientThread(QObject *parent = 0);
	~ClientThread();

	void start();
	void stop();

	void setupClients(const QUrl &baseUri, int count);

signals:
	void statsChanged(const ClientThread::Stats &stats);

private slots:
	void worker_statsChanged(const ClientThread::Stats &stats);

private:
	class Worker;

	Worker *worker;
};

#endif
