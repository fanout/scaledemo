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

#ifndef CLIENT_H
#define CLIENT_H

#include <QObject>

class QUrl;

class Client : public QObject
{
	Q_OBJECT

public:
	Client(const QString &name, QObject *parent = 0);
	~Client();

	bool isStarted() const;
	bool isErrored() const;
	int id() const;
	QString body() const;
	int receivedId() const;
	QString receivedBody() const;

	void start(const QUrl &baseUri, const QString &connectHost, int startDelay = 0);

signals:
	void started(int id, const QString &body);
	void received(int id, const QString &body);
	void error(); // temporary, will try to recover

private:
	class Private;
	friend class Private;
	Private *d;
};

#endif
