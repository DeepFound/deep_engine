/**
 *    Copyright (C) 2010 Deep Software Foundation
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"
#include "cxx/lang/Runnable.h"

#include "cxx/util/Logger.h"
#include "cxx/util/concurrent/CountDownLatch.h"
#include "cxx/util/concurrent/atomic/AtomicInteger.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::atomic;

static AtomicInteger CLIENTS_RUNNING;
static AtomicInteger CLIENTS_WAITING;

class TestThread : public Runnable {
	private:
		CountDownLatch& m_l;
		boolean m_sleep;
	public:
		TestThread(CountDownLatch& l, boolean sleep) : 
			m_l(l),
			m_sleep(sleep) {
		}

		virtual void run() {
			test();
		}

		FORCE_INLINE void test() {
			if (m_sleep == true) {
				Thread::sleep((rand() % 100) + 1);
			}

			if (m_l.getCount() == 0) {
				DEEP_LOG(ERROR, OTHER, "Premature countdown, clients still running: %d\n", CLIENTS_WAITING.get());
				abort();
			}

			CLIENTS_WAITING.decrementAndGet();
			m_l.countDown();
			m_l.await();

			if ((CLIENTS_WAITING.get() != 0) || (m_l.getCount() != 0)) {
				DEEP_LOG(ERROR, OTHER, "Premature notify, clients still running: count=%u, clients=%d\n", m_l.getCount(), CLIENTS_WAITING.get());
				abort();
			}

			CLIENTS_RUNNING.decrementAndGet();
		}
};

FORCE_INLINE void runTest(uinttype count, boolean sleep) {
	CLIENTS_RUNNING.set(count);
	CLIENTS_WAITING.set(count);
	CountDownLatch l(count);

	TestThread** clients = new TestThread*[count];
	Thread** threads = new Thread*[count];

	DEEP_LOG(INFO, OTHER, "%d CLIENTs (sleep=%d)\n", count, sleep);

	for (inttype i = 0; i < count; ++i) {
		clients[i] = new TestThread(l, sleep);
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	l.await();

	if ((CLIENTS_WAITING.get() != 0) || (l.getCount() != 0)) {
		DEEP_LOG(ERROR, OTHER, "Premature notify (root), clients still running: count=%u, clients=%d\n", l.getCount(), CLIENTS_WAITING.get());
		abort();
	}

	// XXX: "join" test threads
	while (CLIENTS_RUNNING.get() != 0) {
		Thread::yield();
	}

	for (inttype i = 0; i < count; ++i) {
		delete threads[i];
		delete clients[i];
	}
	delete [] threads;
	delete [] clients;
}

int main(int argc, char** argv) {

	srand(time(0));

	for (uinttype i=0; i<100; ++i) {
		runTest(i, true);
		runTest(i, false);
	}

	return 0;
}

