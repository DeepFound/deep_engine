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
#include "cxx/util/concurrent/Synchronize.h"
#include "cxx/util/concurrent/atomic/AtomicInteger.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::atomic;

static int NUM_THREADS = 100;

static AtomicInteger CLIENTS_RUNNING;
static AtomicInteger ATOMIC_INT;
static Synchronizable SYNC_LOCK;

class TestThread : public Runnable {
	public:
		TestThread() {
		}

		virtual void run() {
			test();

			CLIENTS_RUNNING.getAndDecrement();
		}

		void test() {

			//Thread::sleep((rand() % 10) + 1);

			for (int i=0; i<20000; i++) {

				ATOMIC_INT.incrementAndGet();

				//Thread::sleep(5);

				//SYNC_LOCK.lock();
				//{
				synchronized(SYNC_LOCK) {
					int v = ATOMIC_INT.decrementAndGet();
					if (v != 0) {
						//DEEP_LOG(INFO, OTHER, " -- WAITING (# waiting %d on loop %d)\n", v, i);

						SYNC_LOCK.wait();

					} else {
						//Thread::sleep(2);

						//DEEP_LOG(INFO, OTHER, " -- NOTIFYING (on loop %d)\n", i);

						SYNC_LOCK.notifyAll();
					}
				}
				//SYNC_LOCK.unlock();

				//Thread::sleep((rand() % 5) + 1);
			}
		}
};

int main(int argc, char** argv) {

	TestThread** clients = new TestThread*[NUM_THREADS];
	Thread** threads = new Thread*[NUM_THREADS];

	DEEP_LOG(INFO, OTHER, "%d CLIENTs\n", NUM_THREADS);

	srand(time(0));

	for (int i = 0; i < NUM_THREADS; i++) {
		CLIENTS_RUNNING.getAndIncrement();

		clients[i] = new TestThread();
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	int count = 0;
	do {
		int numClients = CLIENTS_RUNNING.get();	
		int numBlocked = ATOMIC_INT.get();	
		DEEP_LOG(INFO, OTHER, "  Waiting for %d client(s)...%d blocked\n", numClients, numBlocked);
		if ((numClients == 1) && (numBlocked == 0)) {
			if (++count == 10) {
				DEEP_LOG(INFO, OTHER, "    Attempting RE-NOTIFY\n");
				synchronized(SYNC_LOCK) {
					DEEP_LOG(INFO, OTHER, "      RE-NOTIFY!!!\n");
					SYNC_LOCK.notifyAll();
				}
			}
		}

		Thread::sleep(1000);

	} while (CLIENTS_RUNNING.get() > 0);

	for (int i = 0; i < NUM_THREADS; i++) {
		delete clients[i];
		delete threads[i];
	}

	delete [] clients;
	delete [] threads;

	if (ATOMIC_INT.get() == 0) {
		DEEP_LOG(INFO, OTHER, "SUCCESS\n");

	} else {
		DEEP_LOG(ERROR, OTHER, "FAILED %d\n", ATOMIC_INT.get());
	}

	return 0;
}
