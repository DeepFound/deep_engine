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
#include "cxx/util/concurrent/atomic/AtomicInteger.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent::atomic;

static int NUM_THREADS = 300;

static AtomicInteger CLIENTS_RUNNING;
static AtomicInteger ATOMIC_INT;

class TestThread : public Runnable {

	private:
		boolean m_increment;

	public:
		TestThread(boolean increment) :
			m_increment(increment) {
		}

		virtual ~TestThread() {
		}

		virtual void run() {
			test();

			CLIENTS_RUNNING.getAndDecrement();
		}

		void test() {
			for (int i=0; i<1000000; i++) {
				if (m_increment == true) {
					ATOMIC_INT.incrementAndGet();

				} else {
					ATOMIC_INT.decrementAndGet();
				}

				//Thread::sleep((rand() % 10) + 1);
			}
		}
};

int main(int argc, char** argv) {

	int numOperations = NUM_THREADS / 2;

	TestThread** clients = new TestThread*[NUM_THREADS];
	Thread** threads = new Thread*[NUM_THREADS];

	DEEP_LOG(INFO, OTHER, "%d CLIENTs\n", NUM_THREADS);

	srand(time(0));

	// increment
	for (int i = 0; i < numOperations; i++) {
		CLIENTS_RUNNING.getAndIncrement();

		clients[i] = new TestThread(true);
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	// decrement
	for (int i = numOperations; i < (numOperations*2); i++) {
		CLIENTS_RUNNING.getAndIncrement();

		clients[i] = new TestThread(false);
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	do {
		DEEP_LOG(INFO, OTHER, "    Waiting for %d client(s)...\n", CLIENTS_RUNNING.get());

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
