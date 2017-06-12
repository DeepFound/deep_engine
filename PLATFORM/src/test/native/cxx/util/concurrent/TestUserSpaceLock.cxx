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
#include <stdio.h>
#include <cstdlib>

#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"
#include "cxx/lang/Runnable.h"

#include "cxx/util/concurrent/atomic/AtomicInteger.h"
#include "cxx/util/concurrent/locks/UserSpaceLock.h"

using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::locks;
using namespace cxx::util::concurrent::atomic;

static int NUM_THREADS = 100;

static AtomicInteger CLIENTS_RUNNING;
static UserSpaceLock LOCK;
static boolean FLAG = false;

class TestThread : public Runnable {
	private:
		int m_num;

	public:
		TestThread(int num) :
			m_num(num) {
		}

		virtual void run() {
			test();

			CLIENTS_RUNNING.getAndDecrement();
		}

		void test() {
			for (int i=0; i<200; i++) {

				if (LOCK.tryLock() == true) {

					//printf("CLIENT(%d) try lock: %d\n", m_num, i);

					if (FLAG == true) {
						printf("- FAILED\n");
						exit(-1);
					}

					FLAG = true;

					Thread::sleep((rand() % 10) + 1);

					FLAG = false;

				} else if (i % 10) {

					//printf("CLIENT(%d) blocking: %d\n", m_num, i);

					LOCK.lock();
					{
						//printf("CLIENT(%d) lock: %d\n", m_num, i);

						if (FLAG == true) {
							printf("- FAILED\n");
							exit(-1);
						}

						FLAG = true;

						Thread::sleep((rand() % 10) + 1);

						FLAG = false;
					}

				} else {

					Thread::sleep((rand() % 10) + 1);

					continue;
				}

				//printf("CLIENT(%d) unlock: %d\n", m_num, i);

				LOCK.unlock();
			}
		}
};

int main(int argc, char** argv) {

	TestThread** clients = new TestThread*[NUM_THREADS];
	Thread** threads = new Thread*[NUM_THREADS];

	printf("\n -- %d CLIENTs\n\n", NUM_THREADS);

	srand(System::currentTimeMillis());

	for (int i = 0; i < NUM_THREADS; i++) {
		CLIENTS_RUNNING.getAndIncrement();

		clients[i] = new TestThread(i);
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	do {
		printf("\n -- Waiting for %d client(s)...\n", CLIENTS_RUNNING.get());

		Thread::sleep(1000);

	} while (CLIENTS_RUNNING.get() > 0);

	for (int i = 0; i < NUM_THREADS; i++) {
		delete clients[i];
		delete threads[i];
	}

	delete [] clients;
	delete [] threads;

	return 0;
}
