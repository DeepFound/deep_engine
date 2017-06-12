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
#include <vector>
#include <stdio.h>
#include <cstdlib>

#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"
#include "cxx/lang/Runnable.h"

#define USERSPACE

#include "cxx/util/concurrent/atomic/AtomicLong.h"
#include "cxx/util/concurrent/locks/ReentrantReadWriteLock.h"
#include "cxx/util/concurrent/locks/UserSpaceReadWriteLock.h"

using namespace cxx::util;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::locks;
using namespace cxx::util::concurrent::atomic;

static const int NUM_THREADS = 3;
static const long long MIN_WRITE_LOCKS_ACQUIRED = 10;

static AtomicLong READ_LOCKS_ACQUIRED;
static AtomicLong WRITE_LOCKS_ACQUIRED;

static AtomicLong EPOCH;

static AtomicLong CLIENTS_RUNNING;
#ifdef USERSPACE
static UserSpaceReadWriteLock LOCK;
#else
static ReentrantReadWriteLock LOCK;
#endif

enum State { IDLE, READ, WRITE };

class TestThread : public Runnable {
	protected:
		State m_state;

	public:
		TestThread() : m_state(IDLE) {}

		virtual ~TestThread() {}

		void fail(const char* message) {
			fprintf(stdout, "- FAILED: %s\n", message);
			//exit(-1);
			abort();
		}

		// --- Operations ------------------------------------------------------
		void randSleep() {
			Thread::sleep((rand() % 10) + 1);
		}
		
		void readLock() {
			if (m_state == WRITE) {
				fail("Invalid state!");
			}
			#ifdef USERSPACE
			LOCK.readLock();
			#else
			LOCK.readLock()->lock();
			#endif
			m_state = READ;
		}
		
		void tryReadLock() {
			if (m_state == WRITE) {
				fail("Invalid state!");
			}

			#ifdef USERSPACE
			if (LOCK.tryReadLock() == false) {
			#else
			if (LOCK.readLock()->tryLock() == false) {
			#endif
				return;
			}
			m_state = READ;
		}
		
		void readUnlock() {
			if (m_state != READ) {
				fail("Invalid state!");
			}
			m_state = IDLE;
			#ifdef USERSPACE
			LOCK.readUnlock();
			#else
			LOCK.readLock()->unlock();
			#endif
		}
		
		void writeLock() {
			if (m_state == READ) {
				fail("Invalid state!");
			}
			#ifdef USERSPACE
			LOCK.writeLock();
			#else
			LOCK.writeLock()->lock();
			#endif
			m_state = WRITE;
		}
		
		#ifndef USERSPACE
		void tryWriteLock() {
			if (m_state == READ) {
				fail("Invalid state!");
			}
			if (LOCK.writeLock()->tryLock() == false) {
				return;
			}
			m_state = WRITE;
		}
		#endif
		
		void writeUnlock() {
			if (m_state != WRITE) {
				fail("Invalid state!");
			}
			m_state = IDLE;
			#ifdef USERSPACE
			LOCK.writeUnlock();
			#else
			LOCK.writeLock()->unlock();
			#endif
		}
		// --- END Operations --------------------------------------------------
};

class Reader : public TestThread {
	private:
		const int m_num;
		long long m_epoch;

	public:
		Reader(int num) :
			m_num(num),
			m_epoch(0) {
		}

		virtual ~Reader() {}

		virtual void run() {
			while (WRITE_LOCKS_ACQUIRED.get() < MIN_WRITE_LOCKS_ACQUIRED) {
				switch (m_state) {
				case IDLE:
					while ((EPOCH.get() % 2) != m_num) {
						Thread::yield();
					}
					m_epoch = EPOCH.incrementAndGet();
					readLock();
					READ_LOCKS_ACQUIRED.incrementAndGet();
					break;
				case READ:
					while (EPOCH.get() == m_epoch) {
						Thread::sleep(100);
					}
					randSleep();
					readUnlock();
					break;
				default:
					fail("Invalid state!");
				}
			}
			while (m_state != IDLE) {
				switch (m_state) {
				case IDLE:
					break;
				case READ:
					readUnlock();
					break;
				default:
					fail("Invalid state!");
				}
			}

			CLIENTS_RUNNING.getAndDecrement();
		}

};

class Writer : public TestThread {
	public:
		Writer() {}
		virtual ~Writer() {}

		virtual void run() {
			while (WRITE_LOCKS_ACQUIRED.get() < MIN_WRITE_LOCKS_ACQUIRED) {
				switch (m_state) {
				case IDLE:
					randSleep();
					printf("\n -- WRITER waiting...\n");
					fflush(stdout);
					writeLock();
					WRITE_LOCKS_ACQUIRED.incrementAndGet();
					break;
				case WRITE:
					randSleep();
					writeUnlock();
					break;
				default:
					fail("Invalid state!");
				}
			}
			while (m_state != IDLE) {
				switch (m_state) {
				case IDLE:
					break;
				case WRITE:
					writeUnlock();
					break;
				default:
					fail("Invalid state!");
				}
			}

			CLIENTS_RUNNING.getAndDecrement();
		}

};

int main(int argc, char** argv) {

	TestThread** clients = new TestThread*[NUM_THREADS];
	Thread** threads = new Thread*[NUM_THREADS];

	printf("\n -- %d CLIENTs\n\n", NUM_THREADS);

	srand(System::currentTimeMillis());

	CLIENTS_RUNNING.set(3);

	clients[0] = new Reader(0);
	clients[1] = new Reader(1);
	clients[2] = new Writer();
	for (int i=0; i<NUM_THREADS; ++i) {
		threads[i] = new Thread(clients[i]);
	}

	printf("\n -- Starting READERS...\n");
	fflush(stdout);
	threads[0]->start();
	threads[1]->start();

	printf("\n -- Waiting for READERS to start...\n");
	fflush(stdout);
	while (EPOCH.get() < 1) {
		Thread::yield();
	}

	printf("\n -- Starting WRITER...\n");
	threads[2]->start();

	do {
		printf("\n -- Waiting for %lld client(s) [ r=%lld | w=%lld/%lld ]...\n", CLIENTS_RUNNING.get(), READ_LOCKS_ACQUIRED.get(), WRITE_LOCKS_ACQUIRED.get(), MIN_WRITE_LOCKS_ACQUIRED);
		fflush(stdout);

		Thread::sleep(1000);

	} while (CLIENTS_RUNNING.get() > 0);

	for (int i = 0; i < NUM_THREADS; ++i) {
		delete clients[i];
		delete threads[i];
	}

	delete [] clients;
	delete [] threads;

	return 0;
}
