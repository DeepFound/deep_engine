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

#include "cxx/util/concurrent/atomic/AtomicInteger.h"
#include "cxx/util/concurrent/locks/ReentrantReadWriteLock.h"
#include "cxx/util/concurrent/locks/UserSpaceReadWriteLock.h"

using namespace cxx::util;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::locks;
using namespace cxx::util::concurrent::atomic;

static int NUM_THREADS = 100;
static int NUM_OPS = 1000;

static AtomicInteger CLIENTS_RUNNING;
#ifdef USERSPACE
static UserSpaceReadWriteLock LOCK;
#else
static ReentrantReadWriteLock LOCK;
#endif
static long long VALUE = 0;

enum State { IDLE, READ, WRITE };

class TestThread : public Runnable {
	private:
		class operation {
			private:
				TestThread* m_t;
				void (*m_op)(TestThread*);
			public:
				operation(TestThread* t, void (*op)(TestThread*)) :
					m_t(t),
					m_op(op) {
				}
				void apply() { m_op(m_t); }
		};
		
		int m_num;
		long long m_sample;
		
		State m_state;
		long long m_lockRef;
		
		std::vector<operation> m_readOps;
		std::vector<operation> m_writeOps;
		std::vector<operation> m_idleOps;

	public:
		TestThread(int num) :
			m_num(num),
			m_sample(-1),
			m_state(IDLE),
			m_lockRef(0) {
				m_readOps.push_back(operation(this, readUnlock));
				m_readOps.push_back(operation(this, randSleep));
				m_readOps.push_back(operation(this, sample));
				#ifndef USERSPACE
				m_readOps.push_back(operation(this, readLock));
				m_readOps.push_back(operation(this, tryReadLock));
				#endif

				m_writeOps.push_back(operation(this, writeUnlock));
				m_writeOps.push_back(operation(this, randSleep));
				m_writeOps.push_back(operation(this, sample));
				#ifndef USERSPACE
				m_writeOps.push_back(operation(this, writeLock));
				m_writeOps.push_back(operation(this, tryWriteLock));
				#endif
				m_writeOps.push_back(operation(this, increment));

				m_idleOps.push_back(operation(this, randSleep));
				m_idleOps.push_back(operation(this, readLock));
				m_idleOps.push_back(operation(this, tryReadLock));
				m_idleOps.push_back(operation(this, writeLock));
				m_idleOps.push_back(operation(this, tryWriteLock));
		}

		virtual ~TestThread() {}

		virtual void run() {
			for (int i=0; i<NUM_OPS; i++) {
				step();
			}
			while (m_state != IDLE) {
				switch (m_state) {
				case READ:
					m_readOps[0].apply();
					break;
				case WRITE:
					m_writeOps[0].apply();
					break;
				default:
					break;
				}
			}

			CLIENTS_RUNNING.getAndDecrement();
		}
		
		void fail(const char* message) {
			fprintf(stdout, "- FAILED: %s\n", message);
			//exit(-1);
			abort();
		}
		
		// --- Operations ------------------------------------------------------
		static void randSleep(TestThread* t) { t->randSleep(); }
		void randSleep() {
			Thread::sleep((rand() % 10) + 1);
		}
		
		static void increment(TestThread* t) { t->increment(); }
		void increment() {
			if ((m_state != WRITE) || (m_lockRef <= 0)) {
				fail("Invalid state!");
			}
			m_sample = ++VALUE;
		}
		
		static void sample(TestThread* t) { t->sample(); }
		void sample() {
			long long sample = VALUE;
			if ((m_sample != -1) && (sample != m_sample)) {
				fail("Sample mismatch!");
			}
			m_sample = sample;
		}
		
		static void clearSample(TestThread* t) { t->clearSample(); }
		void clearSample() {
			m_sample = -1;
		}
		
		static void readLock(TestThread* t) { t->readLock(); }
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
			++m_lockRef;
			sample();
		}
		
		static void tryReadLock(TestThread* t) { t->tryReadLock(); }
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
			++m_lockRef;
			sample();
		}
		
		static void readUnlock(TestThread* t) { t->readUnlock(); }
		void readUnlock() {
			if ((m_state != READ) || (m_lockRef <= 0)) {
				fail("Invalid state!");
			}
			if (--m_lockRef == 0) {
				m_state = IDLE;
			}
			sample();
			clearSample();
			#ifdef USERSPACE
			LOCK.readUnlock();
			#else
			LOCK.readLock()->unlock();
			#endif
		}
		
		static void writeLock(TestThread* t) { t->writeLock(); }
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
			++m_lockRef;
			sample();
		}
		
		static void tryWriteLock(TestThread* t) { t->tryWriteLock(); }
		void tryWriteLock() {
			if (m_state == READ) {
				fail("Invalid state!");
			}
			#ifdef USERSPACE
			if (LOCK.tryWriteLock() == false) {
			#else
			if (LOCK.writeLock()->tryLock() == false) {
			#endif
				return;
			}
			m_state = WRITE;
			++m_lockRef;
			sample();
		}
		
		static void writeUnlock(TestThread* t) { t->writeUnlock(); }
		void writeUnlock() {
			if ((m_state != WRITE) || (m_lockRef <= 0)) {
				fail("Invalid state!");
			}
			if (--m_lockRef == 0) {
				m_state = IDLE;
			}
			sample();
			clearSample();
			#ifdef USERSPACE
			LOCK.writeUnlock();
			#else
			LOCK.writeLock()->unlock();
			#endif
		}
		// --- END Operations --------------------------------------------------

		void randStepFrom(std::vector<operation>& ops) {
			ops[rand() % ops.size()].apply();
		}
		
		void step() {
			switch (m_state) {
			case READ:
				randStepFrom(m_readOps);
				break;
			case WRITE:
				randStepFrom(m_writeOps);
				break;
			case IDLE:
				randStepFrom(m_idleOps);
				break;
			default:
				fail("Invalid state!");
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
