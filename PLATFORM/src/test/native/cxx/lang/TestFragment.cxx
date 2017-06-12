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
#include <stdlib.h>
#include <string.h>

#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"
#include "cxx/lang/Runnable.h"
#include "cxx/lang/Memory.h"

#include "cxx/util/concurrent/atomic/AtomicInteger.h"
#include "cxx/util/concurrent/locks/UserSpaceLock.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent::atomic;
using namespace cxx::util::concurrent::locks;

static const double MB = 1024 * 1024;
static const double GB = MB * 1024;

static const double CACHE_SIZE = 10 * GB;
static const double CACHE_THRESHOLD = .90;

static const inttype NUM_THREADS = 32;

static const inttype PAGE_SIZE = 8192;

static const inttype NUM_BUCKETS = 16;
// XXX One sizeclass test
//static const inttype NUM_BUCKETS = 1;
static const inttype NUM_BLOCKS = 1024;
// XXX One sizeclass test
//static const inttype NUM_BLOCKS = 1024*16;
static const inttype BLOCK_THRESHOLD = 100;

inttype BUCKETS[] = {8, 16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240};
// XXX One sizeclass test
//inttype BUCKETS[] = {48};

static AtomicInteger CLIENTS_RUNNING;

static AtomicInteger iterations; // Total number of iterations run, across all threads
static inttype bucketToPurge = 0; // Every n iterations or so we pick a new bucket to purge
static inttype bucketToFill = 0; // Every n iterations or so we pick a new bucket to fill

static AtomicInteger activeArena;
static inttype ARENA1_INDEX = 0;
static inttype ARENA2_INDEX = 1;

class Block {
	private:
		inttype m_objectSize;
		inttype m_maxObjects;
		inttype m_countObjects;

		bool m_hitCachePressure;

		char** m_objects;

		static UserSpaceLock s_lock;
		static longtype s_totalSize;

	public:
		Block(inttype size) {
			m_objectSize = size;
			m_maxObjects = 0;
			m_countObjects = 0;
			m_objects = NULL;
			m_hitCachePressure = false;

			fill();
		}

		~Block() {
			cleanup();
		}

		static longtype getTotalSize() {
			return s_totalSize;
		}

		inttype getCount() {
			return m_countObjects;
		}

		void deleteObject(inttype object) {
			if (m_objects[object] != NULL) {
				free(m_objects[object]);
				m_objects[object] = NULL;
				m_countObjects--;
				s_lock.lock();
				{
					s_totalSize -= m_objectSize;
				}
				s_lock.unlock();
			}
		}

		void cleanup() {
			if (m_objects != NULL) {
				for (inttype i=0; i<m_maxObjects; i++) {
					deleteObject(i);
				}

				free(m_objects);
				m_objects = NULL;

				s_lock.lock();
				{
					s_totalSize -= m_maxObjects * sizeof(char*);
				}
				s_lock.unlock();
			}
		}

		void hitCachePressure() {
			m_hitCachePressure = true;
		}

		// every fill grabs another PAGE_SIZE worth of objects until we hit cache pressure
		void fill() {
			int numObjectsAdded = 0;

			if (!m_hitCachePressure) {
				cleanup();
				m_maxObjects += PAGE_SIZE / m_objectSize;
				#ifdef JEMALLOC
				m_objects = (char**) mallocx(m_maxObjects * sizeof(char*), MALLOCX_ARENA(activeArena.get()));
				#else
				m_objects = (char**) malloc(m_maxObjects * sizeof(char*));
				#endif
				for (inttype i=0; i<m_maxObjects; i++) {
					#ifdef JEMALLOC
					m_objects[i] = (char*) mallocx(m_objectSize, MALLOCX_ARENA(activeArena.get()));
					#else
					m_objects[i] = (char*) malloc(m_objectSize);
					#endif
				}
				numObjectsAdded = m_maxObjects;

				s_lock.lock();
				{
					s_totalSize += numObjectsAdded * sizeof(char*);
				}
				s_lock.unlock();
			} else {
				for (inttype i=0; i<m_maxObjects; i++) {
					if (m_objects[i] == NULL) {
						#ifdef JEMALLOC
						m_objects[i] = (char*) mallocx(m_objectSize, MALLOCX_ARENA(activeArena.get()));
						#else
						m_objects[i] = (char*) malloc(m_objectSize);
						#endif
						numObjectsAdded++;
					}
				}
			}

			m_countObjects = m_maxObjects;

			s_lock.lock();
			{
				s_totalSize += numObjectsAdded * m_objectSize;
			}
			s_lock.unlock();
		}

		void random() {
			inttype object = rand() % m_maxObjects;

			deleteObject(object);
		}

		void fragment() {
			// int keepIndex = rand() % m_objectSize;
			int objectsOnPage = PAGE_SIZE/m_objectSize;
			for (inttype i=0; i<m_maxObjects; i++) {
				if ((i % objectsOnPage) != 0) {
					deleteObject(i);
				}
			}
		}

		void realloc(inttype otherArena) {
			s_lock.lock();
			{
				for (inttype i=0; i<m_maxObjects; i++) {
					if (m_objects[i] != NULL) {
						free(m_objects[i]);
						m_objects[i] = (char*) mallocx(m_objectSize, MALLOCX_ARENA(otherArena));
					}
				}
			}
			s_lock.unlock();
		}
};

longtype Block::s_totalSize = 0;
UserSpaceLock Block::s_lock;

class TestThread : public Runnable {
	public:
		enum State {
			ACTIVE  = 0,
			PURGE   = 1,
			SETTLE  = 2,
			STOPPED = 3
		};

	private:
		inttype m_clientNum;

		State m_state;

		Block* m_blocks[NUM_BUCKETS][NUM_BLOCKS];

	public:
		TestThread(inttype clientNum) :
			m_clientNum(clientNum),
			m_state(ACTIVE) {

			for (inttype i=0; i<NUM_BUCKETS; i++) {
				for (inttype j=0; j<NUM_BLOCKS; j++) {
					m_blocks[i][j] = new Block(BUCKETS[i]);
				}
			}
		}

		virtual ~TestThread() {
		}

		virtual void run() {
			test();

			CLIENTS_RUNNING.getAndDecrement();
		}

		State getState() {
			return m_state;
		}

		void setState(State state) {
			m_state = state;
		}

		inttype calculatePace() {
			return (rand() % 60) + m_clientNum;
		}

		void realloc(inttype otherArena) {
			for (inttype i=0; i<NUM_BUCKETS; i++) {
				for (inttype j=0; j<NUM_BLOCKS; j++) {
					m_blocks[i][j]->realloc(otherArena);
				}
			}
		}

		void test() {
			for (inttype i=0; i<NUM_BUCKETS; i++) {
				for (inttype j=0; j<NUM_BLOCKS; j++) {
					m_blocks[i][j]->fill();
				}
			}

			while (m_state != STOPPED) {
				if (m_state != SETTLE) {
					for (inttype i=0; i<NUM_BUCKETS; i++) {
						for (inttype j=0; j<NUM_BLOCKS; j++) {
							if (m_blocks[i][j] != NULL) {
								m_blocks[i][j]->random();

								if (m_state == ACTIVE) {
									if (m_blocks[i][j]->getCount() <= BLOCK_THRESHOLD) {
										// Even though the current bucket is the one that's underfull,
										// we should go ahead and fill up a single random bucket anyway
										// (all threads trying to fill one bucket is the use case that produces
										// fragmentation)
										m_blocks[bucketToFill][j]->fill();
									}

								} else if (m_state == PURGE) {
									m_blocks[i][j]->hitCachePressure();
									m_blocks[bucketToPurge][j]->fragment();
								}
							}
						}
					}
				}
				iterations.getAndIncrement();
				if (iterations.get() % 25 == 1) {
					Memory::dump(true);
					bucketToPurge = rand() % NUM_BUCKETS;
					bucketToFill = rand() % NUM_BUCKETS;
				}
				Thread::sleep(calculatePace());
			}
		}

		static const char* getStateText(State state) {
			switch(state) {
				case ACTIVE:
					return "ACTIV";
				case PURGE:
					return "PURGE";
				case SETTLE:
					return "SETLE";
				default:
					return "STOPD";
			}
		}
};

class CleanupThread : public Runnable {
	private:
		TestThread** m_clients;
		bool m_need_realloc;

	public:
		CleanupThread(TestThread** clients) : m_clients(clients), m_need_realloc (false) {
		}

		virtual ~CleanupThread() {
		}

		virtual void run() {
			realloc();
		}

		void realloc() {
			while (CLIENTS_RUNNING.get() > 0) {
				doubletype processAllocatedBytes = (doubletype) Memory::getProcessAllocatedBytes();
				doubletype freeListBytes = (doubletype) Memory::getFreeListBytes();
				printf("Considering reallocating %.3f\n", (freeListBytes/processAllocatedBytes));
				fflush(stdout);
				if (m_need_realloc && freeListBytes/processAllocatedBytes > 0.10) {
					printf("Reallocating, will not reallocate for a while\n");
					fflush(stdout);
					// Realloc and switch arena
					inttype currArena = activeArena.get();
					inttype otherArena = (currArena == ARENA1_INDEX) ? ARENA2_INDEX : ARENA1_INDEX;
					for (int i = 0; i < CLIENTS_RUNNING.get(); i++) {
						m_clients[i]->realloc(otherArena);
					}
					activeArena.set(otherArena);
					m_need_realloc = false;
					printf("Finished reallocating\n");
					fflush(stdout);
				} else if (!m_need_realloc && freeListBytes/processAllocatedBytes < 0.9) {
					printf("Can reallocate again\n");
					fflush(stdout);
					m_need_realloc = true;
				} else {
					printf("Doing nothing\n");
					fflush(stdout);
				}
				Thread::sleep(30);
			}
		}
};

longtype memoryUsage(TestThread::State state) {
	longtype processAllocatedBytes = Memory::getProcessAllocatedBytes();

	longtype freeListBytes = Memory::getFreeListBytes();
	longtype consumedBytes = processAllocatedBytes - freeListBytes;

//	Memory::dump();
	printf("(%s): rss: %8.2fM, total(A): %8.2fM, actual: %8.2fM, consumed(A): %8.2fM, freelist(A): %8.2fM\n",
		TestThread::getStateText(state),
		Memory::getRSS() / MB,
		processAllocatedBytes / MB,
		Block::getTotalSize() / MB,
		consumedBytes / MB,
		freeListBytes / MB);

	return consumedBytes;
}

int main(int argc, char** argv) {

	#ifdef JEMALLOC
	size_t size_arena = sizeof(ARENA1_INDEX);
	mallctl("arenas.extend", &ARENA1_INDEX, &size_arena, NULL, 0);
	printf("Arena 1: %d", ARENA1_INDEX);
	activeArena.set(ARENA1_INDEX);
	size_arena = sizeof(ARENA2_INDEX);
	mallctl("arenas.extend", &ARENA2_INDEX, &size_arena, NULL, 0);
	printf("Arena 2: %d", ARENA2_INDEX);
	#endif

	srand(System::currentTimeMillis());

	TestThread** clients = new TestThread*[NUM_THREADS];
	Thread** threads = new Thread*[NUM_THREADS];

	for (int i = 0; i < NUM_THREADS; i++) {
		CLIENTS_RUNNING.getAndIncrement();

		clients[i] = new TestThread(i + 1);
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	CleanupThread* memoryGC = new CleanupThread(clients);
	Thread* memoryGCThread = new Thread(memoryGC);
	memoryGCThread->start();

	inttype settleCount = 0;

	do {
		TestThread::State state = clients[0]->getState();

		longtype consumedBytes = memoryUsage(state);

		if (state == TestThread::ACTIVE) {
			if (consumedBytes > CACHE_SIZE) {
				for (int i = 0; i < NUM_THREADS; i++) { clients[i]->setState(TestThread::PURGE);
				}
			}
		} else if (state == TestThread::PURGE) {
			if (consumedBytes < (CACHE_SIZE * CACHE_THRESHOLD)) {
				for (int i = 0; i < NUM_THREADS; i++) {
					clients[i]->setState(TestThread::SETTLE);
				}
			}
		} else if (state == TestThread::SETTLE) {
			settleCount++;

			if (settleCount == 2) {
				for (int i = 0; i < NUM_THREADS; i++) {
					clients[i]->setState(TestThread::ACTIVE);
				}

				settleCount = 0;
			}
		}

		Memory::releaseAvailableBytes();

		Thread::sleep(30);

	} while (CLIENTS_RUNNING.get() > 0);

	for (int i = 0; i < NUM_THREADS; i++) {
		delete clients[i];
		delete threads[i];
	}

	delete [] clients;
	delete [] threads;

	delete memoryGC;
	delete memoryGCThread;

	return 0;
}
