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


static int TIMEOUT_WAIT = 100; // msec
static int TIMEOUT_NOTIFY = 500; // msec

static int TIMEOUT_ERROR_MARGIN = 10;  // msec

static int COUNT = 1000;
static int NUM_THREADS = 100;

static AtomicInteger CLIENTS_RUNNING;

class Condition : public Synchronizable {
	public:
		Condition() {
		}
};

class ConditionWaiter : public Runnable {
	private:
		int m_num;
		Condition* m_condition;

	public:
		ConditionWaiter(int num, Condition* condition) :
			m_num(num),
			m_condition(condition) {
		}

		virtual void run() {
			test();

			CLIENTS_RUNNING.getAndDecrement();
		}

		void test() {
			long long stime, duration;

			for (int i=0; i<COUNT; i++) {
				synchronized(m_condition) {
					stime = System::currentTimeMillis();

					if (m_condition->wait(TIMEOUT_WAIT) == true) {
						// notified
						duration = System::currentTimeMillis() - stime;
						if(duration > TIMEOUT_WAIT) {
							if ((duration - TIMEOUT_WAIT) > TIMEOUT_ERROR_MARGIN) {
								DEEP_LOG(ERROR, OTHER, "FAILED - CLIENT(%d) Notified LATE: %lld\n", m_num, System::currentTimeMillis() - stime);
								exit(-1);
							}

							DEEP_LOG(WARN, OTHER, "WARN - CLIENT(%d) Notified (late) after: %lld\n", m_num, duration);
						}

						//DEEP_LOG(INFO, OTHER, "OK - CLIENT(%d) Notified after: %lld\n", m_num, duration);

					} else {
						// timed out (or other error)
						duration = System::currentTimeMillis() - stime;
						if(duration > TIMEOUT_WAIT) {
							if ((duration - TIMEOUT_WAIT) > TIMEOUT_ERROR_MARGIN) {
								DEEP_LOG(ERROR, OTHER, "FAILED - CLIENT(%d) Timed out LATE: %lld\n", m_num, duration);
								exit(-1);
							}

							DEEP_LOG(WARN, OTHER, "WARN - CLIENT(%d) Timed out (late) after: %lld\n", m_num, duration);

						} else if (duration < TIMEOUT_WAIT) {
							DEEP_LOG(ERROR, OTHER, "FAILED - CLIENT(%d) Timed out EARLY: %lld\n", m_num, duration);
							exit(-1);
						}

						//DEEP_LOG(INFO, OTHER, "OK - CLIENT(%d) Timed out after: %lld\n", m_num, duration);
					}
				}

				Thread::sleep((rand() % 100) + 1);
			}
		}
};

class ConditionNotifier : public Runnable {
	private:
		Condition* m_condition;

	public:
		ConditionNotifier(Condition* condition) :
			m_condition(condition) {
		}

		virtual void run() {
			test();
		}

		void test() {
			for (int i=0; i<COUNT; i++) {
				Thread::sleep(TIMEOUT_NOTIFY);

				if (CLIENTS_RUNNING.get() == 0) {
					break;
				}

				synchronized(m_condition) {
					//DEEP_LOG(INFO, OTHER, "NOTIFIER Notifying...%d\n", i);

					m_condition->notifyAll();
				}
			}
		}
};

int main(int argc, char** argv) {

	DEEP_LOG(INFO, OTHER, "\n\nWAIT TIMEOUT: %d\n\n", TIMEOUT_WAIT);

	Condition* condition1 = new Condition();
	Condition* condition2 = new Condition();

	ConditionWaiter** waiters = new ConditionWaiter*[NUM_THREADS];
	Thread** threads = new Thread*[NUM_THREADS];

	srand(time(0));

	for (int i = 0; i < NUM_THREADS; i++) {
		CLIENTS_RUNNING.getAndIncrement();

		waiters[i] = new ConditionWaiter(i, (i%2) ? condition1 : condition2);
		threads[i] = new Thread(waiters[i]);
		threads[i]->start();
	}

	ConditionNotifier* notifier1 = new ConditionNotifier(condition1);
	ConditionNotifier* notifier2 = new ConditionNotifier(condition2);

	Thread* notifier1Thread = new Thread(notifier1);
	Thread* notifier2Thread = new Thread(notifier2);

	notifier1Thread->start();

	Thread::sleep(100);

	notifier2Thread->start();

	do {
		DEEP_LOG(INFO, OTHER, "  Waiting for %d client(s)...\n", CLIENTS_RUNNING.get());

		Thread::sleep(1000);

	} while (CLIENTS_RUNNING.get() > 0);

	for (int i = 0; i < NUM_THREADS; i++) {
		delete waiters[i];
		delete threads[i];
	}

	delete notifier1;
	delete notifier1Thread;

	delete notifier2;
	delete notifier2Thread;

	delete condition1;
	delete condition2;

	delete [] waiters;
	delete [] threads;

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");

	return 0;
}
