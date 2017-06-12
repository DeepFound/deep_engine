#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "cxx/util/CommandLineOptions.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent::atomic;
using namespace com::deepis::db::store::relative::core;

static boolean FLAG = false;
static int NUM_THREADS = 100;

static AtomicInteger CLIENTS_RUNNING;
static RealTimeContext<int> RW_LOCK(NUM_THREADS, null);

class TestThread : public Runnable {

public:
	int m_clientNum;

	TestThread(int num) {
		m_clientNum = num;
	}

	virtual ~TestThread() {
	}

	virtual void run() {
		test();

		CLIENTS_RUNNING.getAndDecrement();
	}

	void test() {
		for (int i = 0; i < 100; i++) {

			//DEEP_LOG(DEBUG, OTHER, "  CLIENT_%d waiting for lock %d\n", m_clientNum, i);
			RW_LOCK.writeLock();
			{
				//DEEP_LOG(DEBUG, OTHER, "  CLIENT_%d got lock\n", m_clientNum);
				if (FLAG == true) {
					DEEP_LOG(ERROR, OTHER, "FAILED\n");
					exit(-1);
				}

				FLAG = true;

				Thread::sleep((rand() % 5) + 1);

				FLAG = false;
			}
			RW_LOCK.writeUnlock();

			//DEEP_LOG(INFO, OTHER, "  CLIENT_%d released lock\n", m_clientNum);
		}
	}
};

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	TestThread** clients = new TestThread*[NUM_THREADS];
	Thread** threads = new Thread*[NUM_THREADS];

	DEEP_LOG(INFO, OTHER, " THREADs %d\n", NUM_THREADS);
	srand(time(0));

	for (int i = 0; i < NUM_THREADS; i++) {
		CLIENTS_RUNNING.getAndIncrement();

		clients[i] = new TestThread(i);
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	do {
		DEEP_LOG(INFO, OTHER, "  Waiting for %d thread(s)...\n", CLIENTS_RUNNING.get());
		Thread::sleep(1000);

	} while (CLIENTS_RUNNING.get() > 0);

	for (int i = 0; i < NUM_THREADS; i++) {
		delete clients[i];
		delete threads[i];
	}

	delete [] clients;
	delete [] threads;

	DEEP_LOG(INFO, OTHER, " SUCCESS\n");

	return 0;
}
