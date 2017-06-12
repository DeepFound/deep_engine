#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "cxx/util/CommandLineOptions.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"

using namespace cxx::lang;
using namespace cxx::util;
using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::core;

static int COMMIT = 1000;
static int DATA_SIZE = sizeof(ulongtype);

static boolean RANDOM = false;
static AtomicInteger CLIENTS_RUNNING;

template class RealTimeMap<int>;
static RealTimeMap<int>* MAP = null;

class TestThread : public Synchronizable, public Runnable {

	private:
		int m_factor;
		int m_block;
		longtype m_putTime;

	public:
		TestThread(int factor, int block) :
			m_factor(factor),
			m_block(block),
			m_putTime(0) {
		}

		longtype getPutTime() {
			return m_putTime;
		}

		virtual void run() {
			testPut();

			CLIENTS_RUNNING.getAndDecrement();
		}

		void testPut() {

			nbyte data(DATA_SIZE);

			memset((bytearray) data, 0, DATA_SIZE);

			longtype gstart = System::currentTimeMillis();

			Transaction* tx = Transaction::create();
			tx->begin();
			MAP->associate(tx);

			int start = m_factor * m_block;
			int end = start + m_block;

			int commit = 0;
			for (int i = start; i < end; i++) {

				int key = i;
				if (RANDOM == true) {
					key = rand();
				}

				MAP->put(key, &data, RealTimeMap<int>::STANDARD, tx);

				if (++commit == COMMIT) {
					commit = 0;
					tx->commit(tx->getLevel());
					tx->begin();
				}
			}

			if (commit != 0) {
				DEEP_LOG(INFO, OTHER, "  CLIENT_%d MORE TO COMMIT: %d\n", m_factor, commit);
				tx->commit(tx->getLevel());
			}

			Transaction::destroy(tx);

			longtype gstop = System::currentTimeMillis();

			m_putTime = (gstop - gstart);
		}
};

void startup(boolean durable);
void shutdown();
void verify(int count);

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	CommandLineOptions options(argc, argv);

	int numThreads  = options.getInteger("-t", 200);
	boolean durable = options.getInteger("-d", 1) == 1;
	int count       = options.getInteger("-n", 5000000);
	RANDOM		= options.getInteger("-r", 0);

	int block = count/numThreads;

	startup(durable);

	TestThread** clients = new TestThread*[numThreads];
	Thread** threads = new Thread*[numThreads];

	DEEP_LOG(INFO, OTHER, " CLIENTs %d, BLOCK %d\n", numThreads, block);

	for (int i = 0; i < numThreads; i++) {
		CLIENTS_RUNNING.getAndIncrement();

		clients[i] = new TestThread(i, block);
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	do {
		DEEP_LOG(INFO, OTHER, "   Waiting for %d client(s), map size %lld...\n", CLIENTS_RUNNING.get(), MAP->size());

		Thread::sleep(1000);

	} while (CLIENTS_RUNNING.get() > 0);

	if (RANDOM == false) {
		verify(count);
	}

	shutdown();

	longtype totalPutTime = 0;

	for (int i = 0; i < numThreads; i++) {
		totalPutTime += clients[i]->getPutTime();
		delete clients[i];
		delete threads[i];
	}

	delete [] clients;
	delete [] threads;

	DEEP_LOG(INFO, OTHER, " AVERAGE PUT TIME: %lld\n", totalPutTime/numThreads);
	return 0;
}

void verify(int count) {
	int retkey;
	nbyte data(DATA_SIZE);

	DEEP_LOG(INFO, OTHER, " MAP SIZE: %lld\n", MAP->size());

	#ifdef DEBUG
	if (MAP->size() != count) {
		DEEP_LOG(ERROR, OTHER, "FAILED: size %d expected %lld\n", MAP->size(), count);
		exit(-1);
	}
	#endif

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	for (int i = 0; i < count; i++) {
		boolean result = MAP->get(i, &data, RealTimeMap<int>::NEXT, &retkey, tx);
		if (result == true) {
			int expectedKey = i+1;
			if (retkey != expectedKey) {
				DEEP_LOG(ERROR, OTHER, "FAILED: get NEXT on %d, expected %d, got %d\n", i, expectedKey, retkey);

				retkey = -1;
				result = MAP->get(expectedKey, &data, RealTimeMap<int>::EXACT, &retkey, tx);
				if ((result == false) || (retkey != expectedKey)) {
					DEEP_LOG(ERROR, OTHER, "FAILED: get EXACT on %d, expected %d, got %d\n\n", expectedKey, expectedKey, retkey);

				} else {
					DEEP_LOG(INFO, OTHER, " SUCCESS get EXACT on %d, expected %d, got %d\n\n", expectedKey, expectedKey, retkey);
				}

				exit(-1);
			}
		}
	}

	Transaction::destroy(tx);
}

void startup(boolean durable) {

	longtype options = RealTimeMap<int>::O_CREATE | RealTimeMap<int>::O_DELETE | RealTimeMap<int>::O_FIXEDKEY | RealTimeMap<int>::O_KEYCOMPRESS;

	DEEP_LOG(INFO, OTHER, " DURABLE %d\n", durable);
	Properties::setDurable(durable);

	MAP = new RealTimeMap<int>("./datastore", options, sizeof(int), DATA_SIZE);
	MAP->mount();
	MAP->recover(false);
}

void shutdown() {

	MAP->unmount(false);

	delete MAP;
	MAP = null;
}
