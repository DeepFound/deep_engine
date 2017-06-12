#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "cxx/util/CommandLineOptions.h"

#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"

using namespace cxx::lang;
using namespace cxx::util;
using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::core;

static int COMMIT = 1000;
static int COUNT = 1000000;

static int DATA_SIZE = sizeof(ulongtype);;
static nbyte DATA(DATA_SIZE);

template class RealTimeMap<int>;
static RealTimeMap<int>* MAP = null;

void startup(boolean del);
void shutdown();

void testPut();
void testNext();
void testPrev();

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	CommandLineOptions options(argc, argv);

	COUNT = options.getInteger("-n", COUNT);

	startup(true);

	testPut();
	testNext();
	testPrev();

	shutdown();

	return 0;
}

void startup(boolean del) {

	DEEP_LOG(INFO, OTHER, " START OPEN TIME\n");

	// XXX: O_SINGULAR (no threading) is specified since berkley-db's default mode is non-thread (i.e. apples to apples, expect for one internal thread)
	longtype options = RealTimeMap<int>::O_CREATE | RealTimeMap<int>::O_SINGULAR | RealTimeMap<int>::O_FIXEDKEY;
	if (del == true) {
		options |= RealTimeMap<int>::O_DELETE;
	}

	longtype start = System::currentTimeMillis();

	Properties::setTransactionChunk(COMMIT);

	MAP = new RealTimeMap<int>("./datastore", options, sizeof(int), DATA_SIZE);
	MAP->mount();
	MAP->recover(false);

	longtype stop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, " COMPLETE OPEN TIME: %d, %lld\n", COUNT, (stop-start));
}

void shutdown() {

	MAP->unmount(false);

	delete MAP;
	MAP = null;
}

void testPut() {
	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	int commit = 0;

	for (int i = 1; i <= COUNT; i+= 2) {

		int key = i;
		//DEEP_LOG(ERROR, OTHER, "Put %d\n", key);
		if (MAP->put(key, &DATA, RealTimeMap<int>::UNIQUE, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Put %d, %d\n", key, MAP->getErrorCode());
			exit(-1);
		}

		if (++commit == COMMIT) {
			commit = 0;
			tx->commit(tx->getLevel());
			tx->begin();
		}
	}

	if (commit != 0) {
		DEEP_LOG(INFO, OTHER, "  MORE TO COMMIT: %d\n", commit);
		tx->commit(tx->getLevel());
	}

	Transaction::destroy(tx);

	Thread::sleep(1000);
}

void testNext() {

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	int retkey = 0;

	// XXX: this loop works  (perform 'next' on keys that are in the map)
	//for (int i = 1; i < (COUNT-1); i+=2) {

	// XXX: this loop fails  (perform 'next' on keys that are NOT in the map)
	for (int i = 0; i < COUNT; i+=2) {

		int key = i;
		if (MAP->get(key, &DATA, RealTime::NEXT, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Get next %d\n", key);
			int next_key = key + 1;
			if (MAP->get(next_key, &DATA, RealTime::EXACT) == true) {
				DEEP_LOG(ERROR, OTHER, "--------> Next key in map: %d\n", next_key);
			}
			exit(-1);
		}
	}

	Transaction::destroy(tx);
}

void testPrev() {

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	int retkey = 0;

	for (int i = COUNT; i >= 1; i-=2 ) {

		int key = i;
		if (MAP->get(key, &DATA, RealTime::PREVIOUS, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Get previous %d\n", key);
			int prev_key = key - 1;
			if (MAP->get(prev_key, &DATA, RealTime::EXACT) == true) {
				DEEP_LOG(ERROR, OTHER, "--------> Previous key in map: %d\n", prev_key);
			}
			exit(-1);
		}
	}

	Transaction::destroy(tx);
}

