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

#define RESTART 1

static int COMMIT = 10000;
static int COUNT = 1000000;

static int DATA_SIZE = sizeof(ulongtype);;
static nbyte DATA(DATA_SIZE);

template class RealTimeMap<int>;
static RealTimeMap<int>* MAP = null;

void startup(boolean del);
void shutdown();

void testPut();
void testContains(int start, int end, boolean exists);
void testGet(int start, int end);
void testUpdate();
void testRemove(int start, int end);

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);
	cxx::util::Logger::disableLevel(cxx::util::Logger::INFO);

	CommandLineOptions options(argc, argv);

	COUNT = options.getInteger("-n", COUNT);

	startup(true);

	testPut();
	testContains(0, COUNT, true);
	testGet(0, COUNT);
	testUpdate();

#if RESTART
	DEEP_LOG(INFO, OTHER, "  Wait 15 seconds to complete irt write out before continuing\n");
	for (int i = 0; i < 15; i++) {
		DEEP_LOG(INFO, OTHER, "         ... waiting %d\n", i);
		Thread::sleep(1000);
	}

	testRemove(COUNT / 3, COUNT / 2);

	shutdown();
	startup(false);

	testContains(0, COUNT / 3, true);
	testContains(COUNT / 3, COUNT / 2, false);
	testContains(COUNT / 2, COUNT, true);

	testGet(0, COUNT / 3);
	testGet(COUNT / 2, COUNT);

	testRemove(0, COUNT / 3);
	testRemove(COUNT / 2, COUNT);
#else
	testRemove(0, COUNT);
#endif

	shutdown();

	return 0;
}

void startup(boolean del) {

	DEEP_LOG(INFO, OTHER, " START OPEN TIME\n");

	// XXX: O_SINGULAR (no threading) is specified since berkley-db's default mode is non-thread (i.e. apples to apples, expect for one internal thread)
	longtype options = RealTimeMap<int>::O_CREATE | RealTimeMap<int>::O_SINGULAR | RealTimeMap<int>::O_FIXEDKEY | RealTimeMap<int>::O_KEYCOMPRESS;
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

	int commit = 0;
	Transaction* tx = Transaction::create(true);
	tx->begin();
	MAP->associate(tx);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		int key = i;
		if (MAP->put(key, &DATA, RealTimeMap<int>::UNIQUE, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Create %d, %d\n", i, MAP->getErrorCode());
			exit(-1);
		}

		if ((i % 100000) == 0) {
			longtype lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "   Put %d, %lld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
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

	longtype gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "  PUT TIME: %d, %lld\n", COUNT, (gstop-gstart));

	Thread::sleep(1000);
}

void testContains(int start, int end, boolean exists) {
	Thread::sleep(1000);

	Transaction* tx = Transaction::create(true);
	tx->begin();
	MAP->associate(tx);

	int retkey = -1;

	boolean result = MAP->get(-1, &DATA, RealTimeMap<int>::LAST, &retkey, tx);
	DEEP_LOG(INFO, OTHER, "  last: %d, %d\n", result, retkey);

	result = MAP->get(-1, &DATA, RealTimeMap<int>::FIRST, &retkey, tx);
	DEEP_LOG(INFO, OTHER, "  first: %d, %d\n", result, retkey);

	result = MAP->get(retkey, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	DEEP_LOG(INFO, OTHER, "  next: %d, %d\n", result, retkey);

	result = MAP->get(retkey, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	DEEP_LOG(INFO, OTHER, "  next: %d, %d\n", result, retkey);

	result = MAP->get(retkey, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	DEEP_LOG(INFO, OTHER, "  next: %d, %d\n", result, retkey);

	result = MAP->get(retkey, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	DEEP_LOG(INFO, OTHER, "  orNext: %d, %d\n", result, retkey);

	result = MAP->get(retkey, &DATA, RealTimeMap<int>::PREVIOUS, &retkey, tx);
	DEEP_LOG(INFO, OTHER, "  previous: %d, %d\n", result, retkey);

	result = MAP->get(retkey, &DATA, RealTimeMap<int>::EXACT_OR_PREVIOUS, &retkey, tx);
	DEEP_LOG(INFO, OTHER, "  orPrevious: %d, %d\n", result, retkey);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = start; i < end; i++) {

		int key = i;
		if (exists == true) {
			if (MAP->contains(key, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
				DEEP_LOG(ERROR, OTHER, "FAILED - Not Found %d, %d, %d\n", i, start, end);
				exit(-1);
			}

		} else {
			if (MAP->contains(key, RealTimeMap<int>::EXACT, &retkey, tx) == true) {
				DEEP_LOG(ERROR, OTHER, "FAILED - Found %d, %d, %d\n", i, start, end);
				exit(-1);
			}
		}

		if ((i % 100000) == 0) {
			longtype lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "   Contains %d, %lld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}

	Transaction::destroy(tx);

	longtype gstop = System::currentTimeMillis();

	if (exists == true) {
		DEEP_LOG(INFO, OTHER, "  CONTAINS TIME: %d, %d, %lld\n", start, end, (gstop-gstart));

	} else {
		DEEP_LOG(INFO, OTHER, "  NOT CONTAINS TIME: %d, %d, %lld\n", start, end, (gstop-gstart));
	}
}

void testGet(int start, int end) {

	Transaction* tx = Transaction::create(true);
	tx->begin();
	MAP->associate(tx);

	int retkey = 0;

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = start; i < end; i++) {

		int key = i;
		if (MAP->get(key, &DATA, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Get %d\n", i);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			longtype lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "   Get %d, %lld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}

	Transaction::destroy(tx);

	longtype gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "  GET TIME: %d, %d, %lld\n", start, end, (gstop-gstart));
}

void testUpdate() {

	int commit = 0;
	Transaction* tx = Transaction::create(true);
	tx->begin();
	MAP->associate(tx);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		int key = i;
		if (MAP->put(key, &DATA, RealTimeMap<int>::EXISTING, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Update %d, %d\n", i, MAP->getErrorCode());
			exit(-1);
		}

		if ((i % 100000) == 0) {
			longtype lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "   Update %d, %lld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
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

	longtype gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "  UPDATE TIME: %d, %lld\n", COUNT, (gstop-gstart));
}

void testRemove(int start, int end) {

	int commit = 0;
	Transaction* tx = Transaction::create(true);
	tx->begin();
	MAP->associate(tx);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = start; i < end; i++) {

		int key = i;
		if (MAP->remove(key, &DATA, RealTimeMap<int>::DELETE_RETURN, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Remove %d\n", i);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			longtype lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "   Remove %d, %lld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
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

	longtype gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "  REMOVE TIME: %d, %d, %lld, %lld\n", start, end, MAP->size(), (gstop-gstart));

	Thread::sleep(1000);
}
