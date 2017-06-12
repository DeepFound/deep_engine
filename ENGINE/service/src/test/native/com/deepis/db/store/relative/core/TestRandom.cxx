#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "cxx/util/CommandLineOptions.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"
#include "com/deepis/db/store/relative/core/RealTimeIterator.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::core;

#define RESTART 1
#define VALIDATE 1

static int COMMIT = 1000;
static int COUNT = 5000000;
static int* THE_ARRAY;

static int DATA_SIZE = sizeof(int);
static nbyte DATA(DATA_SIZE);

template class RealTimeMap<int>;
static RealTimeMap<int>* MAP = null;

void startup(boolean del);
void shutdown();

void testPut();
void testContains(int start, int end, boolean exists);
void testGet(int start, int end);
void testWalk();
void testUpdate();
void testRemove(int start, int end);

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	CommandLineOptions options(argc, argv);

	COUNT = options.getInteger("-n", COUNT);
	THE_ARRAY = (int*) malloc(sizeof(int) * COUNT);

	startup(true);

	testPut();

	testContains(0, COUNT, true);
	testGet(0, COUNT);
	testWalk();
	testUpdate();

#if RESTART
	DEEP_LOG(INFO, OTHER, "  Wait 15 seconds to complete irt write out before continuing\n");
	for (int i = 0; i < 15; i++) {
		DEEP_LOG(INFO, OTHER, "          ... waiting %d\n", i);
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
	testWalk();

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
	longtype options = RealTimeMap<int>::O_CREATE | RealTimeMap<int>::O_SINGULAR | RealTimeMap<int>::O_FIXEDKEY | RealTimeMap<int>::O_KEYCOMPRESS | RealTimeMap<int>::O_MEMORYCOMPRESS;
	if (del == true) {
		options |= RealTimeMap<int>::O_DELETE;
		memset(THE_ARRAY, 0, sizeof(int) * COUNT);
	}

	longtype start = System::currentTimeMillis();

	//Properties::setCacheSize(???);
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
	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		int x = rand();
		int key = x;

		#if VALIDATE
			int* p = &key;
			bytearray data = DATA;
			memcpy(data, p, sizeof(int));
		#endif

		if (MAP->put(key, &DATA, RealTimeMap<int>::UNIQUE, tx) == true) {
			THE_ARRAY[i] = x;

		} else {
			THE_ARRAY[i] = -1;
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

	DEEP_LOG(INFO, OTHER, " PUT TIME: %d, %lld\n", COUNT, (gstop-gstart));
}

void testContains(int start, int end, boolean exists) {

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	int retkey = 0;

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = start; i < end; i++) {

		int x = THE_ARRAY[i];
		if (x == -1) {
			continue;
		}

		int key = x;
		if (exists == true) {
			if (MAP->contains(key, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
				DEEP_LOG(ERROR, OTHER, "FAILED - Not Found %d, %d, %d, %d\n", i, key, start, end);
				exit(-1);
			}

		} else {
			if (MAP->contains(key, RealTimeMap<int>::EXACT, &retkey, tx) == true) {
				DEEP_LOG(ERROR, OTHER, "FAILED - Found %d, %d, %d, %d\n", i, key, start, end);
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
		DEEP_LOG(INFO, OTHER, " CONTAINS TIME: %d, %d, %lld\n", start, end, (gstop-gstart));

	} else {
		DEEP_LOG(INFO, OTHER, " NOT CONTAINS TIME: %d, %d, %lld\n", start, end, (gstop-gstart));
	}

	fflush(stdout);
}

void testGet(int start, int end) {

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	int retkey = 0;

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = start; i < end; i++) {

		int x = THE_ARRAY[i];
		if (x == -1) {
			continue;
		}

		int key = x;

		#if VALIDATE
			bytearray data = DATA;
			memset(data, 0, DATA_SIZE);
		#endif

		if (MAP->get(key, &DATA, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Get %d\n", i);
			exit(-1);
		}

		#if VALIDATE
			int* p = &x;
			memcpy(p, data, sizeof(int));
			if (key != x) {
				DEEP_LOG(ERROR, OTHER, "FAILED - Val %d, %d\n", key, x);
				exit(-1);
			}
		#endif

		if ((i % 100000) == 0) {
			longtype lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "   Get %d, %lld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}

	Transaction::destroy(tx);

	longtype gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, " GET TIME: %d, %d, %lld\n", start, end, (gstop-gstart));
}

void testWalk() {

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	RealTimeIterator<int>* iter = new RealTimeIterator<int>(MAP);

	if (MAP->cursor(0, iter, RealTimeMap<int>::FIRST, tx) == false) {
		DEEP_LOG(ERROR, OTHER, "FAILED - Iter %d\n", 0);
		exit(-1);
	}

	const MapEntry<int,Information*,bytetype>* next = iter->next();

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; (next != null); i++) {

		if ((i % 100000) == 0) {
			longtype lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "   Walk Next %d/%d, %lld\n", i, next->getKey(), (lstop-lstart));
			lstart = System::currentTimeMillis();
		}

		next = iter->next();
	}

	Transaction::destroy(tx);

	longtype gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, " WALK TIME: %lld\n", (gstop-gstart));

	delete iter;
}

void testUpdate() {

	int commit = 0;
	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		int x = THE_ARRAY[i];
		if (x == -1) {
			continue;
		}

		int key = x;

		#if VALIDATE
			int* p = &key;
			bytearray data = DATA;
			memcpy(data, p, sizeof(int));
		#endif

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
	longtype gstop = System::currentTimeMillis();

	if (commit != 0) {
		DEEP_LOG(INFO, OTHER, " MORE TO COMMIT: %d\n", commit);
		tx->commit(tx->getLevel());
	}

	Transaction::destroy(tx);

	DEEP_LOG(INFO, OTHER, " UPDATE TIME: %d, %lld\n", COUNT, (gstop-gstart));
	fflush(stdout);
}

void testRemove(int start, int end) {

	int commit = 0;
	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = start; i < end; i++) {

		int x = THE_ARRAY[i];
		if (x == -1) {
			continue;
		}

		int key = x;
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
	longtype gstop = System::currentTimeMillis();

	if (commit != 0) {
		DEEP_LOG(INFO, OTHER, " MORE TO COMMIT: %d\n", commit);
		tx->commit(tx->getLevel());
	}

	Transaction::destroy(tx);

	DEEP_LOG(INFO, OTHER, " REMOVE TIME: %d, %d, %lld, %lld\n", start, end, MAP->size(), (gstop-gstart));
	fflush(stdout);

	Thread::sleep(1000);
}
