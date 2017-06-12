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

static int COMMIT    = 10000;
static int COUNT     = 10000000;
static int DATA_SIZE = sizeof(ulongtype);

static nbyte DATA(DATA_SIZE);

template class RealTimeMap<int>;
static RealTimeMap<int>* MAP = null;

void startup(boolean del);
void shutdown();

void testPut();
void testContains();
void testGet();
void testWalk();

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	CommandLineOptions options(argc, argv);

	COUNT = options.getInteger("-n", COUNT);

	boolean bDynamic = true;
	bDynamic = options.getInteger("-d", bDynamic);

	Properties::setDynamicResources(bDynamic);

	startup(true);

	testPut();
	testContains();
	testGet();
	testWalk();

	shutdown();
	startup(false);

	testContains();
	testGet();
	testWalk();

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

		int key = i;

		#if VALIDATE
			int* p = &key;
			bytearray data = DATA;
			memcpy(data, p, sizeof(int));
		#endif

		MAP->put(key, &DATA, RealTimeMap<int>::STANDARD, tx);

		if ((i % 1000000) == 0) {
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

void testContains() {

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	int retkey = 0;

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		int key = i;
		if (MAP->contains(key, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Contains %d\n", i);
			exit(-1);
		}

		if ((i % 1000000) == 0) {
			longtype lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "   Contains %d, %lld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}

	Transaction::destroy(tx);

	longtype gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, " CONTAINS TIME: %d, %lld\n", COUNT, (gstop-gstart));
}

void testGet() {

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP->associate(tx);

	int retkey = 0;

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		int key = i;

		#if VALIDATE
			bytearray data = DATA;
			memset(data, 0, sizeof(int));
		#endif

		if (MAP->get(key, &DATA, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Get %d\n", i);
			exit(-1);
		}

		#if VALIDATE
			int x = 0;
			int* p = &x;
			memcpy(p, data, sizeof(int));
			if (key != x) {
				DEEP_LOG(ERROR, OTHER, "FAILED - Val %d, %d\n", key, x);
				exit(-1);
			}
		#endif

		if ((i % 1000000) == 0) {
			longtype lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "   Get %d, %lld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}

	Transaction::destroy(tx);

	longtype gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, " GET TIME: %lld\n", (gstop-gstart));
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

		if ((i % 1000000) == 0) {
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
