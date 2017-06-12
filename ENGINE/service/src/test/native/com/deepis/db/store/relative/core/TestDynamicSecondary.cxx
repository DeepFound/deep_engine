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

static int COMMIT = 10000;
static int COUNT = 1000000;

static int DATA_SIZE = sizeof(ulongtype);;
static nbyte DATA(DATA_SIZE);

template class RealTimeMap<int>;

static RealTimeMap<int>* MAP_PRIMARY = null;
static RealTimeMap<int>* MAP_SECONDARY = null;

static Comparator<int>* COMPARATOR;
static KeyBuilder<int>* KEY_BUILDER;

void startup(boolean del);
void shutdown();

void testPut();
void testContains();
void testGet();
void testUpdate();
void testRemove();

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	CommandLineOptions options(argc, argv);

	COUNT = options.getInteger("-n", COUNT);

	startup(true);

	testPut();
	testContains();
	testGet();
	testUpdate();
	testGet();
	testRemove();

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

	MAP_PRIMARY = new RealTimeMap<int>("./datastore", options, sizeof(int), DATA_SIZE);

	COMPARATOR = new Comparator<int>();

	KEY_BUILDER = new KeyBuilder<int>();
	KEY_BUILDER->setOffset(0);

	MAP_SECONDARY = new RealTimeMap<int>("./secondary.datastore", options, sizeof(int), DATA_SIZE, COMPARATOR, KEY_BUILDER);

	MAP_PRIMARY->mount();
	MAP_PRIMARY->recover(false);

	longtype stop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, " COMPLETE OPEN TIME: %d, %lld\n", COUNT, (stop-start));
	fflush(stdout);
}

void shutdown() {

	MAP_PRIMARY->unmount(false);
	MAP_SECONDARY->unmount(false);

	delete MAP_SECONDARY;
	MAP_SECONDARY = null;

	delete MAP_PRIMARY;
	MAP_PRIMARY = null;

	delete KEY_BUILDER;
	KEY_BUILDER = null;

	delete COMPARATOR;
	COMPARATOR = null;
}

void testPut() {

	int commit = 0;
	Transaction* tx = Transaction::create();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		int key = i * 10;

		int* p = &key;
		bytearray data = DATA;
		memcpy(data, p, sizeof(int));

		#if 1
		if (i == (COUNT / 2)) {
			longtype start = System::currentTimeMillis();

			MAP_PRIMARY->associate(MAP_SECONDARY, true, true);
			MAP_SECONDARY->mount();

			longtype stop = System::currentTimeMillis();

			DEEP_LOG(INFO, OTHER, " INDEX TIME: %d, %lld\n", i, (stop-start));
		}
		#endif

		if (MAP_PRIMARY->put(i, &DATA, RealTimeMap<int>::UNIQUE, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Create %d, %d\n", i, MAP_PRIMARY->getErrorCode());
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

	#if 0
	longtype start = System::currentTimeMillis();

	MAP_PRIMARY->associate(MAP_SECONDARY, true, true);
	MAP_SECONDARY->mount();

	longtype stop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, " INDEX TIME: %d, %lld\n", COUNT, (stop-start));
	#endif

	Transaction::destroy(tx);

	longtype gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, " PUT TIME: %d, %lld\n", COUNT, (gstop-gstart));
	fflush(stdout);
}

void testContains() {

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	int retkey = 0;

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		if (MAP_PRIMARY->contains(i, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Contains %d\n", i);
			exit(-1);
		}

		int secondary = i * 10;
		if (MAP_SECONDARY->contains(secondary, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Secondary contains %d\n", secondary);
			exit(-1);
		}

		if ((i % 100000) == 0) {
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
	MAP_PRIMARY->associate(tx);

	int retkey = 0;

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		bytearray data = DATA;
		memset(data, 0, sizeof(int));

		if (MAP_PRIMARY->get(i, &DATA, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Get %d\n", i);
			exit(-1);
		}

		int secondary = i * 10;
		if (MAP_SECONDARY->get(secondary, &DATA, RealTimeMap<int>::EXACT, &retkey, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Secondary get %d\n", secondary);
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

	DEEP_LOG(INFO, OTHER, " GET TIME: %lld\n", (gstop-gstart));
	fflush(stdout);
}

void testUpdate() {

	int commit = 0;
	Transaction* tx = null;
	tx = Transaction::create(true);
	tx->begin();
	MAP_PRIMARY->associate(tx);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		int key = i * 10;

		int* p = &key;
		bytearray data = DATA;
		memcpy(data, p, sizeof(int));

		if (MAP_PRIMARY->put(i, &DATA, RealTimeMap<int>::EXISTING, tx) == false) {
			DEEP_LOG(ERROR, OTHER, "FAILED - Update %d, %d\n", i, MAP_PRIMARY->getErrorCode());
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
		DEEP_LOG(INFO, OTHER, "  MORE TO COMMIT: %d\n", commit);
		tx->commit(tx->getLevel());
	}

	Transaction::destroy(tx);

	DEEP_LOG(INFO, OTHER, "  UPDATE TIME: %d, %lld\n", COUNT, (gstop-gstart));
}

void testRemove() {

	int commit = 0;
	Transaction* tx = Transaction::create();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	longtype gstart = System::currentTimeMillis();
	longtype lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		bytearray data = DATA;
		memset(data, 0, sizeof(int));

		if (MAP_PRIMARY->remove(i, &DATA, RealTimeMap<int>::DELETE_RETURN, tx) == false) {
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

	DEEP_LOG(INFO, OTHER, " REMOVE TIME: %lld\n", (gstop-gstart));
	fflush(stdout);
}
