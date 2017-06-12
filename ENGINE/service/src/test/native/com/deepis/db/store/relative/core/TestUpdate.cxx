#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"

using namespace cxx::lang;
using namespace cxx::util;
using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::core;

static int DATA_SIZE = 50;
static nbyte DATA(DATA_SIZE);
static nbyte DATA2(DATA_SIZE);

int key_0 = 0,
    key_1 = 1,
    key_2 = 2,
    key_3 = 3,
    key_4 = 4,
    key_5 = 5,
    key_6 = 6,
    key_7 = 7,
    key_8 = 8,
    key_9 = 9,
    key_10 = 10,
    key_11 = 11,
    key_12 = 12;

static RealTimeMap<int>* MAP_PRIMARY = null;
static RealTimeMap<int>* MAP_SECONDARY = null;

static Comparator<int>* COMPARATOR = null;
static KeyBuilder<int>* KEY_BUILDER = null;

void startup(bool secondary, bool del = true);
void shutdown(bool secondary);
void clear(bool secondary);

void testUpdateSecondaryDuplicate();
void testUpdatePrimary();
void testRestart();

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	DEEP_LOG(INFO, OTHER, "MAP UPDATE TEST\n\n");
	startup(true);

	testRestart();

	/* XXX:
	testUpdatePrimary();
	*/

	testUpdateSecondaryDuplicate();

	shutdown(true);

	return 0;
}

void startup(bool secondary, bool del) {

	COMPARATOR = new Comparator<int>();

	KEY_BUILDER = new KeyBuilder<int>();
	KEY_BUILDER->setOffset(0);

	// XXX: O_SINGULAR (no threading) is specified since berkley-db's default mode is non-thread (i.e. apples to apples, expect for one internal thread)
	longtype options = RealTimeMap<int>::O_CREATE | RealTimeMap<int>::O_SINGULAR | RealTimeMap<int>::O_FIXEDKEY | RealTimeMap<int>::O_KEYCOMPRESS;
	if (del == true) {
		options = options | RealTimeMap<int>::O_DELETE;
	}

	MAP_PRIMARY = new RealTimeMap<int>("./datastore", options, sizeof(int), DATA_SIZE, COMPARATOR, KEY_BUILDER);

	if (secondary == true) {
		MAP_SECONDARY = new RealTimeMap<int>("./secondary.datastore", options, sizeof(int), DATA_SIZE, COMPARATOR, KEY_BUILDER);

		MAP_PRIMARY->associate(MAP_SECONDARY, false);
	}

	MAP_PRIMARY->mount();

	if (secondary == true) {
		MAP_SECONDARY->mount();
	}
	MAP_PRIMARY->recover(false);
}

void shutdown(bool secondary) {

	MAP_PRIMARY->unmount(false);

	if (secondary == true) {
		MAP_SECONDARY->unmount(false);
		delete MAP_SECONDARY;
		MAP_SECONDARY = null;
	}

	delete MAP_PRIMARY;
	MAP_PRIMARY = null;

	delete KEY_BUILDER;
	KEY_BUILDER = null;

	delete COMPARATOR;
	COMPARATOR = null;
}

void clear() {
	if (MAP_SECONDARY != null) {
		MAP_SECONDARY->unmount();
	}

	MAP_PRIMARY->clear();

	if (MAP_SECONDARY != null) {
		MAP_SECONDARY->mount();
	}
}

void testUpdateSecondaryDuplicate() {
	DEEP_LOG(INFO, OTHER, "TESTING UPDATE SECONDARY DUPLICATE\n");

	Transaction* tx = Transaction::create();

	tx->begin();
	MAP_PRIMARY->associate(tx);

	boolean result;

	clear();

	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	// COMMITTED put
	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);

	p = &key_3;
	data = DATA;
	memcpy(data, p, sizeof(int));

	// COMMITTED put
	MAP_PRIMARY->put(key_3, &DATA, RealTimeMap<int>::UNIQUE, tx);

	tx->commit(tx->getLevel());

	tx->begin();
	MAP_PRIMARY->associate(tx);

	p = &key_3;
	data = DATA;
	memcpy(data, p, sizeof(int));

	// update (cause secondary duplicate)
	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::EXISTING, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "update succeeded, %d should fail due to duplicate secondary\n", key_2);
		exit(1);
	}

	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testUpdatePrimary() {
	DEEP_LOG(INFO, OTHER, "TESTING UPDATE PRIMARY (REMOVE AND PUT WITH NEW PRIMARY)\n");

	inttype retkey;
	boolean result;

	clear();

	Transaction* tx = Transaction::create();

	tx->begin();
	MAP_PRIMARY->associate(tx);

	// COMMITTED put
	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);

	tx->commit(tx->getLevel());

	// level 0
	tx->begin();

	tx->begin();
	MAP_PRIMARY->associate(tx);

	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "put succeeded, should cause duplicate primary, %d\n", key_2);
		exit(1);
	}

	tx->rollback(tx->getLevel());

	tx->begin();
	MAP_PRIMARY->associate(tx);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "get failed, %d\n", key_2);
		exit(1);
	}

	// remove
	result = MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::DELETE_POPULATED, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "remove failed, %d\n", key_2);
		exit(1);
	}

	p = &key_2;
	data = DATA;
	memcpy(data, p, sizeof(int));

	result = MAP_PRIMARY->put(key_3, &DATA, RealTimeMap<int>::UNIQUE, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed put with new primary, %d\n", key_3);
		exit(1);
	}

	tx->commit(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after update primary, %d still present but should have been removed\n", key_2);
		exit(1);
	}

	result = MAP_SECONDARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after update primary, secondary key %d not present\n", key_2);
		exit(1);
	}

	// commit level 0
	tx->commit(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after commit, primary key %d not present\n", key_2);
		exit(1);
	}

	result = MAP_SECONDARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after commit, secondary key %d not present\n", key_2);
		exit(1);
	}

	Transaction::destroy(tx);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testRestart() {
	DEEP_LOG(INFO, OTHER, "TESTING RENAME\n");

	boolean result;

	int retkey;

	clear();

	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	Transaction* tx = Transaction::create();

	tx->begin();
	MAP_PRIMARY->associate(tx);

	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "put failed, %d\n", key_2);
		exit(1);
	}

	tx->commit(tx->getLevel());

	shutdown(true);
	startup(true, false /* no delete */);

	tx->begin();
	MAP_PRIMARY->associate(tx);

	p = &key_2;
	data = DATA;
	memcpy(data, p, sizeof(int));

	result = MAP_SECONDARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx, RealTimeMap<int>::LOCK_WRITE);
	// result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx, RealTimeMap<int>::LOCK_WRITE);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "get failed, %d\n", key_2);
		exit(1);
	}

	result = MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::DELETE_POPULATED, tx, RealTimeMap<int>::LOCK_NONE);
	//result = MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::RETURN, tx, RealTimeMap<int>::LOCK_WRITE);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "remove failed, %d\n", key_2);
		exit(1);
	}

	tx->commit(tx->getLevel());

	Transaction::destroy(tx);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

