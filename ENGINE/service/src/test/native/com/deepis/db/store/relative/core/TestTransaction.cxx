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

/*
 * XXX: This test needs to be re-written using the TestClient harness.
 */

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

void startup(bool secondary);
void shutdown(bool secondary);
void clear();

void testTxNext();
void testTxExactOrNext();
void testTxPrevious();
void testTxExactOrPrevious();
void testTxFirst();
void testTxLast();

void testBeginBeginRemove();
void testBeginBeginPut();

void testBeginPutRollback(boolean subtx);
void testBeginPutUpdateRollbackBeginPut();

void testSameLevelBeginPutUpdate();

void testBeginUpdateBeginUpdateBeginRemoveBeginPut();
void testBeginUpdateBeginUpdateCommitBeginPut();
void testBeginRemoveRollback();

void testBeginPutBeginRemoveRollback();
void testBeginRemoveBeginPutRollback();
void testBeginRemoveBeginRemoveRollback();
void testBeginRemoveBeginPutBeginUpdateRollback();

int main(int argc, char** argv) {

	// XXX: valgrind
	{
		DATA.zero();
		DATA2.zero();
	}

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	DEEP_LOG(INFO, OTHER, "\nMAP TX TEST\n");

	startup(false);

	testTxNext();
	testTxExactOrNext();
	testTxPrevious();
	testTxExactOrPrevious();
	testTxFirst();
	testTxLast();

	testBeginBeginRemove();
	testBeginBeginPut();

	testSameLevelBeginPutUpdate();

	testBeginUpdateBeginUpdateBeginRemoveBeginPut();
	testBeginUpdateBeginUpdateCommitBeginPut();

	testBeginPutRollback(true);
	testBeginPutRollback(false);
	testBeginRemoveRollback();
	testBeginPutUpdateRollbackBeginPut();
	testBeginPutBeginRemoveRollback();
	testBeginRemoveBeginPutRollback();
	testBeginRemoveBeginRemoveRollback();

	shutdown(false);

	//
	// SECONDARY
	//
	DEEP_LOG(INFO, OTHER, "\nMAP TX TEST WITH SECONDARY\n");

	startup(true);

	testBeginBeginRemove();
	testBeginBeginPut();

	testSameLevelBeginPutUpdate();

	testBeginUpdateBeginUpdateBeginRemoveBeginPut();
	testBeginUpdateBeginUpdateCommitBeginPut();

	testBeginPutUpdateRollbackBeginPut();
	testBeginPutRollback(true);
	testBeginPutRollback(false);
	testBeginRemoveRollback();
	testBeginPutBeginRemoveRollback();
	testBeginRemoveBeginPutRollback();
	testBeginRemoveBeginRemoveRollback();

	testBeginRemoveBeginPutBeginUpdateRollback();

	shutdown(true);

	return 0;
}

void startup(bool secondary) {

	// XXX: O_SINGULAR (no threading) is specified since berkley-db's default mode is non-thread (i.e. apples to apples, expect for one internal thread)
	longtype options = RealTimeMap<int>::O_CREATE | RealTimeMap<int>::O_DELETE | RealTimeMap<int>::O_SINGULAR | RealTimeMap<int>::O_FIXEDKEY | RealTimeMap<int>::O_KEYCOMPRESS;
	MAP_PRIMARY = new RealTimeMap<int>("./datastore", options, sizeof(int), DATA_SIZE);

	if (secondary == true) {
		COMPARATOR = new Comparator<int>();

		KEY_BUILDER = new KeyBuilder<int>();
		KEY_BUILDER->setOffset(0);

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

		delete COMPARATOR;
		COMPARATOR = null;
		delete KEY_BUILDER;
		KEY_BUILDER = null;
	}

	delete MAP_PRIMARY;
	MAP_PRIMARY = null;
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

void testTxNext() {
	DEEP_LOG(INFO, OTHER, "TESTING NEXT...\n");

	// clear(); // XXX: no need to clear since this is the first test

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	Transaction* tx_c = Transaction::create();

	int retkey;

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_0, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_5, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_10, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	// TX PUT
	MAP_PRIMARY->put(key_3, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_4, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_6, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_11, &DATA, RealTimeMap<int>::UNIQUE, tx);

	boolean result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	if ((result == false) || (retkey != 3)) {
		DEEP_LOG(ERROR, OTHER, "get next failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_3, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	if ((result == false) || (retkey != 4)) {
		DEEP_LOG(ERROR, OTHER, "get next failed, tx read after 1st tx remove, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_4, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	if ((result == false) || (retkey != 5)) {
		DEEP_LOG(ERROR, OTHER, "get next failed, tx read after 2nd tx remove, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE OF PREVIOUSLY COMMITTED VAL
	MAP_PRIMARY->remove(key_5, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	if ((result == false) || (retkey != 6)) {
		DEEP_LOG(ERROR, OTHER, "get next failed, tx read after 3rd tx remove of previously committed value, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_6, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	if ((result == false) || (retkey != 10)) {
		DEEP_LOG(ERROR, OTHER, "get next failed, tx read after 4th tx remove, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED REMOVE
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->remove(key_10, &DATA, RealTimeMap<int>::DELETE_RETURN, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	if ((result == false) || (retkey != 11)) {
		DEEP_LOG(ERROR, OTHER, "get next failed, tx read after committed remove of previously committed value, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_1, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::NEXT, &retkey, tx);
	if ((result == false) || (retkey != 1)) {
		DEEP_LOG(ERROR, OTHER, "get next failed, tx read after committed put, bad value: %d\n", retkey);
		exit(-1);
	}

	// reset the MAP
	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testTxExactOrNext() {
	DEEP_LOG(INFO, OTHER, "TESTING EXACT OR NEXT...\n");

	clear();

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	Transaction* tx_c = Transaction::create();

	int retkey;

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_0, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_5, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_10, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	// TX PUT
	MAP_PRIMARY->put(key_3, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_4, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_6, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_12, &DATA, RealTimeMap<int>::UNIQUE, tx);

	boolean result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 0)) {
		DEEP_LOG(ERROR, OTHER, "1. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_0, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 3)) {
		DEEP_LOG(ERROR, OTHER, "2. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_3, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 4)) {
		DEEP_LOG(ERROR, OTHER, "3. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_4, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 5)) {
		DEEP_LOG(ERROR, OTHER, "4. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED REMOVE
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->remove(key_5, &DATA, RealTimeMap<int>::DELETE_RETURN, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 6)) {
		DEEP_LOG(ERROR, OTHER, "5. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_6, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_6, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 10)) {
		DEEP_LOG(ERROR, OTHER, "6. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	result = MAP_PRIMARY->get(key_7, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 10)) {
		DEEP_LOG(ERROR, OTHER, "7. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	result = MAP_PRIMARY->get(key_11, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 12)) {
		DEEP_LOG(ERROR, OTHER, "8. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	result = MAP_PRIMARY->get(key_12, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 12)) {
		DEEP_LOG(ERROR, OTHER, "9. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_1, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(key_0, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 1)) {
		DEEP_LOG(ERROR, OTHER, "10. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	result = MAP_PRIMARY->get(key_1, &DATA, RealTimeMap<int>::EXACT_OR_NEXT, &retkey, tx);
	if ((result == false) || (retkey != 1)) {
		DEEP_LOG(ERROR, OTHER, "11. get exact/next failed, bad value: %d\n", retkey);
		exit(-1);
	}

	// reset the MAP
	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testTxPrevious() {
	DEEP_LOG(INFO, OTHER, "TESTING PREVIOUS...\n");

	clear();

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	Transaction* tx_c = Transaction::create();

	int retkey;

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_1, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_5, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_10, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	// TX PUT
	MAP_PRIMARY->put(key_4, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_6, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_11, &DATA, RealTimeMap<int>::UNIQUE, tx);

	boolean result = MAP_PRIMARY->get(key_11, &DATA, RealTimeMap<int>::PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 10)) {
		DEEP_LOG(ERROR, OTHER, "1. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	result = MAP_PRIMARY->get(key_10, &DATA, RealTimeMap<int>::PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 6)) {
		DEEP_LOG(ERROR, OTHER, "2. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_10, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_11, &DATA, RealTimeMap<int>::PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 6)) {
		DEEP_LOG(ERROR, OTHER, "3. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	result = MAP_PRIMARY->get(key_10, &DATA, RealTimeMap<int>::PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 6)) {
		DEEP_LOG(ERROR, OTHER, "4. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_6, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_10, &DATA, RealTimeMap<int>::PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 5)) {
		DEEP_LOG(ERROR, OTHER, "5. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED REMOVE
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->remove(key_5, &DATA, RealTimeMap<int>::DELETE_RETURN, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(key_10, &DATA, RealTimeMap<int>::PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 4)) {
		DEEP_LOG(ERROR, OTHER, "6. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(key_4, &DATA, RealTimeMap<int>::PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 2)) {
		DEEP_LOG(ERROR, OTHER, "7. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testTxExactOrPrevious() {
	DEEP_LOG(INFO, OTHER, "TESTING EXACT OR PREVIOUS...\n");

	clear();

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	Transaction* tx_c = Transaction::create();

	int retkey;

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_1, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_5, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_10, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	// TX PUT
	MAP_PRIMARY->put(key_4, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_6, &DATA, RealTimeMap<int>::UNIQUE, tx);
	MAP_PRIMARY->put(key_11, &DATA, RealTimeMap<int>::UNIQUE, tx);

	boolean result = MAP_PRIMARY->get(key_11, &DATA, RealTimeMap<int>::EXACT_OR_PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 11)) {
		DEEP_LOG(ERROR, OTHER, "1. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_11, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_11, &DATA, RealTimeMap<int>::EXACT_OR_PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 10)) {
		DEEP_LOG(ERROR, OTHER, "2. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED REMOVE
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->remove(key_10, &DATA, RealTimeMap<int>::DELETE_RETURN, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(key_11, &DATA, RealTimeMap<int>::EXACT_OR_PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 6)) {
		DEEP_LOG(ERROR, OTHER, "3. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	result = MAP_PRIMARY->get(key_10, &DATA, RealTimeMap<int>::EXACT_OR_PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 6)) {
		DEEP_LOG(ERROR, OTHER, "4. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_6, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_10, &DATA, RealTimeMap<int>::EXACT_OR_PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 5)) {
		DEEP_LOG(ERROR, OTHER, "5. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(key_3, &DATA, RealTimeMap<int>::EXACT_OR_PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 2)) {
		DEEP_LOG(ERROR, OTHER, "6. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT_OR_PREVIOUS, &retkey, tx);
	if ((result == false) || (retkey != 2)) {
		DEEP_LOG(ERROR, OTHER, "7. get previous failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testTxFirst() {
	DEEP_LOG(INFO, OTHER, "TESTING FIRST...\n");

	clear();

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	Transaction* tx_c = Transaction::create();

	int retkey;

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_3, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_4, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_5, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	boolean result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::FIRST, &retkey, tx);
	if ((result == false) || (retkey != 3)) {
		DEEP_LOG(ERROR, OTHER, "1. first failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED REMOVE
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->remove(key_3, &DATA, RealTimeMap<int>::DELETE_RETURN, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::FIRST, &retkey, tx);
	if ((result == false) || (retkey != 4)) {
		DEEP_LOG(ERROR, OTHER, "2. first failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_4, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::FIRST, &retkey, tx);
	if ((result == false) || (retkey != 5)) {
		DEEP_LOG(ERROR, OTHER, "3. first failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX PUT
	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::FIRST, &retkey, tx);
	if ((result == false) || (retkey != 2)) {
		DEEP_LOG(ERROR, OTHER, "4. first failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX PUT
	MAP_PRIMARY->put(key_1, &DATA, RealTimeMap<int>::UNIQUE, tx);

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::FIRST, &retkey, tx);
	if ((result == false) || (retkey != 1)) {
		DEEP_LOG(ERROR, OTHER, "5. first failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_0, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::FIRST, &retkey, tx);
	if ((result == false) || (retkey != 0)) {
		DEEP_LOG(ERROR, OTHER, "6. first failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testTxLast() {
	DEEP_LOG(INFO, OTHER, "TESTING LAST...\n");

	clear();

	Transaction* tx = Transaction::create();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	Transaction* tx_c = Transaction::create();

	int retkey;

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_4, &DATA, RealTimeMap<int>::UNIQUE, tx_c);
	MAP_PRIMARY->put(key_5, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	boolean result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::LAST, &retkey, tx);
	if ((result == false) || (retkey != 5)) {
		DEEP_LOG(ERROR, OTHER, "1. last failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED REMOVE
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->remove(key_5, &DATA, RealTimeMap<int>::DELETE_RETURN, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::LAST, &retkey, tx);
	if ((result == false) || (retkey != 4)) {
		DEEP_LOG(ERROR, OTHER, "2. last failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX REMOVE
	MAP_PRIMARY->remove(key_4, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::LAST, &retkey, tx);
	if ((result == false) || (retkey != 2)) {
		DEEP_LOG(ERROR, OTHER, "3. last failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX PUT
	MAP_PRIMARY->put(key_3, &DATA, RealTimeMap<int>::UNIQUE, tx);

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::LAST, &retkey, tx);
	if ((result == false) || (retkey != 3)) {
		DEEP_LOG(ERROR, OTHER, "4. last failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// TX PUT
	MAP_PRIMARY->put(key_6, &DATA, RealTimeMap<int>::UNIQUE, tx);

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::LAST, &retkey, tx);
	if ((result == false) || (retkey != 6)) {
		DEEP_LOG(ERROR, OTHER, "5. last failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_7, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	result = MAP_PRIMARY->get(-1, &DATA, RealTimeMap<int>::LAST, &retkey, tx);
	if ((result == false) || (retkey != 7)) {
		DEEP_LOG(ERROR, OTHER, "6. last failed, tx read, bad value: %d\n", retkey);
		exit(-1);
	}

	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginBeginRemove() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / BEGIN / REMOVE -- ASSOCIATE...\n");

	clear();

	Transaction* tx_c = Transaction::create();

	// COMMITTED PUT
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	Transaction* tx = Transaction::create();

	// master transaction
	tx->begin();
	MAP_PRIMARY->associate(tx);

	inttype retkey;
	boolean result;

	// lock in master transaction
	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx, RealTimeMap<int>::LOCK_WRITE);
	if ((result == false) || (retkey != key_2)) {
		DEEP_LOG(ERROR, OTHER, "1. get failed: %d\n", key_2);
		exit(1);
	}

	// sub-transaction
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// remove in sub-transaction
	MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::DELETE_RETURN, tx, RealTimeMap<int>::LOCK_NONE);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "2. failed (before commit sub-transaction), %d still present but was removed in sub-transaction\n", key_2);
		exit(1);
	}

	// commit sub-transaction
	tx->commit(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "3. failed (after commit sub-transaction), %d still present but was removed in sub-transaction\n", key_2);
		exit(1);
	}

	// commit master transaction
	tx->commit(tx->getLevel());

	// COMMITTED GET
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx_c);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "4. failed (after commit master transaction), %d still present but was removed in sub-transaction\n", key_2);
		exit(1);
	}

	tx_c->commit(tx_c->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginBeginPut() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / BEGIN / PUT -- ASSOCIATE...\n");

	clear();

	Transaction* tx = Transaction::create();

	tx->begin();
	MAP_PRIMARY->associate(tx);

	for (int i=0; i<1000; i++) {

		int* p = &i;
		bytearray data = DATA;
		memcpy(data, p, sizeof(int));

		tx->begin();
		MAP_PRIMARY->associate(tx);

		MAP_PRIMARY->put(i, &DATA, RealTimeMap<int>::UNIQUE, tx);

		if ((i % 200) == 0) {
			tx->rollback(tx->getLevel());

		} else {
			tx->commit(tx->getLevel());
		}
	}

	tx->commit(tx->getLevel());

	Transaction::destroy(tx);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginPutRollback(boolean subtx) {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / PUT / ROLLBACK -- SUB-TRANSACTION: %d\n", subtx);

	inttype retkey;
	boolean result;

	clear();

	Transaction* tx = Transaction::create();
	Transaction* tx_c = Transaction::create();

	// level 0
	tx->begin();
	if (subtx == true) {
		tx->begin();
	}
	MAP_PRIMARY->associate(tx);

	// put
	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed put, %d\n", key_2);
		exit(1);
	}

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after put, %d not present\n", key_2);
		exit(1);
	}

	if (subtx == true) {
		tx->rollback(tx->getLevel());

		result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
		if (result == true) {
			DEEP_LOG(ERROR, OTHER, "failed after subtx rollback, %d still present but subtx was rolled back\n", key_2);
			exit(1);
		}

		tx->begin();
		MAP_PRIMARY->associate(tx);

		// put
		result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);
		if (result == false) {
			DEEP_LOG(ERROR, OTHER, "failed 2nd put, %d\n", key_2);
			exit(1);
		}

		result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
		if (result == false) {
			DEEP_LOG(ERROR, OTHER, "failed after 2nd put, %d not present\n", key_2);
			exit(1);
		}

		tx->rollback(tx->getLevel());

		result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
		if (result == true) {
			DEEP_LOG(ERROR, OTHER, "failed after 2nd subtx rollback, %d still present but 2nd subtx was rolled back\n", key_2);
			exit(1);
		}
	}

	// rollback level 0
	tx->rollback(tx->getLevel());

	// COMMITTED GET
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx_c);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d still present but put was rolled back\n", key_2);
		exit(1);
	}

	tx_c->commit(tx_c->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginPutUpdateRollbackBeginPut() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / PUT / UPDATE / ROLLBACK / BEGIN / PUT / ROLLBACK\n");

	inttype retkey;
	boolean result;

	clear();

	Transaction* tx = Transaction::create();
	Transaction* tx_c = Transaction::create();

	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	// level 0
	tx->begin();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// put
	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);

	tx->commit(tx->getLevel());

	tx->begin();
	MAP_PRIMARY->associate(tx);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx, RealTimeMap<int>::LOCK_WRITE);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after put, %d not present\n", key_2);
		exit(1);
	}

	p = &key_3;
	data = DATA2;
	memcpy(data, p, sizeof(int));

	// update
	result = MAP_PRIMARY->put(key_2, &DATA2, RealTimeMap<int>::EXISTING, tx, RealTimeMap<int>::LOCK_NONE);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed put existing, %d not present\n", key_2);
		exit(1);
	}

	tx->commit(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after put, %d not present\n", key_2);
		exit(1);
	}

	// rollback level 0
	tx->rollback(tx->getLevel());

	// COMMITTED GET
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx_c);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d still present but put was rolled back\n", key_2);
		exit(1);
	}

	tx_c->commit(tx_c->getLevel());

	p = &key_2;
	data = DATA2;
	memcpy(data, p, sizeof(int));

	// level 0
	tx->begin();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// put
	result = MAP_PRIMARY->put(key_2, &DATA2, RealTimeMap<int>::UNIQUE, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed during put, %d\n", key_2);
		exit(1);
	}

	tx->commit(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA2, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after put, %d not present\n", key_2);
		exit(1);
	}

	// rollback level 0
	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testSameLevelBeginPutUpdate() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / PUT / UPDATE / GET \n");

	inttype retkey;
	boolean result;

	clear();

	Transaction* tx = Transaction::create();

	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	// level 0
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// put
	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);

	// update
	result = MAP_PRIMARY->put(key_2, &DATA2, RealTimeMap<int>::EXISTING, tx, RealTimeMap<int>::LOCK_NONE);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed put existing, %d not present\n", key_2);
		exit(1);
	}

	// commit 0
	tx->commit(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after put, %d not present\n", key_2);
		exit(1);
	}

	Transaction::destroy(tx);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginUpdateBeginUpdateBeginRemoveBeginPut() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / UPDATE / BEGIN / UPDATE / BEGIN / REMOVE / BEGIN / PUT / ROLLBACK\n");

	inttype retkey;
	boolean result;

	Transaction* tx_c = Transaction::create();

	clear();

	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	// COMMITTED put
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	Transaction* tx = Transaction::create();

	p = &key_3;
	data = DATA;
	memcpy(data, p, sizeof(int));

	tx->begin();
	MAP_PRIMARY->associate(tx);

	// update
	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::EXISTING, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed during put existing in level %d\n", tx->getLevel());
		exit(1);
	}

	p = &key_4;
	data = DATA;
	memcpy(data, p, sizeof(int));

	tx->begin();
	MAP_PRIMARY->associate(tx);

	// update
	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::EXISTING, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed during put existing in level %d\n", tx->getLevel());
		exit(1);
	}

	tx->begin();
	MAP_PRIMARY->associate(tx);

	// remove
	result = MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed during remove in level %d\n", tx->getLevel());
		exit(1);
	}

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after remove, %d still present but was removed in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	p = &key_3;
	data = DATA;
	memcpy(data, p, sizeof(int));

	tx->begin();
	MAP_PRIMARY->associate(tx);

	// put
	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed put, %d duplicate secondary key\n", key_3);
		exit(1);
	}

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after put, %d not present in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	tx->rollback(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d still present but put was rolled back in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	tx->rollback(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d not present but remove was rolled back in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	tx->rollback(tx->getLevel());

	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}
void testBeginUpdateBeginUpdateCommitBeginPut() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / UPDATE / BEGIN / UPDATE / COMMIT / BEGIN / PUT / ROLLBACK\n");

	inttype retkey;
	boolean result;

	Transaction* tx_c = Transaction::create();

	clear();

	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	// COMMITTED put (secondary is 2)
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	Transaction* tx = Transaction::create();

	p = &key_3;
	data = DATA;
	memcpy(data, p, sizeof(int));

	tx->begin();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// update (secondary is 3)
	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::EXISTING, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed during put existing in level %d\n", tx->getLevel());
		exit(1);
	}

	tx->commit(tx->getLevel());

	p = &key_4;
	data = DATA;
	memcpy(data, p, sizeof(int));

	tx->begin();
	MAP_PRIMARY->associate(tx);

	// update (secondary is 4)
	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::EXISTING, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed during put existing in level %d\n", tx->getLevel());
		exit(1);
	}

	tx->commit(tx->getLevel());

	// commit master
	tx->commit(tx->getLevel());

	p = &key_3;
	data = DATA;
	memcpy(data, p, sizeof(int));

	tx->begin();
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// put (secondary is 3) should be OK
	result = MAP_PRIMARY->put(key_1, &DATA, RealTimeMap<int>::UNIQUE, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed put, %d duplicate secondary key\n", key_3);
		exit(1);
	}

	tx->commit(tx->getLevel());

	result = MAP_PRIMARY->get(key_1, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after put, %d not present in level %d\n", key_1, tx->getLevel());
		exit(1);
	}

	tx->rollback(tx->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginRemoveRollback() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / REMOVE / ROLLBACK\n");

	inttype retkey;
	boolean result;

	Transaction* tx_c = Transaction::create();

	clear();

	// COMMITTED put
	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	Transaction* tx = Transaction::create();

	// level 0
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// remove
	MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after remove, %d still present but was removed in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	// rollback level 0
	tx->rollback(tx->getLevel());

	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx_c);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d not present but remove was rolled back\n", key_2);
		exit(1);
	}

	tx_c->commit(tx_c->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginPutBeginRemoveRollback() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / PUT / BEGIN / REMOVE / ROLLBACK / ROLLBACK\n");

	inttype retkey;
	boolean result;

	clear();

	Transaction* tx = Transaction::create();
	Transaction* tx_c = Transaction::create();

	// level 0
	tx->begin();
	MAP_PRIMARY->associate(tx);

	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	// put
	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after put, %d not present in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	// level 1
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// remove
	MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after remove, %d still present but was removed in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	// rollback level 1
	tx->rollback(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d not present but remove was rolled back\n", key_2);
		exit(1);
	}

	// rollback level 0
	tx->rollback(tx->getLevel());

	// COMMITTED GET
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx_c);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d still present but put was rolled back\n", key_2);
		exit(1);
	}

	tx_c->commit(tx_c->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginRemoveBeginPutRollback() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / REMOVE / BEGIN / PUT / ROLLBACK / ROLLBACK\n");

	inttype retkey;
	boolean result;

	Transaction* tx_c = Transaction::create();

	clear();

	// COMMITTED put
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	Transaction* tx = Transaction::create();

	// level 0
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// remove
	MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after remove, %d still present but was removed in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	// level 1
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// put
	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after put, %d not present in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	// rollback level 1
	tx->rollback(tx->getLevel());

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d still present but put was rolled back\n", key_2);
		exit(1);
	}

	// rollback level 0
	tx->rollback(tx->getLevel());

	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx_c);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d not present but remove was rolled back\n", key_2);
		exit(1);
	}

	tx_c->commit(tx_c->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginRemoveBeginRemoveRollback() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / REMOVE / BEGIN / REMOVE / ROLLBACK / ROLLBACK\n");

	inttype retkey;
	boolean result;

	Transaction* tx_c = Transaction::create();

	clear();

	// COMMITTED put
	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	p = &key_3;
	data = DATA;
	memcpy(data, p, sizeof(int));

	MAP_PRIMARY->put(key_3, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	Transaction* tx = Transaction::create();

	// level 0
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// remove
	MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after remove, %d still present but was removed in level %d\n", key_2, tx->getLevel());
		exit(1);
	}

	// level 1
	tx->begin();
	MAP_PRIMARY->associate(tx);

	// remove
	MAP_PRIMARY->remove(key_3, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);

	result = MAP_PRIMARY->get(key_3, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "failed after remove, %d still present but was removed in level %d\n", key_3, tx->getLevel());
		exit(1);
	}

	// rollback level 1
	tx->rollback(tx->getLevel());

	result = MAP_PRIMARY->get(key_3, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d not present but remove was rolled back\n", key_3);
		exit(1);
	}

	// rollback level 0
	tx->rollback(tx->getLevel());

	// COMMITTED GET
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	result = MAP_PRIMARY->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retkey, tx_c);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d not present but remove was rolled back\n", key_2);
		exit(1);
	}

	result = MAP_PRIMARY->get(key_3, &DATA, RealTimeMap<int>::EXACT, &retkey, tx_c);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed after rollback, %d not present but remove was rolled back\n", key_3);
		exit(1);
	}

	tx_c->commit(tx_c->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

void testBeginRemoveBeginPutBeginUpdateRollback() {
	DEEP_LOG(INFO, OTHER, "TESTING BEGIN / REMOVE / BEGIN / PUT / BEGIN / UPDATE / ROLLBACK\n");

	inttype retkey;
	boolean result;

	Transaction* tx_c = Transaction::create();

	clear();

	int* p = &key_2;
	bytearray data = DATA;
	memcpy(data, p, sizeof(int));

	// COMMITTED put (secondary is 2)
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx_c);

	tx_c->commit(tx_c->getLevel());

	Transaction* tx = Transaction::create();

	tx->begin();
	MAP_PRIMARY->associate(tx);

	result = MAP_PRIMARY->remove(key_2, &DATA, RealTimeMap<int>::DELETE_RETURN, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed during remove %d\n", key_2);
		exit(1);
	}

	tx->begin();
	MAP_PRIMARY->associate(tx);

	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::UNIQUE, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed during put %d\n", key_2);
		exit(1);
	}

	tx->begin();
	MAP_PRIMARY->associate(tx);

	p = &key_3;
	data = DATA;
	memcpy(data, p, sizeof(int));

	// update (secondary is 3)
	result = MAP_PRIMARY->put(key_2, &DATA, RealTimeMap<int>::EXISTING, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "failed during put existing in level %d\n", tx->getLevel());
		exit(1);
	}

	// TX get (secondary 3 should be present)
	result = MAP_SECONDARY->get(key_3, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == false) {
		DEEP_LOG(ERROR, OTHER, "get failed, %d not present as secondary key\n", key_3);
		exit(1);
	}

	/* XXX:
	// NON-TX get (secondary 3 should not be present)
	result = MAP_SECONDARY->get(key_3, &DATA, RealTimeMap<int>::EXACT, &retkey, null);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "get succeeded but should have failed, %d is present as secondary key outside of tx\n", retkey);
		exit(1);
	}
	*/

	tx->rollback(tx->getLevel());

	// TX get (secondary 3 should be gone)
	result = MAP_SECONDARY->get(key_3, &DATA, RealTimeMap<int>::EXACT, &retkey, tx);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "get succeeded but should have failed, %d is present as secondary key after rollback\n", retkey);
		exit(1);
	}

	tx->rollback(tx->getLevel());
	tx->rollback(tx->getLevel());

	// NON-TX get (secondary 3 should not be present)
	tx_c->begin();
	MAP_PRIMARY->associate(tx_c);

	result = MAP_SECONDARY->get(key_3, &DATA, RealTimeMap<int>::EXACT, &retkey, tx_c);
	if (result == true) {
		DEEP_LOG(ERROR, OTHER, "get succeeded but should have failed, %d is present as secondary key outside of tx\n", retkey);
		exit(1);
	}

	tx_c->commit(tx_c->getLevel());

	Transaction::destroy(tx);
	Transaction::destroy(tx_c);

	DEEP_LOG(INFO, OTHER, "SUCCESS\n");
}

