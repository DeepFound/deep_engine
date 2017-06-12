#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"
#include "com/deepis/db/store/relative/core/RealTimeIterator.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::core;

static int DATA_SIZE = sizeof(int);
static int key_0 = 0;
static int key_1 = 1;
static int key_2 = 2;
static int key_3 = 3;
static int key_4 = 4;
static int key_5 = 5;
static int key_6 = 6;
static int key_7 = 7;
static int key_8 = 8;
static int key_9 = 9;
static int key_10 = 10;

static nbyte DATA(DATA_SIZE);

template class RealTimeMap<int>;
static RealTimeMap<int>* MAP = null;
static RealTimeMap<int>* SEC = null;
static Comparator<int>* COMPARATOR = null;
static KeyBuilder<int>* KEY_BUILDER = null;
void setData(int key);

void startup(boolean del);
void shutdown();

void forceViewpoint(int key);
void endViewpoints();
void putTransaction(int key, bool existing);
void removeTransaction(int key);

void putRemove();
void verifyRemove(RealTimeMap<int>* map);

void putUpdate();
void verifyUpdate(RealTimeMap<int>* map);

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	//put remove primary

	startup(true);

	DEEP_LOG(INFO, OTHER, " PUT DELETE PRIMARY\n");
	putRemove();

	DEEP_LOG(INFO, OTHER, " VERIFY DELETE PRIMARY\n");
	verifyRemove(MAP);

	DEEP_LOG(INFO, OTHER, "  Wait 15 seconds to index\n");
        for (int i = 0; i < 16; i++) {
                DEEP_LOG(INFO, OTHER, "          ... waiting %d\n", i);
                Thread::sleep(1000);
        }
	
	DEEP_LOG(INFO, OTHER, " VERIFY DELETE PRIMARY AFTER INDEXING\n");
	verifyRemove(MAP);

	shutdown();

	DEEP_LOG(INFO, OTHER, "  Wait 3 seconds to complete irt write out before continuing\n");
	for (int i = 0; i < 3; i++) {
		DEEP_LOG(INFO, OTHER, "          ... waiting %d\n", i);
		Thread::sleep(1000);
	}

	startup(false);

	DEEP_LOG(INFO, OTHER, " VERIFY DELETE PRIMARY AFTER SHUTDOWN/STARTUP\n");
	verifyRemove(MAP);

	shutdown();
	
	//put remove secondary

	startup(true);

	DEEP_LOG(INFO, OTHER, " PUT DELETE SECONDARY\n");
	putRemove();

	DEEP_LOG(INFO, OTHER, " VERIFY DELETE SECONDARY\n");
	verifyRemove(SEC);

	DEEP_LOG(INFO, OTHER, "  Wait 15 seconds to index\n");
        for (int i = 0; i < 16; i++) {
                DEEP_LOG(INFO, OTHER, "          ... waiting %d\n", i);
                Thread::sleep(1000);
        }
        
        DEEP_LOG(INFO, OTHER, " VERIFY DELETE SECONDARY AFTER INDEXING\n");
        verifyRemove(SEC);

	shutdown();

	DEEP_LOG(INFO, OTHER, "  Wait 3 seconds to complete irt write out before continuing\n");
	for (int i = 0; i < 3; i++) {
		DEEP_LOG(INFO, OTHER, "          ... waiting %d\n", i);
		Thread::sleep(1000);
	}

	startup(false);

	DEEP_LOG(INFO, OTHER, " VERIFY DELETE SECONDARY AFTER SHUTDOWN/STARTUP\n");
	verifyRemove(SEC);

	shutdown();

	//put update primary
	
	startup(true);

        DEEP_LOG(INFO, OTHER, " PUT UPDATE PRIMARY\n");
        putUpdate();

        DEEP_LOG(INFO, OTHER, " VERIFY UPDATE PRIMARY\n");
        verifyUpdate(MAP);

	DEEP_LOG(INFO, OTHER, "  Wait 15 seconds to index\n");
        for (int i = 0; i < 16; i++) {
                DEEP_LOG(INFO, OTHER, "          ... waiting %d\n", i);
                Thread::sleep(1000);
        }
        
        DEEP_LOG(INFO, OTHER, " VERIFY UPDATE PRIMARY AFTER INDEXING\n");
        verifyUpdate(MAP);

        shutdown();

        DEEP_LOG(INFO, OTHER, "  Wait 3 seconds to complete irt write out before continuing\n");
        for (int i = 0; i < 3; i++) {
                DEEP_LOG(INFO, OTHER, "          ... waiting %d\n", i);
                Thread::sleep(1000);
        }

        startup(false);

        DEEP_LOG(INFO, OTHER, " VERIFY UPDATE PRIMARY AFTER SHUTDOWN/STARTUP\n");
        verifyUpdate(MAP);

        shutdown();

	//put update secondary

	startup(true);

        DEEP_LOG(INFO, OTHER, " PUT UPDATE SECONDARY\n");
        putUpdate();

        DEEP_LOG(INFO, OTHER, " VERIFY UPDATE SECONDARY\n");
        verifyUpdate(SEC);

	DEEP_LOG(INFO, OTHER, "  Wait 15 seconds to index\n");
        for (int i = 0; i < 16; i++) {
                DEEP_LOG(INFO, OTHER, "          ... waiting %d\n", i);
                Thread::sleep(1000);
        }
        
        DEEP_LOG(INFO, OTHER, " VERIFY UPDATE SECONDAARY AFTER INDEXING\n");
        verifyUpdate(SEC);

        shutdown();

        DEEP_LOG(INFO, OTHER, "  Wait 3 seconds to complete irt write out before continuing\n");
        for (int i = 0; i < 3; i++) {
                DEEP_LOG(INFO, OTHER, "          ... waiting %d\n", i);
                Thread::sleep(1000);
        }

        startup(false);

        DEEP_LOG(INFO, OTHER, " VERIFY UPDATE SECONDARY AFTER SHUTDOWN/STARTUP\n");
        verifyUpdate(SEC);

        shutdown();

	return 0;
}

void startup(boolean del) {

        DEEP_LOG(INFO, OTHER, " STARTUP MAP\n");

        // XXX: O_SINGULAR (no threading) is specified since berkley-db's default mode is non-thread (i.e. apples to apples, expect for one internal thread)
        longtype options = RealTimeMap<int>::O_KEYCOMPRESS | RealTimeMap<int>::O_CREATE | RealTimeMap<int>::O_SINGULAR | RealTimeMap<int>::O_FIXEDKEY;
        if (del == true) {
                options |= RealTimeMap<int>::O_DELETE;
        }

	COMPARATOR = new Comparator<int>();
        KEY_BUILDER = new KeyBuilder<int>();
        KEY_BUILDER->setOffset(0);

        MAP = new RealTimeMap<int>("./datastore", options, sizeof(int), DATA_SIZE, COMPARATOR, KEY_BUILDER);
	SEC = new RealTimeMap<int>("./secondary.datastore", options, sizeof(int), DATA_SIZE, COMPARATOR, KEY_BUILDER);

	MAP->associate(SEC, false);

        MAP->mount();
	SEC->mount();
	MAP->recover(false);
}

void shutdown() {

        DEEP_LOG(INFO, OTHER, " SHUTDOWN MAP\n");

        MAP->unmount(false);
	SEC->unmount(false);

        delete MAP;
        MAP = null;

	delete SEC;
	SEC = null;

	delete COMPARATOR;
	COMPARATOR = null;

	delete KEY_BUILDER;
	KEY_BUILDER = null;
}

// C = Created (anything not deleted)
// D = Deleted
// Dv = Deleted but being viewed by someone

// KEY CASE
// 0    C
// 1	D
// 2	D->D
// 3	D->D->C
// 4	Dv
// 5	Dv->Dv
// 6	Dv->Dv->C
// 7	D->Dv->C
// 8	Dv->D->C
// 9    D->D->D->D->D->D->D->D->D
// 10   D->D->D->D->D->D->D->D->D->C

void putRemove(){

	//case 0
	putTransaction(key_0, false);

	//case 1
   	putTransaction(key_1, false);
	removeTransaction(key_1);

	//case 2
	putTransaction(key_2, false);
	removeTransaction(key_2);
	putTransaction(key_2, false);
	removeTransaction(key_2);

	//case 3
	putTransaction(key_3, false);
	removeTransaction(key_3);
	putTransaction(key_3, false);
	removeTransaction(key_3);
	putTransaction(key_3, false);

	//case 4
	putTransaction(key_4, false);
	forceViewpoint(key_4);
	removeTransaction(key_4);

	//case 5
	putTransaction(key_5, false);
	forceViewpoint(key_5);
	removeTransaction(key_5);
	putTransaction(key_5, false);
	forceViewpoint(key_5);
	removeTransaction(key_5);

	//case 6
	putTransaction(key_6, false);
	forceViewpoint(key_6);
	removeTransaction(key_6);
	putTransaction(key_6, false);
	forceViewpoint(key_6);
	removeTransaction(key_6);
	putTransaction(key_6, false);

	//case 7
	putTransaction(key_7, false);
	removeTransaction(key_7);
	putTransaction(key_7, false);
	forceViewpoint(key_7);
	removeTransaction(key_7);
	putTransaction(key_7, false);

	//case 8
	putTransaction(key_8, false);
	forceViewpoint(key_8);
	removeTransaction(key_8);
	putTransaction(key_8, false);
	removeTransaction(key_8);
	putTransaction(key_8, false);

	//case 9
	for(int x = 0; x < 9; x++){
		putTransaction(key_9, false);
		removeTransaction(key_9);
	}

	//case 10
	for(int x = 0; x < 9; x++){
		putTransaction(key_10, false);
		removeTransaction(key_10);
	}
	putTransaction(key_10, false);
}

void verifyRemove(RealTimeMap<int>* map){

	int retKey;

        Transaction* tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

	//case 0
	if(map->get(key_0, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false){
		DEEP_LOG(ERROR, OTHER, " GET CASE 0 SHOULD SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 0\n");
	}
	//case 1
	if(map->get(key_1, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true){
		DEEP_LOG(ERROR, OTHER, " GET CASE 1 SHOULD NOT SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 1\n");
	}
	//case 2
	if(map->get(key_2, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true){
		DEEP_LOG(ERROR, OTHER, " GET CASE 2 SHOULD NOT SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 2\n");
	}

	//case 3
	if(map->get(key_3, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false){
		DEEP_LOG(ERROR, OTHER, " GET CASE 3 SHOULD SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 3\n");
	}
	//case 4
	if(map->get(key_4, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true){
		DEEP_LOG(ERROR, OTHER, " GET CASE 4 SHOULD NOT SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 4\n");
	}
	//case 5
	if(map->get(key_5, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true){
		DEEP_LOG(ERROR, OTHER, " GET CASE 5 SHOULD NOT SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 5\n");
	}
	//case 6
	if(map->get(key_6, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false){
		DEEP_LOG(ERROR, OTHER, " GET CASE 6 SHOULD SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 6\n");
	}
	//case 7
	if(map->get(key_7, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false){
		DEEP_LOG(ERROR, OTHER, " GET CASE 7 SHOULD SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 7\n");
	}
	//case 8
	if(map->get(key_8, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false){
		DEEP_LOG(ERROR, OTHER, " GET CASE 8 SHOULD SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 8\n");
	}
	//case 9
	if(map->get(key_9, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == true){
		DEEP_LOG(ERROR, OTHER, " GET CASE 9 SHOULD NOT SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 9\n");
	}
	//case 10
	if(map->get(key_10, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false){
		DEEP_LOG(ERROR, OTHER, " GET CASE 10 SHOULD SUCCEED\n");
		exit(-1);
	}
	else{
		DEEP_LOG(INFO, OTHER, " PASSED CASE 10\n");
	}
		
	tx->commit(tx->getLevel());
        Transaction::destroy(tx);
	endViewpoints();

}

// C = Created (anything not deleted)
// U = Force Deleting by Update

// KEY CASE
// 0	U->C
// 1	U->U->C
void putUpdate(){

	//case 0
	putTransaction(key_0, false);
	putTransaction(key_0, true);

	//case 1
	putTransaction(key_1, false);
	putTransaction(key_1, true);
	putTransaction(key_1, true);
}

void verifyUpdate(RealTimeMap<int>* map){

	int retKey;

        Transaction* tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        //case 0
        if(map->get(key_0, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false){
                DEEP_LOG(ERROR, OTHER, " GET CASE 0 SHOULD SUCCEED\n");
                exit(-1);
        }
        else{
                DEEP_LOG(INFO, OTHER, " PASSED CASE 0\n");
        }
        //case 1
        if(map->get(key_1, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false){
                DEEP_LOG(ERROR, OTHER, " GET CASE 1 SHOULD SUCCEED\n");
                exit(-1);
        }
        else{
                DEEP_LOG(INFO, OTHER, " PASSED CASE 1\n");
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);	
}

void setData(int key){

        int* p = &key;
        bytearray data = DATA;
        memcpy(data, p, sizeof(int));
}

Transaction* viewpointTx;
void forceViewpoint(int key){

	int retkey;

	if(viewpointTx == null){
		viewpointTx = Transaction::create();
		viewpointTx->setIsolation(Transaction::REPEATABLE);
		viewpointTx->begin();
		MAP->associate(viewpointTx);
	}

	MAP->get(key, &DATA, RealTimeMap<int>::EXACT, &retkey, viewpointTx, RealTimeMap<int>::LOCK_NONE, null);

}

void endViewpoints(){

	if(viewpointTx != null){	
		viewpointTx->commit(viewpointTx->getLevel());
		Transaction::destroy(viewpointTx);
		viewpointTx = null;
	}
}

void putTransaction(int key, bool existing){

        Transaction* tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        setData(key);
        MAP->put(key, &DATA, existing ? RealTimeMap<int>::EXISTING : RealTimeMap<int>::UNIQUE, tx);

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);
}

void removeTransaction(int key){

        Transaction* tx = Transaction::create();
        tx->begin();
        MAP->associate(tx);

        MAP->remove(key, &DATA, RealTimeMap<int>::DELETE_POPULATED, tx, RealTimeMap<int>::LOCK_WRITE);

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);
}
