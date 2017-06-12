#include <zlib.h>
#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#define DEEP_FORCE_MEMORY_COMPRESSION
#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"
#undef DEEP_FORCE_MEMORY_COMPRESSION

using namespace cxx::lang;
using namespace cxx::util;
using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::core;

template class RealTimeMap<int>;
template class RealTimeMap<nbyte*>;
//fixed size key, fixed size value
static RealTimeMap<int>* FIXEDFIXEDMAP  = null;
//fixed size key, var size value
static RealTimeMap<int>* FIXEDVARMAP    = null;
//var size key, fixed size value
static RealTimeMap<nbyte*>* VARFIXEDMAP = null;
//var size key, var size value
static RealTimeMap<nbyte*>* VARVARMAP   = null;

//for var key maps
static Comparator<nbyte*>* COMPARATOR = null;
static KeyBuilder<nbyte*>* KEY_BUILDER = null;

static int CACHE_SIZE = 1000000; //1 MB
static int DATA_SIZE = 100;
static int KEY_SIZE = 25;
static int ROWS = 0;
static nbyte DATA(DATA_SIZE);

void startup(boolean del);
void shutdown(RealTimeMap<int>* map);
void shutdown(RealTimeMap<nbyte*>* map);

void createFixedFixedData();
void createFixedVarData();
void createVarFixedData();
void createVarVarData();

void verifyFixedFixedData();
void verifyFixedVarData();
void verifyVarFixedData();
void verifyVarVarData();

void setData(int key);
void setVarData(int key, int size);

int main(int argc, char** argv) {
	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);

	RealTimeResource::setInfinitelimit(false);
	DEEP_LOG(INFO, OTHER, " SETTING CACHE TO 1 MB\n");
	Properties::setCacheSize(CACHE_SIZE + (75000 /* .0750MB, see nano segments */));
	//Properties::setSegmentSize(Properties::DEFAULT_SEGMENT_SIZE * 10);

	DEEP_LOG(INFO, OTHER, " STARTING UP\n");
	startup(true);

	DEEP_LOG(INFO, OTHER, " CREATING 2X CACHE SIZE DATA (FIXED KEYS / FIXED VALUES)\n");
	createFixedFixedData();
	Thread::sleep(10000);

	DEEP_LOG(INFO, OTHER, " VERIFYING DATA (FIXED KEYS / FIXED VALUES)\n");
	verifyFixedFixedData();

	shutdown(FIXEDFIXEDMAP);

	DEEP_LOG(INFO, OTHER, " CREATING 2X CACHE SIZE DATA (FIXED KEYS / VAR VALUES)\n");
	createFixedVarData();
	Thread::sleep(1000);

	DEEP_LOG(INFO, OTHER, " VERIFYING DATA (FIXED KEYS / VAR VALUES)\n");
	verifyFixedVarData();

	shutdown(FIXEDVARMAP);

	DEEP_LOG(INFO, OTHER, " CREATING 2X CACHE SIZE DATA (VAR KEYS / FIXED VALUES)\n");
        createVarFixedData();
	Thread::sleep(1000);

        DEEP_LOG(INFO, OTHER, " VERIFYING DATA (VAR KEYS / FIXED VALUES)\n");
        verifyVarFixedData();

        shutdown(VARFIXEDMAP);

	DEEP_LOG(INFO, OTHER, " CREATING 2X CACHE SIZE DATA (VAR KEYS / VAR VALUES)\n");
        createVarVarData();
	Thread::sleep(1000);

        DEEP_LOG(INFO, OTHER, " VERIFYING DATA (VAR KEYS / VAR VALUES)\n");
        verifyVarVarData();

        shutdown(VARVARMAP);

	delete COMPARATOR;
	delete KEY_BUILDER;

	return 0;
}


void createFixedFixedData() {
	
	Transaction* tx = Transaction::create();
	tx->begin();
	FIXEDFIXEDMAP->associate(tx);

	int rowBytes = /*key size*/ sizeof(int) + /*value size*/ DATA_SIZE;
	ROWS = 3 * (CACHE_SIZE / rowBytes);

	for (int x = 0; x < ROWS; x++) {
		setData(x);
		FIXEDFIXEDMAP->put(x, &DATA, RealTimeMap<int>::UNIQUE, tx);

		if(x % 1000 == 0) {
			DEEP_LOG(INFO, OTHER, " COMMITTING AT KEY %d\n", x);
			tx->commit(tx->getLevel());
			tx->begin();
		}
	}

	tx->commit(tx->getLevel());
	Transaction::destroy(tx);
}

void verifyFixedFixedData() {

	int retKey;
	Transaction* tx = Transaction::create();
	tx->begin();
	FIXEDFIXEDMAP->associate(tx);

	for (int x = 0; x < ROWS; x++) {
		if(x % 1000 == 0) {
			DEEP_LOG(INFO, OTHER, " VERIFYING AT KEY %d\n", x);
		}
		if(FIXEDFIXEDMAP->get(x, &DATA, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED GET ON KEY %d\n", x);

			exit(-1);
		}
		int data = 0;
		memcpy(&data, DATA, sizeof(int));
		if(data != x) {
			DEEP_LOG(ERROR, OTHER, " BAD VALUE ON KEY %d: %d\n", x, data);
		}
	}

	tx->commit(tx->getLevel());
	Transaction::destroy(tx);
}

void createFixedVarData() {

        Transaction* tx = Transaction::create();
        tx->begin();
        FIXEDVARMAP->associate(tx);

        int rowBytes = /*key size*/ sizeof(int) + /* value size (min) */ 10;
        ROWS =  CACHE_SIZE / rowBytes;

	char bytes1[10];
	char bytes2[100];

	memset(bytes1, 1, 10);
	nbyte data1(bytes1, 10);

	memset(bytes2, 2, 100);
	nbyte data2(bytes2, 100);

        for (int x = 0; x < ROWS; x++) {
		
		nbyte* data = &data1;
		if((x % 10 == 0)) {
			data = &data2;
		}

                FIXEDVARMAP->put(x, data, RealTimeMap<int>::UNIQUE, tx);

                if(x % 1000 == 0) {
                        DEEP_LOG(INFO, OTHER, " COMMITTING AT KEY %d\n", x);
                        tx->commit(tx->getLevel());
                        tx->begin();
                }
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);
}

void verifyFixedVarData() {

        int retKey;
        Transaction* tx = Transaction::create();
        tx->begin();
        FIXEDVARMAP->associate(tx);

        for (int x = 0; x < ROWS; x++) {
                if(x % 1000 == 0) {
                        DEEP_LOG(INFO, OTHER, " VERIFYING AT KEY %d\n", x);
                }

		nbyte data(1);
                if(FIXEDVARMAP->get(x, &data, RealTimeMap<int>::EXACT, &retKey, tx, RealTimeMap<int>::LOCK_NONE, null) == false) {
                        DEEP_LOG(ERROR, OTHER, " FAILED GET ON KEY %d\n", x);

                        exit(-1);
                }
	
		if ((x % 10) == 0) {
                        if (data.length != 100) {
                                DEEP_LOG(ERROR, OTHER, "FAILED GET SIZE %d\n", x);
                                exit(-1);
                        }

			for(int i = 0; i < 100; i++) {
				if(data[i] != 2) {
					DEEP_LOG(ERROR, OTHER, "FAILED GET VALUE\n");
					exit(-1);
				}
			}

                } else {
                        if (data.length != 10) {
                                DEEP_LOG(ERROR, OTHER, "FAILED GET SIZE %d\n", x);
                                exit(-1);
                        }
			
			for(int i = 0; i < 10; i++) {
                                if(data[i] != 1) {
                                        DEEP_LOG(ERROR, OTHER, "FAILED GET VALUE\n");
					fprintf(stderr, "\ni %d val %d \n", i, data[i]);
                                        exit(-1);
                                }
                        }
                }
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);
}

void createVarFixedData() {

        Transaction* tx = Transaction::create();
        tx->begin();
        VARFIXEDMAP->associate(tx);

        int rowBytes = /*key size min*/ KEY_SIZE + /*value size*/ sizeof(int);
        ROWS =  CACHE_SIZE / rowBytes;

	char bytes[KEY_SIZE];

	for (int x = 0; x < ROWS; x++) {
		
		memset(bytes, 0, KEY_SIZE);
                sprintf(bytes, "%d", x);
                nbyte key(bytes, KEY_SIZE);

                setData(x);

                VARFIXEDMAP->put(&key, &DATA, RealTimeMap<nbyte*>::UNIQUE, tx);

                if(x % 1000 == 0) {
                        DEEP_LOG(INFO, OTHER, " COMMITTING AT KEY %d\n", x);
                        tx->commit(tx->getLevel());
                        tx->begin();
                }
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);
}

void verifyVarFixedData() {

        Transaction* tx = Transaction::create();
        tx->begin();
        VARFIXEDMAP->associate(tx);

	nbyte stack(KEY_SIZE);
        nbyte* retkey = &stack;

	char bytes[KEY_SIZE];

        for (int x = 0; x < ROWS; x++) {

                if(x % 1000 == 0) {
                        DEEP_LOG(INFO, OTHER, " VERIFYING AT KEY %d\n", x);
                }

		memset(bytes, 0, KEY_SIZE);
                sprintf(bytes, "%d", x);
                nbyte key(bytes, KEY_SIZE);

                if(VARFIXEDMAP->get(&key, &DATA, RealTimeMap<nbyte*>::EXACT, &retkey, tx) == false) {
                        DEEP_LOG(ERROR, OTHER, " FAILED GET ON KEY %d\n", x);

                        exit(-1);
                }
                int data = 0;
                memcpy(&data, DATA, sizeof(int));
                if(data != x) {
                        DEEP_LOG(ERROR, OTHER, " BAD VALUE ON KEY %d: %d\n", x, data);
                }
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);
}

void createVarVarData() {

        Transaction* tx = Transaction::create();
        tx->begin();
        VARVARMAP->associate(tx);

        int rowBytes = /*key size*/ KEY_SIZE + /* value size (min) */ 10;
        ROWS = CACHE_SIZE / rowBytes;

	char keyBytes[KEY_SIZE];

	char bytes1[10];
	char bytes2[100];

	memset(bytes1, 1, 10);
	nbyte data1(bytes1, 10);

	memset(bytes2, 2, 100);
	nbyte data2(bytes2, 100);

        for (int x = 0; x < ROWS; x++) {
		
		nbyte* data = &data1;
		if((x % 10 == 0)) {
			data = &data2;
		}

		memset(keyBytes, 0, KEY_SIZE);
                sprintf(keyBytes, "%d", x);
                nbyte key(keyBytes, KEY_SIZE);

                VARVARMAP->put(&key, data, RealTimeMap<nbyte*>::UNIQUE, tx);

                if(x % 1000 == 0) {
                        DEEP_LOG(INFO, OTHER, " COMMITTING AT KEY %d\n", x);
                        tx->commit(tx->getLevel());
                        tx->begin();
                }
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);
}

void verifyVarVarData() {

        Transaction* tx = Transaction::create();
        tx->begin();
        VARVARMAP->associate(tx);
	
	char keyBytes[KEY_SIZE];

	nbyte stack(KEY_SIZE);
        nbyte* retkey = &stack;

        for (int x = 0; x < ROWS; x++) {
                if(x % 1000 == 0) {
                        DEEP_LOG(INFO, OTHER, " VERIFYING AT KEY %d\n", x);
                }

		memset(keyBytes, 0, KEY_SIZE);
                sprintf(keyBytes, "%d", x);
                nbyte key(keyBytes, KEY_SIZE);

		nbyte data(1);
                if(VARVARMAP->get(&key, &data, RealTimeMap<nbyte*>::EXACT, &retkey, tx) == false) {
                        DEEP_LOG(ERROR, OTHER, " FAILED GET ON KEY %d\n", x);

                        exit(-1);
                }
	
		if ((x % 10) == 0) {
                        if (data.length != 100) {
                                DEEP_LOG(ERROR, OTHER, "FAILED GET SIZE %d\n", x);
                                exit(-1);
                        }

			for(int i = 0; i < 100; i++) {
				if(data[i] != 2) {
					DEEP_LOG(ERROR, OTHER, "FAILED GET VALUE\n");
					exit(-1);
				}
			}

                } else {
                        if (data.length != 10) {
                                DEEP_LOG(ERROR, OTHER, "FAILED GET SIZE %d\n", x);
                                exit(-1);
                        }
			
			for(int i = 0; i < 10; i++) {
                                if(data[i] != 1) {
                                        DEEP_LOG(ERROR, OTHER, "FAILED GET VALUE\n");
					fprintf(stderr, "\ni %d val %d \n", i, data[i]);
                                        exit(-1);
                                }
                        }
                }
        }

        tx->commit(tx->getLevel());
        Transaction::destroy(tx);
}

void startup(boolean del) {

	DEEP_LOG(INFO, OTHER, " STARTUP MAPS\n");

	longtype options = RealTimeMap<int>::O_KEYCOMPRESS | RealTimeMap<int>::O_CREATE | RealTimeMap<int>::O_SINGULAR | RealTimeMap<int>::O_FIXEDKEY | RealTimeMap<int>::O_MEMORYCOMPRESS;

	if (del == true) {
		options |= RealTimeMap<int>::O_DELETE;
	}

	Properties::setTransactionChunk(1000);

	FIXEDFIXEDMAP = new RealTimeMap<int>("./datastore1", options, sizeof(int), DATA_SIZE, null, null);
	FIXEDFIXEDMAP->mount();
	FIXEDFIXEDMAP->recover(false);

	FIXEDVARMAP = new RealTimeMap<int>("./datastore2", options, sizeof(int), /* var size values */ -1);
	FIXEDVARMAP->mount(); 
	FIXEDVARMAP->recover(false);

	COMPARATOR = new Comparator<nbyte*>();
	KEY_BUILDER = new KeyBuilder<nbyte*>();
	KEY_BUILDER->setUnpackLength(KEY_SIZE);

	VARFIXEDMAP = new RealTimeMap<nbyte*>("./datastore3", options, /*var size keys*/ -1, sizeof(int), COMPARATOR, KEY_BUILDER);
	VARFIXEDMAP->mount();
	VARFIXEDMAP->recover(false);

	VARVARMAP = new RealTimeMap<nbyte*>("./datastore4", options, /*var size keys*/ -1, /*var size values*/ -1, COMPARATOR, KEY_BUILDER);
	VARVARMAP->mount();
	VARVARMAP->recover(false);
}

void shutdown(RealTimeMap<int>* map) {
	
	DEEP_LOG(INFO, OTHER, " SHUTDOWN MAP\n");

        map->unmount(false);

        delete map;
        map = null;
}

void shutdown(RealTimeMap<nbyte*>* map) {
	
	DEEP_LOG(INFO, OTHER, " SHUTDOWN MAP\n");

        map->unmount(false);

        delete map;
        map = null;
}

void setData(int key) {

        int* p = &key;
        bytearray data = DATA;
	memcpy(data, p, sizeof(int));
}

