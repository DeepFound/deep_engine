#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"
#include "cxx/lang/Runnable.h"
#include "cxx/lang/Memory.h"

#include "cxx/util/Logger.h"

#include "cxx/util/concurrent/atomic/AtomicInteger.h"

#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent::atomic;
using namespace com::deepis::db::store::relative::core;

static AtomicInteger CLIENTS_RUNNING;

static const double KB = 1024;
static const double MB = KB * 1024;
static const double GB = MB * 1024;

static const double CACHE_SIZE         = 32 * GB;

static const inttype NUM_PRIMARY       = 128;         // num primaries, 128 max (see KEY_SIZES, also determines VALUE_SIZE)
static const inttype NUM_SECONDARY     = 2;           // num secondaries per primary, secondary keys are random strings
static const inttype NUM_THREADS       = 12;          // num clients, each client operates on it's own range of keys
static const inttype CHUNK_SIZE        = 1500;        // controls number of puts, updates, deletes, per tx
static const inttype SEED_ROWS_PER_MAP = 1000000;     // lower bounds, actual # of rows is based on NUM_THREADS, NUM_PRIMARY, etc...
static const inttype NUM_OPEN_MAPS     = NUM_PRIMARY; // num maps to leave open besides those currently active

static const inttype KEY_SIZES[]       = {8,  16, 32, 48, 64, 80,  96, 112, 128, 144, 160, 176, 192, 208, 224, 240,
                                          9,  17, 33, 49, 65, 81,  97, 113, 129, 145, 161, 177, 193, 209, 225, 241,
                                          10, 18, 34, 50, 66, 82,  98, 114, 130, 146, 162, 178, 194, 210, 226, 242,
                                          11, 19, 35, 51, 67, 83,  99, 115, 131, 147, 163, 179, 195, 211, 227, 243,
                                          12, 20, 36, 52, 68, 84, 100, 116, 132, 148, 164, 180, 196, 212, 228, 244,
                                          13, 21, 37, 53, 69, 85, 101, 117, 133, 149, 165, 181, 197, 213, 229, 245,
                                          14, 22, 38, 54, 70, 86, 102, 118, 134, 150, 166, 182, 198, 214, 230, 246,
                                          15, 23, 39, 55, 71, 87, 103, 119, 135, 151, 167, 183, 199, 215, 231, 247};

static const inttype SECONDARY_OFFSET  = KEY_SIZES[NUM_PRIMARY - 1];
static const inttype DATA_OFFSET       = SECONDARY_OFFSET * 2;  // 2 * largest key size
static const inttype VALUE_SIZE        = DATA_OFFSET + 256;

static const char CHARACTERS[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ~!@#$%^&*()_+-=`;':,./<>?[]\{}|";

RealTimeMap<nbyte*>* PRIMARY_MAPS[NUM_PRIMARY];
RealTimeMap<nbyte*>* SECONDARY_MAPS[NUM_SECONDARY][NUM_PRIMARY];

KeyBuilder<nbyte*>* KEY_BUILDERS[NUM_PRIMARY];
KeyBuilder<nbyte*>* SECONDARY_KEY_BUILDERS[NUM_PRIMARY];

Comparator<nbyte*>* COMPARATOR;

class TestThread : public Runnable {
	private:
		inttype m_clientNum;
		inttype m_currentPrimaryNum;

		Transaction* m_transaction;

	public:
		TestThread(inttype clientNum) :
			m_clientNum(clientNum),
			m_currentPrimaryNum(0) {

			m_transaction = Transaction::create();
		}

		virtual ~TestThread() {
			Transaction::destroy(m_transaction);
		}

		virtual void run() {
			test();

			CLIENTS_RUNNING.getAndDecrement();
		}

		inttype getCurrentPrimaryNum() {
			return m_currentPrimaryNum;
		}

		void test() {
			inttype start = m_clientNum * CHUNK_SIZE;

			inttype rows = SEED_ROWS_PER_MAP / NUM_THREADS;

			for (inttype primaryNum=0; primaryNum<NUM_PRIMARY; primaryNum++) {
				DEEP_LOG(INFO, OTHER, " -----------------  Client: %3d, Primary: %3d of %d --------------------\n",
					m_clientNum, primaryNum+1, NUM_PRIMARY);

				m_currentPrimaryNum = primaryNum;

				// double the number of rows every 16th map
				if ((primaryNum != 0) && ((primaryNum % 16) == 0)) {
					rows = rows * 2;
				}

				for(int i=0; i<rows; i+=CHUNK_SIZE) {
					doPut(primaryNum, start, CHUNK_SIZE);

					Thread::sleep(calculatePace());

					start += NUM_THREADS * CHUNK_SIZE;
				}
			}
		}

		inttype calculatePace() {
			return 500;
		}

		nbyte* createKey(inttype primaryNum, inttype keyVal) {
			nbyte* key = new nbyte(KEY_SIZES[primaryNum]);
			key->zero();

			memcpy((bytearray)*key, (char*) &keyVal, sizeof(keyVal));

			return key;
		}

		nbyte* createRandomString(inttype length) {
			nbyte* key = new nbyte(length);
			key->zero();

			for (inttype i=0; i<length; i++) {
				(*key)[i] = CHARACTERS[rand() % (sizeof(CHARACTERS)-1)];
			}

			return key;
		}

		nbyte* createRandomSizeString(inttype maxLength) {
			return createRandomString(rand() % maxLength);
		}

		nbyte* createRandomKey(inttype primaryNum) {
			return createRandomString(KEY_SIZES[primaryNum]);
		}

		nbyte* createValue(int primaryNum, nbyte* key) {
			inttype size = DATA_OFFSET;

			nbyte* data = null;

			inttype dataSize = VALUE_SIZE - DATA_OFFSET;
			if (dataSize > 0) {
				// create a large value for every 8th map
				if ((primaryNum != 0) && ((primaryNum % 8) == 0)) {
					dataSize += (2 * KB);
				}

				data = createRandomSizeString(dataSize);

				size += data->length;
			}

			nbyte* value = new nbyte(size);
			value->zero();

			memcpy(((bytearray) *value), ((bytearray) *key), key->length);

			nbyte* secondaryKey = createRandomKey(primaryNum);

			memcpy(((bytearray) *value) + SECONDARY_OFFSET, ((bytearray) *secondaryKey), secondaryKey->length);

			delete secondaryKey;

			if (data != null) {
				memcpy(((bytearray) *value) + DATA_OFFSET, ((bytearray) *data), data->length);

				delete data;
			}

			return value;
		}

		nbyte* updateValue(inttype primaryNum, nbyte* value) {
			nbyte* newSecondaryKey = createRandomKey(primaryNum);

			memcpy(((bytearray) *value) + SECONDARY_OFFSET, ((bytearray) *newSecondaryKey), newSecondaryKey->length);

			delete newSecondaryKey;

			return value;
		}

		void doPut(inttype primaryNum, inttype start, inttype chunk) {
			m_transaction->begin();

			PRIMARY_MAPS[primaryNum]->associate(m_transaction);

			for (inttype i=start; i<start+chunk; i++) {
				nbyte* key = createKey(primaryNum, i);
				nbyte* value = createValue(primaryNum, key);

				if (PRIMARY_MAPS[primaryNum]->put(key, value, RealTimeMap<nbyte*>::UNIQUE, m_transaction) == false) {
					abort();
				}

				delete key;
				delete value;
			}

			m_transaction->commit(m_transaction->getLevel());
		}

		void doUpdate(inttype start, inttype chunk) {
			m_transaction->begin();

			for (inttype primaryNum=0; primaryNum<NUM_PRIMARY; primaryNum++) {
				PRIMARY_MAPS[primaryNum]->associate(m_transaction);
			}

			nbyte* value = new nbyte(VALUE_SIZE);

			for (inttype i=start; i<start+chunk; i++) {
				for (inttype primaryNum=0; primaryNum<NUM_PRIMARY; primaryNum++) {

					if ((i % 13) == 0) {
						nbyte* key = createKey(primaryNum, i);

						if (PRIMARY_MAPS[primaryNum]->get(key, value, RealTime::EXACT, &key, m_transaction, RealTime::LOCK_WRITE) == true) {
							if (PRIMARY_MAPS[primaryNum]->put(key, updateValue(primaryNum, value), RealTime::EXISTING, m_transaction, RealTime::LOCK_NONE) == false) {
								abort();
							}

						} else {
							abort();
						}

						delete key;
					}
				}
			}

			delete value;

			m_transaction->commit(m_transaction->getLevel());
		}

		void doDelete(inttype start, inttype chunk) {
			m_transaction->begin();

			for (inttype primaryNum=0; primaryNum<NUM_PRIMARY; primaryNum++) {
				PRIMARY_MAPS[primaryNum]->associate(m_transaction);
			}

			nbyte* value = new nbyte(VALUE_SIZE);

			for (inttype i=start; i<start+chunk; i++) {
				for (inttype primaryNum=0; primaryNum<NUM_PRIMARY; primaryNum++) {

					if ((i % 17) == 0) {
						nbyte* key = createKey(primaryNum, i);

						if (PRIMARY_MAPS[primaryNum]->get(key, value, RealTime::EXACT, &key, m_transaction, RealTime::LOCK_WRITE) == true) {
							if (PRIMARY_MAPS[primaryNum]->remove(key, value, RealTime::DELETE_POPULATED, m_transaction, RealTime::LOCK_NONE) == false) {
								abort();
							}

						} else {
							abort();
						}

						delete key;
					}
				}
			}

			delete value;

			m_transaction->commit(m_transaction->getLevel());
		}
};

void startup() { 
	longtype options = RealTimeMap<nbyte*>::O_CREATE | RealTimeMap<nbyte*>::O_DELETE | RealTimeMap<nbyte*>::O_ROWSTORE;
	options |= RealTimeMap<nbyte*>::O_KEYCOMPRESS;
	//options |= RealTimeMap<nbyte*>::O_VALUECOMPRESS;
	options |= RealTimeMap<nbyte*>::O_MEMORYCOMPRESS;

	COMPARATOR = new Comparator<nbyte*>();

	for (inttype primaryNum=0; primaryNum<NUM_PRIMARY; primaryNum++) {
		inttype keySize = KEY_SIZES[primaryNum];

		KEY_BUILDERS[primaryNum] = new KeyBuilder<nbyte*>();
		KEY_BUILDERS[primaryNum]->setUnpackLength(keySize);
		KEY_BUILDERS[primaryNum]->setOffset(0);

		SECONDARY_KEY_BUILDERS[primaryNum] = new KeyBuilder<nbyte*>();
		SECONDARY_KEY_BUILDERS[primaryNum]->setUnpackLength(keySize);
		SECONDARY_KEY_BUILDERS[primaryNum]->setOffset(SECONDARY_OFFSET);

		char buf[256];
		sprintf(buf, "./key_%d_datastore", keySize);

		PRIMARY_MAPS[primaryNum] = new RealTimeMap<nbyte*>(buf, options, keySize, -1, COMPARATOR, KEY_BUILDERS[primaryNum]);

		for (inttype secondaryNum=0; secondaryNum<NUM_SECONDARY; secondaryNum++) {
			sprintf(buf, "./key_%d_datastore_secondary_%d", keySize, secondaryNum);

			SECONDARY_MAPS[secondaryNum][primaryNum] = new RealTimeMap<nbyte*>(buf, options, keySize, -1, COMPARATOR, SECONDARY_KEY_BUILDERS[primaryNum]);

			PRIMARY_MAPS[primaryNum]->associate(SECONDARY_MAPS[secondaryNum][primaryNum], false);
		}

		PRIMARY_MAPS[primaryNum]->mount();

		for (inttype secondaryNum=0; secondaryNum<NUM_SECONDARY; secondaryNum++) {
			SECONDARY_MAPS[secondaryNum][primaryNum]->mount();
		}

		PRIMARY_MAPS[primaryNum]->recover(false);
	}
}

void unmount(inttype primaryNum) {
	if (PRIMARY_MAPS[primaryNum] != null) {
		PRIMARY_MAPS[primaryNum]->unmount(false);

		for (inttype secondaryNum=0; secondaryNum<NUM_SECONDARY; secondaryNum++) {
			SECONDARY_MAPS[secondaryNum][primaryNum]->unmount(false);
			delete SECONDARY_MAPS[secondaryNum][primaryNum];
		}

		delete PRIMARY_MAPS[primaryNum];

		delete KEY_BUILDERS[primaryNum];
		delete SECONDARY_KEY_BUILDERS[primaryNum];

		PRIMARY_MAPS[primaryNum] = null;

		DEEP_LOG(INFO, OTHER, "Unmounted Primary %d\n", primaryNum);
	}
}

void shutdown() {
	for (inttype primaryNum=0; primaryNum<NUM_PRIMARY; primaryNum++) {
		unmount(primaryNum);
	}

	delete COMPARATOR;
}

int main(int argc, char** argv) {
	srand(System::currentTimeMillis());

	Logger::enableLevel(Logger::DEBUG);

	Properties::setCacheSize(CACHE_SIZE);

	//Properties::setDurable(true);
	//Properties::setDurableSyncInterval(1000);

	startup();

	TestThread** clients = new TestThread*[NUM_THREADS];
	Thread** threads = new Thread*[NUM_THREADS];

	for (inttype i=0; i<NUM_THREADS; i++) {
		CLIENTS_RUNNING.getAndIncrement();

		clients[i] = new TestThread(i + 1);
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	inttype lastUnmountedPrimaryNum = -1;

	do {
		DEEP_LOG(INFO, OTHER, "Waiting for %d clients...\n", CLIENTS_RUNNING.get());

		if (NUM_OPEN_MAPS < NUM_PRIMARY) {
			int currentPrimaryNum = clients[0]->getCurrentPrimaryNum();

			for (inttype i=1; i<NUM_THREADS; i++) {
				currentPrimaryNum = (clients[i]->getCurrentPrimaryNum() < currentPrimaryNum) ? 
					clients[i]->getCurrentPrimaryNum() : currentPrimaryNum;
			}

			for (inttype primaryNum=lastUnmountedPrimaryNum + 1; primaryNum<currentPrimaryNum - NUM_OPEN_MAPS; primaryNum++) {
				unmount(primaryNum);

				lastUnmountedPrimaryNum = primaryNum;
			}
		}

		Thread::sleep(5000);

	} while (CLIENTS_RUNNING.get() > 0);

	for (inttype i=0; i<NUM_THREADS; i++) {
		delete clients[i];
		delete threads[i];
	}

	delete [] clients;
	delete [] threads;

	shutdown();

	return 0;
}
