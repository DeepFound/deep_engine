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

static int NUM_THREADS = 32;

static AtomicInteger CLIENTS_RUNNING;

static AtomicInteger CHECK;

static int COUNT = 1000;
static int DATA_SIZE = (2 * sizeof(inttype));

RealTimeMap<inttype>* PRIMARY1;
RealTimeMap<inttype>* PRIMARY2;

Comparator<inttype>* COMPARATOR;
KeyBuilder<inttype>* KEY_BUILDER;

inttype KEY = 1;

void startup(bool del);
void shutdown();
void setup();

class TestThread : public Runnable {
	private:
		inttype m_clientNum;

		Transaction* m_transaction;

	public:
		TestThread(int num) :
			m_clientNum(num) {

			m_transaction = Transaction::create();
		}

		virtual ~TestThread() {
			Transaction::destroy(m_transaction);
		}

		virtual void run() {
			test();

			CLIENTS_RUNNING.getAndDecrement();
		}

		void test() {
			for (inttype i = 1; i <= COUNT; i++) {
				inttype retkey = 0;

				nbyte* value = new nbyte(DATA_SIZE);
				value->zero();

				m_transaction->begin();

				PRIMARY1->associate(m_transaction);
				PRIMARY2->associate(m_transaction);

				if (PRIMARY1->get(KEY, value, RealTime::EXACT, &retkey, m_transaction, RealTime::LOCK_WRITE) == false) {
					printf("FAILED - CLIENT(%d) Primary get %d - error code: %d\n", m_clientNum, KEY, PRIMARY1->getErrorCode());
					abort();
				}

				if (CHECK.incrementAndGet() != 1) {
					printf("FAILED - CLIENT(%d) increment CHECK\n", m_clientNum);
					abort();
				}

				inttype nextKey = *((inttype*) (((bytearray) *value) + sizeof(inttype)));
				inttype newNextKey = nextKey + 1;

				nbyte* newvalue = new nbyte(DATA_SIZE);

				memcpy(((bytearray) *newvalue), &KEY, sizeof(inttype));
				memcpy(((bytearray) *newvalue) + sizeof(inttype), &newNextKey, sizeof(inttype));

				// printf("CLIENT(%d) Attempt insert of next key : %d\n", m_clientNum, newNextKey);

				if (PRIMARY1->put(KEY, newvalue, RealTime::EXISTING, m_transaction, RealTime::LOCK_NONE) == false) {
					printf("FAILED - CLIENT(%d) Primary update %d - error code: %d\n", m_clientNum, nextKey, PRIMARY1->getErrorCode());
					abort();
				}

				value->zero();
				memcpy(((bytearray) *value), &nextKey, sizeof(inttype));
				memcpy(((bytearray) *value) + sizeof(inttype), &nextKey, sizeof(inttype));

				if (PRIMARY2->put(nextKey, value, RealTimeMap<nbyte*>::UNIQUE, m_transaction) == false) {
					printf("FAILED - CLIENT(%d) put %d - error code: %d\n", m_clientNum, nextKey, PRIMARY2->getErrorCode());
					abort();
				}

				if (CHECK.decrementAndGet() != 0) {
					printf("FAILED - CLIENT(%d) decrement CHECK\n", m_clientNum);
					abort();
				}

				// commit work on both datastores
				m_transaction->commit(m_transaction->getLevel());

				delete value;
				delete newvalue;

				if (m_transaction->getWaitFlag() == true) {
					printf("FAILED - CLIENT(%d) WAIT flag still present\n", m_clientNum);
					abort();
				}

				Thread::sleep((rand() % 10) + 1);
			}
		}
};

int main(int argc, char** argv) {

	TestThread** clients = new TestThread*[NUM_THREADS];
	Thread** threads = new Thread*[NUM_THREADS];

	startup(true);

	setup();

	printf("%d CLIENTs\n", NUM_THREADS);

	srand(System::currentTimeMillis());

	for (int i = 0; i < NUM_THREADS; i++) {
		CLIENTS_RUNNING.getAndIncrement();

		clients[i] = new TestThread(i);
		threads[i] = new Thread(clients[i]);
		threads[i]->start();
	}

	do {
		printf("    Waiting for %d client(s)...\n", CLIENTS_RUNNING.get());

		Thread::sleep(1000);

	} while (CLIENTS_RUNNING.get() > 0);

	for (int i = 0; i < NUM_THREADS; i++) {
		delete clients[i];
		delete threads[i];
	}

	delete [] clients;
	delete [] threads;

	shutdown();

	return 0;
}

void startup(bool del) {
	longtype options = RealTimeMap<nbyte*>::O_CREATE | RealTimeMap<nbyte*>::O_DELETE | RealTimeMap<nbyte*>::O_ROWSTORE;
	options |= RealTimeMap<nbyte*>::O_KEYCOMPRESS;
	//options |= RealTimeMap<nbyte*>::O_VALUECOMPRESS;
	options |= RealTimeMap<nbyte*>::O_MEMORYCOMPRESS;

	COMPARATOR = new Comparator<inttype>();

	KEY_BUILDER = new KeyBuilder<inttype>();
	KEY_BUILDER->setUnpackLength(sizeof(inttype));
	KEY_BUILDER->setOffset(0);

	PRIMARY1 = new RealTimeMap<inttype>("./datastore1", options, sizeof(inttype), DATA_SIZE, COMPARATOR, KEY_BUILDER);
	PRIMARY2 = new RealTimeMap<inttype>("./datastore2", options, sizeof(inttype), DATA_SIZE, COMPARATOR, KEY_BUILDER);

	PRIMARY1->mount();
	PRIMARY1->recover(false);
	PRIMARY2->mount();
	PRIMARY2->recover(false);
}

void shutdown() {

	PRIMARY1->unmount(false);
	PRIMARY2->unmount(false);

	delete PRIMARY2;
	delete PRIMARY1;

	delete KEY_BUILDER;
	delete COMPARATOR;
}

void setup() {

	Transaction* transaction = Transaction::create();

	transaction->begin();

	PRIMARY1->associate(transaction);

	nbyte* value = new nbyte(DATA_SIZE);
	memcpy(((bytearray) *value), &KEY, sizeof(inttype));
	memcpy(((bytearray) *value) + sizeof(inttype), &KEY, sizeof(inttype));

	if (PRIMARY1->put(KEY, value, RealTimeMap<nbyte*>::UNIQUE, transaction) == false) {
		printf("FAILED - setup: %d\n", PRIMARY1->getErrorCode());
		exit(1);
	}

	transaction->commit(transaction->getLevel());

	Transaction::destroy(transaction);

	delete value;
}
