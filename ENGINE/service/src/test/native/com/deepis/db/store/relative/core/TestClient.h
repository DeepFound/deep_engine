#ifndef TEST_CLIENT_
#define TEST_CLIENT_

#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"

using namespace cxx::lang;
using namespace cxx::util;
using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::core;

class WorkItem {
	private:
		inttype m_operation;
		inttype m_key;
		inttype m_value;

		boolean m_response;

	public:
		enum Operation {
			PUT = 0,
			UPDATE,
			REMOVE,
			BEGIN,
			COMMIT,
			ROLLBACK,
			VERIFY_PRIMARY,
			VERIFY_SECONDARY
		};

		WorkItem(Operation operation, inttype key, inttype value) :
			m_operation(operation),
			m_key(key),
			m_value(value),
			m_response(false) {
		}

		inttype getOperation() const {
			return m_operation;
		}

		inttype getKey() const {
			return m_key;
		}

		inttype getValue() const {
			return m_value;
		}

		void setResponse(boolean response) {
			m_response = response;
		}

		boolean getResponse() const {
			return m_response;
		}
};

class Worker : public Synchronizable, public Runnable {

	private:
		RealTimeMap<inttype>* m_primary;
		RealTimeMap<inttype>* m_secondary;

		Transaction* m_tx;

		WorkItem* m_work;

		boolean m_exiting;
	
	public:
		Worker(RealTimeMap<inttype>* primary, RealTimeMap<inttype>* secondary, Transaction::Isolation isolation) :
			m_primary(primary),
			m_secondary(secondary),
			m_tx(Transaction::create()),
			m_work(null),
			m_exiting(false) {

			m_tx->setIsolation(isolation);
		}

		~Worker() {
			Transaction::destroy(m_tx);
		}

		void run() {
			while(true) {
				synchronized(this) {
					notifyAll();

					wait();
				}
	
				if (m_exiting == true) {
					break;
				}

				processWork();
			}
		}

		void stop() {
			m_exiting = true;

			synchronized(this) {
				notifyAll();
			}
		}

		boolean work(WorkItem::Operation operation, inttype key = -1, inttype value = -1) {
			m_work = new WorkItem(operation, key, value);

			synchronized(this) {
				notifyAll();

				wait();
			}

			boolean status = m_work->getResponse();

			delete m_work;
			m_work = null;

			return status;
		}

	private:
		void processWork() {
			switch(m_work->getOperation()) {
				case WorkItem::PUT:
					m_work->setResponse(put(m_work->getKey(), m_work->getValue()));
					break;

				case WorkItem::UPDATE:
					m_work->setResponse(update(m_work->getKey(), m_work->getValue()));
					break;

				case WorkItem::REMOVE:
					m_work->setResponse(remove(m_work->getKey()));
					break;

				case WorkItem::BEGIN:
					m_work->setResponse(begin());
					break;

				case WorkItem::COMMIT:
					m_work->setResponse(commit());
					break;

				case WorkItem::ROLLBACK:
					m_work->setResponse(rollback());
					break;

				case WorkItem::VERIFY_PRIMARY:
					m_work->setResponse(verifyPrimary(m_work->getKey(), m_work->getValue() ? true : false));
					break;

				case WorkItem::VERIFY_SECONDARY:
					m_work->setResponse(verifySecondary(m_work->getKey(), m_work->getValue() ? true : false));
					break;

				default:
					throw InvalidException("Unknown work item");
			}
		}

		boolean begin() {
			m_tx->begin();

			m_primary->associate(m_tx);

			return true;
		}

		boolean rollback() {
			m_tx->rollback(m_tx->getLevel());

			return true;
		}

		boolean commit() {
			m_tx->commit(m_tx->getLevel());

			return true;
		}

		boolean put(inttype key, inttype value) {
			nbyte data(sizeof(inttype));

			memcpy((bytearray) data, &value, sizeof(inttype));

			// XXX: statement tx
			begin();

			boolean status = m_primary->put(key, &data, RealTimeMap<inttype>::UNIQUE, m_tx);

			if (status == true) {
				commit();

			} else {
				rollback();
			}

			return status;
		}

		boolean update(inttype key, inttype value) {
			nbyte data(sizeof(inttype));

			memcpy((bytearray) data, &value, sizeof(inttype));

			// XXX: statement tx
			begin();

			boolean status = m_primary->put(key, &data, RealTimeMap<inttype>::EXISTING, m_tx);

			if (status == true) {
				commit();

			} else {
				rollback();
			}

			return status;
		}

		boolean remove(inttype key) {
			nbyte data(sizeof(inttype));

			// XXX: statement tx
			begin();

			boolean status = m_primary->remove(key, &data, RealTimeMap<inttype>::DELETE_RETURN, m_tx);

			if (status == true) {
				commit();

			} else {
				rollback();
			}

			return status;
		}

		boolean getPrimary(inttype key, inttype* value) {
			inttype retkey;
			nbyte data(sizeof(inttype));

			// XXX: statement tx
			begin();

			boolean status = m_primary->get(key, &data, RealTimeMap<inttype>::EXACT, &retkey, m_tx);
			if (status == true) {
				*value = *((inttype*) (bytearray) data);

				commit();

			} else {
				rollback();
			}

			return status;
		}

		boolean getSecondary(inttype key, inttype* value) {
			inttype retkey;
			nbyte data(sizeof(inttype));

			// XXX: statement tx
			begin();

			boolean status = m_secondary->get(key, &data, RealTimeMap<inttype>::EXACT, &retkey, m_tx);
			if (status == true) {
				*value = *((inttype*) (bytearray) data);

				commit();

			} else {
				rollback();
			}

			return status;
		}

		boolean verifyPrimary(inttype key, boolean expected) {
			inttype value;

			if (getPrimary(key, &value) != expected) {
				if (expected == true) {
					DEEP_LOG(ERROR, OTHER, "Key %d not visible from primary\n", key);

				} else {
					DEEP_LOG(ERROR, OTHER, "Key %d still visible from primary\n", key);
				}

				exit(1);
			}

			return true;
		}

		boolean verifySecondary(inttype key, boolean expected) {
			inttype value;

			if (getSecondary(key, &value) != expected) {
				if (expected == true) {
					DEEP_LOG(ERROR, OTHER, "Key %d not visible from secondary\n", key);

				} else {
					DEEP_LOG(ERROR, OTHER, "Key %d still visible from secondary\n", key);
				}

				exit(1);
			}
	
			return true;
		}
};

// All methods in this class intentionally block the caller.
//
// Transactions and threads have a 1 to 1 relationship.
// Each transaction requires a separate thread, although the actual test
// should run sequentially.
class TestClient {

	private:
		Thread* m_thread;
		Worker* m_worker;

	public:
		TestClient(RealTimeMap<inttype>* primary, RealTimeMap<inttype>* secondary, Transaction::Isolation isolation) {
			m_worker = new Worker(primary, secondary, isolation);
			m_thread = new Thread(m_worker);
		}

		~TestClient() {
			delete m_worker;
			delete m_thread;
		}

		void start() {
			m_thread->start();

			synchronized(m_worker) {
				m_worker->wait();
			}
		}

		void stop() {
			m_worker->stop();

			// XXX: join not supported in platform yet
			//m_thread->join();

			Thread::sleep(100);
		}

		boolean begin() {
			return m_worker->work(WorkItem::BEGIN);
		}

		boolean rollback() {
			return m_worker->work(WorkItem::ROLLBACK);
		}

		boolean commit() {
			return m_worker->work(WorkItem::COMMIT);
		}

		boolean put(inttype key, inttype value) {
			return m_worker->work(WorkItem::PUT, key, value);
		}

		boolean update(inttype key, inttype value) {
			return m_worker->work(WorkItem::UPDATE, key, value);
		}

		boolean remove(inttype key) {
			return m_worker->work(WorkItem::REMOVE, key);
		}

		boolean verifyPrimary(inttype key, boolean expected) {
			return m_worker->work(WorkItem::VERIFY_PRIMARY, key, expected ? 1 : 0);
		}

		boolean verifySecondary(inttype key, boolean expected) {
			return m_worker->work(WorkItem::VERIFY_SECONDARY, key, expected ? 1 : 0);
		}
};

#endif
