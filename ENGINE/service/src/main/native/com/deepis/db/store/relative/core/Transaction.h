/**
 *    Copyright (C) 2010 Deep Software Foundation
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_TRANSACTION_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_TRANSACTION_H_ 

#include "cxx/util/Logger.h"

#include "cxx/util/HashSet.h"
#include "cxx/util/TreeMap.h"
#include "cxx/util/ArrayList.h"

#include "cxx/util/concurrent/Synchronize.h"
#include "cxx/util/concurrent/locks/ReentrantLock.h"

//#ifdef DEEP_USERLOCK
	#include "cxx/util/concurrent/locks/UserSpaceLock.h"
//#else
	//#include "cxx/util/concurrent/locks/ReentrantLock.h"
//#endif

#include "com/deepis/db/store/relative/util/BasicArray.h"
#include "com/deepis/db/store/relative/util/InvalidException.h"
#include "com/deepis/db/store/relative/util/ConcurrentObject.h"
#include "com/deepis/db/store/relative/util/LockableHashSet.h"
#include "com/deepis/db/store/relative/util/MeasuredRandomAccessFile.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/Information.h"
#include "com/deepis/db/store/relative/core/RealTimeAtomic.h"

using namespace cxx::util;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::locks;
using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

class Transaction;

class Conductor {

	CXX_LANG_MEMORY_DEBUG_DECL()

	protected:
		BasicArray<ConcurrentObject*> m_contexts;

		Transaction* m_transaction;

		boolean m_loadData:1;
		boolean m_override:1;
		boolean m_uniqueChecks:1;

		boolean m_reserved;
		ulongtype m_reservedKey;
		ulongtype m_reservedBlock;

		ulongtype m_transactionId;

	public:
		inttype m_mergeOptimize;
		inttype m_createStats;
		inttype m_deleteStats;
		inttype m_updateStats;
		#if 0
		inttype m_readStats;
		#endif
		longtype m_totalSize;
		longtype m_userSpaceSize;

	public:
		FORCE_INLINE Conductor(Transaction* transaction, inttype indexes):
			m_contexts(indexes, false),
			m_transaction(transaction),
			m_loadData(false),
			m_override(false),
			m_uniqueChecks(false),
			m_reserved(false),
			m_reservedKey(0),
			m_reservedBlock(0),
			m_transactionId(0) {

			m_mergeOptimize = 0;
			m_createStats = 0;
			m_deleteStats = 0;
			m_updateStats = 0;
			#if 0
			m_readStats = 0;
			#endif
			m_totalSize = 0;
			m_userSpaceSize = 0;

			CXX_LANG_MEMORY_DEBUG_INIT()
		}

		virtual ~Conductor(void) {
			CXX_LANG_MEMORY_DEBUG_CLEAR()
		}

		FORCE_INLINE Transaction* getTransaction(void) const {
			return (Transaction*) m_transaction;
		}

		FORCE_INLINE ConcurrentObject* getContext(inttype index) {
			return m_contexts.get(index);
		}

		FORCE_INLINE void setLoadData(boolean flag) {
			m_loadData = flag;
		}

		FORCE_INLINE boolean getLoadData(void) const {
			return m_loadData;
		}

		FORCE_INLINE void setOverride(boolean flag) {
			m_override = flag;
		}

		FORCE_INLINE boolean getOverride(void) const {
			return m_override;
		}

		FORCE_INLINE void setUniqueChecks(boolean flag) {
			m_uniqueChecks = flag;
		}

		FORCE_INLINE boolean getUniqueChecks(void) const {
			return m_uniqueChecks;
		}

		FORCE_INLINE void setReserved(boolean reserved) {
			m_reserved = reserved;
			if (m_reserved == false) {
				m_reservedKey = 0;
				m_reservedBlock = 0;
			}
		}

		FORCE_INLINE boolean getReserved(void) const {
			return (m_reserved == true) || (m_reservedBlock != 0);
		}

		FORCE_INLINE void setReservedKey(ulongtype reservedKey) {
			m_reservedKey = reservedKey;
		}

		FORCE_INLINE ulongtype getReservedKey(void) const {
			#ifdef DEEP_DEBUG 
			if (getReserved() == false) {
				DEEP_LOG(ERROR, OTHER, "Invalid reservation: reserved key requested in non-reserving state\n");

				throw InvalidException("Invalid reservation: reserved key requested in non-reserving state");
			}	
			#endif

			return m_reservedKey;
		}

		FORCE_INLINE void setReservedBlock(ulongtype reserved) {
			m_reservedBlock = reserved;
		}

		FORCE_INLINE ulongtype getReservedBlock(void) const {
			return m_reservedBlock;
		}

		FORCE_INLINE void setTransactionId(ulongtype transactionId) {
			m_transactionId = transactionId;
		}

		FORCE_INLINE ulongtype getTransactionId(void) const {
			return m_transactionId;
		}

		virtual inttype size(void) const = 0;

		virtual const char* getFilePath(void) const = 0; 

		virtual void index(inttype index, ConcurrentObject* context) = 0;
		virtual void clearIndex(void) = 0;

		virtual void prepare(shorttype level) = 0;

		#ifdef DEEP_SYNCHRONIZATION_GROUPING
		virtual void commit(shorttype level, bytetype mode /* 0=non-durable, 2=durable-phase1, 1=durable-phase2 */) = 0;
		#else
		virtual void commit(shorttype level) = 0;
		#endif

		virtual void rollback(shorttype level) = 0;
};

class Transaction : public SynchronizableSafe {

	CXX_LANG_MEMORY_DEBUG_DECL()

	public:
		enum Isolation {
			SERIALIZABLE = 0,
			REPEATABLE = 1,
			SNAPSHOT = 2,
			COMMITTED = 3,
			UNCOMMITTED = 4
		};

	private:
		TreeMap<longtype,Conductor*>::TreeMapEntrySet m_conductorSet;
		ArrayList<MeasuredRandomAccessFile*> m_streamFiles;
		TreeMap<longtype,Conductor*> m_conductorMap;
		BasicArray<longtype> m_deactivedConductors;
		LockableHashSet<StoryLock*> m_readLockSet;
		Conductor* m_hotConductor;
		longtype m_hotIdentifier;

		//#ifdef DEEP_USERLOCK
		UserSpaceLock m_conductorLock;
		//#else
		//ReentrantLock m_conductorLock;
		//#endif

		Isolation m_isolation;

		const ushorttype m_identifier;
		uinttype m_viewpoint;

		ushorttype m_waitIdentifier;
		ushorttype m_lastFileIndex;
		uinttype m_sequence;

		ushorttype m_retry;
		shorttype m_level;

		uinttype m_refs;

		volatile boolean m_wait;
		boolean m_dirty:1;
		boolean m_large:1;
		boolean m_purge:1;
		#ifdef DEEP_COMPRESS_PRIMARY_READ
		boolean m_fill:1;
		#endif
		boolean m_holddown:1;

		boolean m_roll:1;
		boolean m_rollWait:1;
		ushorttype m_streamIndex;
		uinttype m_streamPosition;

		static ushorttype s_theIdentifier;
		static ushorttype s_maxIdentifier;

		static uinttype s_viewpointMinimum;
		static uinttype s_viewpointSequence;

		#ifdef DEEP_USERLOCK
		static UserSpaceLock s_transactionLock;
		#else
		static ReentrantLock s_transactionLock;
		#endif

		static ulongtype s_identifier;
		static ReentrantLock s_identifierLock;
		static HashSet<longtype> s_identifiers;

		static Transaction::Isolation s_isolation;
		static BasicArray<uinttype> s_sequences;
		static BasicArray<Transaction*> s_transactions;

	public:
		inline Transaction* reassign(StoryLock* storyLock) {
			Transaction* owner = null;

			s_transactionLock.lock();
			{
				for (int i = 0; (owner == null) && (i < (s_maxIdentifier + 1)); i++) {
					Transaction* tx = s_transactions.get(i);
					if ((tx == null) || (tx == this)) {
						continue;
					}

					CXX_LANG_MEMORY_DEBUG_ASSERT(tx);

					tx->getReadLockSet()->lock();
					{
						if (tx->getReadLockSet()->HashSet<StoryLock*>::contains(storyLock) == true) {
							storyLock->setCredentials(tx->getIdentifier(), storyLock->getSequence());
							owner = tx;
						}
					}
					tx->getReadLockSet()->unlock();
				}
			}
			s_transactionLock.unlock();

			return owner;
		}

	private:
		inline void reassign(void) {
			if (m_readLockSet.size() != 0) {
				HashSet<StoryLock*>::KeySetIterator iter(&m_readLockSet);
				while (iter.HashSet<StoryLock*>::KeySetIterator::hasNext() == true) {
					StoryLock* storyLock = iter.HashSet<StoryLock*>::KeySetIterator::next();
					storyLock->lock();
					{
						if (storyLock->getIdentifier() == getIdentifier()) {
							m_wait = true;

							if (storyLock->getSequence() > 1) {
								reassign(storyLock);

							} else {
								storyLock->setCredentials(0, storyLock->getSequence());
								storyLock->setShare(false);
							}
						}

						storyLock->decrementSequence();
					}
					storyLock->unlock();

					//#ifdef DEEP_READ_STORYLOCK_REFERENCING
					storyLock->decref();
					//#endif
				}

				m_readLockSet.lock();
				{
					m_readLockSet.HashSet<StoryLock*>::clear();
				}
				m_readLockSet.unlock();
			}
		}

		inline void signal(void) {
			if (++m_sequence == 0) {
				m_sequence++;
			}

			s_sequences.set(getIdentifier(), getSequence());

			// XXX: ignore synchronized syntax for performance (see: Synchronizable)
			lock();
			{
				if (m_wait == true) {
					notifyAll();

					m_wait = false;
				}
			}
			unlock();
		}

	public:
		FORCE_INLINE static BasicArray<uinttype>* getSequences(void) {
			return &s_sequences;
		}

		FORCE_INLINE static Transaction* getTransaction(inttype id, boolean incref = true) {
			if (incref == false) {
				return s_transactions.get(id);
			}

			s_transactionLock.lock();
			Transaction* ret = s_transactions.get(id);
			if (ret != null) {
				ret->incref();
			}
			s_transactionLock.unlock();
			return ret;
		}

	public:
		FORCE_INLINE static boolean viewing(uinttype viewlimit) {
			return (viewlimit != 0) && (s_viewpointMinimum != 0) && (viewlimit >= s_viewpointMinimum);
		}

		FORCE_INLINE static uinttype getMinimumViewpoint(void) {
			return s_viewpointMinimum;
		}

		FORCE_INLINE static uinttype getCurrentViewpoint(void) {
			return s_viewpointSequence;
		}

		FORCE_INLINE static uinttype getNextViewpoint(void) {
			uinttype viewpoint = 0;

			s_transactionLock.lock();
			{
				viewpoint = ++s_viewpointSequence;
				if (s_viewpointMinimum == 0) {
					s_viewpointMinimum = viewpoint;
				}
			}
			s_transactionLock.unlock();

			return viewpoint;
		}

		FORCE_INLINE static void setGlobalIsolation(Transaction::Isolation isolation) {
			if (isolation == SNAPSHOT) {
				DEEP_LOG(ERROR, OTHER, "Invalid isolation: snapshot not supported\n");

				throw InvalidException("Invalid isolation: snapshot not supported");
			}

			s_isolation = isolation;
		}

		FORCE_INLINE static Transaction::Isolation getGlobalIsolation(void) {
			return s_isolation;
		}

		static Transaction* create(boolean streamline = false /* TODO */) {
			Transaction* tx = null;

			s_transactionLock.lock();
			{
				for (ulongtype i = 1; true; i++) {
					if (++s_theIdentifier >= Properties::DEFAULT_TRANSACTION_SIZE) {
						s_theIdentifier = 1; // XXX: 0 reserved for future streamline
					}

					if (s_transactions.get(s_theIdentifier) == null) {
						break;
					}

					CXX_LANG_MEMORY_DEBUG_ASSERT(s_transactions.get(s_theIdentifier));

					if ((i % Properties::DEFAULT_TRANSACTION_SIZE /* XXX: every ushorttype cycle */) == 0) {
						DEEP_LOG(WARN, OTHER, "Transaction (create) - resource unavailable: %lld\n", i);

						s_transactionLock.unlock();

						Thread::sleep(1000);

						s_transactionLock.lock();
					}
				}

				if (s_theIdentifier > s_maxIdentifier) {
					#if 0
					DEEP_LOG(DEBUG, OTHER, "Transaction (create) - maximum identifier: %d to %d\n", s_maxIdentifier, s_theIdentifier);
					#endif

					s_maxIdentifier = s_theIdentifier;
				}

				tx = new Transaction(s_theIdentifier, s_sequences.get(s_theIdentifier));
				s_transactions.set(tx->getIdentifier(), tx);
			}
			s_transactionLock.unlock();

			return tx;
		}

		static void destroy(Transaction* tx) {
			if (tx->getDirty() == true) {
				DEEP_LOG(ERROR, OTHER, "Invalid transaction: content not committed\n");

				throw InvalidException("Invalid transaction: content not committed");
			}

			s_transactionLock.lock();
			{
				s_transactions.set(tx->getIdentifier(), null);

				uinttype destroySequence = (tx->getSequence() + 1 == 0) ? (tx->getSequence() + 2) : (tx->getSequence() + 1);
				s_sequences.set(tx->getIdentifier(), destroySequence /* due to new tx of same id initializing to zero */);

				ushorttype maxIdentifier = s_maxIdentifier; s_maxIdentifier = 0;
				for (int i = 0; i < (maxIdentifier + 1); i++) {
					Transaction* tx = s_transactions.get(i);
					if (tx == null) {
						if (i < s_theIdentifier) {
							if (i == 0) {
								s_theIdentifier = 0; // XXX: 0 reserved for future streamline

							} else {
								s_theIdentifier = i - 1;
							}
						}

						continue;
					}

					CXX_LANG_MEMORY_DEBUG_ASSERT(tx);

					if (i > s_maxIdentifier) {
						s_maxIdentifier = i;
					}
				}

				#if 0
				if (s_maxIdentifier < maxIdentifier) {
					DEEP_LOG(DEBUG, OTHER, "Transaction (destroy) - minimum identifier: %d to %d\n", maxIdentifier, s_maxIdentifier);
				}
				#endif
			}
			s_transactionLock.unlock();

			delete tx;
		}

		static void cleanup(longtype identifier, bytetype index = -1) {
			s_transactionLock.lock();
			{
				for (int i = 0; i < (s_maxIdentifier + 1); i++) {
					Transaction* tx = s_transactions.get(i);
					if (tx == null) {
						continue;
					}

					CXX_LANG_MEMORY_DEBUG_ASSERT(tx);

					if (index != -1) {
						tx->indexConductor(identifier, index, null);

					} else {
						tx->destroyConductor(identifier);
					}
				}
			}
			s_transactionLock.unlock();
		}

		static void clobber(void) {
			s_transactionLock.lock();
			{
				s_theIdentifier = 0;
				s_maxIdentifier = 0;

				s_transactions.clear();
				s_sequences.clear();
			}
			s_transactionLock.unlock();
		}

		static void minimum(boolean log = false) {
			inttype empty = 0;
			inttype stale = 0;
			inttype views = 0;

			s_transactionLock.lock();
			{
				uinttype viewpoint = 0;
				for (int i = 0; i < (s_maxIdentifier + 1); i++) {
					Transaction* tx = s_transactions.get(i);
					if (tx == null) {
						empty++;
						continue;
					}

					CXX_LANG_MEMORY_DEBUG_ASSERT(tx);

					if (tx->getViewpoint() == 0) {
						stale++;
						continue;
					}

					if (((viewpoint == 0) || (viewpoint > tx->getViewpoint()))) {
						viewpoint = tx->getViewpoint();
					}

					views++;
				}

				/* TODO: sequence number will wrap if system is never forced to idle
				if (viewpoint == 0) {
					s_viewpointSequence = 0;
				}
				*/

				s_viewpointMinimum = viewpoint;
			}
			s_transactionLock.unlock();

			if (log == true) {
				DEEP_LOG(DEBUG, VIEWS, "txids: empty: %d, stale: %d, views: %d\n", empty, stale, views);
			}
		}

		static longtype addIdentifier(boolean primary) {
			longtype identifier = 0;

			if (primary == true) {
				s_identifierLock.lock();
				{
					s_identifier++;

					for (;; s_identifier++) {
						if (s_identifiers.contains(s_identifier) == false) {
							identifier = s_identifier;
							s_identifiers.add(s_identifier);
							break;
						}
					}
				}
				s_identifierLock.unlock();
			}

			return identifier;
		}

		static void removeIdentifier(longtype identifier, boolean associated, bytetype index) {
			// XXX: primary or secondary during association, remove entire conductor from each tx
			if (associated == false) {
				s_identifierLock.lock();
				{
					Transaction::cleanup(identifier);

					s_identifiers.remove(identifier);
				}
				s_identifierLock.unlock();

			// XXX: secondary during dynamic add/drop, remove only context from conductor in each tx
			} else {
				Transaction::cleanup(identifier, index);
			}
		}

	public:
		inline static boolean lockable(StoryLine& storyLine, ushorttype id, boolean share) {
			boolean lockable = true;

			ushorttype identifier = storyLine.getLockIdentifier();
			if (storyLine.getSharing() == false) {
				if ((identifier != 0) && (identifier != id)) {
					uinttype sequence = getSequences()->get(identifier);
					if (storyLine.getLockSequence() != sequence) {
						storyLine.release();

					} else {
						lockable = false;
					}
				}

			} else if ((share == false) || (identifier != id)) {
				lockable = false;
			}

			return lockable;
		}

		inline static boolean deletable(Information* info) {
			return (info->getDeleting() == true) && (info->getLevel() == Information::LEVEL_COMMIT);
		}

	public:
		inline Transaction(const ushorttype identifier, const uinttype sequence):
			m_conductorSet(true),
			m_conductorMap(Properties::DEFAULT_TRANSACTION_ORDER, false, true),
			m_readLockSet(HashSet<StoryLock*>::INITIAL_CAPACITY, false, true),
			m_hotConductor(null),
			m_hotIdentifier(-1),
			//#ifdef DEEP_USERLOCK
			// nothing to do
			//#else
			//m_conductorLock(false),
			//#endif
			m_isolation(getGlobalIsolation()),
			m_identifier(identifier),
			m_viewpoint(0),
			m_waitIdentifier(0),
			m_lastFileIndex(0),
			m_sequence(sequence),
			m_retry(0),
			m_level(-1),
			m_refs(0),
			m_wait(false),
			m_dirty(false),
			m_large(false),
			m_purge(false),
			#ifdef DEEP_COMPRESS_PRIMARY_READ
			m_fill(false),
			#endif
			m_holddown(false),
			m_roll(false),
			m_rollWait(false),
			m_streamIndex(0),
			m_streamPosition(0) {

			// XXX: init m_conductorSet relationship with m_conductorMap
			m_conductorMap.entrySet(&m_conductorSet);

			CXX_LANG_MEMORY_DEBUG_INIT()

			setDirty(false);
		}

		virtual ~Transaction(void) {
			ensureSafeToDestroy();

			CXX_LANG_MEMORY_DEBUG_CLEAR()

			if (getDirty() == true) {
				setDirty(false);
			}
		}

		FORCE_INLINE virtual boolean safeToDestroy(void) {
			return (__sync_or_and_fetch(&m_refs, 0) == 0);
		}

		FORCE_INLINE void incref() {
			__sync_add_and_fetch(&m_refs, 1);
		}

		FORCE_INLINE void decref() {
			if (__sync_fetch_and_sub(&m_refs, 1) == 0) {
				#ifdef DEEP_DEBUG
				DEEP_LOG(ERROR, OTHER, "Transaction ref count underflow\n");
				throw InvalidException("Transaction ref count underflow");
				#endif
			}
		}

		FORCE_INLINE void release(void) {
			m_waitIdentifier = 0;
			m_retry = 0;
		}

		FORCE_INLINE shorttype getLevel(void) const {
			return m_level;
		}

		FORCE_INLINE uinttype getViewpoint(void) const {
			return m_viewpoint;
		}

		FORCE_INLINE void setLastFileIndex(ushorttype fileIndex) {
			m_lastFileIndex = fileIndex;
		}

		FORCE_INLINE ushorttype getLastFileIndex() const {
			return m_lastFileIndex;
		}

		FORCE_INLINE uinttype getSequence(void) const {
			return m_sequence;
		}

		FORCE_INLINE ushorttype getIdentifier(void) const {
			return m_identifier;
		}

		FORCE_INLINE LockableHashSet<StoryLock*>* getReadLockSet(void) {
			return &m_readLockSet;
		}

		FORCE_INLINE ArrayList<MeasuredRandomAccessFile*>* getStreamFiles(void) {
			return &m_streamFiles;
		}

		inline void setDirty(boolean dirty) {
			m_dirty = dirty;

			if (m_dirty == false) {
				m_viewpoint = 0;
				m_large = false;
				m_purge = false;
				#ifdef DEEP_COMPRESS_PRIMARY_READ
				m_fill = false;
				#endif
				m_level = -1;
				m_retry = 0;
				m_rollWait = false;

				signal();
			}
		}

		FORCE_INLINE boolean getDirty(void) const {
			return m_dirty;
		}

		FORCE_INLINE void setLarge(boolean large) {
			m_large = large;
		}

		FORCE_INLINE boolean getLarge(void) const {
			return m_large;
		}

		FORCE_INLINE void setPurge(boolean purge) {
			m_purge = purge;
		}

		FORCE_INLINE boolean getPurge(void) const {
			return m_purge;
		}

		FORCE_INLINE void setRetry(ushorttype retry) {
			m_retry = retry;
		}

		FORCE_INLINE ushorttype getRetry(void) const {
			return m_retry;
		}

		FORCE_INLINE void setIsolation(Isolation isolation) {
			m_isolation = isolation;
		}

		FORCE_INLINE Isolation getIsolation(void) const {
			return m_isolation;
		}

		FORCE_INLINE void setWaitFlag(boolean wait) {
			m_wait = wait;
		}

		FORCE_INLINE boolean getWaitFlag(void) const {
			return m_wait;
		}

		#ifdef DEEP_COMPRESS_PRIMARY_READ
		FORCE_INLINE void setFillFlag(boolean fill) {
			m_fill = fill;
		}

		FORCE_INLINE boolean getFillFlag(void) const {
			return m_fill;
		}
		#endif

		FORCE_INLINE void setRoll(const boolean roll) {
			m_roll = roll;
		}

		FORCE_INLINE boolean getRoll(void) const {
			return m_roll;
		}

		FORCE_INLINE void setRollWait(const boolean rollWait) {
			m_rollWait = rollWait;
		}

		FORCE_INLINE boolean getRollWait(void) const {
			return m_rollWait;
		}

		FORCE_INLINE void setStreamIndex(ushorttype streamIndex) {
			m_streamIndex = streamIndex;
		}

		FORCE_INLINE ushorttype getStreamIndex(void) {
			return m_streamIndex;
		}

		FORCE_INLINE void setStreamPosition(uinttype streamPosition) {
			m_streamPosition = streamPosition;
		}

		FORCE_INLINE uinttype getStreamPosition(void) const {
			return m_streamPosition;
		}

		FORCE_INLINE void setWaitIdentifier(ushorttype identifier) {
			m_waitIdentifier = identifier;
		}

		FORCE_INLINE ushorttype getWaitIdentifier(void) const {
			return m_waitIdentifier;
		}

		FORCE_INLINE void initConductor(Conductor* conductor, longtype identifier, longtype time) {
			m_conductorLock.lock();
			{
				// XXX: creation and destruction should be executed on the same transactional thread
				releaseConductors(-1);

				m_conductorMap.TreeMap<longtype,Conductor*>::put(identifier, conductor);

				m_hotConductor = conductor;
				m_hotIdentifier = identifier;
			}
			m_conductorLock.unlock();
		}

		FORCE_INLINE Conductor* getConductor(longtype identifier) {
			Conductor* conductor = null;

			m_conductorLock.lock();
			{
				if (m_hotIdentifier != identifier) {
					m_hotConductor = m_conductorMap.TreeMap<longtype,Conductor*>::get(identifier);
					m_hotIdentifier = identifier;
				}

				conductor = m_hotConductor;
			}
			m_conductorLock.unlock();

			CXX_LANG_MEMORY_DEBUG_ASSERT(conductor);

			return conductor;
		}

		FORCE_INLINE void destroyConductor(longtype identifier) {
			m_conductorLock.lock();
			{
				Conductor* conductor = m_conductorMap.TreeMap<longtype,Conductor*>::get(identifier);
				if (conductor != null) {
					// XXX: creation and destruction should be executed on the same transactional thread
					m_deactivedConductors.add(identifier, true);

					CXX_LANG_MEMORY_DEBUG_ASSERT(conductor);

					conductor->clearIndex();

					if (m_hotIdentifier == identifier) {
						m_hotConductor = null;
						m_hotIdentifier = -1;
					}
				}
			}
			m_conductorLock.unlock();
		}

		FORCE_INLINE void indexConductor(longtype identifier, bytetype index, ConcurrentObject* context) {
			m_conductorLock.lock();
			{
				Conductor* conductor = m_conductorMap.TreeMap<longtype,Conductor*>::get(identifier);
				if (conductor != null) {
					CXX_LANG_MEMORY_DEBUG_ASSERT(conductor);

					conductor->index(index, context);
				}
			}
			m_conductorLock.unlock();
		}

		// XXX: creation and destruction should be executed on the same transactional thread
		FORCE_INLINE void releaseConductors(longtype time) {
			if (m_deactivedConductors.size() != 0) {
				for (int i = 0; i < m_deactivedConductors.size(); i++) {
					longtype identifier = m_deactivedConductors.get(i);

					Conductor* conductor = m_conductorMap.TreeMap<longtype,Conductor*>::remove(identifier);
					CXX_LANG_MEMORY_DEBUG_ASSERT(conductor);

					delete conductor;
				}

				m_deactivedConductors.clear();
			}
		}

	// XXX: the following are the only methods to be used publicly
	public:
		FORCE_INLINE shorttype begin(void) {
			if (m_viewpoint == 0) {
				m_viewpoint = getNextViewpoint();
			}

			return ((m_level + 1) == -1) ? -1 : ++m_level;
		}

		FORCE_INLINE void prepare(shorttype level) {
			/* TODO: finish prepare logic
			ConductorEntrySetIterator* iter = (ConductorEntrySetIterator*) m_conductorSet.reset();
			while (iter->ConductorEntrySetIterator::hasNext()) {
				MapEntry<longtype,Conductor*>* entry = iter->ConductorEntrySetIterator::next();

				Conductor* conductor = entry->getValue();
				CXX_LANG_MEMORY_DEBUG_ASSERT(conductor);

				// TODO: finish deactive logic
				//if (conductor->getDeactivated() == false) {
					conductor->prepare(level);
				//}
			}
			*/
		}

		FORCE_INLINE void commit(shorttype level) {
			if (getDirty() == true) {

				boolean requiresACP = RealTimeAtomic::useAtomicCommit();

				// multi table commits require deep.trt file write to maintain durability / consistency
				requiresACP = (requiresACP == true) && ((Properties::getDurable() == true) && (level == 0) && (m_conductorSet.size() > 1));

				// only need ACP if there is more than one table that actually contains a write
				if (requiresACP == true) {
					ubytetype nonZeroCount = 0;
					char* dbname = null;
					ConductorEntrySetIterator* iter = (ConductorEntrySetIterator*) m_conductorSet.reset();

					while (iter->ConductorEntrySetIterator::hasNext()) {
						MapEntry<longtype,Conductor*>* entry = iter->ConductorEntrySetIterator::next();

						Conductor* conductor = entry->getValue();
						if (conductor->size() != 0) {
							nonZeroCount++;		

							// TODO: support cross database transactions
							if (dbname == null) {
								dbname = RealTimeAtomic::getDatabaseName(conductor->getFilePath());	
							} else {
								char* dbname2 = RealTimeAtomic::getDatabaseName(conductor->getFilePath());
								String dbnameStr(dbname2);
								delete[] dbname2;
								if (String(dbname).equals(&dbnameStr) == false) {
									requiresACP = false;
									break;
								}
							}
						}
					}

					if (dbname != null) {
						delete[] dbname;
					}
					requiresACP = (requiresACP == true) && (nonZeroCount > 1);
				}

				RealTimeAtomic* atomicCommit = null;
				ulongtype transactionId = 0;

				if (level == 0) {
					reassign();

					#ifdef DEEP_SYNCHRONIZATION_GROUPING
					if (Properties::getDurable() == true) {
						MeasuredRandomAccessFile::planSynchronizeGlobally();

						ConductorEntrySetIterator* iter = (ConductorEntrySetIterator*) m_conductorSet.reset();
						while (iter->ConductorEntrySetIterator::hasNext()) {
							MapEntry<longtype,Conductor*>* entry = iter->ConductorEntrySetIterator::next();

							Conductor* conductor = entry->getValue();
							CXX_LANG_MEMORY_DEBUG_ASSERT(conductor);

							if (requiresACP == true) {
								if (atomicCommit == null) {
									atomicCommit = RealTimeAtomic::getInstance(RealTimeAtomic::getDatabaseName(conductor->getFilePath()), RealTimeAtomic::WRITE);
									transactionId = atomicCommit->getTransactionId();
								}
								conductor->setTransactionId(transactionId);

							} else {
								conductor->setTransactionId(0);
							}


							// TODO: finish deactive logic
							//if (conductor->getDeactivated() == false) {
								conductor->commit(level, 2 /* durable-phase1 */);
							//}
						}

						if (requiresACP == true) {
							atomicCommit->writeEndTransaction(transactionId);
							MeasuredRandomAccessFile::prepareSynchronizeGlobally(atomicCommit->getFile());
						}

						if (Properties::getDurableSyncInterval() == 0) {
							boolean holddown = false;
							if (Properties::getDurableHoldDownTime() != 0) {
								for (int i = 0; (i < (s_maxIdentifier + 1)); i++) {
									Transaction* tx = s_transactions.get(i);
									if ((tx != null) && (tx != this) && (tx->getDirty() == true)) {
										if (m_holddown == true) {
											Thread::sleep(Properties::getDurableHoldDownTime());
										}

										holddown = true;
										break;
									}
								}
							}

							longtype time = MeasuredRandomAccessFile::performSynchronizeGlobally(true /* decrement */, holddown);
							if (time < Properties::getDurableHoldDownThreshold()) {
								m_holddown = false;

							} else {
								m_holddown = true;
							}
						}
					}
					#endif
				}

				ConductorEntrySetIterator* iter = (ConductorEntrySetIterator*) m_conductorSet.reset();
				while (iter->ConductorEntrySetIterator::hasNext()) {
					MapEntry<longtype,Conductor*>* entry = iter->ConductorEntrySetIterator::next();

					Conductor* conductor = entry->getValue();

					#ifndef DEEP_SYNCHRONIZATION_GROUPING
					if (requiresACP == true) {
						if (atomicCommit == null) {
							atomicCommit = RealTimeAtomic::getInstance(RealTimeAtomic::getDatabaseName(conductor->getFilePath()), RealTimeAtomic::WRITE);
							transactionId = atomicCommit->getTransactionId();
						}
						conductor->setTransactionId(transactionId);

					} else {
						conductor->setTransactionId(0);
					}
					#endif

					CXX_LANG_MEMORY_DEBUG_ASSERT(conductor);

					// TODO: finish deactive logic
					//if (conductor->getDeactivated() == false) {
						#ifdef DEEP_SYNCHRONIZATION_GROUPING
						conductor->commit(level, Properties::getDurable() /* 0=non-durable, 1=durable-phase2 */);
						#else
						conductor->commit(level);
						#endif
					//}
				}

				#ifndef DEEP_SYNCHRONIZATION_GROUPING
				if (requiresACP == true) {
					atomicCommit->writeEndTransaction(transactionId);
					atomicCommit->getFile()->syncPerform(false /* deactivate */);
				}
				#endif
			}

			if (level == 0) {
				setDirty(false);

			} else {
				m_level = level - 1;
			}
		}

		FORCE_INLINE void rollback(shorttype level) {
			if (getDirty() == true) {

				if (level == 0) {
					reassign();
				}

				ConductorEntrySetIterator* iter = (ConductorEntrySetIterator*) m_conductorSet.reset();
				while (iter->ConductorEntrySetIterator::hasNext()) {
					MapEntry<longtype,Conductor*>* entry = iter->ConductorEntrySetIterator::next();

					Conductor* conductor = entry->getValue();
					CXX_LANG_MEMORY_DEBUG_ASSERT(conductor);

					// TODO: finish deactive logic
					//if (conductor->getDeactivated() == false) {
						conductor->rollback(level);
					//}
				}
			}

			if (level == 0) {
				setDirty(false);

			} else {
				m_level = level - 1;
			}
		}

	typedef TreeMap<longtype,Conductor*>::TreeMapEntrySet::EntrySetIterator ConductorEntrySetIterator;
};

ushorttype Transaction::s_theIdentifier(0);
ushorttype Transaction::s_maxIdentifier(0);

uinttype Transaction::s_viewpointMinimum(0);
uinttype Transaction::s_viewpointSequence(0);

#ifdef DEEP_USERLOCK
UserSpaceLock Transaction::s_transactionLock;
#else
ReentrantLock Transaction::s_transactionLock;
#endif

ulongtype Transaction::s_identifier(0);
ReentrantLock Transaction::s_identifierLock(false);
HashSet<longtype> Transaction::s_identifiers(HashSet<longtype>::INITIAL_CAPACITY, false);

Transaction::Isolation Transaction::s_isolation(COMMITTED);
BasicArray<uinttype> Transaction::s_sequences(Properties::DEFAULT_TRANSACTION_SIZE, false);
BasicArray<Transaction*> Transaction::s_transactions(Properties::DEFAULT_TRANSACTION_SIZE, true);

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_TRANSACTION_H_*/
