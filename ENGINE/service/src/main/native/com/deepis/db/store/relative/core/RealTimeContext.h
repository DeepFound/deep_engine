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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECONTEXT_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECONTEXT_H_ 

#include "cxx/util/TreeMap.h"
#include "cxx/util/Converter.h"

#include "cxx/util/concurrent/locks/UserSpaceReadWriteLock.h"

#include "com/deepis/db/store/relative/util/ConcurrentObject.h"
#include "com/deepis/db/store/relative/util/ConcurrentContainer.h"
#include "com/deepis/db/store/relative/util/PermissionException.h"

#include "com/deepis/db/store/relative/core/Segment.h"
#include "com/deepis/db/store/relative/core/Information.h"
#include "com/deepis/db/store/relative/core/Transaction.h"
#include "com/deepis/db/store/relative/core/RealTimeBuilder.h"
#include "com/deepis/db/store/relative/core/RealTimeConverter.h"
#include "com/deepis/db/store/relative/core/RealTimeCondition.h"

using namespace cxx::util;
using namespace cxx::util::concurrent::locks;
using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename K>
class ThreadContext : public ConcurrentObject {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;
	typedef typename SegTreeMap::TreeMapEntryIterator MapInformationEntryIterator;

	public:
		// XXX: generic statistics for specific contexts
		#ifdef DEEP_IO_STATS
		ulongtype m_statsCount1;
		ulongtype m_statsCount2;
		ulongtype m_statsCount3;
		ulongtype m_statsCount4;
		boolean m_statsForFiles;
		#endif

	private:
		MapInformationEntryIterator* m_iterator;
		Transaction* m_transaction;
		InfoRefBuf m_infoRefBuf;
		XInfoRef* m_infoRef;
		Segment<K>* m_segment;

		RealTimeCondition<K>* m_condition;

		boolean m_globalLock;

		boolean m_purgeSetup;
		K m_purgeCursor;

		inttype m_errorCode;
		K m_errorKey;

		K m_key1;
		K m_key2;
		K m_key3;

		// XXX: inverse primary reads
		chartype m_keyBuffer[3096];
		ushorttype m_keyOffset[16];
		ushorttype m_keyLength[16];

	public:
		ThreadContext(const KeyBuilder<K>* builder):
			#ifdef DEEP_IO_STATS
			m_statsCount1(0),
			m_statsCount2(0),
			m_statsCount3(0),
			m_statsCount4(0),
			m_statsForFiles(false),
			#endif
			m_iterator(null),
			m_transaction(null),
			m_infoRef(null),
			m_segment(null),
			m_condition(null),
			m_globalLock(false),
			m_purgeSetup(false),
			m_errorCode(0) {

			m_purgeCursor = (K) Converter<K>::NULL_VALUE;
			m_errorKey = (K) Converter<K>::NULL_VALUE;

			if (builder == null) {
				m_key1 = (K) Converter<K>::NULL_VALUE;
				m_key2 = (K) Converter<K>::NULL_VALUE;
				m_key3 = (K) Converter<K>::NULL_VALUE;

			} else {
				m_key1 = builder->newKey();
				m_key2 = builder->newKey();
				m_key3 = builder->newKey();
			}
		}

		virtual ~ThreadContext(void) {
			if (m_purgeCursor != (K) Converter<K>::NULL_VALUE) {
				Converter<K>::destroy(m_purgeCursor);
			}

			// XXX: m_errorKey references m_key3

			if (m_key1 != (K) Converter<K>::NULL_VALUE) {
				Converter<K>::destroy(m_key1);
				Converter<K>::destroy(m_key2);
				Converter<K>::destroy(m_key3);
			}

			setInformation(null);
		}

		FORCE_INLINE void setIterator(MapInformationEntryIterator* iterator) {
			m_iterator = iterator;
		}

		FORCE_INLINE MapInformationEntryIterator* getIterator(void) const {
			return m_iterator;
		}

		FORCE_INLINE void setTransaction(Transaction* tx) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(m_transaction);

			m_transaction = tx;

			CXX_LANG_MEMORY_DEBUG_ASSERT(m_transaction);
		}

		FORCE_INLINE Transaction* getTransaction(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(m_transaction);

			return m_transaction;
		}

		FORCE_INLINE InfoRefBuf& getInfoRefBuf() {
			return m_infoRefBuf;
		}

		FORCE_INLINE void setInformation(XInfoRef* infoRef) {
			if (m_infoRef != null) {
				CXX_LANG_MEMORY_DEBUG_ASSERT(m_infoRef->getInfo());
				Converter<Information*>::destroy(m_infoRef->getInfo());
				Converter<XInfoRef*>::destroy(m_infoRef);
			}

			m_infoRef = infoRef;

			if (m_infoRef != null) {
				m_infoRef->getInfo()->incref();
				CXX_LANG_MEMORY_DEBUG_ASSERT(m_infoRef->getInfo());
			}
		}

		FORCE_INLINE XInfoRef* getInformation(void) const {
			if (m_infoRef != null) {
				CXX_LANG_MEMORY_DEBUG_ASSERT(m_infoRef->getInfo());
			}

			return m_infoRef;
		}

		FORCE_INLINE void setSegment(Segment<K>* segment) {
			/* XXX: can't assume valid due to segment unlock
			CXX_LANG_MEMORY_DEBUG_ASSERT(m_segment);
			*/

			m_segment = segment;

			CXX_LANG_MEMORY_DEBUG_ASSERT(m_segment);
		}

		FORCE_INLINE Segment<K>* getSegment(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(m_segment);

			return m_segment;
		}

		#ifdef CXX_LANG_MEMORY_DEBUG
		FORCE_INLINE void setTransaction(Transaction* tx, boolean ignore) {
			if (ignore == false) {
				setTransaction(tx);

			} else {
				m_transaction = tx;
			}
		}

		FORCE_INLINE Transaction* getTransaction(boolean ignore) const {
			if (ignore == false) {
				return getTransaction();

			} else {
				return m_transaction;
			}
		}
		#endif

		FORCE_INLINE void setCondition(RealTimeCondition<K>* condition) {
			m_condition = condition;
		}

		FORCE_INLINE RealTimeCondition<K>* getCondition(void) {
			return m_condition;
		}

		FORCE_INLINE void setKey1(K key1) {
			m_key1 = key1;
		}

		FORCE_INLINE K getKey1(void) const {
			return m_key1;
		}

		FORCE_INLINE void setKey2(K key2) {
			m_key2 = key2;
		}

		FORCE_INLINE K getKey2(void) const {
			return m_key2;
		}

		FORCE_INLINE void setKey3(K key3) {
			m_key3 = key3;
		}

		FORCE_INLINE K getKey3(void) const {
			return m_key3;
		}

		FORCE_INLINE void setPurgeSetup(boolean flag) {
			m_purgeSetup = flag;
		}

		FORCE_INLINE boolean getPurgeSetup(void) const {
			return m_purgeSetup;
		}

		FORCE_INLINE void setPurgeCursor(K key) {
			m_purgeCursor = key;
		}

		FORCE_INLINE K getPurgeCursor(void) const {
			return m_purgeCursor;
		}

		FORCE_INLINE void setErrorCode(int code) {
			m_errorCode = code;
		}

		FORCE_INLINE int getErrorCode(void) const {
			return m_errorCode;
		}

		FORCE_INLINE void setErrorKey(K key) {
			m_errorKey = key;
		}

		FORCE_INLINE K getErrorKey(void) const {
			return m_errorKey;
		}

		FORCE_INLINE chartype* getKeyBuffer(void) {
			return m_keyBuffer;
		}

		FORCE_INLINE ushorttype* getKeyOffset(void) {
			return m_keyOffset;
		}

		FORCE_INLINE ushorttype* getKeyLength(void) {
			return m_keyLength;
		}
};

template<typename K>
class RealTimeContext {

	private:
		const KeyBuilder<K>* m_keyBuilder;
		ConcurrentContainer<ThreadContext<K>*> m_container;

		UserSpaceReadWriteLock m_lock;

	public:
		FORCE_INLINE RealTimeContext(inttype threads, const KeyBuilder<K>* keyBuilder):
			m_keyBuilder(keyBuilder),
			m_container(threads, true),
			m_lock() {
			
		}

		FORCE_INLINE ~RealTimeContext(void) {
			m_container.free();
		}

		FORCE_INLINE ConcurrentContainer<ThreadContext<K>*>* getContainer() {
			return &m_container;
		}

		FORCE_INLINE ThreadContext<K>* getContext(longtype identifier = 0) {
			if (identifier == 0) {
				identifier = (longtype) pthread_self();
			}

			ThreadContext<K>* context = m_container.instance(identifier);
			if (context == null) {
				context = new ThreadContext<K>(m_keyBuilder);

				#if 0
				RETRY:
				if (m_container.add(context, identifier) == false) {
					Thread::sleep(Properties::DEFAULT_PACE_REALTIME);

					goto RETRY;
				}
				#else
				if (m_container.add(context, identifier) == false) {
					delete context;

					DEEP_LOG(ERROR, OTHER, "Context limit exceeded\n");

					throw PermissionException("Context limit exceeded");
				}
				#endif
			}

			#ifdef DEEP_DEBUG
			ThreadContext<K>* debugContext = m_container.instance(identifier);
			if (debugContext != context) {
				DEEP_LOG(ERROR, OTHER, "Thread context mismatch, %p != %p\n", context, debugContext);
				throw InvalidException("Thread context mismatch");
			}
			#endif

			return context;
		}

		FORCE_INLINE void removeContext(ThreadContext<K>* context) {
			m_container.remove(context);
		}

		FORCE_INLINE RealTimeCondition<K>* getCondition(void) {
			return getContext()->getCondition();
		}

		FORCE_INLINE void setCondition(RealTimeCondition<K>* condition) {
			getContext()->setCondition(condition);
		}

		FORCE_INLINE void setErrorCode(int code) {
			getContext()->setErrorCode(code);
		}

		FORCE_INLINE int getErrorCode(void) {
			return getContext()->getErrorCode();
		}

		FORCE_INLINE void setErrorKey(K key) {
			getContext()->setErrorKey(key);
		}

		FORCE_INLINE K getErrorKey(void) {
			return getContext()->getErrorKey();
		}

		FORCE_INLINE boolean tryReadLock() {
			return m_lock.tryReadLock();
		}

		FORCE_INLINE void readLock() {
			m_lock.readLock();
		}

		FORCE_INLINE void readUnlock() {
			m_lock.readUnlock();
		}

		FORCE_INLINE void writeLock() {
			m_lock.writeLock();
		}

		FORCE_INLINE void writeUnlock() {
			m_lock.writeUnlock();
		}
};

template<typename K>
class ContextHandle {

	private:
		ThreadContext<K>* m_context;

	public:
		FORCE_INLINE ContextHandle(ThreadContext<K>* context):
			m_context(context) {
		}

		FORCE_INLINE ContextHandle(ThreadContext<K>* context, Segment<K>* segment):
			m_context(context) {

			m_context->setSegment(segment);
		}

		FORCE_INLINE ~ContextHandle(void) {
			release();
		}

		FORCE_INLINE Segment<K>* getSegment() {
			return m_context->getSegment();
		}

		FORCE_INLINE void assign(Segment<K>* next) {
			Segment<K>* previous = getSegment();
			if (previous != null) {
				previous->unlock();
			}

			m_context->setSegment(next);
		}

		FORCE_INLINE void unassign(void) {
			m_context->setSegment(null);
		}

		FORCE_INLINE void release(void) {
			Segment<K>* segment = getSegment();
			if (segment != null) {
				segment->unlock();
			}

			unassign();
		}
};

#define CONTEXT_STACK_OBJECT(name) contextHandle##name
#define CONTEXT_STACK_HANDLE(K,name,context) ContextHandle<K> CONTEXT_STACK_OBJECT(name)(context);

#define CONTEXT_STACK_ASSIGN(object,name) CONTEXT_STACK_OBJECT(name).assign(object);
#define CONTEXT_STACK_RELEASE(name) CONTEXT_STACK_OBJECT(name).release();
#define CONTEXT_STACK_UNASSIGN(name) CONTEXT_STACK_OBJECT(name).unassign();
#define CONTEXT_STACK_HANDLER(K,context,object,name) ContextHandle<K> CONTEXT_STACK_OBJECT(name)(context, object);

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECONTEXT_H_*/
