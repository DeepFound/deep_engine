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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_CONCURRENTCONTAINER_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_CONCURRENTCONTAINER_H_ 

#include "cxx/util/HashMap.h"

#include "cxx/util/concurrent/atomic/AtomicInteger.h"
#include "cxx/util/concurrent/atomic/AtomicBoolean.h"

#include "cxx/util/concurrent/locks/UserSpaceLock.h"
#include "com/deepis/db/store/relative/util/BasicArray.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent::locks;
using namespace cxx::util::concurrent::atomic;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

template<typename E /* extends ConcurrentObject */>
class ConcurrentContainer {

	private:
		boolean m_deleteValue;
		mutable UserSpaceReadWriteLock m_rwlock;
		HashMap<longtype,E> m_instances;

	public:
		ConcurrentContainer(inttype capacity, boolean deleteValue = false) :
			m_deleteValue(deleteValue),
			m_rwlock(),
			// XXX: map 2x capacity with resizing off for concurrent behavior
			m_instances(capacity * HashMap<longtype,E>::RESIZE_FACTOR, false /* delete keys */, false /* delete values */, true /* fixed */) {
		}

		virtual ~ConcurrentContainer(void) {
			// nothing to do for now
		}

		FORCE_INLINE E instance(longtype instance) const {
			m_rwlock.readLock();

			E element = m_instances.HashMap<longtype,E>::get(instance);

			m_rwlock.readUnlock();

			return element;
		}

		FORCE_INLINE boolean add(E element, longtype instance) {
			m_rwlock.writeLock();

			#ifdef DEEP_DEBUG
			if (instance == -1) {
				DEEP_LOG(ERROR, OTHER, "Invalid instance identifier\n");
				throw InvalidException("Invalid instance identifier");
			}
			#endif

			const inttype size = m_instances.HashMap<longtype,E>::size();
			if (size >= m_instances.HashMap<longtype,E>::capacity()) {
				m_rwlock.writeUnlock();
				return false;
			}

			element->setInstance(instance);
			E removed = m_instances.HashMap<longtype,E>::put(instance, element);

			if ((m_deleteValue == true) && (removed != null)) {
				Converter<E>::destroy(removed);
			}

			m_rwlock.writeUnlock();

			return true;
		}

		FORCE_INLINE void remove(E element) {
			m_rwlock.writeLock();

			m_instances.remove(element->getInstance());
			if (m_deleteValue == true) {
				Converter<E>::destroy(element);
			}

			m_rwlock.writeUnlock();
		}


		FORCE_INLINE void free() {
			m_rwlock.writeLock();

			m_instances.clear(false /* delete keys */, m_deleteValue /* delete values */);

			m_rwlock.writeUnlock();
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_CONCURRENTCONTAINER_H_*/
