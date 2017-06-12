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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_LOCKABLEBASICARRAY_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_LOCKABLEBASICARRAY_H_ 

#include "cxx/util/concurrent/locks/UserSpaceLock.h"
#include "cxx/util/concurrent/locks/ReentrantLock.h"

#include "com/deepis/db/store/relative/util/BasicArray.h"

using namespace cxx::util::concurrent::locks;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

template<typename E>
class LockableBasicArray /* : public BasicArray<E>, public Lockable */ {

	public:
		CXX_LANG_MEMORY_DEBUG_DECL()

	private:
		BasicArray<E> m_array;

		//#ifdef DEEP_USERLOCK
		//UserSpaceLock m_lock;
		//#else
		ReentrantLock m_lock;
		//#endif

		UserSpaceLock m_arrayLock;

	public:
		LockableBasicArray(void):
			m_array(10, false),
			m_lock(false) {
			CXX_LANG_MEMORY_DEBUG_INIT()
		}

		LockableBasicArray(inttype initialCapacity, boolean deleteValue) :
			m_array(initialCapacity, deleteValue),
			m_lock(false) {
			CXX_LANG_MEMORY_DEBUG_INIT()
		}

		LockableBasicArray(LockableBasicArray<E>* array) :
			m_array(10, false),
			m_lock(false) {
			CXX_LANG_MEMORY_DEBUG_INIT()

			array->m_arrayLock.lock();
			{
				if (array->m_array.capacity() > m_array.capacity()) {
					m_array.resize(array->m_array.capacity());
				}
				m_array.copy(&(array->m_array));
			}
			array->m_arrayLock.unlock();
		}

		virtual ~LockableBasicArray() {
			CXX_LANG_MEMORY_DEBUG_CLEAR()
		}

		FORCE_INLINE void lock() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_lock.lock();
		}

		FORCE_INLINE boolean tryLock() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_lock.tryLock();
		}

		FORCE_INLINE void unlock() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_lock.unlock();
		}

		FORCE_INLINE BasicArray<E>& getArray() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_array;
		}

		FORCE_INLINE boolean limit(void) /* const */ {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			boolean lmt;

			m_arrayLock.lock();
			{
				lmt = m_array.limit();
			}
			m_arrayLock.unlock();

			return lmt;
		}

		FORCE_INLINE inttype set(inttype index, E element) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			inttype idx;

			m_arrayLock.lock();
			{
				idx = m_array.set(index, element);
			}
			m_arrayLock.unlock();

			return idx;
		}

		FORCE_INLINE inttype add(E element, boolean realloc = false, inttype thresh = 1024) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			inttype index;

			m_arrayLock.lock();
			{
				index = m_array.add(element, realloc, thresh);
			}
			m_arrayLock.unlock();

			return index;
		}

		FORCE_INLINE E get(inttype index) /* const */ {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			E element;

			m_arrayLock.lock();
			{
				element = m_array.get(index);
			}
			m_arrayLock.unlock();

			return element;
		}

		FORCE_INLINE E remove(inttype index) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			E element;

			m_arrayLock.lock();
			{
				element = m_array.remove(index);
			}
			m_arrayLock.unlock();

			return element;
		}

		FORCE_INLINE boolean contains(E element) /* const */ {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			boolean found = false;

			m_arrayLock.lock();
			{
				found = m_array.contains(element);
			}
			m_arrayLock.unlock();

			return found;
		}

		FORCE_INLINE inttype indexOf(E element) /* const */ {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			inttype ret = -1;

			m_arrayLock.lock();
			{
				ret = m_array.indexOf(element);
			}
			m_arrayLock.unlock();

			return ret;
		}

		void clear(inttype capacity = -1, boolean force = false) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_arrayLock.lock();
			{
				m_array.clear(capacity, force);
			}
			m_arrayLock.unlock();
		}

		FORCE_INLINE void copy(BasicArray<E>* array) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_arrayLock.lock();
			{
				m_array.copy(array);
			}
			m_arrayLock.unlock();
		}

		FORCE_INLINE void copy(LockableBasicArray<E>* array) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			array->m_arrayLock.lock();
			copy(&(array->m_array));
			array->m_arrayLock.unlock();
		}

		FORCE_INLINE inttype capacity(void) /* const */ {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			inttype cap;

			m_arrayLock.lock();
			{
				cap = m_array.capacity();
			}
			m_arrayLock.unlock();

			return cap;
		}

		FORCE_INLINE inttype size(inttype nsize) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			inttype osize;

			m_arrayLock.lock();
			{
				osize = m_array.size(nsize);
			}
			m_arrayLock.unlock();

			return osize;
		}

		FORCE_INLINE inttype size(void) /* const */ {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			inttype sz;

			m_arrayLock.lock();
			{
				sz = m_array.size();
			}
			m_arrayLock.unlock();

			return sz;
		}

		FORCE_INLINE void empty(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_arrayLock.lock();
			{
				m_array.empty();
			}
			m_arrayLock.unlock();
		}

		FORCE_INLINE void resize(inttype capacity) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_arrayLock.lock();
			{
				m_array.resize(capacity);
			}
			m_arrayLock.unlock();
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_LOCKABLEBASICARRAY_H_*/
