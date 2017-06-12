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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_BASICARRAY_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_BASICARRAY_H_ 

#include "cxx/lang/System.h"
#include "cxx/util/Logger.h"
#include "cxx/util/Converter.h"
#include "cxx/util/Collections.h"

#include "com/deepis/db/store/relative/util/InvalidException.h"

using namespace cxx::lang;
using namespace cxx::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

// XXX: this class is based on cxx:util::ArrayList and designed for fixed-size performant use cases

template<typename E, boolean reallocDefault = false>
class BasicArray {

	private:
		E* m_array;
		inttype m_size;
		inttype m_capacity;
		boolean m_deleteValue;

		BasicArray& operator=(BasicArray& array);

	public:
		FORCE_INLINE void resize(inttype capacity) {
			E* array = (E*) malloc(capacity * sizeof(E));
			memcpy(array, m_array, m_capacity * sizeof(E));
			free(m_array);
			m_array = array;
			m_capacity = capacity;
		}

	public:
		BasicArray(inttype capacity = 10, boolean deleteValue = false):
			m_array((E*) malloc(capacity * sizeof(E))), m_size(0), m_capacity(capacity), m_deleteValue(deleteValue) {
			memset(m_array, 0, sizeof(E) * capacity);
		}

		BasicArray(BasicArray<E,reallocDefault>* array):
			m_array((E*) malloc(array->m_capacity * sizeof(E))), m_size(array->m_size), m_capacity(array->m_capacity), m_deleteValue(false) {
			memcpy(m_array, array->m_array, array->m_capacity * sizeof(E));
		}

		BasicArray(const BasicArray<E,reallocDefault>& array) :
			m_array((E*) malloc(array.m_capacity * sizeof(E))), m_size(array.m_size), m_capacity(array.m_capacity), m_deleteValue(array.m_deleteValue) {
			memcpy(m_array, array.m_array, array.m_capacity * sizeof(E));
		}

		virtual ~BasicArray(void) {
			clear();

			free(m_array);
		}

		FORCE_INLINE boolean limit(void) const {
			return ((m_size + 1) > m_capacity);
		}

		FORCE_INLINE inttype set(inttype index, E element, boolean realloc = reallocDefault, inttype thresh = 1024) {
			if (index >= m_capacity) {
				if (realloc == false) {
					DEEP_LOG(ERROR, OTHER, "Invalid array index: %d\n", index);

					#ifdef DEEP_DEBUG
					throw InvalidException("Invalid array index");
					#else
					return -1;
					#endif
				}

				inttype required = (index - m_capacity) + 1;
				resize((m_capacity <= thresh) ? (m_capacity + 5 + required) : (((m_capacity * 3) / 2) + required));
			}

			m_array[index] = element;
			return index;
		}

		FORCE_INLINE inttype add(E element, boolean realloc = reallocDefault, inttype thresh = 1024) {
			if ((m_size + 1) > m_capacity) {
				if (realloc == false) {
					#ifdef DEEP_DEBUG
					DEEP_LOG(ERROR, OTHER, "Invalid array capacity: %d\n", m_capacity);

					throw InvalidException("Invalid array capacity");
					#else
					return -1;
					#endif
				}

				resize(m_capacity <= thresh ? m_capacity + 5 : ((m_capacity * 3) / 2));
			}

			m_array[m_size++] = element;
			return m_size - 1;
		}

		FORCE_INLINE E get(inttype index) const {
			#ifdef DEEP_DEBUG
			if (index >= m_capacity) {
				DEEP_LOG(ERROR, OTHER, "Invalid array index: %d\n", index);

				throw InvalidException("Invalid array index");
			}
			#endif

			return m_array[index];
		}

		FORCE_INLINE E remove(inttype index) {
			#ifdef DEEP_DEBUG
			if (index >= m_capacity) {
				DEEP_LOG(ERROR, OTHER, "Invalid array index: %d\n", index);

				throw InvalidException("Invalid array index");
			}
			#endif

			E element = m_array[index];

			inttype move = (m_size - index) - 1;
			if (move > 0) {
				System::arraycopy(m_array, index + 1, m_array, index, move);
			}

			m_size--;
			return element;
		}

		FORCE_INLINE boolean contains(E element) const {
			boolean found = false;

			for (inttype i = 0; i < size(); i++) {
				if (m_array[i] == element) {
					found = true;
					break;
				}
			}

			return found;
		}

		FORCE_INLINE inttype indexOf(E element) const {
			inttype index = -1;

			for (inttype i = 0; i < size(); i++) {
				if (m_array[i] == element) {
					index = i;
					break;
				}
			}

			return index;
		}

		void clear(inttype capacity = -1, boolean force = false) {
			if (m_deleteValue == true) {
				for (inttype i = 0; i < size(); i++) {
					Converter<E>::destroy(m_array[i]);
				}
			}

			if ((capacity > 0) && ((m_capacity < capacity) || (m_capacity > (capacity * 2)) || (force == true))) {
				free(m_array);

				m_capacity = ((capacity * 3) / 2);
				m_array = (E*) malloc(m_capacity * sizeof(E));
			}

			m_size = 0;

			empty();
		}

		FORCE_INLINE void copy(BasicArray<E,reallocDefault>* array) {
			#ifdef DEEP_DEBUG
			if (m_capacity < array->m_size) {
				DEEP_LOG(ERROR, OTHER, "Invalid array capacity: %d\n", m_capacity);

				throw InvalidException("Invalid array capacity");
			}
			#endif

			memcpy(m_array, array->m_array, array->m_size * sizeof(E));

			m_size = array->m_size;
		}

		FORCE_INLINE inttype capacity(void) const {
			return m_capacity;
		}

		FORCE_INLINE inttype size(inttype nsize) {
			inttype osize = m_size;
			m_size = nsize;
			return osize;
		}

		FORCE_INLINE inttype size(void) const {
			return m_size;
		}

		FORCE_INLINE boolean isEmpty(void) const {
			return size() == 0;
		}

		FORCE_INLINE void empty(void) {
			memset(m_array, 0, sizeof(E) * m_capacity);
		}
		
		class BasicArrayIterator : public Iterator<E> {
			private:
				inttype m_cursor;
				BasicArray<E,reallocDefault>* m_array;

			public:
				BasicArrayIterator(BasicArray<E,reallocDefault>* array) : m_cursor(0), m_array(array) {
				}
				FORCE_INLINE virtual boolean hasNext() {
					return m_cursor < m_array->size();
				}
				FORCE_INLINE virtual const E next() {
					// TODO: throw NoSuchElementException when end is each (XXX: check will be a performance hit)
					return m_array->get(m_cursor++);
				}
				FORCE_INLINE virtual void remove() {
					E element = m_array->remove(--m_cursor);
					if (m_array->m_deleteValue) {
						Converter<E>::destroy(element);
					}
				}
		};
		
		FORCE_INLINE BasicArrayIterator* iterator() {
			return new BasicArrayIterator(this);
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_BASICARRAY_H_*/
