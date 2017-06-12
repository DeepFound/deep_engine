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
#ifndef CXX_UTIL_CONCURRENT_FREELIST_H_
#define CXX_UTIL_CONCURRENT_FREELIST_H_

namespace cxx { namespace util { namespace concurrent {

template<typename T>
class FreeList {

	private:
		static const inttype CAPACITY = 10;

		T** m_elements;
		inttype m_length;
		volatile inttype m_size;

		FORCE_INLINE inttype incref(void) {
			return __sync_add_and_fetch(&m_size, 1);
		}

		FORCE_INLINE inttype decref(void) {
			return __sync_sub_and_fetch(&m_size, 1);
		}

	public:
		explicit FreeList(inttype capacity = CAPACITY) : m_elements(new T*[capacity]), m_length(capacity), m_size(0) {
		}

		virtual ~FreeList() {
			for (int i = 0; i < m_size; i++) {
				delete m_elements[i];
			}

			delete [] m_elements;
		}

		FORCE_INLINE T* create() {
			inttype size = decref();
			if (size < 0) {
				incref();
			}

			return size < 0 ? new T() : m_elements[size];
		}

		FORCE_INLINE void destroy(T* ptr) {
			inttype size = incref();
			if (size <= m_length) {
				m_elements[size - 1] = ptr;

			} else {
				delete ptr;
			}
		}

		FORCE_INLINE T get(inttype index) const {
			return m_elements[index];
		}

		FORCE_INLINE boolean isEmpty() const {
			return m_size == 0;
		}

		FORCE_INLINE int size() const {
			return m_size;
		}
};

} } } // namespace

#endif /*CXX_UTIL_CONCURRENT_FREELIST_H_*/
