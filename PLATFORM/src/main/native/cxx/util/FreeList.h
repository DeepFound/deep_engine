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
#ifndef CXX_UTIL_FREELIST_H_
#define CXX_UTIL_FREELIST_H_

namespace cxx { namespace util {

template<typename T>
class FreeList {
	private:
		static const int CAPACITY = 10;
		T** m_freeItems;
		int m_length;
		int m_size;

	public:
		explicit FreeList(int capacity = CAPACITY) : m_freeItems(new T*[capacity]), m_length(capacity), m_size(0) {
		}
		virtual ~FreeList() {
			for (int i=0; i<m_size; i++) {
				delete m_freeItems[i];
			}
			delete [] m_freeItems;
		}

		inline T* create() {
			return m_size <= 0 ? new T() : m_freeItems[--m_size];
		}

		inline void destroy(T* freeItem) {
			if (m_size >= m_length) {
				delete freeItem;

			} else {
				m_freeItems[m_size++] = freeItem;
			}
		}

		inline boolean isEmpty() const {
			return m_size == 0;
		}

		inline int size() const {
			return m_size;
		}
	};

} } // namespace

#endif /*CXX_UTIL_FREELIST_H_*/
