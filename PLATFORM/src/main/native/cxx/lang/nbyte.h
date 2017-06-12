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
#ifndef CXX_LANG_NBYTE_H_
#define CXX_LANG_NBYTE_H_

#include <stdlib.h>
#include <string.h>

#include "cxx/lang/types.h"

namespace cxx { namespace lang {

class nbyte {

	CXX_LANG_MEMORY_DEBUG_DECL()

	private:
		bytearray m_data;

	public:
		// XXX: volatile is for realloc
		volatile const inttype length;

	private:
		boolean m_alloc;

		nbyte(const nbyte& bytes);
		nbyte& operator=(nbyte& bytes);

	public:
		FORCE_INLINE nbyte(const inttype size):
			m_data((bytearray) malloc(size)),
			length(size),
			m_alloc(true) {

			CXX_LANG_MEMORY_DEBUG_INIT()
		}

		FORCE_INLINE nbyte(const nbyte* bytes):
			m_data((bytearray) malloc(bytes->length)),
			length(bytes->length),
			m_alloc(true) {

			CXX_LANG_MEMORY_DEBUG_INIT()

			memcpy(m_data, (voidarray) *bytes, length);
		}

		FORCE_INLINE nbyte(const bytearray data, const inttype size):
			m_data((bytearray) data),
			length(size),
			m_alloc(false) {

			CXX_LANG_MEMORY_DEBUG_INIT()
		}

		FORCE_INLINE nbyte(const voidarray data, const inttype size):
			m_data((bytearray) data),
			length(size),
			m_alloc(false) {

			CXX_LANG_MEMORY_DEBUG_INIT()
		}

		FORCE_INLINE ~nbyte(void) {
			CXX_LANG_MEMORY_DEBUG_CLEAR()

			if (m_alloc == true) {
				free(m_data);
			}
		}

		FORCE_INLINE void zero(void) {
			memset(m_data, 0x0, length);
		}

		FORCE_INLINE boolean isZero(void) {
			return (length == 0) || ((m_data[0] == 0) && (memcmp(m_data, m_data + 1, length - 1) == 0));
		}

		FORCE_INLINE nbyte* alloc(const inttype size) {
			// XXX: user-beware there is no deallocation
			m_data = (bytearray) malloc(size);

			int* address = (int*) &length;
			(*address) = size;

			m_alloc = true;

			return this;
		}

		FORCE_INLINE nbyte* realloc(const inttype size) {
			m_data = (bytearray) ::realloc(m_data, size);

			int* address = (int*) &length;
			(*address) = size;

			return this;
		}

		FORCE_INLINE nbyte* extend (const inttype size) {
			bytearray data = (bytearray) malloc(size);

			#ifdef DEEP_DEBUG
			memset(data, 0x0, size);
			#endif 

			memcpy(data, m_data, length);

			if (m_alloc == true) {
				free(m_data);
			}

			m_data = data;
			
			int* address = (int*) &length;
			(*address) = size;

			m_alloc = true;

			return this;
		}

		FORCE_INLINE nbyte* reassign(const bytearray data, const inttype size) {
			// XXX: user-beware there is no deallocation
			m_data = (bytearray) data;

			int* address = (int*) &length;
			(*address) = size;

			return this;
		}

		FORCE_INLINE bytetype& operator[] (const inttype pos) {
			return m_data[pos];
		}

		FORCE_INLINE bytetype operator[] (const inttype pos) const {
			return m_data[pos];
		}

		FORCE_INLINE operator bytearray(void) const {
			return (bytearray) m_data;
		}

		FORCE_INLINE operator voidarray(void) const {
			return (voidarray) m_data;
		}

		// XXX: cxx::lang::Object method, though byte is a primitive
		FORCE_INLINE nbyte* clone() const {
			nbyte* bytes = new nbyte(length);
			memcpy((voidarray) *bytes, m_data, length);
			return bytes;
		}

		// XXX: cxx::lang::Object method, though byte is a primitive
		FORCE_INLINE long hashCode(void) const {
			long hash = 0;
			if (length > 0) {
				long offset = 0;
				const bytearray val = m_data;
				for (int i = 0; i < length; i++) {
					hash = 31 * hash + val[offset++];
				}
			}

			return hash;
		}

		// XXX: cxx::lang::Object method, though byte is a primitive
		FORCE_INLINE boolean equals(const nbyte* bytes) const {
			return bytes != null && (length != bytes->length) ? false : memcmp(m_data, bytes->m_data, length) == 0;
		}

		// XXX: cxx::lang::Object method, though byte is a primitive
		FORCE_INLINE inttype compareTo(const nbyte* obj) const {
			if (length == obj->length) {
				return memcmp(m_data, obj->m_data, length);

			} else {
				return length - obj->length;
			}
		}
};

} } // namespace

#endif /*CXX_LANG_NBYTE_H_*/
