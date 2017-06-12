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
#ifndef CXX_UTIL_COMPOSITEKEY
#define CXX_UTIL_COMPOSITEKEY

#include "cxx/lang/Object.h"
#include "cxx/lang/nbyte.h"
#include "cxx/lang/UnsupportedOperationException.h"

using namespace cxx::lang;

namespace cxx { namespace util {

class CompositeKey : public nbyte {
	public:
		CompositeKey(const inttype size):
			nbyte(size) {
		}

		CompositeKey(const nbyte* bytes):
			nbyte(bytes) {
		}

		CompositeKey(const bytearray data, const inttype size):
			nbyte(data, size) {
		}

		CompositeKey(const voidarray data, const inttype size):
			nbyte(data, size) {
		}
};

template<typename T>
inline int compareNumber(const bytearray o1, const bytearray o2) {
	return *((T*) o1) < *((T*) o2) ? -1 : ((*((T*) o1) == *((T*) o2)) ? 0 : 1);
}

inline int compareByteArray(const bytearray o1, const bytearray o2, int size) {
	nbyte key1(o1, size);
	nbyte key2(o2, size);
	return key1.compareTo(&key2);
}

inline int compareString(const bytearray o1, const bytearray o2, int size) {
	return strncmp(o1, o2, size);
}

class KeyPart : public Object {

	public:
		static const bytetype BYTEARRAY = 0;
		static const bytetype BYTE      = 1;
		static const bytetype CHAR      = 2;
		static const bytetype LONG      = 3;
		static const bytetype SHORT     = 4;
		static const bytetype FLOAT     = 5;
		static const bytetype DOUBLE    = 6;
		static const bytetype STRING    = 7;
		static const bytetype INTEGER   = 8;

		static int getKeyPartSize(bytetype type, int size) {
			if (size == -1) {
				switch(type) {
					case KeyPart::INTEGER:
						return sizeof(inttype);

					case KeyPart::LONG:
						return sizeof(longtype);

					case KeyPart::SHORT:
						return sizeof(shorttype);

					case KeyPart::FLOAT:
						return sizeof(floattype);

					case KeyPart::DOUBLE:
						return sizeof(doubletype);

					case KeyPart::STRING:
					case KeyPart::BYTEARRAY:
					default:
						throw UnsupportedOperationException("Invalid key part size");
				}

			} else {
				return size;
			}
		}

	private:
		int m_type;
		int m_size;
		int m_offset;

	public:
		KeyPart(bytetype type, int size, int offset = -1) :
			m_type(type),
			m_size(getKeyPartSize(type, size)),
			m_offset(offset) {
		}

		inline int getSize() const {
			return m_size;
		}

		inline int getType() const {
			return m_type;
		}

		inline int getOffset() const {
			return m_offset;
		}
};
} } // namespace

#endif /*CXX_UTIL_COMPOSITEKEY*/
