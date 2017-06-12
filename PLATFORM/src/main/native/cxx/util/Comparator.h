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
#ifndef CXX_UTIL_COMPARATOR_H_
#define CXX_UTIL_COMPARATOR_H_ 

#include "cxx/lang/Object.h"
#include "cxx/lang/nbyte.h"
#include "cxx/lang/Long.h"
#include "cxx/lang/Integer.h"
#include "cxx/lang/String.h"

#include "cxx/util/ArrayList.h"
#include "cxx/util/CompositeKey.h"

using namespace cxx::lang;

namespace cxx { namespace util {

template<typename T>
class Comparator {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const T o1, const T o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const T o1, const T o2) const {
		#endif
			return ((Comparable*) o1)->compareTo((Comparable*) o2);
		}
};

template<>
class Comparator<longtype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const longtype o1, const longtype o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const longtype o1, const longtype o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<ulongtype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const ulongtype o1, const ulongtype o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const ulongtype o1, const ulongtype o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<doubletype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const double o1, const double o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const double o1, const double o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<floattype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const float o1, const float o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const float o1, const float o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<inttype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const inttype o1, const inttype o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const inttype o1, const inttype o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<uinttype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const uinttype o1, const uinttype o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const uinttype o1, const uinttype o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<shorttype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const shorttype o1, const shorttype o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const shorttype o1, const shorttype o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<ushorttype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const ushorttype o1, const ushorttype o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const ushorttype o1, const ushorttype o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<chartype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const chartype o1, const chartype o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const chartype o1, const chartype o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<uchartype>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const uchartype o1, const uchartype o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const uchartype o1, const uchartype o2) const {
		#endif
			return (o1 < o2 ? -1 : ((o1 == o2) ? 0 : 1));
		}
};

template<>
class Comparator<Integer*>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const Integer* o1, const Integer* o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const Integer* o1, const Integer* o2) const {
		#endif
			return (o1->intValue() < o2->intValue() ? -1 : (o1->intValue() == o2->intValue() ? 0 : 1));
		}
};

template<>
class Comparator<Long*>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const Long* o1, const Long* o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const Long* o1, const Long* o2) const {
		#endif
			return (o1->longValue() < o2->longValue() ? -1 : (o1->longValue() == o2->longValue() ? 0 : 1));
		}
};

template<>
class Comparator<String*>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const String* o1, const String* o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const String* o1, const String* o2) const {
		#endif
			return o1->compareTo(o2);
		}
};

template<>
class Comparator<nbyte*>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const nbyte* o1, const nbyte* o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const nbyte* o1, const nbyte* o2) const {
		#endif
			return o1->compareTo(o2);
		}
};

template<>
class Comparator<CompositeKey*>  {
	private:
		ArrayList<KeyPart*> m_keyParts;

	public:
		Comparator() :
			m_keyParts(3, true) {
		}

		FORCE_INLINE void addKeyPart(bytetype type, int size = -1) {
			m_keyParts.add(new KeyPart(type, size));
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const CompositeKey* o1, const CompositeKey* o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const CompositeKey* o1, const CompositeKey* o2) const {
		#endif
			register int cursor = 0;
			register int cmp = 0;
			register int i = 0;

			register int size = m_keyParts.size();

			do {
				const KeyPart* keyPart = m_keyParts.get(i);

				switch(keyPart->getType()) {
					case KeyPart::INTEGER:
						cmp = compareNumber<inttype>(*o1 + cursor, *o2 + cursor);
						break;
					case KeyPart::LONG:
						cmp = compareNumber<longtype>(*o1 + cursor, *o2 + cursor);
						break;
					case KeyPart::SHORT:
						cmp = compareNumber<shorttype>(*o1 + cursor, *o2 + cursor);
						break;
					case KeyPart::FLOAT:
						cmp = compareNumber<floattype>(*o1 + cursor, *o2 + cursor);
						break;
					case KeyPart::DOUBLE:
						cmp = compareNumber<doubletype>(*o1 + cursor, *o2 + cursor);
						break;
					case KeyPart::STRING:
						cmp = compareString(*o1 + cursor, *o2 + cursor, keyPart->getSize());
						break;
					case KeyPart::BYTEARRAY:
						cmp = compareByteArray(*o1 + cursor, *o2 + cursor, keyPart->getSize());
						break;
				}

				if (cmp != 0) break;

				cursor += keyPart->getSize();

			} while (++i < size);

			#ifdef COM_DEEPIS_DB_CARDINALITY
			if ((pos != null) && (i > *pos)) {
				*pos = i;
			}
			#endif

			return cmp;
		}
};

} } // namespace

#endif /*CXX_UTIL_COMPARATOR_H_*/
