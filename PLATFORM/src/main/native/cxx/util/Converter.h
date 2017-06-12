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
#ifndef CXX_UTIL_CONVERTER_H_
#define CXX_UTIL_CONVERTER_H_

#include <float.h>
#include <limits.h>

#include "cxx/lang/Object.h"
#include "cxx/lang/nbyte.h"
#include "cxx/lang/Long.h"
#include "cxx/lang/Short.h"
#include "cxx/lang/Float.h"
#include "cxx/lang/Double.h"
#include "cxx/lang/Integer.h"
#include "cxx/lang/String.h"
#include "cxx/lang/UnsupportedOperationException.h"

#include "cxx/util/CompositeKey.h"

using namespace cxx::lang;

namespace cxx { namespace util {

template<typename T>
class Converter {
	public:
		static const T NULL_VALUE;
		static const T RESERVE;

		FORCE_INLINE static int hashCode(const T o) {
			return ((Object*) o)->hashCode();
		}

		FORCE_INLINE static boolean equals(T o1, T o2) {
			return ((Object*) o1)->equals((Object*) o2);
		}

		FORCE_INLINE static void destroy(T o) {
			delete o;
		}

		FORCE_INLINE static void validate(T o) {
			// TODO: throw UnsupportedOperationException("Invalid validate request");
		}

		FORCE_INLINE static const bytearray toData(T o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<typename T>
const T Converter<T>::NULL_VALUE = null;
template<typename T>
const T Converter<T>::RESERVE = (T)&Converter<T>::NULL_VALUE;

template<>
class Converter<boolean> {
	public:
		static const boolean NULL_VALUE;
		static const boolean RESERVE;

		FORCE_INLINE static int hashCode(const boolean o) {
			return (int) o;
		}

		FORCE_INLINE static boolean equals(boolean o1, boolean o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(boolean o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(boolean o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const boolean& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

// XXX: place holders
template<>
class Converter<chartype> {
	public:
		static const chartype NULL_VALUE;
		static const chartype RESERVE;

		FORCE_INLINE static int hashCode(const chartype o) {
			return (int) o;
		}

		FORCE_INLINE static boolean equals(chartype o1, chartype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(chartype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(chartype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const chartype& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

// XXX: uchartype equivalent to the boolean type
template<>
class Converter<uchartype> {
	public:
		static const uchartype NULL_VALUE;
		static const uchartype RESERVE;

		FORCE_INLINE static int hashCode(const uchartype o) {
			return (int) o;
		}

		FORCE_INLINE static boolean equals(uchartype o1, uchartype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(uchartype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(uchartype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const uchartype& o) {
			return (const bytearray) &o;
		}
};

template<>
class Converter<shorttype> {
	public:
		static const shorttype NULL_VALUE;
		static const shorttype RESERVE;

		FORCE_INLINE static int hashCode(const shorttype o) {
			return (int) o;
		}

		FORCE_INLINE static boolean equals(shorttype o1, shorttype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(shorttype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(shorttype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const shorttype& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<ushorttype> {
	public:
		static const ushorttype NULL_VALUE;
		static const ushorttype RESERVE;

		FORCE_INLINE static int hashCode(const ushorttype o) {
			return (int) o;
		}

		FORCE_INLINE static boolean equals(ushorttype o1, ushorttype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(ushorttype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(ushorttype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const ushorttype& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<inttype> {
	public:
		static const inttype NULL_VALUE;
		static const inttype RESERVE;

		FORCE_INLINE static int hashCode(const inttype o) {
			return (int) o;
		}

		FORCE_INLINE static boolean equals(inttype o1, inttype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(inttype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(inttype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const inttype& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<uinttype> {
	public:
		static const uinttype NULL_VALUE;
		static const uinttype RESERVE;

		FORCE_INLINE static int hashCode(const uinttype o) {
			return (int) o;
		}

		FORCE_INLINE static boolean equals(uinttype o1, uinttype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(uinttype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(uinttype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const uinttype& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<floattype> {
	public:
		static const floattype NULL_VALUE;
		static const floattype RESERVE;

		FORCE_INLINE static int hashCode(const floattype o) {
			return (int) o;
		}

		FORCE_INLINE static boolean equals(floattype o1, floattype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(floattype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(floattype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const floattype& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<longtype> {
	public:
		static const longtype NULL_VALUE;
		static const longtype RESERVE;

		FORCE_INLINE static int hashCode(const longtype o) {
			return (int) (o ^ (o >> 32));
		}

		FORCE_INLINE static boolean equals(longtype o1, longtype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(longtype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(longtype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const longtype& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<ulongtype> {
	public:
		static const ulongtype NULL_VALUE;
		static const ulongtype RESERVE;

		FORCE_INLINE static int hashCode(const ulongtype o) {
			return (int) (o ^ (o >> 32));
		}

		FORCE_INLINE static boolean equals(ulongtype o1, ulongtype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(ulongtype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(ulongtype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const ulongtype& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<doubletype> {
	public:
		static const doubletype NULL_VALUE;
		static const doubletype RESERVE;

		FORCE_INLINE static int hashCode(const doubletype o) {
			return (int) o;
		}

		FORCE_INLINE static boolean equals(doubletype o1, doubletype o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(doubletype o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(doubletype o) {
			// nothing to do
		}

		FORCE_INLINE static const bytearray toData(const doubletype& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<Object*> {
	public:
		static const Object* NULL_VALUE;
		static const Object* RESERVE;

		FORCE_INLINE static int hashCode(const Object* o) {
			return o->hashCode();
		}

		FORCE_INLINE static boolean equals(Object* o1, Object* o2) {
			return o1->equals(o2);
		}

		FORCE_INLINE static void destroy(Object* o) {
			delete o;
		}

		FORCE_INLINE static void validate(Object* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}

		FORCE_INLINE static const bytearray toData(const CompositeKey* o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<Short*> {
	public:
		static const Short* NULL_VALUE;
		static const Short* RESERVE;

		FORCE_INLINE static int hashCode(const Short* o) {
			return o->hashCode();
		}

		FORCE_INLINE static boolean equals(Short* o1, Short* o2) {
			return o1->shortValue() == o2->shortValue();
		}

		FORCE_INLINE static void destroy(Short* o) {
			delete o;
		}

		FORCE_INLINE static void validate(Short* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}

		FORCE_INLINE static const bytearray toData(const Short* o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<Integer*> {
	public:
		static const Integer* NULL_VALUE;
		static const Integer* RESERVE;

		FORCE_INLINE static int hashCode(const Integer* o) {
			return o->hashCode();
		}

		FORCE_INLINE static boolean equals(Integer* o1, Integer* o2) {
			return o1->intValue() == o2->intValue();
		}

		FORCE_INLINE static void destroy(Integer* o) {
			delete o;
		}

		FORCE_INLINE static void validate(Integer* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}

		FORCE_INLINE static const bytearray toData(const Integer* o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<Float*> {
	public:
		static const Float* NULL_VALUE;
		static const Float* RESERVE;

		FORCE_INLINE static int hashCode(const Float* o) {
			return o->hashCode();
		}

		FORCE_INLINE static boolean equals(Float* o1, Float* o2) {
			return o1->floatValue() == o2->floatValue();
		}

		FORCE_INLINE static void destroy(Float* o) {
			delete o;
		}

		FORCE_INLINE static void validate(Float* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}

		FORCE_INLINE static const bytearray toData(const Float* o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<Long*> {
	public:
		static const Long* NULL_VALUE;
		static const Long* RESERVE;

		FORCE_INLINE static int hashCode(const Long* o) {
			return o->hashCode();
		}

		FORCE_INLINE static boolean equals(Long* o1, Long* o2) {
			return o1->longValue() == o2->longValue();
		}

		FORCE_INLINE static void destroy(Long* o) {
			delete o;
		}

		FORCE_INLINE static void validate(Long* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}

		FORCE_INLINE static const bytearray toData(const Long* o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<String*> {
	public:
		static const String* NULL_VALUE;
		static const String* RESERVE;

		FORCE_INLINE static int hashCode(const String* o) {
			return o->hashCode();
		}

		FORCE_INLINE static boolean equals(String* o1, String* o2) {
			return (o1->compare((String&) *o2) == 0);
		}

		FORCE_INLINE static void destroy(String* o) {
			delete o;
		}

		FORCE_INLINE static void validate(String* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}

		FORCE_INLINE static const bytearray toData(const String* o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<nbyte*> {
	public:
		static const nbyte* NULL_VALUE;
		static const nbyte* RESERVE;

		FORCE_INLINE static int hashCode(const nbyte* o) {
			return o->hashCode();
		}

		FORCE_INLINE static boolean equals(nbyte* o1, nbyte* o2) {
			return o1->equals(o2);
		}

		FORCE_INLINE static void destroy(nbyte* o) {
			delete o;
		}

		FORCE_INLINE static void validate(nbyte* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}

		FORCE_INLINE static const bytearray toData(const nbyte* o) {
			return *o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<>
class Converter<CompositeKey*> {
	public:
		static const CompositeKey* NULL_VALUE;
		static const CompositeKey* RESERVE;

		FORCE_INLINE static int hashCode(const CompositeKey* o) {
			throw UnsupportedOperationException("Invalid CompositeKey hashCode");
		}

		FORCE_INLINE static boolean equals(CompositeKey* o1, CompositeKey* o2) {
			return o1->equals(o2);
		}

		FORCE_INLINE static void destroy(CompositeKey* o) {
			delete o;
		}

		FORCE_INLINE static void validate(CompositeKey* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}

		FORCE_INLINE static const bytearray toData(const CompositeKey* o) {
			return *o;
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

} } // namespace

#endif /*CXX_UTIL_CONVERTER_H_*/
