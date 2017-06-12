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
#ifndef CXX_UTIL_MAPENTRY_H_
#define CXX_UTIL_MAPENTRY_H_

#include "cxx/lang/types.h"

// XXX: see MEMORY_DEBUG_VALIDATE
#include "cxx/util/Converter.h"

namespace cxx { namespace util {

template<typename K, typename V, typename Ctx = void*>
class MapEntry /*: public Object */ {
	private:
		K m_key;
		V m_value;

	public:
		MapEntry(K key, V value, Ctx ctx):
			m_key(key),
			m_value(value) {
		}

		FORCE_INLINE void setKey(K key, Ctx ctx) {
			CXX_LANG_MEMORY_DEBUG_VALIDATE(K, m_key);

			m_key = key;

			CXX_LANG_MEMORY_DEBUG_VALIDATE(K, m_key);
		}

		FORCE_INLINE K getKey(void) const {
			CXX_LANG_MEMORY_DEBUG_VALIDATE(K, m_key);

			return m_key;
		}

		FORCE_INLINE void setValue(V value, Ctx ctx) {
			CXX_LANG_MEMORY_DEBUG_VALIDATE(V, m_value);

			m_value = value;

			CXX_LANG_MEMORY_DEBUG_VALIDATE(V, m_value);
		}

		FORCE_INLINE V getValue(void) const {
			CXX_LANG_MEMORY_DEBUG_VALIDATE(V, m_value);

			return m_value;
		}

		#ifdef CXX_LANG_MEMORY_DEBUG
		FORCE_INLINE void setKey(K key, Ctx ctx, boolean ignore) {
			if (ignore == false) {
				setKey(key, ctx);

			} else {
				m_key = key;
			}
		}

		FORCE_INLINE K getKey(boolean ignore) const {
			if (ignore == false) {
				return getKey();

			} else {
				return m_key;
			}
		}

		FORCE_INLINE void setValue(V value, Ctx ctx, boolean ignore) {
			if (ignore == false) {
				setValue(value, ctx);

			} else {
				m_value = value;
			}
		}

		FORCE_INLINE V getValue(boolean ignore) const {
			if (ignore == false) {
				return getValue();

			} else {
				return m_value;
			}
		}
		#endif
};

template<typename K, typename V, typename Ctx>
class Converter<MapEntry<K,V,Ctx>*> {
	public:
		static const MapEntry<K,V,Ctx>* NULL_VALUE;
		static const MapEntry<K,V,Ctx>* RESERVE;

		#if 0
		FORCE_INLINE static int hashCode(const MapEntry<K,V,Ctx>* o) {
			return ((Object*) o)->hashCode();
		}

		FORCE_INLINE static boolean equals(MapEntry<K,V,Ctx>* o1, MapEntry<K,V,Ctx>* o2) {
			return ((Object*) o1)->equals((Object*) o2);
		}
		#endif

		FORCE_INLINE static void destroy(MapEntry<K,V,Ctx>* o) {
			delete o;
		}

		FORCE_INLINE static void destroy(MapEntry<K,V,Ctx>* o, Ctx ctx) {
			delete o;
		}

		FORCE_INLINE static void validate(MapEntry<K,V,Ctx>* o) {
			// TODO: throw UnsupportedOperationException("Invalid validate request");
		}

		FORCE_INLINE static const bytearray toData(MapEntry<K,V,Ctx>* o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* Converter<MapEntry<K,V,Ctx>*>::NULL_VALUE = null;
template<typename K, typename V, typename Ctx>
const MapEntry<K,V,Ctx>* Converter<MapEntry<K,V,Ctx>*>::RESERVE = (MapEntry<K,V,Ctx>*)&Converter<MapEntry<K,V,Ctx>*>::NULL_VALUE;

} } // namespace

#endif /*CXX_UTIL_MAPENTRY_H_*/
