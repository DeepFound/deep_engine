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
#ifndef CXX_UTIL_MAP_H_
#define CXX_UTIL_MAP_H_

#include "cxx/lang/Object.h"

#include "cxx/util/MapEntry.h"
#include "cxx/util/Set.h"
#include "cxx/util/Collection.h"
#include "cxx/util/Converter.h"

using namespace cxx::lang;

namespace cxx { namespace util {

template<typename K, typename V, typename Ctx = void*>
class Map : public Object {
	protected:
		static const K NULL_KEY;
		static const V NULL_VALUE;

	public:
		Map() {
		}

		virtual ~Map(void) = 0;

		virtual V put(K key, V val) = 0;
		virtual void putAll(const Map<K,V,Ctx>* map) = 0;

		virtual V remove(const K key) = 0;

		virtual const V get(const K key) const = 0;

		virtual boolean containsKey(const K key) const = 0;
		virtual boolean containsValue(const V val) const = 0;

		virtual boolean isEmpty() const = 0;
		virtual int size() const = 0;
		virtual void clear() = 0;

		virtual Set<MapEntry<K,V,Ctx>* >* entrySet() = 0;
		virtual Set<K>* keySet() = 0;

		virtual Collection<V>* values() = 0;
};

template<typename K, typename V, typename Ctx>
const K Map<K,V,Ctx>::NULL_KEY = (const K)Converter<K>::NULL_VALUE;

template<typename K, typename V, typename Ctx>
const V Map<K,V,Ctx>::NULL_VALUE = (const V)Converter<V>::NULL_VALUE;

template<typename K, typename V, typename Ctx>
inline Map<K,V,Ctx>::~Map() {}

} } // namespace

#endif /*CXX_UTIL_MAP_H_*/
