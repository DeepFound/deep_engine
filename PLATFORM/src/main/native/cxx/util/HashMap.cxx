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
#ifndef CXX_UTIL_HASHMAP_CXX_
#define CXX_UTIL_HASHMAP_CXX_

#include <string.h>

#include "cxx/util/HashMap.h"

using namespace cxx::util;

static long POLYS[] = {
	4 + 3,
	8 + 3,
	16 + 3,
	32 + 5,
	64 + 3,
	128 + 3,
	256 + 29,
	512 + 17,
	1024 + 9,
	2048 + 5,
	4096 + 83,
	8192 + 27,
	16384 + 43,
	32768 + 3,
	65536 + 45,
	131072 + 9,
	262144 + 39,
	524288 + 39,
	1048576 + 9,
	2097152 + 5,
	4194304 + 3,
	8388608 + 33,
	16777216 + 27,
	33554432 + 9,
	67108864 + 71,
	134217728 + 39,
	268435456 + 9,
	536870912 + 5,
	1073741824 + 83,
	0
};

namespace cxx { namespace util {

template<typename K, typename V, typename Ctx>
const Converter<K> HashMap<K,V,Ctx>::CONVERTER;

template<typename K, typename V, typename Ctx>
const int HashMap<K,V,Ctx>::RESIZE_FACTOR = 2;

template<typename K, typename V, typename Ctx>
const int HashMap<K,V,Ctx>::RESIZE_LOW_WATER = 8;

template<typename K, typename V, typename Ctx>
const int HashMap<K,V,Ctx>::RESIZE_HIGH_WATER = 3;

template<typename K, typename V, typename Ctx>
const int HashMap<K,V,Ctx>::INITIAL_CAPACITY = 16;

template<typename K, typename V, typename Ctx>
HashMap<K,V,Ctx>::HashMap(int cap, boolean delkey, boolean delval, boolean fixed):
#if 0
	HashMap<K,V,Ctx>(&HashMap<K,V,Ctx>::CONVERTER, cap, delkey, delval) {
#else
	m_poly(0),
	m_fill(0),
	m_size(0),
	m_capacity(0),
	m_entries(0),
	m_stateFlags(0),
	m_table(null),
	m_converter(&HashMap<K,V,Ctx>::CONVERTER),
	m_reserve((MapEntry<K,V,Ctx>*) &m_fill) {

	m_ctx = Converter<Ctx>::NULL_VALUE;

	if (cap != 0) {
		resize(cap);
	}

	setDeleteKey(delkey);
	setDeleteValue(delval);

	setFixed(fixed);
#endif
}

template<typename K, typename V, typename Ctx>
HashMap<K,V,Ctx>::HashMap(const Converter<K>* converter, int cap, boolean delkey, boolean delval, boolean fixed):
	m_poly(0),
	m_fill(0),
	m_size(0),
	m_capacity(0),
	m_entries(0),
	m_stateFlags(0),
	m_table(null),
	m_converter(converter),
	m_reserve((MapEntry<K,V,Ctx>*) &m_fill) {

	m_ctx = Converter<Ctx>::NULL_VALUE;

	if (cap != 0) {
		resize(cap);
	}

	setDeleteKey(delkey);
	setDeleteValue(delval);

	setFixed(fixed);
}

template<typename K, typename V, typename Ctx>
HashMap<K,V,Ctx>::~HashMap() {
	clear();
}

template<typename K, typename V, typename Ctx>
int HashMap<K,V,Ctx>::resize(int minused) {
	register int i = 0;
	register int size = 0;
	register long poly = 0;
	register int oldsize = m_size;
	register MapEntry<K,V,Ctx>** oldtable = m_table;

	for (i = 0, size = 4;; i++, size <<= 1) {
		if (i > ((int) (sizeof(POLYS) / sizeof(POLYS[0])))) {
			return -1;
		}

		if (size > minused) {
			poly = POLYS[i];
			break;
		}
	}

	m_capacity = size;

	int msize = size * sizeof(MapEntry<K,V,Ctx>*);
	MapEntry<K,V,Ctx>** table = (MapEntry<K,V,Ctx>**) malloc(msize);
	memset(table, 0, msize);

	m_fill = 0;
	m_entries = 0;
	m_size = size;
	m_poly = poly;
	m_table = table;

	for (i = 0; i < oldsize; i++) {
		register MapEntry<K,V,Ctx>* p = oldtable[i];
		if ((p != null) && (p != m_reserve)) {
			register int index = putIndex(p->getKey(), false);
			insertObject(p, index);
		}
	}

	free(oldtable);
	return m_size;
}

template<typename K, typename V, typename Ctx>
MapEntry<K,V,Ctx>* HashMap<K,V,Ctx>::removeObject(int index) {
	MapEntry<K,V,Ctx>* p = m_table[index];
	if ((p != null) && (p != m_reserve)) {
		m_table[index] = (MapEntry<K,V,Ctx>*) m_reserve;
		m_entries--;
		m_fill++;
	}

	// TODO: handle external iterator cursors
	if (((m_entries * RESIZE_LOW_WATER) < m_size) && (getFixed() == false)) {
		resize(m_entries * RESIZE_FACTOR);
	}

	return p;
}

template<typename K, typename V, typename Ctx>
MapEntry<K,V,Ctx>* HashMap<K,V,Ctx>::insertObject(MapEntry<K,V,Ctx>* x, int index) {
	if (m_table[index] == null) {
		m_fill++;
	}

	MapEntry<K,V,Ctx>* o = m_table[index];
	m_table[index] = x;
	m_entries++;

	return o;
}

template<typename K, typename V, typename Ctx>
int HashMap<K,V,Ctx>::putIndex(const K key, boolean check) {
	if ((check == true) && ((m_fill * RESIZE_HIGH_WATER) >= (m_size * RESIZE_FACTOR)) && (getFixed() == false)) {
		resize(m_entries * RESIZE_FACTOR);
	}

	int index = -1;
	if (check == true) {
		index = getIndex(key, true /* match slot */);
		MapEntry<K,V,Ctx>* x = m_table[index];
		if ((x == null) || (x == m_reserve)) {
			index = getIndex(key, false /* empty slot */);
		}

	} else {
		index = getIndex(key, false);
	}

	return index;
}

template<typename K, typename V, typename Ctx>
int HashMap<K,V,Ctx>::getIndex(const K key, boolean match) const {
	register long hash = m_converter->hashCode(key);
	register unsigned int mask = m_size - 1;
	register int i = (~hash) & mask;

	register MapEntry<K,V,Ctx>* p = m_table[i];
	if (p == null) {
		return i;

	} else if ((p == m_reserve) && (match == false)) {
		return i;

	} else if ((p != m_reserve) && m_converter->equals(p->getKey(), key)) {
		return i;
	}

	register long incr = (hash ^ ((long) hash >> 3)) & mask;
	if (incr == 0) {
		incr = mask;

	} else if (incr > mask) {
		incr ^= m_poly;
	}

	for (;;) {
		p = m_table[(i + incr) &mask];
		if (p == null) {
			return (i + incr) &mask;

		} else if ((p == m_reserve) && (match == false)) {
			return (i + incr) &mask;

		} else if ((p != m_reserve) && m_converter->equals(p->getKey(), key)) {
			return (i + incr) & mask;
		}

		incr = incr << 1;
		if (incr > mask) {
			incr ^= m_poly;
		}
	}

	return i;
}

template<typename K, typename V, typename Ctx>
V HashMap<K,V,Ctx>::put(K key, V val, K* retkey, boolean* status) {
	if (m_table == null) {
		resize(INITIAL_CAPACITY);
	}

	MapEntry<K,V,Ctx>* p = new MapEntry<K,V,Ctx>(key, val, getMapContext());
	val = Map<K,V>::NULL_VALUE;

	int index = putIndex(key, true);
	MapEntry<K,V,Ctx>* x = insertObject(p, index);
	if ((x != null) && (x != m_reserve)) {
		val = x->getValue();

		if (status != null) {
			*status = true;
		}

		if (retkey != null) {
			*retkey = x->getKey();

		} else if ((getDeleteKey() == true) && (key != x->getKey())) {
			m_converter->destroy(x->getKey());
		}

		Converter<MapEntry<K,V,Ctx>*>::destroy(x, getMapContext());

		// XXX: since ::insertObject always increments, account for replacement
		m_entries--;

	} else {
		if (status != null) {
			*status = false;
		}
	}

	return val;
}

template<typename K, typename V, typename Ctx>
void HashMap<K,V,Ctx>::putAll(const Map<K,V>* map, Map<K,V>* fillmap) {
	Set<MapEntry<K,V,Ctx>* >* set = ((HashMap<K,V,Ctx>*) map)->entrySet();

	K retkey = Map<K,V>::NULL_KEY;
	boolean status;
	Iterator<MapEntry<K,V,Ctx>* >* iter = set->iterator();
	while (iter->hasNext()) {
		const MapEntry<K,V,Ctx>* entry = (const MapEntry<K,V,Ctx>*) iter->next();
		if (fillmap != null) {
			V val = put(entry->getKey(), entry->getValue(), &retkey, &status);
			if (status == true) {
				fillmap->put(retkey, val);
			}

		} else {
			put(entry->getKey(), entry->getValue());
		}
	}

	delete iter;
	delete set;
}

template<typename K, typename V, typename Ctx>
V HashMap<K,V,Ctx>::remove(const K key, K* retkey, boolean* status) {
	if (m_table == null) {
		if (status != null) {
			*status = false;
		}

		return Map<K,V>::NULL_VALUE;
	}

	V val = Map<K,V>::NULL_VALUE;

	int index = getIndex(key, true);
	MapEntry<K,V,Ctx>* x = removeObject(index);
	if ((x != null) && (x != m_reserve)) {
		val = x->getValue();

		if (status != null) {
			*status = true;
		}

		if (retkey != null) {
			*retkey = x->getKey();

		} else if (getDeleteKey() == true) {
			m_converter->destroy(x->getKey());
		}

		Converter<MapEntry<K,V,Ctx>*>::destroy(x, getMapContext());

	} else {
		if (status != null) {
			*status = false;
		}
	}

	return val;
}

template<typename K, typename V, typename Ctx>
const V HashMap<K,V,Ctx>::get(const K key, K* retkey, boolean* status) const {
	if (m_table == null) {
		if (status != null) {
			*status = false;
		}

		return Map<K,V>::NULL_VALUE;
	}

	V val = Map<K,V>::NULL_VALUE;

	int index = getIndex(key, true);
	MapEntry<K,V,Ctx>* x = m_table[index];
	if ((x != null) && (x != m_reserve)) {
		if (status != null) {
			*status = true;
		}

		if (retkey != null) {
			*retkey = x->getKey();
		}

		val = x->getValue();

	} else {
		if (status != null) {
			*status = false;
		}
	}

	return val;
}

template<typename K, typename V, typename Ctx>
boolean HashMap<K,V,Ctx>::containsKey(const K key) const {
	boolean status;

	get(key, null, &status);

	return status;
}

template<typename K, typename V, typename Ctx>
boolean HashMap<K,V,Ctx>::containsValue(const V val) const {
	// TODO
	return false;
}

template<typename K, typename V, typename Ctx>
boolean HashMap<K,V,Ctx>::isEmpty() const {
	return (size() == 0);
}

template<typename K, typename V, typename Ctx>
int HashMap<K,V,Ctx>::size() const {
	return m_entries;
}

template<typename K, typename V, typename Ctx>
int HashMap<K,V,Ctx>::capacity() const {
	return m_capacity;
}

template<typename K, typename V, typename Ctx>
void HashMap<K,V,Ctx>::clear(boolean delkey, boolean delval) {
	if (m_table == null) {
		return;
	}

	register MapEntry<K,V,Ctx>** table = m_table;
	m_table = null;

	register int i, n = m_size;
	m_size = 0;

	m_fill = 0;
	m_entries = 0;

	for (i = 0; i < n; i++) {
		MapEntry<K,V,Ctx>* x = table[i];
		if ((x != null) && (x != m_reserve)) {
			if (delkey == true) {
				m_converter->destroy(x->getKey());
			}

			if (delval == true) {
				Converter<V>::destroy(x->getValue());
			}

			Converter<MapEntry<K,V,Ctx>*>::destroy(x, getMapContext());
		}
	}

	free(table);
}

template<typename K, typename V, typename Ctx>
Set<MapEntry<K,V,Ctx>* >* HashMap<K,V,Ctx>::entrySet(Set<MapEntry<K,V,Ctx>* >* fillset) {
	if (fillset != null) {
		// TODO: check type
		((EntrySet<>*) fillset)->reset(this);

	} else {
		fillset = new EntrySet<>(this);
	}

	return fillset;
}

template<typename K, typename V, typename Ctx>
Set<K>* HashMap<K,V,Ctx>::keySet(Set<K>* fillset) {
	if (fillset != null) {
		// TODO: check type
		((KeySet*) fillset)->reset(this);

	} else {
		fillset = new KeySet(this);
	}

	return fillset;
}

template<typename K, typename V, typename Ctx>
Collection<V>* HashMap<K,V,Ctx>::values() {
	return null;
}

} } // namespace

#endif /*CXX_UTIL_HASHMAP_CXX_*/
