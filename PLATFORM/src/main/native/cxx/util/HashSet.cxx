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
#ifndef CXX_UTIL_HASHSET_CXX_
#define CXX_UTIL_HASHSET_CXX_

#include <string.h>

#include "cxx/util/Converter.cxx"
#include "cxx/util/HashSet.h"

using namespace cxx::util;

static long HASH_POLYS[] = {
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

template<typename E>
const int HashSet<E>::RESIZE_FACTOR = 2;

template<typename E>
const int HashSet<E>::RESIZE_LOW_WATER = 8;

template<typename E>
const int HashSet<E>::RESIZE_HIGH_WATER = 3;

template<typename E>
const int HashSet<E>::INITIAL_CAPACITY = 16;

template<typename E>
HashSet<E>::HashSet(int cap, boolean delelem, boolean fixed):
	m_poly(0),
	m_fill(0),
	m_size(0),
	m_entries(0),
	m_table(null),
	m_stateFlags(0) {

	if (cap != 0) {
		resize(cap);
	}

	setDeleteElement(delelem);

	setFixed(fixed);
}

template<typename E>
HashSet<E>::~HashSet() {
	clear();
}

template<typename E>
int HashSet<E>::resize(int minused) {
	int i = 0;
	int size = 0;
	long poly = 0;
	int oldsize = m_size;
	E* oldtable = m_table;

	for (i = 0, size = 4;; i++, size <<= 1) {
		if (i > ((int) (sizeof(HASH_POLYS) / sizeof(HASH_POLYS[0])))) {
			return -1;
		}

		if (size > minused) {
			poly = HASH_POLYS[i];
			break;
		}
	}

	int msize = size * sizeof(E);
	E* table = (E*) malloc(msize);
	memset(table, (longtype) Set<E>::NULL_VALUE, msize);

	m_fill = 0;
	m_entries = 0;
	m_size = size;
	m_poly = poly;
	m_table = table;

	for (i = 0; i < oldsize; i++) {
		E p = oldtable[i];
		if ((p != Set<E>::NULL_VALUE) && (p != HashSet<E>::RESERVE)) {
			int index = putIndex(p, false);
			insertObject(p, index);
		}
	}

	free(oldtable);
	return m_size;
}

template<typename E>
E HashSet<E>::removeObject(int index) {
	E p = m_table[index];
	if ((p != Set<E>::NULL_VALUE) && (p != HashSet<E>::RESERVE)) {
		m_table[index] = HashSet<E>::RESERVE;
		m_entries--;
		m_fill++;
	}

	// TODO: handle external iterator cursors
	if (((m_entries * RESIZE_LOW_WATER) < m_size) && (getFixed() == false)) {
		resize(m_entries * RESIZE_FACTOR);
	}

	return p;
}

template<typename E>
E HashSet<E>::insertObject(E x, int index) {
	if (m_table[index] == Set<E>::NULL_VALUE) {
		m_fill++;
	}

	E o = m_table[index];
	m_table[index] = x;
	m_entries++;

	return o;
}

template<typename E>
int HashSet<E>::putIndex(const E x, boolean check) {
	if ((check == true) && ((m_fill * RESIZE_HIGH_WATER) >= (m_size * RESIZE_FACTOR)) && (getFixed() == false)) {
		resize(m_entries * RESIZE_FACTOR);
	}

	int index = -1;
	if (check == true) {
		index = getIndex(x, true /* match slot */);
		E o = m_table[index];
		if ((o == Set<E>::NULL_VALUE) || (o == HashSet<E>::RESERVE)) {
			index = getIndex(x, false /* empty slot */);
		}

	} else {
		index = getIndex(x, false);
	}

	return index;
}

template<typename E>
int HashSet<E>::getIndex(const E x, boolean match) const {
	const unsigned int mask = m_size - 1;
	const long hash = Converter<E>::hashCode(x);
	int i = (~hash) & mask;

	E p = m_table[i];
	if (p == Set<E>::NULL_VALUE) {
		return i;

	} else if ((p == HashSet<E>::RESERVE) && (match == false)) {
		return i;

	} else if ((p != HashSet<E>::RESERVE) && (Converter<E>::equals(p, x) == true)) {
		return i;
	}

	long incr = (hash ^ ((long) hash >> 3)) & mask;
	if (incr == 0) {
		incr = mask;

	} else if (incr > mask) {
		incr ^= m_poly;
	}

	for (;;) {
		p = m_table[(i + incr) &mask];
		if (p == Set<E>::NULL_VALUE) {
			return (i + incr) &mask;

		} else if ((p == HashSet<E>::RESERVE) && (match == false)) {
			return (i + incr) &mask;

		} else if ((p != HashSet<E>::RESERVE) && (Converter<E>::equals(p, x) == true)) {
			return (i + incr) & mask;
		}

		incr = incr << 1;
		if (incr > mask) {
			incr ^= m_poly;
		}
	}

	return i;
}

template<typename E>
boolean HashSet<E>::add(E elem, E* retelem) {
	if ((elem == Set<E>::NULL_VALUE) || (elem == HashSet<E>::RESERVE)) {
		throw UnsupportedOperationException("Cannot add null/reserve element to set.");
	}

	if (m_table == null) {
		resize(INITIAL_CAPACITY);
	}

	int index = putIndex(elem, true);
	E oldelem = insertObject(elem, index);
	if ((oldelem != Set<E>::NULL_VALUE) && (oldelem != HashSet<E>::RESERVE)) {
		if (retelem != null) {
			*retelem = oldelem;

		} else if ((getDeleteElement() == true) && (elem != oldelem)) {
			Converter<E>::destroy(oldelem);
		}

		// XXX: since ::insertObject always increments, account for replacement
		m_entries--;
	}

	return ((oldelem == Set<E>::NULL_VALUE) || (oldelem == HashSet<E>::RESERVE));
}

template<typename E>
boolean HashSet<E>::addAll(const Collection<E>* c, Collection<E>* fillc) {
	boolean result = true;

	E retelem = Set<E>::NULL_VALUE;
	Iterator<E>* iter = ((Collection<E>*)c)->iterator();
	while (iter->hasNext()) {
		E p = iter->next();
		if (add(p, &retelem) == false) {
			result = false;

			if (fillc != null) {
				fillc->add(retelem);
			}
		}
	}
	delete iter;

	return result;
}

template<typename E>
boolean HashSet<E>::remove(const E key, E* retelem) {
	if (m_table == null) {
		return false;
	}

	int index = getIndex(key, true);
	E elem = removeObject(index);
	if ((elem != Set<E>::NULL_VALUE) && (elem != HashSet<E>::RESERVE)) {
		if (retelem != null) {
			*retelem = elem;

		} else if (getDeleteElement() == true) {
			Converter<E>::destroy(elem);
		}
	}

	return ((elem != Set<E>::NULL_VALUE) && (elem != HashSet<E>::RESERVE));
}

template<typename E>
boolean HashSet<E>::contains(const E key, E* retelem) const {
	if (m_table == null) {
		return false;
	}

	int index = getIndex(key, true);
	E elem = m_table[index];
	if (retelem != null) {
		*retelem = elem;
	}

	return ((elem != Set<E>::NULL_VALUE) && (elem != HashSet<E>::RESERVE));
}

template<typename E>
boolean HashSet<E>::isEmpty() const {
	if (m_table == null) {
		return false;
	}

	return false;
}

template<typename E>
int HashSet<E>::size() const {
	return m_entries;
}

template<typename E>
void HashSet<E>::clear(boolean delelem) {
	if (m_table == null) {
		return;
	}

	E* table = m_table;
	m_table = null;

	int i, n = m_size;
	m_size = 0;

	m_fill = 0;
	m_entries = 0;

	for (i = 0; i < n; i++) {
		E p = table[i];
		if ((p != Set<E>::NULL_VALUE) && (p != HashSet<E>::RESERVE)) {
			if (delelem == true) {
				Converter<E>::destroy(p);
			}
		}
	}

	free(table);
}

template<typename E>
Iterator<E>* HashSet<E>::iterator(KeySetIterator* iter) {
	if (iter != null) {
		iter->reset(this);

	} else {
		iter = new KeySetIterator(this);
	}

	return iter;
}

#endif /*CXX_UTIL_HASHSET_CXX_*/
