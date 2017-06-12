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
#include "cxx/util/Converter.h"

#ifndef CXX_UTIL_NUMBERRANGESET_H_
#define CXX_UTIL_NUMBERRANGESET_H_

#include "cxx/lang/Object.h"

#include "cxx/util/Collection.h"
#include "cxx/util/Comparator.h"
#include "cxx/util/TreeSet.h"
#include "cxx/util/TreeSet.cxx"

using namespace cxx::lang;

namespace cxx { namespace util {

template<typename N = ushorttype, typename BitSet = ubytetype, typename D = N>
class NumberRangeSet/* : public Collection<E> */ {
private:
	static const D RANGESIZE = sizeof(BitSet)*8;
	static const Comparator<D> D_CMP; 

	boolean m_empty;
	N m_index;
	BitSet m_range;

public:
	FORCE_INLINE NumberRangeSet() :
		m_empty(true),
		m_index(0),
		m_range(0) {
	}
	
	FORCE_INLINE NumberRangeSet(const N index, const BitSet range) :
		m_empty(false),
		m_index(index),
		m_range(range) {
	}

	FORCE_INLINE ~NumberRangeSet() {
		/* XXX: nothing to do */
	}

	FORCE_INLINE N getIndex() const {
		return m_index;
	}

	FORCE_INLINE BitSet getRange() const {
		return m_range;
	}

	FORCE_INLINE boolean add(const N index) {
		boolean overflow = false;
		return add(index, overflow);
	}

	FORCE_INLINE boolean add(const N index, boolean& overflow) {
		if (m_empty == true) {
			m_empty = false;
			m_index = index;
			overflow = false;
			return true;
		}
		if (index == m_index) {
			overflow = false;
			return true;
		}

		if (index > m_index) {
			const D diff = index - m_index;
			overflow = (diff > RANGESIZE) || ((m_range >> (RANGESIZE - diff)) != 0);

			m_range <<= diff;
			m_range |= (1 << (diff-1));
			m_index = index;

			return true;
		}

		const D diff = m_index - index;
		if (diff > RANGESIZE) {
			overflow = true;
			return false;
		}

		overflow = false;
		m_range |= (1 << (diff-1));
		return true;
	}

	FORCE_INLINE boolean addAll(const NumberRangeSet<N,BitSet,D>& set, boolean& overflow) {
		if (set.m_empty == true) {
			return true;
		}

		const N setIndex = set.m_index;
		if ((m_empty == true) || (setIndex > m_index)) {
			add(setIndex, overflow);
			m_range |= set.m_range;
			return true;
		}

		NumberRangeSet<N,BitSet,D> normalized(set);
		normalized.add(m_index, overflow);
		m_range |= normalized.m_range;

		return contains(setIndex);
	}

	FORCE_INLINE boolean contains(const N index) const {
		if ((m_empty == true) || (index > m_index)) {
			return false;
		}

		if (index == m_index) {
			return true;
		}

		const D diff = m_index - index;
		if (diff > RANGESIZE) {
			return false;
		}

		return (m_range & (1 << (diff-1))) != 0;
	}

	FORCE_INLINE TreeSet<D>* toSet() const {
		TreeSet<D>* set = new TreeSet<D>(&D_CMP);

		if (m_empty == false) {
			set->add(m_index);

			for (D x = 0; x < RANGESIZE; x++) {
				if ((m_range & (1 << (x))) != 0) {
					if ((m_index - (x + 1)) >= 0) {
						set->add(m_index - (x + 1));
					}
				}
			}
		}
		return set;
	}

	FORCE_INLINE boolean isEmpty() const {
		return m_empty;
	}

	FORCE_INLINE void clear() {
		m_empty = true;
		m_index = 0;
		m_range = 0;
	}
};

template<typename N, typename BitSet, typename D>
const Comparator<D> NumberRangeSet<N, BitSet, D>::D_CMP;

} } // namespace

#endif /* CXX_UTIL_NUMBERRANGESET_H_ */
