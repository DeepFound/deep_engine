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
#ifndef CXX_UTIL_HASHSET_H_
#define CXX_UTIL_HASHSET_H_

#include "cxx/util/Set.h"

namespace cxx { namespace util {

template<typename E>
class HashSet : public Set<E> {

	private:
		inttype m_poly;
		inttype m_fill;
		inttype m_size;
		inttype m_entries;
		E* m_table;
		bytetype m_stateFlags;

	private:
		inline int putIndex(const E x, boolean check);
		inline int getIndex(const E x, boolean match) const;

		E removeObject(int index);
		E insertObject(E x, int index);

		FORCE_INLINE void setDeleteElement(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x02 : m_stateFlags & ~0x02;
		}

		FORCE_INLINE boolean getDeleteElement() const {
			return (m_stateFlags & 0x02) != 0;
		}

		FORCE_INLINE void setFixed(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x01 : m_stateFlags & ~0x01;
		}

		FORCE_INLINE boolean getFixed() const {
			return (m_stateFlags & 0x01) != 0;
		}

	protected:
		virtual int resize(int minused);

	public:
		class KeySetIterator;

		static const E RESERVE;

		static const int RESIZE_FACTOR;
		static const int RESIZE_LOW_WATER;
		static const int RESIZE_HIGH_WATER;

		static const int INITIAL_CAPACITY;

		/*
		HashSet(const Collecton* c);
		*/
		HashSet(int cap = INITIAL_CAPACITY, boolean delelem = false, boolean fixed = false);

		HashSet(const HashSet<E>& set) :
			m_poly(set.m_poly),
			m_fill(set.m_fill),
			m_size(set.m_size),
			m_entries(set.m_entries),
			m_table((E*)malloc(set.m_size*sizeof(E))),
			m_stateFlags(set.m_stateFlags) {
			memcpy(m_table, set.m_table, set.m_size*sizeof(E));
		}

		virtual ~HashSet();

		virtual boolean add(E elem, E* retelem);
		virtual boolean add(E elem) {
			return add(elem, null);
		}

		virtual boolean addAll(const Collection<E>* c, Collection<E>* fillc);
		virtual boolean addAll(const Collection<E>* c) {
			return addAll(c, null);
		}

		virtual boolean remove(const E key, E* retelem);
		virtual boolean remove(const E key) {
			return remove(key, null);
		}

		virtual boolean contains(const E key, E* retelem) const;
		virtual boolean contains(const E key) const {
			return contains(key, null);
		}

		virtual boolean isEmpty() const;
		virtual int size() const;

		virtual void clear(boolean delelem);
		virtual void clear() {
			clear(getDeleteElement());
		}

		virtual Iterator<E>* iterator(KeySetIterator* iter);
		virtual Iterator<E>* iterator() {
			return iterator(null);
		}

	friend class KeySetIterator;

	class KeySetIterator : public Iterator<E> {

		private:
			int m_prev;
			int m_next;
			HashSet<E>* m_set;

			KeySetIterator();

			FORCE_INLINE void reset(HashSet<E>* set) {
				m_prev = 0;
				m_next = 0;
				m_set = set;
				moveNext();
			}

			FORCE_INLINE void moveNext() {
				while (m_next < m_set->m_size) {
					E p = m_set->m_table[m_next];
					if ((p != Set<E>::NULL_VALUE) && (p != HashSet<E>::RESERVE)) {
						break;
					}

					m_next++;
				}
			}

		public:
			FORCE_INLINE KeySetIterator(HashSet<E>* set):
				m_prev(0),
				m_next(0),
				m_set(set) {
				moveNext();
			}

			FORCE_INLINE virtual ~KeySetIterator() {
			}

			FORCE_INLINE virtual boolean hasNext() {
				return (m_next < m_set->m_size);
			}

			FORCE_INLINE virtual const E next() {
				// TODO: throw NoSuchElementException when end is each (XXX: check will be a performance hit)
				m_prev = m_next;
				E p = m_set->m_table[m_next++];
				moveNext();
				return p;
			}

			FORCE_INLINE virtual void remove() {
				// TODO: address cursor and resize
				E p = m_set->m_table[m_prev];
				m_set->remove(p);
				m_next = m_prev;
				moveNext();
			}

		friend class HashSet;
	};
};

template<typename E>
const E HashSet<E>::RESERVE = (const E)Converter<E>::RESERVE;

} } // namespace

#endif /*CXX_UTIL_HASHSET_H_*/
