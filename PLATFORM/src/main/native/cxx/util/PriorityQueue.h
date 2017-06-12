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

#ifndef CXX_UTIL_PRIORITYQUEUE_H_
#define CXX_UTIL_PRIORITYQUEUE_H_

#include "cxx/lang/Object.h"

#include "cxx/util/ArrayList.h"
#include "cxx/util/Collection.h"
#include "cxx/util/Comparator.h"

using namespace cxx::lang;

namespace cxx { namespace util {

template<typename E, typename Cmp = Comparator<E> >
class PriorityQueue /* : public Collection<E> */ {
private:
	static const int INITIAL_CAPACITY = 4;

	const Cmp* m_comparator;
	boolean m_deleteValue : 1;
	ArrayList<E> m_elements;

	FORCE_INLINE static inttype parentOf(const inttype child) {
		return (child - 1) >> 1;
	}

	FORCE_INLINE static inttype leftChild(const inttype parent) {
		return (parent << 1) + 1;
	}

	#if 0
	FORCE_INLINE static inttype rightChild(const inttype parent) {
		return (parent << 1) + 2;
	}
	#endif

	FORCE_INLINE static void swap(ArrayList<E>& array, const inttype e1, const inttype e2) {
		E v1 = array.get(e1);
		array.set(e1, array.get(e2));
		array.set(e2, v1);
	}

	FORCE_INLINE inttype reheapifyLeft(const inttype left) {
		const inttype parent = parentOf(left);
		if (m_comparator->compare(get(left), get(parent)) < 0) {
			swap(m_elements, left, parent);
			return parent;
		}
		return -1;
	}

	FORCE_INLINE inttype reheapifyRight(const inttype right) {
		if (right == 0) {
			return -1;
		}
		const inttype parent = parentOf(right);
		if (m_comparator->compare(get(parent), get(right)) > 0) {
			swap(m_elements, parent, right);
			return parent;
		}
		return -1;
	}

	FORCE_INLINE inttype reheapifyParent(const inttype parent) {
		const inttype left = leftChild(parent);
		const inttype right = left+1;
		if (left >= size()) {
			return -1;
		}

		inttype minChild = left;
		if ((right < size()) && (m_comparator->compare(get(left), get(right)) > 0)) {
			minChild = right;
		}

		if (m_comparator->compare(get(parent), get(minChild)) > 0) {
			swap(m_elements, parent, minChild);
			return minChild;
		}

		return -1;
	}

	FORCE_INLINE static boolean isLeft(const inttype child) {
		return child & 0x01;
	}

	FORCE_INLINE void reheapifyUp(inttype child) {
		while(child != -1) {
			if (isLeft(child) == true) {
				child = reheapifyLeft(child);
			} else {
				child = reheapifyRight(child);
			}
		}
	}

	FORCE_INLINE void reheapifyDown(inttype parent) {
		while(parent != -1) {
			parent = reheapifyParent(parent);
		}
	}

public:
	PriorityQueue(const Cmp* comparator, inttype capacity = INITIAL_CAPACITY, boolean deleteValue = false) :
		m_comparator(comparator),
		m_deleteValue(deleteValue),
		m_elements(capacity, deleteValue) {
	}

	FORCE_INLINE void add(E v) {
		m_elements.add(v);
		reheapifyUp(size()-1);
	}

	FORCE_INLINE boolean addAll(Collection<E>* c) {
		Iterator<E>* i = c->iterator();
		while (i->hasNext()) {
			add((E)i->next());
		}
		delete i;
		return !c->isEmpty();
	}

	FORCE_INLINE E removeAt(inttype i) {
		E v = m_elements.get(i);
		if (size()-i > 1) {
			swap(m_elements, i, size()-1);
			m_elements.remove(size()-1);
			reheapifyDown(i);
		} else {
			m_elements.remove(size()-1);
		}
		return v;
	}

	FORCE_INLINE E remove(void) {
		return removeAt(0);
	}

	FORCE_INLINE void clear(void) {
		m_elements.clear();
	}

	FORCE_INLINE inttype indexOf(const E e) const {
		return m_elements.indexOf(e);
	}

	FORCE_INLINE boolean remove(const E e, E* removed = null) {
		const inttype i = indexOf(e);
		if (i < 0) {
			return false;
		}

		if (removed != null) {
			*removed = removeAt(i);
		} else {
			removeAt(i);
		}

		return true;
	}

	FORCE_INLINE E get(inttype i) const {
		return m_elements.get(i);
	}

	FORCE_INLINE void set(inttype index, E element) {
		throw UnsupportedOperationException("Unsupported operation: set(int, E)");
	}

	FORCE_INLINE inttype capacity() const {
		return m_elements.capacity();
	}

	FORCE_INLINE inttype size() const {
		return m_elements.size();
	}

	FORCE_INLINE inttype size(inttype nsize) {
		throw UnsupportedOperationException("Unsupported operation: size(int)");
	}

	FORCE_INLINE void resize(inttype capacity) {
		throw UnsupportedOperationException("Unsupported operation: resize(int)");
	}

	FORCE_INLINE boolean isEmpty() const {
		return m_elements.isEmpty();
	}

	FORCE_INLINE Iterator<E>* iterator() {
		return m_elements.iterator();
	}

	FORCE_INLINE ArrayList<E>& array() {
		return m_elements;
	}
};

} } // namespace

#endif /* CXX_UTIL_PRIORITYQUEUE_H_ */
