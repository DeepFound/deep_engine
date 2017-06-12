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
#ifndef CXX_UTIL_ARRAYLIST_H_
#define CXX_UTIL_ARRAYLIST_H_

#include "cxx/util/AbstractList.h"
#include "cxx/util/Converter.h"
#include "cxx/util/RandomAccess.h"
#include "cxx/lang/Math.h"
#include "cxx/lang/System.h"
#include "cxx/lang/Cloneable.h"
#include "cxx/io/Serializable.h"

using namespace std;
using namespace cxx::lang;

namespace cxx { namespace util {

// XXX: Note that this class now uses Converter<E>::NULL_VALUE to denote null for
//      a given type E. This may cause unexpected behavior when using this class
//      with primitive types, as one value from the type must be chosen to denote null.
template<typename E>
class ArrayList: public AbstractList<E>, RandomAccess, Cloneable, cxx::io::Serializable {
	private:
		static const int INITIAL_CAPACITY = 4;
		E* m_elements;
		inttype m_length;
		inttype m_size;
		boolean m_deleteValue : 1;

		void deleteValues() {
			if (m_deleteValue) {
				for (int i = 0; i < m_size; i++) {
					Converter<E>::destroy(m_elements[i]);
				}
			}
		}
		void resize() {
			int newLength = m_size * 3 / 2; // make capacity 50% more than size
			if (newLength < INITIAL_CAPACITY) {
				return;
			}
			m_length = newLength;
			E* newArray = new E[m_length];
			memcpy(newArray, m_elements, sizeof(E)*m_size);
			delete [] m_elements;
			m_elements = newArray;
		}
		inline void checkRange(const int index, const int end) const {
			if (index < 0 || index > end) {
				throw IndexOutOfBoundsException((String("Index: ")+=String::valueOf(index)+", Size: "+String::valueOf(m_size)).c_str());
			}
		}

	public:
		FORCE_INLINE void setDeleteValue(boolean flag) {
			m_deleteValue = flag;
		}

		FORCE_INLINE boolean getDeleteValue() const {
			return m_deleteValue;
		}

	public:
		explicit ArrayList(int initialCapacity = INITIAL_CAPACITY, boolean deleteValue = false) :
			m_elements(new E[initialCapacity]), m_length(initialCapacity), m_size(0), m_deleteValue(deleteValue) {
		}

		ArrayList(const ArrayList<E>& l) :
			m_elements(new E[l.m_length]),
			m_length(l.m_length),
			m_size(l.m_size),
			m_deleteValue(l.m_deleteValue) {
			memcpy(m_elements, l.m_elements, sizeof(E)*l.m_size);
		}

		virtual ~ArrayList() {
			deleteValues();
			delete [] m_elements;
		}

		FORCE_INLINE virtual inttype capacity() const {
			return m_length;
		}

		virtual int size() const {
			return m_size;
		}
		virtual boolean isEmpty() const {
			return m_size == 0;
		}
		virtual boolean contains(const E element) const {
			for (int i = 0; i < m_size; i++) {
				const E e = m_elements[i];
				if (e == element ||
						(e != Converter<E>::NULL_VALUE && Converter<E>::equals(e,element))) {
					return true;
				}
			}
			return false;
		}

		class ArrayListIterator : public Iterator<E> {

			private:
				int m_cursor;
				ArrayList<E>* m_arrayList;

			public:
				ArrayListIterator(ArrayList<E>* arrayList) : m_cursor(0), m_arrayList(arrayList) {
				}
				virtual boolean hasNext() {
					return m_cursor < m_arrayList->size();
				}
				virtual const E next() {
					// TODO: throw NoSuchElementException when end is each (XXX: check will be a performance hit)
					return m_arrayList->m_elements[m_cursor++];
				}
				virtual void remove() {
					E element = m_arrayList->remove(--m_cursor);
					if (m_arrayList->m_deleteValue) {
						Converter<E>::destroy(element);
					}
				}
			};

		virtual Iterator<E>* iterator() {
			return new ArrayListIterator(this);
		}

		virtual array<E>* toArray() const {
			return toArray(new array<E>(m_size));
		}
		virtual array<E>* toArray(array<E>* array) const {
			int copyLength = Math::min(array->length, m_size);
			for (int i = 0; i < copyLength; i++) {
				(*array)[i] = m_elements[i];
			}
			return array;
		}

		virtual boolean add(E element) {
			if (m_size == m_length) {
				resize();
			}
			m_elements[m_size] = element;
			m_size++;
			return true;
		}
		virtual void add(const int index, E element) {
			#ifdef DEEP_DEBUG
			checkRange(index, m_size);
			#endif
			if (m_size == m_length) {
				resize();
			}
			if (m_size > index) {
				System::arraycopy(m_elements, index, m_elements, index+1, m_size-index);
			}
			m_elements[index] = element;
			m_size++;
		}

		virtual E remove(const int index) {
			#ifdef DEEP_DEBUG
			checkRange(index, m_size-1);
			#endif
			E element = m_elements[index];
			int moveCount = (m_size-index)-1;
			if (moveCount > 0) {
				System::arraycopy(m_elements, index+1, m_elements, index, moveCount);
			}
			m_size--;
			if (m_size < m_length / 3) {
				resize();
			}
			return element;
		}

		virtual boolean remove(const E element) {
			E removedElement = (E) Converter<E>::NULL_VALUE;
			boolean removed = remove(element, &removedElement);
			if (m_deleteValue && removed) {
				Converter<E>::destroy(removedElement);
			}
			return removed;
		}
		virtual boolean remove(const E element, E* removedElement) {
			const int i = indexOf(element);
			if (i >= 0) {
				*removedElement = remove(i);
				return true;
			}
			return false;
		}

		virtual boolean containsAll(const Collection<E>* c) const {
			Iterator<E>* i = ((Collection<E>*)c)->iterator();
			while (i->hasNext()) {
				if (!contains((E)i->next())) {
					return false;
				}
			}
			delete i;
			return true;
		}

		virtual boolean addAll(Collection<E>* c) {
			Iterator<E>* i = c->iterator();
			while (i->hasNext()) {
				add((E)i->next());
			}
			delete i;
			return !c->isEmpty();
		}
		virtual boolean addAll(int index, Collection<E>* c) {
			#ifdef DEEP_DEBUG
			checkRange(index, m_size-1);
			#endif
			Iterator<E>* i = c->iterator();
			while (i->hasNext()) {
				add(index, (E)i->next());
				index++;
			}
			delete i;
			return !c->isEmpty();
		}

		virtual boolean removeAll(const Collection<E>* c) {
			boolean removed = false;
			Iterator<E>* i = ((Collection<E>*)c)->iterator();
			while (i->hasNext()) {
				if (remove((E)i->next())) {
					removed = true;
				}
			}
			delete i;
			return removed;
		}
		virtual boolean retainAll(const Collection<E>* c) {
			boolean removed = false;
			Iterator<E>* i = iterator();
			while (i->hasNext()) {
				if (!c->contains(i->next())) {
					i->remove();
					removed = true;
				}
			}
			delete i;
			return removed;
		}

		virtual void clear() {
			reset();
			resize();
		}
		virtual void reset() {
			deleteValues();
			m_size = 0;
		}

		virtual boolean equals(const List<E>* list) const {
			return false;
		}
		virtual boolean equals(const ArrayList<E>* arrayList) const {
			if (this == arrayList) {
				return true;
			}

			if (m_size != arrayList->m_size) {
				return false;
			}

			for (int i = 0; i < m_size; i++) {
				const E element = m_elements[i];
				if (element == arrayList->m_elements[i] || (element != Converter<E>::NULL_VALUE && Converter<E>::equals(element,arrayList->m_elements[i]))) {
					continue;

				} else {
					return false;
				}
			}

			return true;
		}
		virtual int hashCode() {
			int hashCode = 1;
			for (int i = 0; i < m_size; i++) {
				E element = m_elements[i];
				hashCode = 31*hashCode + (element == Converter<E>::NULL_VALUE ? 0 : Converter<E>::hashCode(element));
			}

			return hashCode;
		}

		virtual E get(const int index) const {
			#ifdef DEEP_DEBUG
			checkRange(index, m_size-1);
			#endif
			return m_elements[index];
		}
		virtual E set(const int index, E element) {
			#ifdef DEEP_DEBUG
			checkRange(index, m_size-1);
			#endif
			E oldValue = m_elements[index];
			m_elements[index] = element;
			return oldValue;
		}

		virtual int indexOf(const E element) const {
			for (int i = 0; i < m_size; i++) {
				const E e = m_elements[i];
				if (e == element || (e != Converter<E>::NULL_VALUE && Converter<E>::equals(e,element))) {
					return i;
				}
			}

			return -1;
		}
		virtual int lastIndexOf(const E element) const {
			for (int i = m_size - 1; i >= 0; i--) {
				const E e = m_elements[i];
				if (e == element ||
						(e != Converter<E>::NULL_VALUE && Converter<E>::equals(e,element))) {
					return i;
				}
			}
			return -1;
		}

		FORCE_INLINE inttype size(inttype nsize) {
			throw UnsupportedOperationException("Unsupported operation: size(int)");
		}

		FORCE_INLINE void resize(inttype capacity) {
			throw UnsupportedOperationException("Unsupported operation: resize(int)");
		}

		class ArrayListListIterator : public ListIterator<E> {
			private:
				int m_cursor;
				ArrayList<E>* m_arrayList;
				boolean m_forward;

			public:
				ArrayListListIterator(ArrayList<E>* arrayList, int index=0) : m_cursor(index), m_arrayList(arrayList), m_forward(true) {
				}
				virtual boolean hasNext() {
					return m_cursor < m_arrayList->size();
				}
				virtual const E next() {
					// TODO: add bounds check? (there will be an unnecessary performance hit if hasNext() is used)
					m_forward = true;
					return m_arrayList->m_elements[m_cursor++];
				}
				virtual boolean hasPrevious() {
					return m_cursor > 0;
				}
				virtual const E previous() {
					// TODO: add bounds check? (there will be an unnecessary performance hit if hasPrevious() is used)
					m_forward = false;
					return m_arrayList->m_elements[--m_cursor];
				}
				virtual int nextIndex() {
					return m_cursor;
				}
				virtual int previousIndex() {
					return m_cursor-1;
				}
				virtual void remove() {
					E element = m_arrayList->remove(m_forward ? --m_cursor : m_cursor);
					if (m_arrayList->m_deleteValue) {
						Converter<E>::destroy(element);
					}
				}
				virtual void set(E element) {
					m_arrayList->set(m_forward ? --m_cursor : m_cursor, element);
				}
				virtual void add(E element) {
					m_arrayList->add(m_cursor++, element);
				}
			};

		virtual ListIterator<E>* listIterator() {
			return new ArrayListListIterator(this);
		}
		virtual ListIterator<E>* listIterator(const int index) {
			return new ArrayListListIterator(this, index);
		}
};

} } // namespace

#endif /*CXX_UTIL_ARRAYLIST_H_*/
