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
#ifndef CXX_UTIL_COPYONWRITEARRAYLIST_H_
#define CXX_UTIL_COPYONWRITEARRAYLIST_H_

#include "cxx/util/List.h"
#include "cxx/util/RandomAccess.h"
#include "cxx/util/concurrent/Synchronize.h"
#include "cxx/util/concurrent/atomic/AtomicLong.h"
#include "cxx/util/concurrent/locks/ReentrantLock.h"
#include "cxx/lang/Math.h"
#include "cxx/lang/basearray.h"
#include "cxx/lang/System.h"
#include "cxx/lang/Cloneable.h"
#include "cxx/lang/UnsupportedOperationException.h"
#include "cxx/io/Serializable.h"

using namespace std;
using namespace cxx::lang;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::atomic;
using namespace cxx::util::concurrent::locks;

namespace cxx { namespace util { namespace concurrent {

template<typename E>
class CopyOnWriteArrayList: public List<E>, RandomAccess, Cloneable, cxx::io::Serializable {

	private:
		class SharedArray {
			friend class CopyOnWriteArrayList<E>;
			E* m_elements;
			const int m_length;
			AtomicLong m_refcount;
		public:
			SharedArray(const int length = 0) :
				m_length(length) {
				m_elements = (E*)(m_length == 0 ? null : new E[m_length]);
				m_refcount.incrementAndGet();
			}
			~SharedArray() {
				delete [] m_elements;
			}
			inline SharedArray* incref() {
				m_refcount.incrementAndGet();
				return this;
			}
			inline boolean decref() {
				boolean deleted = (m_refcount.decrementAndGet() <= 0);
				if (deleted) {
					delete this;
				}
				return deleted;
			}
			inline void deleteValues() {
				for (int i=0; i<m_length; i++) {
					delete m_elements[i];
				}
			}
		};
		class RaiiSharedArray {
			SharedArray* m_sharedArray;
		public:
			RaiiSharedArray(SharedArray* sharedArray) : m_sharedArray(sharedArray) {
			}
			inline ~RaiiSharedArray() {
				m_sharedArray->decref();
			}
			inline SharedArray* getSharedArray() const {
				return m_sharedArray;
			}
		};

		SharedArray* m_sharedArray;
		boolean m_deleteValue;
		Lockable* m_lock;

		void deleteValues() {
			if (m_deleteValue) {
				m_sharedArray->deleteValues();
			}
		}
		inline void checkRange(const int index, const int end) const {
			if (index < 0 || index > end) {
				throw IndexOutOfBoundsException((String("Index: ")+=index+", Size: "+m_sharedArray->m_length).c_str());
			}
		}
		inline boolean eq(const E lhs, const E rhs) const {
			return lhs == rhs || (lhs != null && lhs->equals(rhs));
		}
		inline E _remove(const int index) {
			SharedArray* oldArray = m_sharedArray;
			const E element = oldArray->m_elements[index];
			int oldLength = oldArray->m_length;
			SharedArray* newArray = new SharedArray(oldLength-1);
			if (newArray->m_length > 0) {
				memcpy(newArray->m_elements, oldArray->m_elements, index*sizeof(E));
				memcpy(newArray->m_elements+index, oldArray->m_elements+(index+1), (oldLength-(index+1))*sizeof(E));
			}
			_setSharedArray(newArray);
			return (E)element;
		}
		inline boolean contains(SharedArray* sharedArray, const E element) const {
			const int length = sharedArray->m_length;
			const E* elements = sharedArray->m_elements;
			for (int i=0; i<length; i++) {
				const E e = elements[i];
				if (eq(e, element)) {
					return true;
				}
			}
			return false;
		}
		inline void _setSharedArray(SharedArray* sharedArray) {
			m_sharedArray->decref(); // release the old array
			m_sharedArray = sharedArray;
		}
		inline SharedArray* captureSharedArray() const {
			SharedArray* sharedArray = null;
			synchronized (m_lock) {
				sharedArray = m_sharedArray->incref();
			}
			return sharedArray;
		}

	public:
		explicit CopyOnWriteArrayList(boolean deleteValue = false) :
			m_sharedArray(new SharedArray()), m_deleteValue(deleteValue), m_lock(new ReentrantLock()) {
		}
		explicit CopyOnWriteArrayList(const Collection<E>* c, boolean deleteValue = false) :
			m_sharedArray(new SharedArray(c->size())), m_deleteValue(deleteValue), m_lock(new ReentrantLock()) {
			int index = 0;
			Iterator<E>* i = c->iterator();
			while (i->hasNext()) {
				m_sharedArray->m_elements[index++] = (E)i->next();
			}
			delete i;
		}
		explicit CopyOnWriteArrayList(const basearray<E>* a, boolean deleteValue = false) :
			m_sharedArray(new SharedArray(a->length)), m_deleteValue(deleteValue), m_lock(new ReentrantLock()) {
			for (int i=0; i<a->length; i++) {
				m_sharedArray->m_elements[i] = (*a)[i];
			}
		}

		virtual ~CopyOnWriteArrayList() {
			synchronized (m_lock) {
				deleteValues();
				m_sharedArray->decref();
			}
			delete m_lock;
			m_lock = null;
		}

		virtual basearray<E>* getArray() {
			return toArray();
		}

		virtual void setArray(basearray<E>* array) {
			SharedArray* newSharedArray = new SharedArray(array->length);
			for (int i=0; i<array->length; i++) {
				newSharedArray->m_elements[i] = (const E) (*array)[i];
			}
			synchronized (m_lock) {
				if (m_deleteValue) {
					for (int i=0; i<m_sharedArray->m_length; i++) {
						const E element = m_sharedArray->m_elements[i];
						if (!contains(newSharedArray, element)) {
							delete element;
						}
					}
				}
				_setSharedArray(newSharedArray);
			}
		}

		virtual int size() const {
			return m_sharedArray->m_length;
		}
		virtual boolean isEmpty() const {
			return m_sharedArray->m_length == 0;
		}
		virtual boolean contains(const Object* object) const {
			return contains((E)object);
		}
		virtual boolean contains(const E element) const {
			RaiiSharedArray sharedArray(captureSharedArray());
			boolean c = contains(sharedArray.getSharedArray(), element);
			return c;
		}

		virtual Iterator<E>* iterator() {
			return new COWIterator(captureSharedArray());
		}

		virtual array<E>* toArray() const {
			return toArray(new array<E>(m_sharedArray->m_length));
		}
		virtual array<E>* toArray(array<E>* array) const {
			RaiiSharedArray sharedArray(captureSharedArray());
			int copyLength = Math::min(array->length, sharedArray.getSharedArray()->m_length);
			for (int i=0; i<copyLength; i++) {
				(*array)[i] = (E)sharedArray.getSharedArray()->m_elements[i];
			}
			return array;
		}

		virtual boolean add(Object* element) {
			return add((E)element);
		}
		virtual boolean add(E element) {
			synchronized (m_lock) {
				SharedArray* oldArray = m_sharedArray;
				int oldLength = oldArray->m_length;
				SharedArray* newArray = new SharedArray(oldLength+1);
				if (oldLength > 0) {
					memcpy(newArray->m_elements, oldArray->m_elements, oldLength*sizeof(E));
				}
				newArray->m_elements[oldLength] = element;
				_setSharedArray(newArray);
			}
			return true;
		}
		virtual void add(const int index, Object* object) {
			add(index, (E)object);
		}
		virtual void add(const int index, E element) {
			synchronized (m_lock) {
				SharedArray* oldArray = m_sharedArray;
				checkRange(index, oldArray->m_length);
				int length = oldArray->m_length;
				SharedArray* newArray = new SharedArray(length+1);
				if (index > 0) {
					memcpy(newArray->m_elements, oldArray->m_elements, index*sizeof(E));
				}
				memcpy(newArray->m_elements+(index+1), oldArray->m_elements+index, (length-index)*sizeof(E));
				newArray->m_elements[index] = element;
				_setSharedArray(newArray);
			}
		}

		virtual E remove(const int index) {
			E element = null;
			synchronized (m_lock) {
				checkRange(index, m_sharedArray->m_length-1);
				element = _remove(index);
			}
			return element;
		}

		virtual boolean remove(const Object* element) {
			return remove((const E)element);
		}
		virtual boolean remove(E element) {
			E removedElement = null;
			boolean removed = remove(element, &removedElement);
			if (m_deleteValue && removed) {
				delete removedElement;
			}
			return removed;
		}
		virtual boolean remove(const E element, E* removedElement) {
			boolean retval = false;
			synchronized (m_lock) {
				const int i = indexOf(element);
				if (i >= 0) {
					*removedElement = _remove(i);
					retval = true;
				}
			}
			return retval;
		}

		virtual boolean containsAll(const Collection<E>* c) const {
			RaiiSharedArray sharedArray(captureSharedArray());
			Iterator<E>* i = ((Collection<E>*)c)->iterator();
			boolean retval = true;
			while (i->hasNext()) {
				if (!contains(sharedArray.getSharedArray(), (E)i->next())) {
					retval = false;
					break;
				}
			}
			delete i;
			return retval;
		}

		virtual boolean addAll(Collection<E>* c) {
			synchronized (m_lock) {
				const SharedArray* oldArray = m_sharedArray;
				SharedArray* newArray = new SharedArray(oldArray->m_length + c->size());
				if (oldArray->m_length > 0) {
					memcpy(newArray->m_elements, oldArray->m_elements, oldArray->m_length*sizeof(E));
				}
				int index = oldArray->m_length;
				Iterator<E>* i = c->iterator();
				while (i->hasNext()) {
					newArray->m_elements[index++] = (E)i->next();
				}
				delete i;
				_setSharedArray(newArray);
			}
			return !c->isEmpty();
		}
		virtual boolean addAll(int index, Collection<E>* c) {
			synchronized (m_lock) {
				const SharedArray* oldArray = m_sharedArray;
				checkRange(index, oldArray->m_length-1);
				SharedArray* newArray = new SharedArray(oldArray->m_length + c->size());
				if (index > 0) {
					memcpy(newArray->m_elements, oldArray->m_elements, index*sizeof(E));
				}
				Iterator<E>* i = c->iterator();
				while (i->hasNext()) {
					newArray->m_elements[index] = (E)i->next();
					index++;
				}
				delete i;
				memcpy(newArray->m_elements+index+c->size(), oldArray->m_elements+index, (oldArray->m_length - index)*sizeof(E));
				_setSharedArray(newArray);
			}
			return !c->isEmpty();
		}

		virtual boolean removeAll(const Collection<E>* c) {
			synchronized (m_lock) {
				const SharedArray* oldArray = m_sharedArray;
				E newElements[oldArray->m_length];
				int newLength = 0;
				for (int i=0; i < oldArray->m_length; i++) {
					if (!c->contains(oldArray->m_elements[i])) {
						newElements[newLength++] = oldArray->m_elements[i];
					}
				}
				if (newLength != oldArray->m_length) {
					SharedArray* newArray = new SharedArray(newLength);
					if (newLength > 0) {
						memcpy(newArray->m_elements, newElements, newLength*sizeof(E));
					}
					_setSharedArray(newArray);
					return true;
				}
			}
			return false;
		}
		virtual boolean retainAll(const Collection<E>* c) {
			synchronized (m_lock) {
				const SharedArray* oldArray = m_sharedArray;
				E newElements[oldArray->m_length];
				int newLength = 0;
				for (int i=0; i < oldArray->m_length; i++) {
					if (c->contains(oldArray->m_elements[i])) {
						newElements[newLength++] = oldArray->m_elements[i];
					}
				}
				if (newLength != oldArray->m_length) {
					SharedArray* newArray = new SharedArray(newLength);
					if (newLength > 0) {
						memcpy(newArray->m_elements, newElements, newLength*sizeof(E));
					}
					_setSharedArray(newArray);
					return true;
				}
			}
			return false;
		}

		virtual void clear() {
			reset();
		}
		virtual void reset() {
			synchronized (m_lock) {
				deleteValues();
				_setSharedArray(new SharedArray());
			}
		}

		virtual boolean equals(const Object* object) const {
			const CopyOnWriteArrayList<E>* cowArrayList = dynamic_cast<const CopyOnWriteArrayList<E>*>(object);
			return cowArrayList == null ? false : equals(cowArrayList);
		}
		virtual boolean equals(const List<E>* list) const {
			return false;
		}
		virtual boolean equals(const CopyOnWriteArrayList<E>* cowArrayList) const {
			if (this == cowArrayList) {
				return true;
			}
			RaiiSharedArray lhs(captureSharedArray());
			RaiiSharedArray rhs(cowArrayList->captureSharedArray());
			SharedArray* lhsSA = lhs.getSharedArray();
			SharedArray* rhsSA = rhs.getSharedArray();
			if (lhsSA->m_length != rhsSA->m_length) {
				return false;
			}
			for (int i=0; i<lhsSA->m_length; i++) {
				if (eq(lhsSA->m_elements[i], rhsSA->m_elements[i])) {
					continue;
				} else {
					return false;
				}
			}
			return true;
		}
		virtual int hashCode() {
			RaiiSharedArray rsa(captureSharedArray());
			const SharedArray* sa = rsa.getSharedArray();
			int hashCode = 1;
			for (int i=0; i<sa->m_length; i++) {
				const E element = sa->m_elements[i];
				hashCode = 31*hashCode + (element == null ? 0 : element->hashCode());
			}
			return hashCode;
		}

		virtual E get(const int index) const {
			E value = null;
			synchronized (m_lock) {
				checkRange(index, m_sharedArray->m_length-1);
				value = (E) m_sharedArray->m_elements[index];
			}
			return value;
		}
		virtual E set(const int index, Object* object) {
			return set(index, (E)object);
		}
		virtual E set(const int index, E element) {
			E oldValue = null;
			synchronized (m_lock) {
				const SharedArray* oldArray = m_sharedArray;
				checkRange(index, oldArray->m_length-1);
				oldValue = (E) oldArray->m_elements[index];
				if (oldValue != element) {
					SharedArray* newArray = new SharedArray(oldArray->m_length);
					memcpy(newArray->m_elements, oldArray->m_elements, oldArray->m_length*sizeof(E));
					newArray->m_elements[index] = element;
					_setSharedArray(newArray);
				}
			}
			return oldValue;
		}

		virtual int indexOf(const Object* object) const {
			return indexOf((E)object);
		}
		virtual int indexOf(const E element) const {
			RaiiSharedArray rsa(captureSharedArray());
			const SharedArray* sa = rsa.getSharedArray();
			for (int i=0; i<sa->m_length; i++) {
				if (eq(sa->m_elements[i], element)) {
					return i;
				}
			}
			return -1;
		}
		virtual int lastIndexOf(const Object* object) const {
			return lastIndexOf((E)object);
		}
		virtual int lastIndexOf(const E element) const {
			RaiiSharedArray rsa(captureSharedArray());
			const SharedArray* sa = rsa.getSharedArray();
			for (int i=sa->m_length-1; i>=0; i--) {
				if (eq(sa->m_elements[i], element)) {
					return i;
				}
			}
			return -1;
		}

		class COWIterator : public ListIterator<E> {
		private:
			int m_cursor;
			SharedArray* m_sharedArray;
		public:
			COWIterator(SharedArray* sharedArray, int index=0) :
				m_cursor(index), m_sharedArray(sharedArray) {
			}
			virtual ~COWIterator() {
				m_sharedArray->decref();
			}
			virtual boolean hasNext() {
				return m_cursor < m_sharedArray->m_length;
			}
			virtual const E next() {
				// TODO: add bounds check? (there will be an unnecessary performance hit if hasNext() is used)
				return m_sharedArray->m_elements[m_cursor++];
			}
			virtual boolean hasPrevious() {
				return m_cursor > 0;
			}
			virtual const E previous() {
				// TODO: add bounds check? (there will be an unnecessary performance hit if hasPrevious() is used)
				return m_sharedArray->m_elements[--m_cursor];
			}
			virtual int nextIndex() {
				return m_cursor;
			}
			virtual int previousIndex() {
				return m_cursor-1;
			}
			virtual void remove() {
				throw new UnsupportedOperationException();
			}
			virtual void set(E element) {
				throw new UnsupportedOperationException();
			}
			virtual void add(E element) {
				throw new UnsupportedOperationException();
			}
		};
		virtual ListIterator<E>* listIterator() {
			return new COWIterator(captureSharedArray());
		}
		virtual ListIterator<E>* listIterator(const int index) {
			return new COWIterator(captureSharedArray(), index);
		}
};

} } } // namespace

#endif /*CXX_UTIL_COPYONWRITEARRAYLIST_H_*/
