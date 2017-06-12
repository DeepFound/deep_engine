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
#ifndef CXX_UTIL_ABSTRACTLIST_H_
#define CXX_UTIL_ABSTRACTLIST_H_

#include "cxx/util/Converter.h"
#include "cxx/lang/array.h"
#include "cxx/lang/UnsupportedOperationException.h"
#include "cxx/util/AbstractCollection.h"
#include "cxx/util/List.h"

using namespace cxx::lang;

namespace cxx { namespace util {

// XXX: Note that this class now uses Converter<E>::NULL_VALUE to denote null for
//      a given type E. This may cause unexpected behavior when using this class
//      with primitive types, as one value from the type must be chosen to denote null.
template<typename E>
class AbstractList: public AbstractCollection<E>, public List<E> {

	protected:
		AbstractList() {
		}

	public:
		virtual ~AbstractList() {
		}

		virtual int size() const {
			return 0;
		}

		virtual boolean isEmpty() const {
			return size() == 0;
		}

		virtual boolean contains(const E o) {
			return indexOf(o) >= 0;
		}

		// TODO: make Iterator template
		//virtual Iterator<E>* iterator() const = 0;

		virtual boolean add(E element) {
			add(size(), element);
			return true;
		}
		virtual boolean remove(E o) {
			int i = indexOf(o);
			if (i >= 0) {
				remove(i);
				return true;
			}
			return false;
		}

		virtual boolean containsAll(const Collection<E>* c) const = 0;

		virtual boolean addAll(Collection<E>* c) = 0;
		virtual boolean addAll(int index, Collection<E>* c) = 0;

		virtual boolean removeAll(const Collection<E>* c) = 0;
		virtual boolean retainAll(const Collection<E>* c) = 0;

		virtual void clear() {
			removeRange(0, size());
		}

		virtual E get(const int index) const = 0;
		virtual E set(const int index, E element) {
			throw UnsupportedOperationException();
		}
		virtual void add(const int index, E element) {
			throw UnsupportedOperationException();
		}
		virtual E remove(const int index) {
			throw UnsupportedOperationException();
		}

		virtual int indexOf(const E o) {
			ListIterator<E>* i = listIterator();
			if (o == Converter<E>::NULL_VALUE) {
				while (i->hasNext()) {
					if (i->next() == Converter<E>::NULL_VALUE) {
						return i->previousIndex();
					}
				}
			} else {
				while (i->hasNext()) {
					if (Converter<E>::equals(o,i->next())) {
						return i->previousIndex();
					}
				}
			}
			delete i;
			return -1;
		}
		virtual int lastIndexOf(const E o) {
			ListIterator<E>* i = listIterator(size());
			if (o == Converter<E>::NULL_VALUE) {
			    while (i->hasPrevious()) {
				if (i->previous() == Converter<E>::NULL_VALUE) {
					return i->nextIndex();
				}
			    }

			} else {
			    while (i->hasPrevious()) {
				if (Converter<E>::equals(o,i->previous())) {
					return i->nextIndex();
				}
			    }
			}

			delete i;
			return -1;
		}

		virtual ListIterator<E>* listIterator() = 0;
		virtual ListIterator<E>* listIterator(const int index) = 0;

	protected:
		void removeRange(int fromIndex, int toIndex) {
			ListIterator<E>* it = listIterator(fromIndex);
			for (int i = 0, n = toIndex-fromIndex; i < n; i++) {
				it->next();
				it->remove();
			}

			delete it;
		}
};

} } // namespace

#endif /*CXX_UTIL_ABSTRACTLIST_H_*/
