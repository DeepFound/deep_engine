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
#ifndef CXX_UTIL_LIST_H_
#define CXX_UTIL_LIST_H_

#include "cxx/lang/Object.h"
#include "cxx/util/Collection.h"
#include "cxx/util/Iterator.h"
#include "cxx/util/ListIterator.h"

using namespace cxx::lang;

namespace cxx { namespace util {

template<typename E>
class List : public Collection<E> {
	public:
		virtual ~List() = 0;

		// TODO: make Iterator template
		//virtual Iterator<E>* iterator() const = 0;

		virtual array<E>* toArray() const = 0;
		virtual array<E>* toArray(array<E>* array) const = 0;

		virtual boolean add(E e) = 0;
		virtual boolean remove(E o) = 0;

		virtual boolean containsAll(const Collection<E>* c) const = 0;

		virtual boolean addAll(Collection<E>* c) = 0;
		virtual boolean addAll(int index, Collection<E>* c) = 0;

		virtual boolean removeAll(const Collection<E>* c) = 0;
		virtual boolean retainAll(const Collection<E>* c) = 0;

		virtual void clear() = 0;

		virtual boolean equals(const List<E>* o) const = 0;
		virtual int hashCode() = 0;

		virtual E get(const int index) const = 0;
		virtual E set(const int index, E element) = 0;
		virtual void add(const int index, E element) = 0;
		virtual E remove(const int index) = 0;

		virtual int indexOf(const E o) const = 0;
		virtual int lastIndexOf(const E o) const = 0;

		virtual ListIterator<E>* listIterator() = 0;
		virtual ListIterator<E>* listIterator(const int index) = 0;
};

template<typename E>
inline List<E>::~List() {}

} } // namespace

	#endif /*CXX_UTIL_LIST_H_*/
