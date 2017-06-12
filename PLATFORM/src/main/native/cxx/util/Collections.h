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
#ifndef CXX_UTIL_COLLECTIONS_H_
#define CXX_UTIL_COLLECTIONS_H_

#include "cxx/lang/UnsupportedOperationException.h"
#include "cxx/util/Comparator.h"
#include "cxx/util/List.h"
#include "cxx/util/ArrayList.h"

using namespace cxx::lang;

namespace cxx { namespace util {

class Collections {

	public:
		template<typename E>
		static void sort(List<E>* list);

		template<typename E>
		static void insertionSort(List<E>* list);

		template<typename E, typename L, typename Cmp>
		static void insertionSort(L* list, Cmp* cmp);

		template<typename E>
		static void mergeSort(List<E>* list);

		template<typename E>
		static void mergeSort1(List<E>* list);

		template<typename E>
		static void mergeSort2(List<E>* list);

		template<typename E, typename L, typename Cmp>
		static void mergeSort2(L* list, Cmp* cmp);

		template<typename E>
		static void reverse(ArrayList<E>* list);
		// TODO: static void reverse(List<E>* list);

		template<typename E,typename I>
		class UnmodifiableIterator : public Iterator<E> {
			private:
				I* m_iter;
			public:
				UnmodifiableIterator(const I* iter) : m_iter(const_cast<I*>(iter)) {
				}
				virtual ~UnmodifiableIterator() {
					delete m_iter;
				}
				FORCE_INLINE virtual boolean hasNext() {
					return m_iter->hasNext();
				}
				
				FORCE_INLINE virtual const E next() {
					return m_iter->next();
				}
				
				FORCE_INLINE virtual void remove() {
					throw UnsupportedOperationException();
				}
		};

		template<typename E>
		FORCE_INLINE static UnmodifiableIterator< E,Iterator<E> >* unmodifiableIterator(const List<E>* l) {
			return unmodifiableIterator< E,Iterator<E> >(const_cast<List<E>*>(l)->iterator());
		}

		template<typename E, typename I>
		FORCE_INLINE static UnmodifiableIterator< E,I >* unmodifiableIterator(const I* iter) {
			return new UnmodifiableIterator< E,I >(iter);
		}
		
		template<typename E, typename I, typename C>
		FORCE_INLINE static UnmodifiableIterator< E,I >* unmodifiableIteratorFrom(const C* collection) {
			return unmodifiableIterator< E,I >(const_cast<C*>(collection)->iterator());
		}
		
		template<typename E, typename I>
		class IteratorComparator {
			private:
				Comparator<E>* m_comparator;
			
			public:
				FORCE_INLINE IteratorComparator(Comparator<E>* comparator) :
					m_comparator(comparator) {
				}
				
				#ifdef COM_DEEPIS_DB_CARDINALITY
				FORCE_INLINE int compare(I* i1, I* i2, inttype* pos = null) const {
				#else
				FORCE_INLINE int compare(I* i1, I* i2) const {
				#endif
					while(i1->hasNext()) {
						if(i2->hasNext() == false) {
							return 1;
						}
						
						inttype cmp = m_comparator->compare(i1->next(), i2->next());
						if(cmp != 0) {
							return cmp;
						}
					}
					
					if(i2->hasNext()) {
						return -1;
					}
					
					return 0;
				}
		};

	private:
		static const inttype SORT_THRESHOLD = 100;
};

template<typename E>
inline void Collections::sort(List<E>* list) {
	//mergeSort(list);
	//mergeSort1(list);
	mergeSort2(list);
}

template<typename E>
inline void Collections::insertionSort(List<E>* list) {
	Comparator<E> cmp;
	insertionSort<E,List<E>,Comparator<E> >(list, &cmp);
}

template<typename E, typename L, typename Cmp>
inline void Collections::insertionSort(L* list, Cmp* comparator) {
	inttype size = list->size();
	for (inttype i=1; i<size; i++) {
		for (inttype j=i; j>0; j--) {
			E e = list->get(j);
			E p = list->get(j-1);

			if (comparator->compare(e,p) < 0) {
				list->set(j, p);
				list->set(j-1, e);

			} else {
				break;
			}
		}
	}
}

template<typename E>
inline void Collections::mergeSort(List<E>* list) {
	int size = list->size();
	if (size <= SORT_THRESHOLD) {
		insertionSort(list);
		return;
	}

	ArrayList<E> same;
	ArrayList<E> lesser;
	ArrayList<E> greater;
	inttype pivot = list->size() / 2;

	for (int i = 0; i < list->size(); i++) {
		E e = list->get(i);
		E p = list->get(pivot);

		if (e->compareTo(p) > 0) {
			greater.add(e);

		} else if (e->compareTo(p) < 0) {
			lesser.add(e);

		} else {
			same.add(e);
		}
	}

	mergeSort(&lesser);

	mergeSort(&greater);

	inttype index = 0;
	for (int i = 0; i < lesser.size(); i++) {
		E e = lesser.get(i);
		list->set(index++, e);
	}

	for (int i = 0; i < same.size(); i++) {
		E e = same.get(i);
		list->set(index++, e);
	}

	for (int i = 0; i < greater.size(); i++) {
		E e = greater.get(i);
		list->set(index++, e);
	}
}

template<typename E>
inline void Collections::mergeSort1(List<E>* list) {
	int size = list->size();
	if (size <= SORT_THRESHOLD) {
		insertionSort(list);
		return;
	}

	ArrayList<E>* same = new ArrayList<E>(list->size());
	ArrayList<E>* lesser = new ArrayList<E>(list->size());
	ArrayList<E>* greater = new ArrayList<E>(list->size());
	inttype pivot = list->size() / 2;

	for (int i = 0; i < list->size(); i++) {
		E e = list->get(i);
		E p = list->get(pivot);

		if (e->compareTo(p) > 0) {
			greater->add(e);

		} else if (e->compareTo(p) < 0) {
			lesser->add(e);

		} else {
			same->add(e);
		}
	}

	mergeSort(lesser);

	mergeSort(greater);

	inttype index = 0;
	for (int i = 0; i < lesser->size(); i++) {
		E e = lesser->get(i);
		list->set(index++, e);
	}

	for (int i = 0; i < same->size(); i++) {
		E e = same->get(i);
		list->set(index++, e);
	}

	for (int i = 0; i < greater->size(); i++) {
		E e = greater->get(i);
		list->set(index++, e);
	}
	
	delete same;
	delete lesser;
	delete greater;
}

template<typename E>
inline void Collections::mergeSort2(List<E>* list) {
	Comparator<E> cmp;
	return mergeSort2<E,List<E>,Comparator<E> >(list, &cmp);
}

template<typename E, typename L, typename Cmp>
inline void Collections::mergeSort2(L* list, Cmp* comparator) {
	inttype size = list->size();
	if (size <= SORT_THRESHOLD) {
		insertionSort<E,L,Cmp>(list, comparator);
		return;
	}

	inttype middle = size / 2;

	ArrayList<E> left(middle);
	for (inttype i=0; i<middle; i++) {
		left.add(list->get(i));
	}

	ArrayList<E> right(size-middle);
	for (inttype i=middle; i<size; i++) {
		right.add(list->get(i));
	}

	mergeSort2<E,ArrayList<E>,Cmp>(&left, comparator);
	mergeSort2<E,ArrayList<E>,Cmp>(&right, comparator);

	inttype index = 0;
	inttype leftIndex = 0;
	inttype rightIndex = 0;

	while ((leftIndex < middle) && (rightIndex < (size-middle))) {
		inttype cmp = comparator->compare(left.get(leftIndex),right.get(rightIndex));
		list->set(index++, (cmp < 0) ? left.get(leftIndex++) : right.get(rightIndex++));
	}

	while (leftIndex < middle) {
		list->set(index++, left.get(leftIndex++));
	}

	while (rightIndex < (size-middle)) {
		list->set(index++, right.get(rightIndex++));
	}
}

template<typename E>
inline void Collections::reverse(ArrayList<E>* list) {
// TODO: inline void Collections::reverse(List<E>* list) {
	for (int start = 0, end = list->size() - 1; start < end; start++, end--) {
		list->set(start, list->set(end, list->get(start)));
	}
}

} } // namespace

#endif /*CXX_UTIL_COLLECTIONS_H_*/
