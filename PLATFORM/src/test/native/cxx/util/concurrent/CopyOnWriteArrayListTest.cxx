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
#include <assert.h>

#include "cxx/lang/Long.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"
#include "cxx/util/concurrent/CopyOnWriteArrayList.h"

using namespace cxx::lang;
using namespace cxx::util;

//static const int ARRAY_SIZE = 11300;
static const int ARRAY_SIZE = 10;
static Long* longValues[ARRAY_SIZE];

void executingTest(const char* testName) {
	DEEP_LOG(INFO, OTHER, "-------------------------- %s\n", testName);
}

void testSize() {
	executingTest("testSize()");

	CopyOnWriteArrayList<Long*> al;

	assert(0 == al.size());
	al.add(longValues[0]);

	assert(1 == al.size());
}

void testIsEmpty() {
	executingTest("testIsEmpty()");

	CopyOnWriteArrayList<Long*> al;
	assert(al.isEmpty());
}

void testContains() {
	executingTest("testContains()");

	CopyOnWriteArrayList<Long*> al;

	al.add(longValues[0]);
	assert(al.contains(longValues[0]));

	al.add(longValues[ARRAY_SIZE-1]);
	assert(al.contains(longValues[ARRAY_SIZE-1]));

	Long* zero = new Long(0);
	assert(al.contains(zero));
	delete zero;
}

void testIterator() {
	executingTest("testIterator()");

	CopyOnWriteArrayList<Long*> al;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(longValues[i]);
	}

	Iterator<Long*>* i = al.iterator();
	int index = 0;
	while (i->hasNext()) {
		assert(i->next()->equals(longValues[index++]));
	}
	delete i;

//	i = al.iterator();
//	while (i->hasNext()) {
//		i->next();
//		i->remove();
//	}
//	delete i;
//	assert(al.isEmpty());
}

void testToArray() {
	executingTest("testToArray()");

	CopyOnWriteArrayList<Long*> al;
	array<Long*> a(ARRAY_SIZE);
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(longValues[i]);
		a[i] = longValues[i];
	}
	array<Long*>* newArray = al.toArray();
	assert(a.equals(newArray));
	delete newArray;
}

void testToArrayArray() {
	executingTest("testToArrayArray()");

	CopyOnWriteArrayList<Long*> al;
	array<Long*>* a = new array<Long*>(ARRAY_SIZE);
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(longValues[i]);
		(*a)[i] = longValues[i];
	}
	array<Long*>* copy = new array<Long*>(ARRAY_SIZE);
	al.toArray(copy);
	assert(a->equals(copy));

	delete a;
	delete copy;
}

void testAddElementObject() {
	executingTest("testAddElementObject()");

	CopyOnWriteArrayList<Long*> al;
	Object* zero = new Long(0);
	al.add(zero);
	assert(al.contains(zero));
	al.remove(zero);
	delete zero;
}

void testAddElement() {
	executingTest("testAddElement()");

	CopyOnWriteArrayList<Long*> al;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(longValues[i]);
	}
	for (int i=0; i<ARRAY_SIZE; i++) {
		assert(longValues[i]->equals(al.get(i)));
	}
}

void testAddIndexElement() {
	executingTest("testAddIndexElement()");

	CopyOnWriteArrayList<Long*> al;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(i, longValues[i]);
	}
	for (int i=0; i<ARRAY_SIZE; i++) {
		assert(longValues[i]->equals(al.get(i)));
	}
}

void testRemoveIndex() {
	executingTest("testRemoveIndex()");

	CopyOnWriteArrayList<Long*> al;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(i, longValues[i]);
	}
	assert(ARRAY_SIZE == al.size());
	assert(al.remove(ARRAY_SIZE/2));
	assert(ARRAY_SIZE-1 == al.size());
	while (!al.isEmpty()) {
		Long* value = al.get(0);
		Long* removedValue = al.remove(0);
		assert(value->equals(removedValue));
	}
}

void testRemoveElement() {
	executingTest("testRemoveElement()");

	CopyOnWriteArrayList<Long*> al;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(i, longValues[i]);
	}
	assert(ARRAY_SIZE == al.size());
	Long* removedElement = null;
	assert(al.remove(longValues[ARRAY_SIZE/2], &removedElement));
	assert(longValues[ARRAY_SIZE/2]->equals(removedElement));
	assert(ARRAY_SIZE-1 == al.size());
}

void testContainsAll() {
	executingTest("testContainsAll()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}

	CopyOnWriteArrayList<Long*> al2(true);
	for (int i=0; i<ARRAY_SIZE; i++) {
		al2.add(i, new Long(i));
	}

	assert(al1.containsAll(&al2));
}

void testAddAll() {
	executingTest("testAddAll()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}

	CopyOnWriteArrayList<Long*> al2;
	al2.addAll(&al1);

	assert(al2.equals(&al1));
}

void testAddAllIndex() {
	executingTest("testAddAllIndex()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}

	CopyOnWriteArrayList<Long*> al2;
	al2.addAll(&al1);
	al2.addAll(0, &al1);

	//assert(al2.equals(&al1));
	assert(2*ARRAY_SIZE == al2.size());
}

void testRemoveAll() {
	executingTest("testRemoveAll()");

	CopyOnWriteArrayList<Long*> al1;
	CopyOnWriteArrayList<Long*> al2;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
		al2.add(i, longValues[i]);
	}
	al1.removeAll(&al2);
	assert(al1.isEmpty());
}

void testRetainAll() {
	executingTest("testRetainAll()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}
	CopyOnWriteArrayList<Long*> al2;
	for (int i=0; i<ARRAY_SIZE-5; i++) {
		al2.add(i, longValues[i]);
	}
	al1.retainAll(&al2);
	assert(ARRAY_SIZE-5 == al1.size());
}

void testClear() {
	executingTest("testClear()");

	CopyOnWriteArrayList<Long*> al;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(i, longValues[i]);
	}
	assert(ARRAY_SIZE == al.size());
	al.clear();
	assert(al.isEmpty());
}

void testReset() {
	executingTest("testReset()");

	CopyOnWriteArrayList<Long*> al;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(i, longValues[i]);
	}
	assert(ARRAY_SIZE == al.size());
	al.reset();
	assert(al.isEmpty());
}

void testEqualsObject() {
	executingTest("testEqualsObject()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}

	CopyOnWriteArrayList<Long*> al2(true);
	for (int i=0; i<ARRAY_SIZE; i++) {
		al2.add(i, new Long(i));
	}
	// TODO: pass Object pointers
	//assert((dynamic_cast<Object*>(&al1))->equals(dynamic_cast<Object*>(&al2)));
	assert(al1.equals(&al2));
}

void testEqualsArrayList() {
	executingTest("testEqualsArrayList()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}

	CopyOnWriteArrayList<Long*> al2(true);
	for (int i=0; i<ARRAY_SIZE; i++) {
		al2.add(i, new Long(i));
	}

	assert(al1.equals(&al2));
}

void testHashCode() {
	executingTest("testHashCode()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}
	assert(0 != al1.hashCode());
}

void testGetIndex() {
	executingTest("testGetIndex()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}
	for (int i=ARRAY_SIZE-1; i>=0; i--) {
		Long* l = new Long(i);
		assert(al1.get(i)->equals(l));
		delete l;
	}
}

void testSetIndexElement() {
	executingTest("testSetIndexElement()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}
	for (int i=0; i<ARRAY_SIZE; i++) {
		Long* l = new Long(i);
		assert(al1.set(i, l)->equals(l));
		delete l;
	}
}

void testIndexOf() {
	executingTest("testIndexOf()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}
	for (int i=0; i<ARRAY_SIZE; i++) {
		Long* l = new Long(i);
		assert(i == al1.indexOf(l));
		delete l;
	}
	al1.set(3, longValues[5]);
	assert(3 == al1.indexOf(longValues[5]));
}

void testLastIndexOf() {
	executingTest("testLastIndexOf()");

	CopyOnWriteArrayList<Long*> al1;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}
	for (int i=0; i<ARRAY_SIZE; i++) {
		al1.add(i, longValues[i]);
	}
	for (int i=0; i<ARRAY_SIZE; i++) {
		Long* l = new Long(i);
		assert(i+ARRAY_SIZE == al1.lastIndexOf(l));
		delete l;
	}
}

void testListIterator() {
	executingTest("testListIterator()");

	CopyOnWriteArrayList<Long*> al;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(longValues[i]);
	}

	ListIterator<Long*>* i = al.listIterator();
	int index = 0;
	while (i->hasNext()) {
		assert(i->next()->equals(longValues[index++]));
		assert(index == i->nextIndex());
	}
	delete i;

//	i = al.listIterator();
//	while (i->hasNext()) {
//		i->next();
//		i->remove();
//	}
//	delete i;
//	assert(al.isEmpty());

	i = al.listIterator();
	const Long* next = i->next();
	const Long* previous = i->previous();
	assert(next == previous);
	delete i;
}

void testListIteratorIndex() {
	executingTest("testListIteratorIndex()");

	CopyOnWriteArrayList<Long*> al;
	for (int i=0; i<ARRAY_SIZE; i++) {
		al.add(longValues[i]);
	}

	ListIterator<Long*>* i = al.listIterator(ARRAY_SIZE);
	int index = ARRAY_SIZE-1;
	while (i->hasPrevious()) {
		assert(i->previous()->equals(longValues[index--]));
		assert(index == i->previousIndex());
	}
	delete i;

//	i = al.listIterator(ARRAY_SIZE);
//	while (i->hasPrevious()) {
//		i->previous();
//		i->remove();
//	}
//	delete i;
//	assert(al.isEmpty());
}

int main(int argc, char** argv) {
	for (int i = 0; i < ARRAY_SIZE; i++) {
		longValues[i] = new Long(i);
	}

	testSize();
	testIsEmpty();
	testContains();
	testIterator();
	testToArray();
	testToArrayArray();
	testAddElement();
	testAddIndexElement();
	testRemoveIndex();
	testRemoveElement();
	testContainsAll();
	testAddAll();
	testAddAllIndex();
	testRemoveAll();
	testRetainAll();
	testClear();
	testReset();
	testEqualsObject();
	testEqualsArrayList();
	testHashCode();
	testGetIndex();
	testSetIndexElement();
	testIndexOf();
	testLastIndexOf();
	testListIterator();
	testListIteratorIndex();

	for (int i = 0; i < ARRAY_SIZE; i++) {
		delete longValues[i];
	}
}
