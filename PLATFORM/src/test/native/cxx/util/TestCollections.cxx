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
#include "cxx/lang/String.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"
#include "cxx/util/Collections.h"

#include "cxx/util/ArrayList.h"

using namespace cxx::lang;
using namespace cxx::util;

static const int DATA_SIZE = 10000;

class SortedObject : public Comparable {
	private:
		String m_name;
		int m_data;

	public:
		SortedObject(String name, int data) :
			m_name(name),
			m_data(data) {
		}

		SortedObject(int data) :
			m_name(""),
			m_data(data) {
		}

		virtual int compareTo(const Object* obj) const {
			int d = ((SortedObject*) obj)->m_data;

			return (m_data < d) ? -1 : ((m_data == d) ? 0 : 1);
		}

		virtual boolean equals(const Object* obj) const {
			return m_name == ((SortedObject*) obj)->m_name;
		}

		void dump() {
			//printf("name: %s, data: %d\n", m_name.c_str(), m_data);
			printf("%d\n", m_data);
		}

};

int main(int argc, char** argv) {
	ArrayList<SortedObject*> list(DATA_SIZE);
	ArrayList<SortedObject*> mergeSortList(DATA_SIZE);
	ArrayList<SortedObject*> mergeSort2List(DATA_SIZE);
	ArrayList<SortedObject*> insertionSortList(DATA_SIZE);

	srand(System::currentTimeMillis());

	char buffer[32];

	for (inttype i=0; i<DATA_SIZE; i++) {
		sprintf(buffer, "Object: %d", i);
		list.add(new SortedObject(buffer, (rand() % DATA_SIZE) + 1));
	}

	for (int i=0; i<list.size(); i++) {
		mergeSortList.add(list.get(i));
		mergeSort2List.add(list.get(i));
		insertionSortList.add(list.get(i));
	}

	/*
	printf("List before sort\n");
	for (int i=0; i<list.size(); i++) {
		list.get(i)->dump();
	}
	*/

	printf("MERGE SORT : ");
	longtype gstart = System::currentTimeMillis();
	Collections::mergeSort(&mergeSortList);
	longtype gstop = System::currentTimeMillis();
	printf("%lld\n", gstop - gstart);

	printf("MERGE 2 SORT : ");
	gstart = System::currentTimeMillis();
	Collections::mergeSort2(&mergeSort2List);
	gstop = System::currentTimeMillis();
	printf("%lld\n", gstop - gstart);

	printf("INSERTION SORT : ");
	gstart = System::currentTimeMillis();
	Collections::insertionSort(&insertionSortList);
	gstop = System::currentTimeMillis();
	printf("%lld\n", gstop - gstart);

	/*
	printf("List after sort\n");
	for (int i=0; i<insertionSortList.size(); i++) {
		insertionSortList.get(i)->dump();
	}
	*/

	for (int i=0; i<DATA_SIZE; i++) {
		SortedObject* merge = mergeSortList.get(i);
		SortedObject* merge2 = mergeSort2List.get(i);
		SortedObject* insertion = insertionSortList.get(i);

		if ((merge->compareTo(merge2) != 0) &&
			(merge->compareTo(insertion) != 0)) {

			printf("FAILED: Sort result mismatch\n");
			exit(1);
		}
	}
}
