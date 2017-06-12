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
#include "cxx/lang/Long.h"
#include "cxx/lang/String.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "cxx/util/HashMap.h"
#include "cxx/util/TreeMap.h"
#include "cxx/util/TreeSet.h"

#include "cxx/util/HashMap.cxx"
#include "cxx/util/HashSet.cxx"
#include "cxx/util/TreeMap.cxx"
#include "cxx/util/TreeSet.cxx"

using namespace std;
using namespace cxx::lang;
using namespace cxx::util;

static int COUNT = 1000000;

static Comparator<long long> LONG_COMPARE;

template class HashMap<long long,long long>;
template class HashSet<long long>;

template class TreeMap<long long,long long>;
template class TreeSet<long long>;

void testMapRet() {
	long long newkey = 1;
	long long dupkey = 1;
	HashMap<long long, long long> map(16, false, false);

	DEEP_LOG(INFO, OTHER, "EQUALS: %d\n", (newkey == dupkey));
	DEEP_LOG(INFO, OTHER, "COMPARES: %d\n", LONG_COMPARE.compare(newkey, dupkey));

	long long oldkey = map.put(newkey, newkey);
	DEEP_LOG(INFO, OTHER, "OLD KEY: %lld\n", oldkey);

	long long retkey = 0;
	long long getkey = map.remove(newkey, &retkey);
	DEEP_LOG(INFO, OTHER, "GET KEY: %lld\n", getkey);
	DEEP_LOG(INFO, OTHER, "RET KEY: %lld\n", retkey);
}

void testHashSet() {
	HashSet<long long> set(COUNT * 2, false);

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		set.add(key);

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Add %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	long gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "ADD TIME: %d, %d, %ld\n", COUNT, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		if (set.contains(key) == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! contains: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Contains %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "CONTAINS TIME: %d, %d, %ld\n", COUNT, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		if (set.remove(key) == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! REMOVE: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Remove %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "REMOVE TIME: %d, %d, %ld\n", COUNT, set.size(), (gstop-gstart));
}

void testHashMap() {
	HashMap<long long, long long> map(COUNT * 2, false, false);

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		map.put(key, key);

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Put %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	long gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "PUT TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		boolean status;
		map.get(key, null, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! get: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Get %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "GET TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		boolean status;
		map.remove(key, null, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! REMOVE: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Remove %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "REMOVE TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));
}

void testTreeFunc() {
	TreeSet<long long> set(&LONG_COMPARE, 37, false);
	TreeMap<long long,long long> map(&LONG_COMPARE, 3, false, false);

	long long key1 = 1;
	long long key2 = 2;
	long long key3 = 3;
	long long key4 = 4;
	long long key5 = 5;

	set.add(key1);
	set.add(key2);
	set.add(key3);
	set.add(key4);
	set.add(key5);

	DEEP_LOG(INFO, OTHER, "FIRST: %lld\n", set.first());
	DEEP_LOG(INFO, OTHER, "LAST: %lld\n", set.last());
	DEEP_LOG(INFO, OTHER, "LOWER: %lld\n", set.lower(key3));
	DEEP_LOG(INFO, OTHER, "HIGHER: %lld\n", set.higher(key3));
	DEEP_LOG(INFO, OTHER, "FLOOR: %lld\n", set.floor(key3));
	DEEP_LOG(INFO, OTHER, "CEILING: %lld\n", set.ceiling(key3));

	Iterator<long long>* iter1 = set.iterator();
	while (iter1->hasNext()) {
		long long item = iter1->next();
		DEEP_LOG(INFO, OTHER, " NEXT: %lld\n", item);
		iter1->remove();
	}
	delete iter1;
	DEEP_LOG(INFO, OTHER, "SIZE: %d\n", set.size());

	map.put(key1, key1);
	map.put(key2, key2);
	map.put(key3, key3);
	map.put(key4, key4);
	map.put(key5, key5);

	DEEP_LOG(INFO, OTHER, "FIRST: %lld\n", map.firstKey());
	DEEP_LOG(INFO, OTHER, "LAST: %lld\n", map.lastKey());
	DEEP_LOG(INFO, OTHER, "LOWER: %lld\n", map.lowerKey(key3));
	DEEP_LOG(INFO, OTHER, "HIGHER: %lld\n", map.higherKey(key3));
	DEEP_LOG(INFO, OTHER, "FLOOR: %lld\n", map.floorKey(key3));
	DEEP_LOG(INFO, OTHER, "CEILING: %lld\n", map.ceilingKey(key3));

	Set<MapEntry<long long,long long>* >* entrySet = map.entrySet();
	Iterator<MapEntry<long long,long long>* >* iter2 = entrySet->iterator();
	while (iter2->hasNext()) {
		MapEntry<long long,long long>* entry = iter2->next();
		long long key = entry->getKey();
		long long value = entry->getValue();
		DEEP_LOG(INFO, OTHER, " NEXT: %lld, %lld\n", key, value);
		iter2->remove();
	}
	delete entrySet;
	delete iter2;
	DEEP_LOG(INFO, OTHER, "SIZE: %d\n", map.size());
}

void testTreeSet() {
	TreeSet<long long> set(&LONG_COMPARE, 37, false);

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		set.add(key);

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Add %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	long gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "ADD TIME: %d, %d, %ld\n", COUNT, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		if (set.contains(key) == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! contains: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Contains %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "CONTAINS TIME: %d, %d, %ld\n", COUNT, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		if (set.remove(key) == false) {
			DEEP_LOG(INFO, OTHER, " FAILED !!! REMOVE: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Remove %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "REMOVE TIME: %d, %d, %ld\n", COUNT, set.size(), (gstop-gstart));
}

void testTreeMap() {
	TreeMap<long long, long long> map(&LONG_COMPARE, 37, false, false);

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		map.put(key, key);

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Put %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	long gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "PUT TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		boolean status;
		map.get(key, null, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! get: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Get %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "GET TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		boolean status;
		map.remove(key, null, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! REMOVE: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Remove %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "REMOVE TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));
}

void testTreeMapSequential() {
	TreeMap<long long, long long> map(&LONG_COMPARE, 37, false, false);

	int half = COUNT / 2;
	int third = COUNT / 3;

	int b1 = half;
	int e1 = half + third;
	int b2 = 0;
	int e2 = half;
	int b3 = e1;
	int e3 = COUNT;
	boolean sequential = false;

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	sequential = false;
#if 1
	for (int i = b1; i < e1; i++) {
		long long key = i;
		if (sequential == false) {
			map.add(key, key, &sequential, true);

		} else {
			map.add(key, key);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Add %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	//long gstop = System::currentTimeMillis();

	//DEEP_LOG(INFO, OTHER, "ADD TIME(1): %d, %d, %ld\n", e1, map.size(), (gstop-gstart));

	//gstart = System::currentTimeMillis();
	//lstart = System::currentTimeMillis();
	sequential = false;
	for (int i = b2; i < e2; i++) {
		long long key = i;
		if (sequential == false) {
			map.add(key, key, &sequential, true);

		} else {
			map.add(key, key);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Add %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	//gstop = System::currentTimeMillis();

	//DEEP_LOG(INFO, OTHER, "ADD TIME(2): %d, %d, %ld\n", e2, map.size(), (gstop-gstart));

	//gstart = System::currentTimeMillis();
	//lstart = System::currentTimeMillis();
	sequential = false;
	for (int i = b3; i < e3; i++) {
		long long key = i;
		if (sequential == false) {
			map.add(key, key, &sequential, true);

		} else {
			map.add(key, key);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Add %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	//gstop = System::currentTimeMillis();

	//DEEP_LOG(INFO, OTHER, "ADD TIME(3): %d, %d, %ld\n", e3, map.size(), (gstop-gstart));
#else
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		if (sequential == false) {
			map.add(key, key, &sequential, true);

		} else {
			map.add(key, key);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Add %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
#endif
	long gstop = System::currentTimeMillis();
	DEEP_LOG(INFO, OTHER, "ADD TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		boolean status;
		map.get(key, null, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! get: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Get %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "GET TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		boolean status;
		map.remove(key, null, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! REMOVE: %lld\n", key);
			exit(-1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Remove %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "REMOVE TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));
}

int main(int argc, char** argv) {
	if (argc == 2) {
		COUNT = atoi(argv[1]);
	}

#if 1
	DEEP_LOG(INFO, OTHER, "---------------------------- TEST RETURN\n");
	testMapRet();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST HASH-SET\n");
	testHashSet();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST HASH-MAP\n");
	testHashMap();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST TREE-FUNC\n");
	testTreeFunc();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST TREE-SET\n");
	testTreeSet();
#endif

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST TREE-MAP\n");
	testTreeMap();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST TREE-MAP Sequential\n");
	testTreeMapSequential();

	return 0;
}
