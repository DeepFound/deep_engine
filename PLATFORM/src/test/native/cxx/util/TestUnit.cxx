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

using namespace cxx::lang;
using namespace cxx::util;

template class HashMap<Long*,Long*>;
template class HashSet<Long*>;
template class TreeMap<Long*,Long*>;
template class TreeSet<Long*>;

static int COUNT = 1000000;

void testMapRet();

void testTreeFunc();

void testTreeMap();

void testTreeSet();

void testHashSet();

void testHashMap();

Comparator<Long*> longComparator;

int main(int argc, char** argv) {
	if (argc == 2) {
		COUNT = atoi(argv[1]);
	}

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST RETURN\n");
	testMapRet();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST TREE-FUNC\n");
	testTreeFunc();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST TREE-SET\n");
	testTreeSet();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST TREE-MAP\n");
	testTreeMap();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST HASH-SET\n");
	testHashSet();

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST HASH-MAP\n");
	testHashMap();

	return 0;
}

void testTreeSet() {
	TreeSet<Long*> set(&longComparator, 3, true);

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		Long* key = new Long(i);
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
		Long key(i);
		if (set.contains(&key) == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! contains: %lld\n", key.longValue());
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
		Long key(i);
		if (set.remove(&key) == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! REMOVE: %lld\n", key.longValue());
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
	TreeMap<Long*, Long*> map(&longComparator, 3, false, true);

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		Long* key = new Long(i);
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
		Long key(i);
		const Long* x = map.get(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! get: %lld\n", key.longValue());
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

	//map.dump();

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		Long key(i);
		Long* x = map.remove(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! REMOVE: %lld\n", key.longValue());
			exit(-1);

		} else {
			delete x;
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

void testHashSet() {
	HashSet<Long*> set(COUNT * 2, false);

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		Long* key = new Long(i);
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
		Long key(i);
		if (set.contains(&key) == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! contains: %lld\n", key.longValue());
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
		Long key(i);
		if (set.remove(&key) == false) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! REMOVE: %lld\n", key.longValue());
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
	HashMap<Long*, Long*> map(COUNT * 2, false, true);

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		Long* key = new Long(i);
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
		Long key(i);
		const Long* x = map.get(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! get: %lld\n", key.longValue());
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
		Long key(i);
		Long* x = map.remove(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! REMOVE: %lld\n", key.longValue());
			exit(-1);

		} else {
			delete x;
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

void testMapRet() {
	Long* newkey = new Long(1);
	Long* dupkey = new Long(1);
	HashMap<Long*, Long*> map(16, false, false);

	DEEP_LOG(INFO, OTHER, "EQUALS: %d\n", newkey->equals(dupkey));
	DEEP_LOG(INFO, OTHER, "COMPARES: %d\n", newkey->compareTo(dupkey));
	delete dupkey; dupkey = null;

	Long* oldkey = map.put(newkey, newkey);
	DEEP_LOG(INFO, OTHER, "OLD KEY: %p\n", oldkey);

	Long* retkey = null;
	Long* getkey = map.remove(newkey, &retkey);
	DEEP_LOG(INFO, OTHER, "GET KEY: %lld\n", getkey->longValue());
	DEEP_LOG(INFO, OTHER, "RET KEY: %lld\n", retkey->longValue());
	delete getkey; getkey = null;
}

void testTreeFunc() {
	TreeSet<Long*> set(&longComparator, 3, false);
	TreeMap<Long*,Long*> map(&longComparator, 3, false, true);

	Long* key1 = new Long(1);
	Long* key2 = new Long(2);
	Long* key3 = new Long(3);
	Long* key4 = new Long(4);
	Long* key5 = new Long(5);

	set.add(key1);
	set.add(key2);
	set.add(key3);
	set.add(key4);
	set.add(key5);

	DEEP_LOG(INFO, OTHER, "FIRST: %lld\n", ((Long*) set.first())->longValue());
	DEEP_LOG(INFO, OTHER, "LAST: %lld\n", ((Long*) set.last())->longValue());
	DEEP_LOG(INFO, OTHER, "LOWER: %lld\n", ((Long*) set.lower(key3))->longValue());
	DEEP_LOG(INFO, OTHER, "HIGHER: %lld\n", ((Long*) set.higher(key3))->longValue());
	DEEP_LOG(INFO, OTHER, "FLOOR: %lld\n", ((Long*) set.floor(key3))->longValue());
	DEEP_LOG(INFO, OTHER, "CEILING: %lld\n", ((Long*) set.ceiling(key3))->longValue());

	Iterator<Long*>* iter1 = set.iterator();
	while (iter1->hasNext()) {
		Long* item = (Long*) iter1->next();
		DEEP_LOG(INFO, OTHER, " NEXT: %lld\n", item->longValue());
		iter1->remove();
	}
	delete iter1;
	DEEP_LOG(INFO, OTHER, "SIZE: %d\n", set.size());

	map.put(key1, key1);
	map.put(key2, key2);
	map.put(key3, key3);
	map.put(key4, key4);
	map.put(key5, key5);

	DEEP_LOG(INFO, OTHER, "FIRST: %lld\n", ((Long*) map.firstKey())->longValue());
	DEEP_LOG(INFO, OTHER, "LAST: %lld\n", ((Long*) map.lastKey())->longValue());
	DEEP_LOG(INFO, OTHER, "LOWER: %lld\n", ((Long*) map.lowerKey(key3))->longValue());
	DEEP_LOG(INFO, OTHER, "HIGHER: %lld\n", ((Long*) map.higherKey(key3))->longValue());
	DEEP_LOG(INFO, OTHER, "FLOOR: %lld\n", ((Long*) map.floorKey(key3))->longValue());
	DEEP_LOG(INFO, OTHER, "CEILING: %lld\n", ((Long*) map.ceilingKey(key3))->longValue());

	Set<MapEntry<Long*,Long*>* >* entrySet = map.entrySet();
	Iterator<MapEntry<Long*,Long*>* >* iter2 = entrySet->iterator();
	while (iter2->hasNext()) {
		MapEntry<Long*,Long*>* entry = (MapEntry<Long*,Long*>*) iter2->next();
		Long* key = (Long*) entry->getKey();
		Long* value = (Long*) entry->getValue();
		DEEP_LOG(INFO, OTHER, " NEXT: %lld, %lld\n", key->longValue(), value->longValue());
		iter2->remove();
		delete value;
	}
	delete entrySet;
	delete iter2;
	DEEP_LOG(INFO, OTHER, "SIZE: %d\n", map.size());
}
