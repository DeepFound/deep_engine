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
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "cxx/util/TreeMap.h"
#include "cxx/util/TreeMap.cxx"

#include "cxx/util/ArrayList.h"

using namespace cxx::lang;
using namespace cxx::util;

template class TreeMap<int,int>;
template class TreeMap<long long,long long>;
template class TreeMap<Long*,Long*>;
template class TreeMap<String*,String*>;

Comparator<Long*> LongComparator;
Comparator<int> intComparator;
Comparator<long long> longlongComparator;


int testTreeMap();
int testTreeMapRandomInsert();
int testTreeMapRandomRemove();
int testTreeMapPrimLong();
int testTreeMapPrimInt();
int testTreeMapSize();

int main(int argc, char** argv) {
	int result = testTreeMapPrimInt();

	if (result) {
		return result;
	}

	result = testTreeMapRandomInsert();
	if (result) {
		return result;
	}

	result = testTreeMapPrimLong();
	if (result) {
		return result;
	}

	result = testTreeMap();
	if (result) {
		return result;
	}

	result = testTreeMapSize();
	if (result) {
		return result;
	}

	/*
	for (int i=1; i<=5; i++) {
		DEEP_LOG(INFO, OTHER, " Random test %d of %d\n", i, 5);
		result = testTreeMapRandomRemove();
		if (result) {
			return result;
		}
	}
	*/

	return 0;
}

int testTreeMapRandomInsert() {

	TreeMap<Long*, Long*> map(&LongComparator, false, true);

	const long MAX_KEY = 1000000;

	const long TREE_SIZE = 100000;

	srand(time(0));
	
	DEEP_LOG(INFO, OTHER, "CREATING TREE RANDOMLY WITH SIZE %ld \n", TREE_SIZE);
	for (long i = 0; i < TREE_SIZE; i++) {
		long randLong = (rand() % MAX_KEY) + 1;
		Long* key = new Long(randLong);
		map.put(key, key);
	}

	ArrayList<Long*> keys;

	const MapEntry<Long*, Long*>* entry = map.firstEntry();
	while (entry != null) {
		Long* key = (Long*) entry->getKey();
		keys.add(key);
		entry = map.higherEntry(entry->getKey());
	}

	long index = keys.size() - 1;
	entry = map.lastEntry();

	DEEP_LOG(INFO, OTHER, "VERIFYING HIGHER ENTRY TREE WALK MATCHES LOWER ENTRY TREE WALK\n");

	while (index >= 0) {
		if (keys.get(index)->longValue() != entry->getKey()->longValue()) {
			DEEP_LOG(ERROR, OTHER, "INVALID LAST/LOWER ENTRY EXPECTED %lld got %lld\n", keys.get(index)->longValue(), entry->getKey()->longValue());
                        return 1;
		}

		entry = map.lowerEntry(entry->getKey());
		index--;
	}

	DEEP_LOG(INFO, OTHER, "ALL ENTRIES MATCHED\n");

	return 0;
}

int testTreeMapRandomRemove() {
	TreeMap<Long*, Long*> map(&LongComparator, 3, false, true);

	int RANDOM_REMOVALS = 50000;

	int COUNT = 1000000;

	long gstart = System::currentTimeMillis();
	for (int i = 0; i <= COUNT; i++) {
		Long* key = new Long(i);
		map.put(key, key);
	}
	long gstop = System::currentTimeMillis();
	DEEP_LOG(INFO, OTHER, "PUT TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	srand(time(0));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < RANDOM_REMOVALS; i++) {
		int randomKey = (rand() % COUNT) + 1;
		Long rKey(randomKey);

		Long* value = map.remove(&rKey);
		if (value != null) {
			delete value;
		}
	}
	gstop = System::currentTimeMillis();
	DEEP_LOG(INFO, OTHER, "RANDOM REMOVAL TIME: %d, %d, %ld\n", -1, RANDOM_REMOVALS, (gstop-gstart));

	DEEP_LOG(INFO, OTHER, "VERIFYING LOWER / FLOOR / HIGHER / CEILING AFTER REMOVES\n");
	for (int i = 1; i < COUNT; i++) {
		Long key(i);

		Long* x = (Long*) map.get(&key);
		if (x == null) {
			//DEEP_LOG(INFO, OTHER, " Key: %d not found\n", i);

		} else {
			//DEEP_LOG(INFO, OTHER, " Key: %d found\n", i);
		}

		//
		// LOWER
		//
		x = (Long*) map.lowerKey(&key);
		if (x != null) {
			//DEEP_LOG(INFO, OTHER, " Key: %d has lower key of : %lld\n", i, (x->longValue()));
		}

		for (int j=i-1; j >= 0; j--) {
			Long rKey(j);

			if (map.containsKey(&rKey)) {
				if (x != null) {
					if (x->longValue() == j) {
						break;

					} else {
						DEEP_LOG(ERROR, OTHER, "Lower failed for key: %d (invalid) expected: %d\n", i, j);
						return 1;
					}

				} else {
					DEEP_LOG(ERROR, OTHER, "Lower failed for key: %d (null)\n", i);
					return 1;
				}
			}
		}

		//
		// FLOOR
		//
		x = (Long*) map.floorKey(&key);
		if (x != null) {
			//DEEP_LOG(INFO, OTHER, " Key: %d has floor key of : %lld\n", i, (x->longValue()));
		}

		for (int j=i; j >= 0; j--) {
			Long rKey(j);

			if (map.containsKey(&rKey)) {
				if (x != null) {
					if (x->longValue() == j) {
						break;

					} else {
						DEEP_LOG(ERROR, OTHER, "Floor failed for key: %d (invalid) expected: %d\n", i, j);
						return 1;
					}

				} else {
					DEEP_LOG(ERROR, OTHER, "Floor failed for key: %d (null)\n", i);
					return 1;
				}

			}
		}

		//
		// HIGHER
		//
		x = (Long*) map.higherKey(&key);
		if (x != null) {
			//DEEP_LOG(INFO, OTHER, " Key: %d has higher key of : %lld\n", i, (x->longValue()));
		}

		for (int j=i+1; j <= COUNT; j++) {
			Long rKey(j);

			if (map.containsKey(&rKey)) {
				if (x != null) {
					if (x->longValue() == j) {
						break;

					} else {
						DEEP_LOG(ERROR, OTHER, "Higher failed for key: %d (invalid), expected: %d\n", i, j);
						return 1;
					}

				} else {
					DEEP_LOG(ERROR, OTHER, "Higher failed for key: %d (null)\n", i);
					return 1;
				}

			}
		}

		//
		// CEILING
		//
		x = (Long*) map.ceilingKey(&key);
		if (x != null) {
			//DEEP_LOG(INFO, OTHER, " Key: %d has ceiling key of : %lld\n", i, (x->longValue()));
		}

		for (int j=i; j <= COUNT; j++) {
			Long rKey(j);

			if (map.containsKey(&rKey)) {
				if (x != null) {
					if (x->longValue() == j) {
						break;

					} else {
						DEEP_LOG(ERROR, OTHER, "Ceiling failed for key: %d (invalid) expected: %d\n", i, j);
						return 1;
					}

				} else {
					DEEP_LOG(ERROR, OTHER, "Ceiling failed for key: %d (null)\n", i);
					return 1;
				}
			}
		}
	}

	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR\n");
	Set<MapEntry<Long*,Long*>* >* entrySet = map.entrySet();
	Iterator<MapEntry<Long*,Long*>* >* iter = entrySet->iterator();
	for (int i = 0; i <= COUNT; i++) {
		Long rKey(i);
		if (map.containsKey(&rKey)) {
			MapEntry<Long*,Long*>* entry = (MapEntry<Long*,Long*>*) iter->next();
			Long* key = (Long*) entry->getKey();
			if (key->longValue() != i) {
				DEEP_LOG(ERROR, OTHER, "ITERATOR FAILED: expected %d, got %lld\n", i, key->longValue());
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, " ITERATOR SUCCESS: expected %lld, got %lld\n", i, key->longValue());
			}
		}
	}
	delete entrySet;
	delete iter;

	int mapSize = map.size();
	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR RANDOM REMOVE\n");
	DEEP_LOG(INFO, OTHER, "MAP SIZE BEFORE RANDOM REMOVE: %d\n", mapSize);
	entrySet = map.entrySet();
	iter = entrySet->iterator();
	int removeCount = 0;
	while (iter->hasNext()) {
		MapEntry<Long*,Long*>* entry = (MapEntry<Long*,Long*>*) iter->next();
		if ((rand() % 10) == 2) {
			Long key(entry->getKey()->longValue());
			Long* value = entry->getValue();

			//DEEP_LOG(INFO, OTHER, " RANDOMLY REMOVING : %lld\n", key.longValue());
			iter->remove();
			removeCount++;
			delete value;

			if (map.containsKey(&key)) {
				DEEP_LOG(ERROR, OTHER, "ITERATOR RANDOM REMOVE FAILED: %lld\n", key.longValue());
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, " ITERATOR REMOVE SUCCESS: %lld\n", key.longValue());
			}
		}
	}
	delete entrySet;
	delete iter;

	if ((mapSize - removeCount) != map.size()) {
		DEEP_LOG(ERROR, OTHER, " FAILED TO RANDOMLY REMOVE ENTRIES: %d \n", map.size());
		return 1;
	}

	DEEP_LOG(INFO, OTHER, "MAP SIZE AFTER RANDOM REMOVE: %d\n", map.size());
	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR AFTER RANDOM REMOVE\n");
	entrySet = map.entrySet();
	iter = entrySet->iterator();
	for (int i = 0; i <= COUNT; i++) {
		Long rKey(i);
		if (map.containsKey(&rKey)) {
			MapEntry<Long*,Long*>* entry = (MapEntry<Long*,Long*>*) iter->next();
			Long* key = (Long*) entry->getKey();
			if (key->longValue() != i) {
				DEEP_LOG(ERROR, OTHER, "ITERATOR FAILED: expected %d, got %lld\n", i, key->longValue());
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, " ITERATOR SUCCESS: expected %lld, got %lld\n", i, key->longValue());
			}
		}
	}
	delete entrySet;
	delete iter;

	return 0;
}

int testTreeMap() {
	TreeMap<Long*, Long*> map(&LongComparator, 3, false, true);

	int COUNT = 1000000;

	long gstart = System::currentTimeMillis();
	for (int i = 0; i <= COUNT; i += 2) {
		Long* key = new Long(i);
		map.put(key, key);
	}
	long gstop = System::currentTimeMillis();
	DEEP_LOG(INFO, OTHER, "PUT TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	Long* first = map.firstKey();
	if (first == null || first->longValue() != 0) {
		DEEP_LOG(ERROR, OTHER, "  !   <FAILED> FIRST: %lld\n", first->longValue());
		return 1;
	}

	Long* last = map.lastKey();
	if (last == null || last->longValue() != COUNT) {
		DEEP_LOG(ERROR, OTHER, "  !   <FAILED> LAST: %lld\n", last->longValue());
		return 1;
	}

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		Long key(i);
		Long* x = (Long*) map.lowerKey(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> LOWER: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key.longValue() != (x->longValue() + 2))) ||
			    ((i%2!=0) && (key.longValue() != (x->longValue() + 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> LOWER: %d, %lld\n", i, x->longValue());
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) LOWER: %d, %lld\n", i, x->longValue());
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "LOWER TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		Long key(i);
		Long* x = (Long*) map.floorKey(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> FLOOR: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key.longValue() != x->longValue())) ||
			    ((i%2!=0) && (key.longValue() != (x->longValue() + 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> FLOOR: %d, %lld\n", i, x->longValue());
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) FLOOR: %d, %lld\n", i, x->longValue());
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "FLOOR TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		Long key(i);
		Long* x = (Long*) map.higherKey(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> HIGHER: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key.longValue() != (x->longValue() - 2))) ||
			    ((i%2!=0) && (key.longValue() != (x->longValue() - 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> HIGHER: %d, %lld\n", i, x->longValue());
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) HIGHER: %d, %lld\n", i, x->longValue());
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "HIGHER TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		Long key(i);
		Long* x = (Long*) map.ceilingKey(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> CEILING: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key.longValue() != x->longValue())) ||
			    ((i%2!=0) && (key.longValue() != (x->longValue() - 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> CEILING: %d, %lld\n", i, x->longValue());
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) CEILING: %d, %lld\n", i, x->longValue());
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "CEILING TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR\n");
	Set<MapEntry<Long*,Long*>* >* entrySet = map.entrySet();
	Iterator<MapEntry<Long*,Long*>* >* iter = entrySet->iterator();
	for (int i = 0; i <= COUNT; i += 2) {
		MapEntry<Long*,Long*>* entry = (MapEntry<Long*,Long*>*) iter->next();
		Long* key = (Long*) entry->getKey();
		if (key->longValue() != i) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR FAILED: expected %d, got %lld\n", i, key->longValue());
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR SUCCESS: expected %lld, got %lld\n", i, key->longValue());
		}
	}

	delete entrySet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING HEAD MAP\n");
	Long toKey(100);
	MapEntry<Long*,Long*>* curEntry = null;
	SortedMap<Long*,Long* >* headMap = map.headMap(&toKey);
	entrySet = headMap->entrySet();
	iter = entrySet->iterator();
	for (int i = 0; iter->hasNext(); i += 2) {
		curEntry = iter->next();
		if (i != curEntry->getKey()->longValue()) {
			DEEP_LOG(ERROR, OTHER, "HEAD MAP FAILED: expected %d, got %lld\n", i, curEntry->getKey()->longValue());
			return 1;
		}
	}

	if (curEntry->getKey()->longValue() != toKey.longValue()) {
		DEEP_LOG(ERROR, OTHER, "HEAD MAP FAILED: expected %lld, got %lld\n", toKey.longValue(), curEntry->getKey()->longValue());
		return 1;
	}

	delete headMap;
	delete entrySet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING SUB MAP\n");
	Long subFromKey(100);
	Long subToKey(200);
	SortedMap<Long*,Long* >* subMap = map.subMap(&subFromKey, &subToKey);
	entrySet = subMap->entrySet();
	iter = entrySet->iterator();
	for (int i = subFromKey.longValue(); iter->hasNext(); i += 2) {
		curEntry = iter->next();
		if (i != curEntry->getKey()->longValue()) {
			DEEP_LOG(ERROR, OTHER, "SUB MAP FAILED: expected %d, got %lld\n", i, curEntry->getKey()->longValue());
			return 1;
		}
	}

	if (curEntry->getKey()->longValue() != subToKey.longValue()) {
		DEEP_LOG(ERROR, OTHER, "SUB MAP FAILED: expected %lld, got %lld\n", subToKey.longValue(), curEntry->getKey()->longValue());
		return 1;
	}

	if (subMap->firstKey()->longValue() != subFromKey.longValue()) {
		DEEP_LOG(ERROR, OTHER, "SUB MAP FIRST FAILED: expected %lld, got %lld\n", subFromKey.longValue(), subMap->firstKey()->longValue());
		return 1;
	}

	if (subMap->lastKey()->longValue() != subToKey.longValue()) {
		DEEP_LOG(ERROR, OTHER, "SUB MAP LAST FAILED: expected %lld, got %lld\n", subToKey.longValue(), subMap->lastKey()->longValue());
		return 1;
	}

	Long key(150);
	if (subMap->get(&key)->longValue() != key.longValue()) {
		DEEP_LOG(ERROR, OTHER, "SUB MAP GET FAILED: expected %lld, got %lld\n", key.longValue(), subMap->get(&key)->longValue());
		return 1;
	}

	delete subMap;
	delete entrySet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING TAIL MAP\n");
	Long fromKey(map.size() - 100 - 1);
	SortedMap<Long*,Long* >* tailMap = map.tailMap(&fromKey);
	entrySet = tailMap->entrySet();
	iter = entrySet->iterator();
	for (int i = fromKey.longValue(); iter->hasNext(); i += 2) {
		curEntry = iter->next();
		if (i != curEntry->getKey()->longValue()) {
			DEEP_LOG(ERROR, OTHER, "TAIL MAP FAILED: expected %d, got %lld\n", i, curEntry->getKey()->longValue());
			return 1;
		}
	}

	if (curEntry->getKey()->longValue() != map.lastEntry()->getKey()->longValue()) {
		DEEP_LOG(ERROR, OTHER, "TAIL MAP FAILED: expected %lld, got %lld\n", map.lastEntry()->getKey()->longValue(), curEntry->getKey()->longValue());
		return 1;
	}

	delete tailMap;
	delete entrySet;
	delete iter;

	int mapSize = map.size();
	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR REMOVE\n");
	DEEP_LOG(INFO, OTHER, "MAP SIZE BEFORE REMOVE: %d\n", mapSize);
	entrySet = map.entrySet();
	iter = entrySet->iterator();
	int removeCount = 0;
	while (iter->hasNext()) {
		MapEntry<Long*,Long*>* entry = (MapEntry<Long*,Long*>*) iter->next();
		Long key(entry->getKey()->longValue());
		Long* value = entry->getValue();

		iter->remove();
		removeCount++;
		delete value;

		if (map.containsKey(&key)) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR REMOVE FAILED: %lld\n", key.longValue());
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR REMOVE SUCCESS: %lld\n", key.longValue());
		}
	}

	delete entrySet;
	delete iter;

	if (removeCount != mapSize) {
		DEEP_LOG(ERROR, OTHER, "FAILED TO REMOVE ALL ENTRIES: %d entries left\n", map.size());
		return 1;
	}

	DEEP_LOG(INFO, OTHER, "MAP SIZE AFTER REMOVE: %d\n", map.size());

	return 0;
}

int testTreeMapPrimLong() {
	TreeMap<long long, long long> map(&longlongComparator, 3, false, false);

	int COUNT = 1000000;

	long gstart = System::currentTimeMillis();
	for (int i = 0; i <= COUNT; i += 2) {
		long long key = i;
		map.put(key, key);
	}
	long gstop = System::currentTimeMillis();
	DEEP_LOG(INFO, OTHER, "PUT TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		long long key = i;
		boolean status;
		long long x = map.lowerKey(key, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> LOWER: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key != (x + 2))) ||
			    ((i%2!=0) && (key != (x + 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> LOWER: %d, %lld\n", i, x);
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) LOWER: %d, %lld\n", i, x);
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "LOWER TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		long long key = i;
		boolean status;
		long long x = map.floorKey(key, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> FLOOR: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key != x)) ||
			    ((i%2!=0) && (key != (x + 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> FLOOR: %d, %lld\n", i, x);
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) FLOOR: %d, %lld\n", i, x);
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "FLOOR TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		boolean status;
		long long x = map.higherKey(key, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> HIGHER: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key != (x - 2))) ||
			    ((i%2!=0) && (key != (x - 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> HIGHER: %d, %lld\n", i, x);
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) HIGHER: %d, %lld\n", i, x);
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "HIGHER TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key= i;
		boolean status;
		long long x = map.ceilingKey(key, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> CEILING: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key != x)) ||
			    ((i%2!=0) && (key != (x - 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> CEILING: %d, %lld\n", i, x);
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) CEILING: %d, %lld\n", i, x);
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "CEILING TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR\n");
	Set<MapEntry<long long,long long>* >* entrySet = map.entrySet();
	Iterator<MapEntry<long long,long long>* >* iter = entrySet->iterator();
	for (int i = 0; i <= COUNT; i += 2) {
		MapEntry<long long,long long>* entry = (MapEntry<long long,long long>*) iter->next();
		long long key = entry->getKey();
		if (key != i) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR FAILED: expected %d, got %lld\n", i, key);
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR SUCCESS: expected %lld, got %lld\n", i, key);
		}
	}

	delete entrySet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING HEAD MAP\n");
	long long toKey =100;
	MapEntry<long long,long long>* curEntry = null;
	SortedMap<long long,long long>* headMap = map.headMap(toKey);
	entrySet = headMap->entrySet();
	iter = entrySet->iterator();
	for (int i = 0; iter->hasNext(); i += 2) {
		curEntry = iter->next();
		if (i != curEntry->getKey()) {
			DEEP_LOG(ERROR, OTHER, "HEAD MAP FAILED: expected %d, got %lld\n", i, curEntry->getKey());
			return 1;
		}
	}

	if (curEntry->getKey() != toKey) {
		DEEP_LOG(ERROR, OTHER, "HEAD MAP FAILED: expected %lld, got %lld\n", toKey, curEntry->getKey());
		return 1;
	}

	delete headMap;
	delete entrySet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING SUB MAP\n");
	long long subFromKey = 100;
	long long subToKey = 200;
	SortedMap<long long,long long>* subMap = map.subMap(subFromKey, subToKey);
	entrySet = subMap->entrySet();
	iter = entrySet->iterator();
	for (int i = subFromKey; iter->hasNext(); i += 2) {
		curEntry = iter->next();
		if (i != curEntry->getKey()) {
			DEEP_LOG(ERROR, OTHER, "SUB MAP FAILED: expected %d, got %lld\n", i, curEntry->getKey());
			return 1;
		}
	}

	if (curEntry->getKey() != subToKey) {
		DEEP_LOG(ERROR, OTHER, "SUB MAP FAILED: expected %lld, got %lld\n", subToKey, curEntry->getKey());
		return 1;
	}

	if (subMap->firstKey() != subFromKey) {
		DEEP_LOG(ERROR, OTHER, "SUB MAP FIRST FAILED: expected %lld, got %lld\n", subFromKey, subMap->firstKey());
		return 1;
	}

	if (subMap->lastKey() != subToKey) {
		DEEP_LOG(ERROR, OTHER, "SUB MAP LAST FAILED: expected %lld, got %lld\n", subToKey, subMap->lastKey());
		return 1;
	}

	long long key = 150;
	if (subMap->get(key) != key) {
		DEEP_LOG(ERROR, OTHER, "SUB MAP GET FAILED: expected %lld, got %lld\n", key, subMap->get(key));
		return 1;
	}

	delete subMap;
	delete entrySet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING TAIL MAP\n");
	long long fromKey = map.size() - 100 - 1;
	SortedMap<long long,long long>* tailMap = map.tailMap(fromKey);
	entrySet = tailMap->entrySet();
	iter = entrySet->iterator();
	for (int i = fromKey; iter->hasNext(); i += 2) {
		curEntry = iter->next();
		if (i != curEntry->getKey()) {
			DEEP_LOG(ERROR, OTHER, "TAIL MAP FAILED: expected %d, got %lld\n", i, curEntry->getKey());
			return 1;
		}
	}

	if (curEntry->getKey() != map.lastEntry()->getKey()) {
		DEEP_LOG(ERROR, OTHER, "TAIL MAP FAILED: expected %lld, got %lld\n", map.lastEntry()->getKey(), curEntry->getKey());
		return 1;
	}

	delete tailMap;
	delete entrySet;
	delete iter;

	int mapSize = map.size();
	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR REMOVE\n");
	DEEP_LOG(INFO, OTHER, "MAP SIZE BEFORE REMOVE: %d\n", mapSize);
	entrySet = map.entrySet();
	iter = entrySet->iterator();
	int removeCount = 0;
	while (iter->hasNext()) {
		MapEntry<long long,long long>* entry = (MapEntry<long long,long long>*) iter->next();
		long long key = entry->getKey();
		//long long value = entry->getValue();

		iter->remove();
		removeCount++;
		//delete value;

		if (map.containsKey(key)) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR REMOVE FAILED: %lld\n", key);
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR REMOVE SUCCESS: %lld\n", key);
		}
	}

	delete entrySet;
	delete iter;

	if (removeCount != mapSize) {
		DEEP_LOG(ERROR, OTHER, "FAILED TO REMOVE ALL ENTRIES: %d entries left\n", map.size());
		return 1;
	}

	DEEP_LOG(INFO, OTHER, "MAP SIZE AFTER REMOVE: %d\n", map.size());

	return 0;
}

int testTreeMapPrimInt() {
	TreeMap<int, int> map(&intComparator, 3, false, false);

	int COUNT = 1000000;

	long gstart = System::currentTimeMillis();
	for (int i = 0; i <= COUNT; i += 2) {
		int key = i;
		map.put(key, key);
	}
	long gstop = System::currentTimeMillis();
	DEEP_LOG(INFO, OTHER, "PUT TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		int key = i;
		boolean status;
		int x = map.lowerKey(key, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> LOWER: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key != (x + 2))) ||
			    ((i%2!=0) && (key != (x + 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> LOWER: %d, %d\n", i, x);
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) LOWER: %d, %d\n", i, x);
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "LOWER TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		int key = i;
		boolean status;
		int x = map.floorKey(key, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> FLOOR: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key != x)) ||
			    ((i%2!=0) && (key != (x + 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> FLOOR: %d, %d\n", i, x);
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) FLOOR: %d, %d\n", i, x);
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "FLOOR TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		int key = i;
		boolean status;
		int x = map.higherKey(key, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> HIGHER: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key != (x - 2))) ||
			    ((i%2!=0) && (key != (x - 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> HIGHER: %d, %d\n", i, x);
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) HIGHER: %d, %\n", i, x);
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "HIGHER TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		int key= i;
		boolean status;
		int x = map.ceilingKey(key, &status);
		if (status == false) {
			DEEP_LOG(ERROR, OTHER, "  !   <FAILED> CEILING: %d\n", i);
			return 1;

		} else {
			if (((i%2==0) && (key != x)) ||
			    ((i%2!=0) && (key != (x - 1)))) {
				DEEP_LOG(ERROR, OTHER, "  !   <FAILED> CEILING: %d, %d\n", i, x);
				return 1;

			} else {
				//DEEP_LOG(INFO, OTHER, "       FOUND :-) CEILING: %d, %\n", i, x);
			}
		}
	}
	gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "CEILING TIME: %d, %d, %ld\n", -1, map.size(), (gstop-gstart));

	DEEP_LOG(INFO, OTHER, "VERIFYING TREE ITERATOR NEXT\n");
	TreeIterator<MapEntry<int,int>* >* treeIter = map.iterator(map.firstKey());
	for (int i = 0; i <= COUNT; i += 2) {
		MapEntry<int,int>* entry = (MapEntry<int,int>*) treeIter->next();
		int key = entry->getKey();
		if (key != i) {
			DEEP_LOG(ERROR, OTHER, "TREE ITERATOR FAILED NEXT: expected %d, got %d\n", i, key);
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR SUCCESS: expected %d, got %d\n", i, key);
		}
	}

	DEEP_LOG(INFO, OTHER, "VERIFYING TREE ITERATOR PREVIOUS\n");
	treeIter = map.iterator(map.lastKey(), treeIter);
	treeIter->next();
	for (int i = COUNT; i >= 0; i -= 2) {
		MapEntry<int,int>* entry = (MapEntry<int,int>*) treeIter->previous();
		int key = entry->getKey();
		if (key != i) {
			DEEP_LOG(ERROR, OTHER, "TREE ITERATOR FAILED PREVIOUS: expected %d, got %d\n", i, key);
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR SUCCESS: expected %d, got %d\n", i, key);
		}
	}

	delete treeIter;

	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR\n");
	Set<MapEntry<int,int>* >* entrySet = map.entrySet();
	Iterator<MapEntry<int,int>* >* iter = entrySet->iterator();
	for (int i = 0; i <= COUNT; i += 2) {
		MapEntry<int,int>* entry = (MapEntry<int,int>*) iter->next();
		int key = entry->getKey();
		if (key != i) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR FAILED: expected %d, got %d\n", i, key);
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR SUCCESS: expected %d, got %d\n", i, key);
		}
	}

	delete entrySet;
	delete iter;

	int mapSize = map.size();
	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR REMOVE\n");
	DEEP_LOG(INFO, OTHER, "MAP SIZE BEFORE REMOVE: %d\n", mapSize);
	entrySet = map.entrySet();
	iter = entrySet->iterator();
	int removeCount = 0;
	while (iter->hasNext()) {
		MapEntry<int,int>* entry = (MapEntry<int,int>*) iter->next();
		int key = entry->getKey();
		//long long value = entry->getValue();

		iter->remove();
		removeCount++;
		//delete value;

		if (map.containsKey(key)) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR REMOVE FAILED: %d\n", key);
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR REMOVE SUCCESS: %d\n", key);
		}
	}

	delete entrySet;
	delete iter;

	if (removeCount != mapSize) {
		DEEP_LOG(ERROR, OTHER, "FAILED TO REMOVE ALL ENTRIES: %d entries left\n", map.size());
		return 1;
	}

	DEEP_LOG(INFO, OTHER, "MAP SIZE AFTER REMOVE: %d\n", map.size());

	return 0;
}

int testTreeMapSize() {
	TreeMap<long long, long long> map(&longlongComparator, 3, false, false);

	int COUNT = 10000000;
	int size = 0;
	long long retkey;
	bool status;

	for (int i = 0; i <= COUNT; i += 2) {
		long long key = i;
		map.put(key, key);
		size++;
	}

	DEEP_LOG(INFO, OTHER, "MAP SIZE AFTER FIRST PUT: %d, EXPECT: %d\n", map.size(), size);

	if (size != map.size()) {
		DEEP_LOG(ERROR, OTHER, "FAILED - map size after first put: %d, expected: %d\n", map.size(), size);
		return 1;
	}

	for (int i = 1; i <= COUNT; i += 2) {
		long long key = i;
		map.put(key, key);
		size++;
	}

	DEEP_LOG(INFO, OTHER, "MAP SIZE AFTER SECOND PUT: %d, EXPECT: %d\n", map.size(), size);

	if (size != map.size()) {
		DEEP_LOG(ERROR, OTHER, "FAILED - map size after second put: %d, expected: %d\n", map.size(), size);
		return 1;
	}

	for (int i = 1; i <= COUNT; i += 3) {
		long long key = i;
		map.remove(key, &retkey, (boolean*) &status);
		if (status == true) {
			size--;
		}
	}

	DEEP_LOG(INFO, OTHER, "MAP SIZE AFTER FIRST REMOVE: %d, EXPECT: %d\n", map.size(), size);

	if (size != map.size()) {
		DEEP_LOG(ERROR, OTHER, "FAILED - map size after remove: %d, expected: %d\n", map.size(), size);
		return 1;
	}

	for (int i = 1; i <= COUNT; i += 10) {
		long long key = i;
		map.remove(key, &retkey, (boolean*) &status);
		if (status == true) {
			size--;
		}
	}

	DEEP_LOG(INFO, OTHER, "MAP SIZE AFTER SECOND REMOVE: %d, EXPECT: %d\n", map.size(), size);

	if (size != map.size()) {
		DEEP_LOG(ERROR, OTHER, "FAILED - map size after remove: %d, expected: %d\n", map.size(), size);
		return 1;
	}

	return 0;
}


