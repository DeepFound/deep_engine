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

#include "cxx/util/TreeSet.h"
#include "cxx/util/TreeSet.cxx"

using namespace std;
using namespace cxx::lang;
using namespace cxx::util;

template class TreeSet<int>;
template class TreeSet<long long>;
template class TreeSet<Long*>;


int testTreeSet();
int testTreeSetPrimitive();
int testTreeSetSize();

int main(int argc, char** argv) {
	int result = testTreeSet();
	if (result == 0) {
		result = testTreeSetPrimitive();
		if (result == 0) {
			result = testTreeSetSize();
		}
	}

	return result;
}

int testTreeSet() {
	TreeSet<Long*> set;

	int COUNT = 1000000;

	long gstart = System::currentTimeMillis();
	for (int i = 0; i <= COUNT; i += 2) {
		Long* key = new Long(i);
		set.add(key);
	}
	long gstop = System::currentTimeMillis();
	DEEP_LOG(INFO, OTHER, "PUT TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	Long* first = set.first();
	if (first == null || first->longValue() != 0) {
		DEEP_LOG(ERROR, OTHER, "  !   <FAILED> FIRST: %lld\n", first->longValue());
		return 1;
	}

	Long* last = set.last();
	if (last == null || last->longValue() != COUNT) {
		DEEP_LOG(ERROR, OTHER, "  !   <FAILED> LAST: %lld\n", last->longValue());
		return 1;
	}

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		Long key(i);
		Long* x = set.lower(&key);
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

	DEEP_LOG(INFO, OTHER, "LOWER TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		Long key(i);
		Long* x = set.floor(&key);
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

	DEEP_LOG(INFO, OTHER, "FLOOR TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		Long key(i);
		Long* x = set.higher(&key);
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

	DEEP_LOG(INFO, OTHER, "HIGHER TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		Long key(i);
		Long* x = set.ceiling(&key);
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

	DEEP_LOG(INFO, OTHER, "CEILING TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR\n");
	Iterator<Long* >* iter = set.iterator();
	for (int i = 0; i <= COUNT; i += 2) {
		Long* key = iter->next();
		if (key->longValue() != i) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR FAILED: expected %d, got %lld\n", i, key->longValue());
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR SUCCESS: expected %lld, got %lld\n", i, key->longValue());
		}
	}

	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING HEAD SET\n");
	Long toKey(100);
	Long* lastKey = null;
	SortedSet<Long* >* headSet = set.headSet(&toKey);
	iter = headSet->iterator();
	for (int i = 0; iter->hasNext(); i += 2) {
		lastKey = iter->next();
		if (i != lastKey->longValue()) {
			DEEP_LOG(ERROR, OTHER, "HEAD SET FAILED: expected %d, got %lld\n", i, lastKey->longValue());
			return 1;
		}
	}

	if (lastKey->longValue() != toKey.longValue()) {
		DEEP_LOG(ERROR, OTHER, "HEAD SET FAILED: expected %lld, got %lld\n", toKey.longValue(), lastKey->longValue());
		return 1;
	}

	delete headSet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING SUB SET\n");
	Long subFromKey(100);
	Long subToKey(200);
	SortedSet<Long* >* subSet = set.subSet(&subFromKey, &subToKey);
	iter = subSet->iterator();
	for (int i = subFromKey.longValue(); iter->hasNext(); i += 2) {
		lastKey = iter->next();
		if (i != lastKey->longValue()) {
			DEEP_LOG(ERROR, OTHER, "SUB SET FAILED: expected %d, got %lld\n", i, lastKey->longValue());
			return 1;
		}
	}

	if (lastKey->longValue() != subToKey.longValue()) {
		DEEP_LOG(ERROR, OTHER, "SUB SET FAILED: expected %lld, got %lld\n", subToKey.longValue(), lastKey->longValue());
		return 1;
	}

	if (subSet->first()->longValue() != subFromKey.longValue()) {
		DEEP_LOG(ERROR, OTHER, "SUB SET FIRST FAILED: expected %lld, got %lld\n", subFromKey.longValue(), subSet->first()->longValue());
		return 1;
	}

	if (subSet->last()->longValue() != subToKey.longValue()) {
		DEEP_LOG(ERROR, OTHER, "SUB SET LAST FAILED: expected %lld, got %lld\n", subToKey.longValue(), subSet->last()->longValue());
		return 1;
	}

	delete subSet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING TAIL SET\n");
	Long fromKey(set.size() - 100 - 1);
	SortedSet<Long* >* tailSet = set.tailSet(&fromKey);
	iter = tailSet->iterator();
	for (int i = fromKey.longValue(); iter->hasNext(); i += 2) {
		lastKey = iter->next();
		if (i != lastKey->longValue()) {
			DEEP_LOG(ERROR, OTHER, "TAIL SET FAILED: expected %d, got %lld\n", i, lastKey->longValue());
			return 1;
		}
	}

	if (lastKey->longValue() != set.last()->longValue()) {
		DEEP_LOG(ERROR, OTHER, "TAIL SET FAILED: expected %lld, got %lld\n", set.last()->longValue(), lastKey->longValue());
		return 1;
	}

	delete tailSet;
	delete iter;

	int setSize = set.size();
	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR REMOVE\n");
	DEEP_LOG(INFO, OTHER, "MAP SIZE BEFORE REMOVE: %d\n", setSize);
	iter = set.iterator();
	int removeCount = 0;
	while (iter->hasNext()) {
		Long* key = iter->next();
		Long tkey(key->longValue());

		iter->remove();
		removeCount++;
		delete key;

		if (set.contains(&tkey)) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR REMOVE FAILED: %lld\n", tkey.longValue());
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR REMOVE SUCCESS: %lld\n", key.longValue());
		}
	}

	delete iter;

	if (removeCount != setSize) {
		DEEP_LOG(ERROR, OTHER, "FAILED TO REMOVE ALL ENTRIES: %d entries left\n", set.size());
		return 1;
	}

	DEEP_LOG(INFO, OTHER, "SIZE AFTER REMOVE: %d\n", set.size());

	return 0;
}

int testTreeSetPrimitive() {
	TreeSet<long long> set;

	int COUNT = 1000000;

	long gstart = System::currentTimeMillis();
	for (int i = 0; i <= COUNT; i += 2) {
		long long key = i;
		set.add(key);
	}
	long gstop = System::currentTimeMillis();
	DEEP_LOG(INFO, OTHER, "PUT TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		long long key = i;
		boolean status = false;
		long long x = set.lower(key, &status);
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

	DEEP_LOG(INFO, OTHER, "LOWER TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 1; i <= COUNT; i++) {
		long long key = i;
		boolean status = false;
		long long x = set.floor(key, &status);
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

	DEEP_LOG(INFO, OTHER, "FLOOR TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		boolean status = false;
		long long x = set.higher(key, &status);
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

	DEEP_LOG(INFO, OTHER, "HIGHER TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	gstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {
		long long key = i;
		boolean status = false;
		long long x = set.ceiling(key, &status);
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

	DEEP_LOG(INFO, OTHER, "CEILING TIME: %d, %d, %ld\n", -1, set.size(), (gstop-gstart));

	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR\n");
	Iterator<long long >* iter = set.iterator();
	for (int i = 0; i <= COUNT; i += 2) {
		long long key = iter->next();
		if (key != i) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR FAILED: expected %d, got %lld\n", i, key);
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR SUCCESS: expected %lld, got %lld\n", i, key->longValue());
		}
	}

	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING HEAD SET\n");
	long long toKey = 100;
	long long lastKey = -1;
	SortedSet<long long>* headSet = set.headSet(toKey);
	iter = headSet->iterator();
	for (int i = 0; iter->hasNext(); i += 2) {
		lastKey = iter->next();
		if (i != lastKey) {
			DEEP_LOG(ERROR, OTHER, "HEAD SET FAILED: expected %d, got %lld\n", i, lastKey);
			return 1;
		}
	}

	if (lastKey != toKey) {
		DEEP_LOG(ERROR, OTHER, "HEAD SET FAILED: expected %lld, got %lld\n", toKey, lastKey);
		return 1;
	}

	delete headSet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING SUB SET\n");
	long long subFromKey = 100;
	long long subToKey = 200;
	SortedSet<long long>* subSet = set.subSet(subFromKey, subToKey);
	iter = subSet->iterator();
	for (int i = subFromKey; iter->hasNext(); i += 2) {
		lastKey = iter->next();
		if (i != lastKey) {
			DEEP_LOG(ERROR, OTHER, "SUB SET FAILED: expected %d, got %lld\n", i, lastKey);
			return 1;
		}
	}

	if (lastKey != subToKey) {
		DEEP_LOG(ERROR, OTHER, "SUB SET FAILED: expected %lld, got %lld\n", subToKey, lastKey);
		return 1;
	}

	if (subSet->first() != subFromKey) {
		DEEP_LOG(ERROR, OTHER, "SUB SET FIRST FAILED: expected %lld, got %lld\n", subFromKey, subSet->first());
		return 1;
	}

	if (subSet->last() != subToKey) {
		DEEP_LOG(ERROR, OTHER, "SUB SET LAST FAILED: expected %lld, got %lld\n", subToKey, subSet->last());
		return 1;
	}

	delete subSet;
	delete iter;

	DEEP_LOG(INFO, OTHER, "VERIFYING TAIL SET\n");
	long long fromKey = set.size() - 100 - 1;
	SortedSet<long long>* tailSet = set.tailSet(fromKey);
	iter = tailSet->iterator();
	for (int i = fromKey; iter->hasNext(); i += 2) {
		lastKey = iter->next();
		if (i != lastKey) {
			DEEP_LOG(ERROR, OTHER, "TAIL SET FAILED: expected %d, got %lld\n", i, lastKey);
			return 1;
		}
	}

	if (lastKey != set.last()) {
		DEEP_LOG(ERROR, OTHER, "TAIL SET FAILED: expected %lld, got %lld\n", set.last(), lastKey);
		return 1;
	}

	delete tailSet;
	delete iter;

	int setSize = set.size();
	DEEP_LOG(INFO, OTHER, "VERIFYING ITERATOR REMOVE\n");
	DEEP_LOG(INFO, OTHER, "MAP SIZE BEFORE REMOVE: %d\n", setSize);
	iter = set.iterator();
	int removeCount = 0;
	while (iter->hasNext()) {
		long long key = iter->next();

		iter->remove();
		removeCount++;

		if (set.contains(key)) {
			DEEP_LOG(ERROR, OTHER, "ITERATOR REMOVE FAILED: %lld\n", key);
			return 1;

		} else {
			//DEEP_LOG(INFO, OTHER, " ITERATOR REMOVE SUCCESS: %lld\n", key.longValue());
		}
	}

	delete iter;

	if (removeCount != setSize) {
		DEEP_LOG(ERROR, OTHER, "FAILED TO REMOVE ALL ENTRIES: %d entries left\n", set.size());
		return 1;
	}

	DEEP_LOG(INFO, OTHER, "SIZE AFTER REMOVE: %d\n", set.size());


	return 0;
}

int testTreeSetSize() {
	TreeSet<long long> set;

	int COUNT = 10000000;
	int size = 0;

	for (int i = 0; i <= COUNT; i += 2) {
		long long key = i;
		set.add(key);
		size++;
	}

	DEEP_LOG(INFO, OTHER, "SIZE AFTER FIRST PUT: %d, EXPECT: %d\n", set.size(), size);

	if (size != set.size()) {
		DEEP_LOG(ERROR, OTHER, "FAILED - size after first put: %d, expected: %d\n", set.size(), size);
		return 1;
	}

	for (int i = 1; i <= COUNT; i += 2) {
		long long key = i;
		set.add(key);
		size++;
	}

	DEEP_LOG(INFO, OTHER, "SIZE AFTER SECOND PUT: %d, EXPECT: %d\n", set.size(), size);

	if (size != set.size()) {
		DEEP_LOG(ERROR, OTHER, "FAILED - size after second put: %d, expected: %d\n", set.size(), size);
		return 1;
	}

	for (int i = 1; i <= COUNT; i += 3) {
		long long key = i;
		if (set.remove(key) == true) {
			size--;
		}
	}

	DEEP_LOG(INFO, OTHER, "SIZE AFTER FIRST REMOVE: %d, EXPECT: %d\n", set.size(), size);

	if (size != set.size()) {
		DEEP_LOG(ERROR, OTHER, "FAILED - size after remove: %d, expected: %d\n", set.size(), size);
		return 1;
	}

	for (int i = 1; i <= COUNT; i += 10) {
		long long key = i;
		if (set.remove(key) == true) {
			size--;
		}
	}

	DEEP_LOG(INFO, OTHER, "SIZE AFTER SECOND REMOVE: %d, EXPECT: %d\n", set.size(), size);

	if (size != set.size()) {
		DEEP_LOG(ERROR, OTHER, "FAILED - size after remove: %d, expected: %d\n", set.size(), size);
		return 1;
	}

	return 0;
}

