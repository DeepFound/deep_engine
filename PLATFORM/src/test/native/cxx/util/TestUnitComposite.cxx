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

template class TreeMap<CompositeKey*, CompositeKey*>;

static int COUNT = 1000000;

void testTreeMap();

Comparator<CompositeKey*> compositeKeyComparator;

int main(int argc, char** argv) {
	if (argc == 2) {
		COUNT = atoi(argv[1]);
	}

	compositeKeyComparator.addKeyPart(KeyPart::INTEGER);
	compositeKeyComparator.addKeyPart(KeyPart::INTEGER);
	compositeKeyComparator.addKeyPart(KeyPart::INTEGER);

	DEEP_LOG(INFO, OTHER, "---------------------------- TEST TREE-MAP\n");
	testTreeMap();

	return 0;
}

void testTreeMap() {
	#ifdef COM_DEEPIS_DB_CARDINALITY
	TreeMap<CompositeKey*, CompositeKey*> map(&compositeKeyComparator, 23, false, false, 3);
	#else
	TreeMap<CompositeKey*, CompositeKey*> map(&compositeKeyComparator, 23, false, false);
	#endif

	bytetype k[4 + 4 + 4];

	CompositeKey* retkey = new CompositeKey(4 + 4 + 4);

	long gstart = System::currentTimeMillis();
	long lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		CompositeKey* key = new CompositeKey(4 + 4 + 4);
		bytearray bytes = (bytearray) *key;

		int key1 = i % 7;
		int key2 = i % 3;
		int key3 = i;
		memcpy(bytes, &key1, 4);
		memcpy(bytes + 4, &key2, 4);
		memcpy(bytes + 8, &key3, 4);

		boolean status = false;

		map.put(key, key, &retkey, &status);
		if (status) {
			exit(1);
		}

		if ((i % 100000) == 0) {
			long lstop = System::currentTimeMillis();
			DEEP_LOG(INFO, OTHER, "Put %d, %ld\n", i, (lstop-lstart));
			lstart = System::currentTimeMillis();
		}
	}
	long gstop = System::currentTimeMillis();

	DEEP_LOG(INFO, OTHER, "PUT TIME: %d, %d, %ld\n", COUNT, map.size(), (gstop-gstart));

	#ifdef COM_DEEPIS_DB_CARDINALITY
	const inttype* uniqueKeys = map.getCardinality();
	for (int i=0; i<3; i++) {
		DEEP_LOG(INFO, OTHER, "KEY PART %d: UNIQUE KEYS: %d\n", i, uniqueKeys[i]);
	}
	#endif

	gstart = System::currentTimeMillis();
	lstart = System::currentTimeMillis();
	for (int i = 0; i < COUNT; i++) {

		int key1 = i % 7;
		int key2 = i % 3;
		int key3 = i;
		memcpy(k, &key1, 4);
		memcpy(k + 4, &key2, 4);
		memcpy(k + 8, &key3, 4);

		CompositeKey key((const bytearray)k, 4 + 4 + 4);

		const CompositeKey* x = map.get(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! get: %d\n", i);
			exit(-1);

		} else if (i != *((int*) ((bytearray) (*x) + 8))) {
			DEEP_LOG(ERROR, OTHER, "FAILED !!! get: %d\n", i);
			exit(-1);

		} else {
			//DEEP_LOG(INFO, OTHER, " success val %d\n", *((int*) (bytearray) *x));
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

		int key1 = i % 7;
		int key2 = i % 3;
		int key3 = i;
		memcpy(k, &key1, 4);
		memcpy(k + 4, &key2, 4);
		memcpy(k + 8, &key3, 4);

		CompositeKey key((const bytearray)k, 4 + 4 + 4);

		CompositeKey* x = map.remove(&key);
		if (x == null) {
			DEEP_LOG(ERROR, OTHER, " FAILED !!! REMOVE: %d\n", i);
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

	#ifdef COM_DEEPIS_DB_CARDINALITY
	uniqueKeys = map.getCardinality();
	for (int i=0; i<3; i++) {
		DEEP_LOG(INFO, OTHER, "KEY PART %d: UNIQUE KEYS: %d\n", i, uniqueKeys[i]);
	}
	#endif

}
