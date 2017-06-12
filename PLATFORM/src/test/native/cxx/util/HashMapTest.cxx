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

#include "cxx/util/HashMap.h"
#include "cxx/util/HashMap.cxx"

#include "cxx/util/ArrayList.h"

using namespace cxx::lang;
using namespace cxx::util;

template class HashMap<inttype,inttype>;
template class HashMap<Object*,inttype>;

Converter<Object*> ObjectComparator;
Converter<inttype> intComparator;

inttype NUM_KEYS = 1000000;
inttype MAX_KEY = 100000000;

int testReplaceHashMap();
int testReplaceHashMapPrimitive();

int main(int argc, char** argv) {
	int result = testReplaceHashMap();

	if (result) {
		return result;
	}

	result = testReplaceHashMapPrimitive();

	if (result) {
		return result;
	}

	return 0;
}

int testReplaceHashMap() {
	HashMap<Object*,inttype> map(256, false);

	inttype expectedSize = 0;

	ArrayList<Object*> keys;
	ArrayList<Object*> removedKeys;

	DEEP_LOG(INFO, OTHER, "\n\ntestReplaceHashMap()\n\n");

	for (inttype i=0; i<NUM_KEYS; i++) {
		Object* key = new Object();

		if (map.containsKey(key) == false) {
			keys.add(key);

		} else {
			i--;
			continue;
		}

		map.put(key, 1);
	}

	expectedSize = keys.size();

	if ((map.size() != expectedSize) || (map.size() != NUM_KEYS)) {
		DEEP_LOG(ERROR, OTHER, "FAILED - map size after init: %d, expectedSize: %d or %d\n", map.size(), expectedSize, NUM_KEYS);
		return 1;
	}

	DEEP_LOG(INFO, OTHER, "Size after put: %d, expectedSize: %d\n", map.size(), expectedSize);

	// remove every 5th key
	for (inttype i=0; i<keys.size(); i+=5) {
		Object* o = keys.get(i);

		map.remove(o);
		expectedSize--;

		removedKeys.add(o);

		if (map.size() != expectedSize) {
			DEEP_LOG(ERROR, OTHER, "FAILED - map size after remove: %d, expectedSize: %d\n", map.size(), expectedSize);
			return 1;
		}
	}

	DEEP_LOG(INFO, OTHER, "Size after remove: %d, expectedSize: %d, numRemoved: %d\n", map.size(), expectedSize, removedKeys.size());

	// put back half of the removed keys back
	for (inttype i=removedKeys.size()-1; i>=removedKeys.size()/2; i--) {
		Object* o = removedKeys.get(i);

		map.put(o, 0);

		expectedSize++;

		if (map.size() != expectedSize) {
			DEEP_LOG(ERROR, OTHER, "FAILED - map size after put back (%p): %d, expectedSize: %d\n", o, map.size(), expectedSize);
			return 1;
		}
	}

	// put back or replace all removed keys
	for (inttype i=0; i<removedKeys.size(); i++) {
		Object* o = removedKeys.get(i);

		if (map.containsKey(o) == false) {
			expectedSize++;
		}

		map.put(o, 2);

		if (map.size() != expectedSize) {
			DEEP_LOG(ERROR, OTHER, "FAILED - map size after replace or put back: %d, expectedSize: %d\n", map.size(), expectedSize);
			return 1;
		}
	}

	for (inttype i=0; i<keys.size(); i++) {
		delete keys.get(i);
	}

	return 0;
}

int testReplaceHashMapPrimitive() {

	srand(time(0));

	HashMap<inttype,inttype> map(256, false);

	inttype expectedSize = 0;

	inttype numKeys = 0;
	inttype keys[NUM_KEYS];

	inttype numRemoved = 0;
	inttype removedKeys[NUM_KEYS];

	DEEP_LOG(INFO, OTHER, "\n\ntestReplaceHashMapPrimitive()\n\n");

	for (inttype i=0; i<NUM_KEYS; i++) {
		inttype key = (rand() % MAX_KEY);

		if (map.containsKey(key) == false) {
			keys[numKeys++] = key;

		} else {
			i--;
			continue;
		}

		map.put(key, 1);
	}

	expectedSize = numKeys;

	if ((map.size() != expectedSize) || (map.size() != NUM_KEYS)) {
		DEEP_LOG(ERROR, OTHER, "FAILED - map size after init: %d, expectedSize: %d or %d\n", map.size(), expectedSize, NUM_KEYS);
		return 1;
	}

	DEEP_LOG(INFO, OTHER, "Size after put: %d, expectedSize: %d\n", map.size(), expectedSize);

	// remove every 5th key
	for (inttype i=0; i<numKeys; i+=5) {
		inttype o = keys[i];

		map.remove(o);
		expectedSize--;

		removedKeys[numRemoved++] = o;

		if (map.size() != expectedSize) {
			DEEP_LOG(ERROR, OTHER, "FAILED - map size after remove: %d, expectedSize: %d\n", map.size(), expectedSize);
			return 1;
		}
	}

	DEEP_LOG(INFO, OTHER, "Size after remove: %d, expectedSize: %d, numRemoved: %d\n", map.size(), expectedSize, numRemoved);

	// put back half of the removed keys back
	for (inttype i=numRemoved-1; i>=numRemoved/2; i--) {
		inttype o = removedKeys[i];

		map.put(o, 0);

		expectedSize++;

		if (map.size() != expectedSize) {
			DEEP_LOG(ERROR, OTHER, "FAILED - map size after put back (%d): %d, expectedSize: %d\n", o, map.size(), expectedSize);
			return 1;
		}
	}

	// put back or replace all removed keys
	for (inttype i=0; i<numRemoved; i++) {
		inttype o = removedKeys[i];

		if (map.containsKey(o) == false) {
			expectedSize++;
		}

		map.put(o, 2);

		if (map.size() != expectedSize) {
			DEEP_LOG(ERROR, OTHER, "FAILED - map size after replace or put back: %d, expectedSize: %d\n", map.size(), expectedSize);
			return 1;
		}
	}

	return 0;
}
