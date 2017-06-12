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

#include "cxx/lang/Integer.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "cxx/util/concurrent/FreeList.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent;

void executingTest(const char* testName) {
	DEEP_LOG(INFO, OTHER, "-------------------------- %s\n", testName);
}

void testSize() {
	executingTest("testGetSize()");

	FreeList<Integer> freeList;

	assert(0 == freeList.size());
	freeList.destroy(new Integer(1));
	assert(1 == freeList.size());
	freeList.destroy(new Integer(2));
	assert(2 == freeList.size());
	freeList.destroy(new Integer(3));
	assert(3 == freeList.size());
}

void testIsEmpty() {
	executingTest("testIsEmpty()");

	FreeList<Integer> freeList;

	assert(freeList.isEmpty());
	freeList.destroy(new Integer(10));
	assert(!freeList.isEmpty());
	Integer* newInteger = freeList.create();
	assert(Integer(10).equals(newInteger));
	assert(freeList.isEmpty());
	freeList.destroy(newInteger);
}

void testFree() {
	executingTest("testFree()");

	FreeList<Integer> freeList;

	Integer* newInteger = freeList.create();
	assert(Integer(0).equals(newInteger));
	assert(freeList.isEmpty());
	freeList.destroy(newInteger);
	assert(!freeList.isEmpty());
	newInteger = freeList.create();
	assert(freeList.isEmpty());
}

void testAllocate() {
	executingTest("testAllocate()");

	int count = 10;
	Integer** items = new Integer*[count];

	FreeList<Integer> freeList(count);

	for (int i=0; i<count; i++) {
		items[i] = freeList.create();
	}
	assert(freeList.isEmpty());
	for (int i=0; i<count; i++) {
		freeList.destroy(items[i]);
	}
	assert(count == freeList.size());
}

void testPerformance() {
	executingTest("testPerformance()");

	int count = 10000;
	Integer** items = new Integer*[count];

	int trials = 1000;

	DEEP_LOG(INFO, OTHER, "Object Count: %d\n", count*trials);
	FreeList<Integer> freeList(count);
	long long start = System::nanoTime();
	for (int i=0; i<trials; i++) {
		for (int c=0; c<count; c++) {
			items[c] = freeList.create();
		}
		for (int c=0; c<count; c++) {
			freeList.destroy(items[c]);
		}
	}
	long long freeListNanos = System::nanoTime() - start;
	DEEP_LOG(INFO, OTHER, "FreeList nanos: %lld\n", freeListNanos);
	DEEP_LOG(INFO, OTHER, "FreeList Ops/sec: %lld\n", (2L * (long long) count * (long long) trials * 1000000000L)/freeListNanos);

	start = System::nanoTime();
	for (int i=0; i<trials; i++) {
		for (int c=0; c<count; c++) {
			items[c] = new Integer();
		}
		for (int c=0; c<count; c++) {
			delete items[c];
		}
	}
	long long newDeleteNanos = System::nanoTime() - start;
	DEEP_LOG(INFO, OTHER, "new/delete nanos: %lld\n", newDeleteNanos);
	DEEP_LOG(INFO, OTHER, "new/delete Ops/sec: %lld\n", (2L * (long long) count * (long long) trials * 1000000000L)/newDeleteNanos);

	DEEP_LOG(INFO, OTHER, "FreeList speedup over new/delete: %f\n", ((float)newDeleteNanos)/freeListNanos);
}

int main(int argc, char** argv) {
	testSize();
	testIsEmpty();
	testFree();
	testAllocate();
	testPerformance();
}
