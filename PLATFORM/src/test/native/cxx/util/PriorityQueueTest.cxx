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
#include <stdlib.h>
#include <assert.h>

#include "cxx/util/Collections.h"
#include "cxx/util/Logger.h"
#include "cxx/util/PriorityQueue.h"

using namespace cxx::lang;
using namespace cxx::util;

#define RMAX 20000

void printList(Iterator<longtype>* iter) {
	const char* sep = "";
	while(iter->hasNext()) {
		printf("%s%lld", sep, iter->next());
		sep = " ";
	}
	printf("\n");
	delete iter;
}

static void testPermutation(ArrayList<longtype>& permutation) {
	Comparator<longtype> cmp;
	ArrayList<longtype> sorted;
	sorted.addAll(&permutation);
	Collections::sort<longtype>(&sorted);

	PriorityQueue<longtype> queue(&cmp);
	queue.addAll(&permutation);

	for (int i=0; i<sorted.size(); ++i) {
		longtype e = queue.remove();
		if (e != sorted.get(i)) {
			abort();
		}
	}
}

static void permute(ArrayList<longtype>& source, ArrayList<longtype>& dest, int i = -1) {
	ArrayList<longtype> smaller;
	smaller.addAll(&source);
	
	if (i >= 0) {
		longtype e = smaller.remove(i);
		dest.add(e);
	}

	if (smaller.isEmpty()) {
		testPermutation(dest);
	}

	for (int j=0; j<smaller.size(); ++j) {
		permute(smaller, dest, j);
	}

	if (i >= 0) {
		dest.remove(dest.size()-1);
	}
}

static void testPermutations() {
	ArrayList<longtype> l;
	ArrayList<longtype> permutation;
	
	for (int i=0; i<5; ++i) {
		l.add((longtype)i);
		permute(l, permutation);
	}
}

static void compare(PriorityQueue<longtype>& queue, ArrayList<longtype>& list, longtype r) {
	if (queue.size() == 0 && list.size() == 0) {
		return;
	}
	
	Collections::sort<longtype>(&list);
	if (queue.get(0) != list.get(0)) {
		abort();
	}
}

static void testRandom() {
	Comparator<longtype> cmp;
	PriorityQueue<longtype> queue(&cmp);
	ArrayList<longtype> list;

	ArrayList<longtype> log(10000);

	long i = 0;
	do {
		ArrayList<longtype> bList;
		bList.addAll(&list);
		ArrayList<longtype> bQueue;
		bQueue.addAll(&queue.array());

		long r = rand() % RMAX;
		if (r < i) {
			queue.remove();
			list.remove(0);
			r = -1;
		} else {
			queue.add(r);
			list.add(r);
		}
		log.add(r);
		compare(queue,list,r);
		++i;
	} while (list.size() > 0);

	if (queue.size() != list.size()) {
		abort();
	}
}

int main(int argc, char** argv) {
	testPermutations();
	testRandom();
}
