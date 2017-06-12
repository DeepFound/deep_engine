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

#include "cxx/util/ArrayList.h"
#include "cxx/util/Logger.h"
#include "cxx/util/NumberRangeSet.h"

using namespace cxx::lang;
using namespace cxx::util;

typedef ushorttype N;
typedef ubytetype BitSet;
typedef NumberRangeSet<N, BitSet> NumRangeSet;
typedef NumberRangeSet<N&, BitSet&, N> NumRefRangeSet;
typedef ArrayList<N> NumList;

const N MIN = 0;

static void printList(NumList& l) {
	Iterator<N>* iter = l.iterator();
	const char* sep = "";
	while(iter->hasNext()) {
		printf("%s%u", sep, iter->next());
		sep = " ";
	}
	printf("\n");
	delete iter;
}

static void testPermutation(NumList& permutation) {
	//printList(permutation);
	if (permutation.size() == 0) {
		return;
	}
	NumRangeSet set;

	N min = permutation.get(0);
	N max = min;
	for (inttype i=0; i<permutation.size(); ++i) {
		N elt = permutation.get(i);
		const boolean added = set.add(elt);
		if (added != set.contains(elt)) {
			abort();
		}

		if (elt > max) {
			max = elt;
		}
		if (elt < min) {
			min = elt;
		}
	}

	for (inttype i=0; i<permutation.size(); ++i) {
		N elt = permutation.get(i);
		const boolean shouldContain = (max >= elt) && ((max - elt) <= sizeof(BitSet)*8);
		//fprintf(stderr, "--- %u (%u)\n", elt, shouldContain);
		if (set.contains(elt) != shouldContain) {
			abort();
		}
	}

	TreeSet<N>* treeSet = set.toSet();
	Iterator<N>* iter = treeSet->iterator();
	while (iter->hasNext()) {
		N index = iter->next();
		
		if (set.contains(index) != true) {
			//fprintf(stderr, "\nBAD INDEX %d INDEX %d RANGE %d CONTAINS %d ", index, set.getIndex(), set.getRange(), set.contains(index)); 
			abort();
		}
	}

	delete iter;
	delete treeSet;

	if ((max+1) > max) {
		max = max+1;
	}
	if ((min-1) < min) {
		min = min-1;
	}
	for (N elt = min; elt <= max; ++elt) {
		const boolean setContains = set.contains(elt);
		const boolean listContains = permutation.indexOf(elt) != -1;
		if ((setContains == true) && (listContains == false)) {
			abort();
		}
		/* if (setContains != listContains) {
			fprintf(stderr, "%u (%u != %u)\n", elt, setContains, listContains);
		} */
	}
}

static void permute(NumList& source, NumList& dest, int i = -1) {
	NumList smaller;
	smaller.addAll(&source);

	if (i >= 0) {
		N e = smaller.remove(i);
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

static void testCombinations(NumList& l) {
	{
		NumList permutation;
		permute(l, permutation);
	}
	if (l.size() == 1) {
		return;
	}

	for (inttype i=0; i < l.size(); ++i) {
		NumList smaller;
		NumList permutation;
		for (inttype j=0; j<l.size(); ++j) {
			if (i != j) {
				smaller.add(l.get(j));
			}
		}
		permute(smaller, permutation);
		testCombinations(smaller);
	}
}

static void testPermutations() {
	NumList l;
	NumList permutation;
	
	for (int i=0; i<10; ++i) {
		l.add((N)i);
	}

	testCombinations(l);
}

int main(int argc, char** argv) {
	testPermutations();

	return 0;
}
