#include  <signal.h>
#include  <sys/wait.h>

#include "cxx/lang/Thread.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"

#include "cxx/util/CommandLineOptions.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.cxx"
#include "com/deepis/db/store/relative/util/Versions.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::core;

int TEST_RUNS = 3;

Comparator<longtype> COMPARATOR;
Segment<longtype> SEGMENT(&COMPARATOR, 23,Versions::GET_PROTOCOL_CURRENT());

HashMap<ushorttype,longtype> stats;

void calculateStats(inttype vsize = 1500);

void printFileRange() {
	printf("FILE RANGE: ");

	for (int i=Properties::DEFAULT_SEGMENT_FILE_RANGE-1; i>=0; i--) {
		if (SEGMENT.getPagingRange() & (1 << i)) {
			printf("1");

		} else {
			printf("0");
		}
	}

	printf( "   ----> (");
	for (int i=Properties::DEFAULT_SEGMENT_FILE_RANGE-1; i>=0; i--) {
		if (SEGMENT.getPagingRange() & (1 << i)) {
			printf("%u ", SEGMENT.getCurrentPagingIndex() - i - 1);
		}
	}


	printf(") INDEX: %u\n\n", SEGMENT.getCurrentPagingIndex());
}

int main(int argc, char** argv) {
	for (ushorttype offset = 0; offset < (TEST_RUNS * Properties::DEFAULT_SEGMENT_FILE_RANGE); offset += (Properties::DEFAULT_SEGMENT_FILE_RANGE+1)) {
		SEGMENT.setPaged(true);

		ushorttype index0 = offset;
		ushorttype index1 = offset + 1;
		ushorttype index2 = offset + 2;
		ushorttype index3 = offset + 3;
		ushorttype index4 = offset + 4;
		ushorttype index5 = offset + 5;
		ushorttype index6 = offset + 6;
		ushorttype index7 = offset + 7;
		ushorttype index8 = offset + 8;
		ushorttype index9 = offset + 9;

		//
		// NOTE: SEGMENT.hasPagingIndex(0) after a SEGMENT.resetPagingIndex will product false positive
		// although false positives are ok since they are inclusive
		//
		if (//(SEGMENT.hasPagingIndex(index0) == true)  ||
			(SEGMENT.hasPagingIndex(index1) == true)  ||
			(SEGMENT.hasPagingIndex(index2) == true)  ||
			(SEGMENT.hasPagingIndex(index3) == true)  ||
			(SEGMENT.hasPagingIndex(index4) == true)  ||
			(SEGMENT.hasPagingIndex(index5) == true)  ||
			(SEGMENT.hasPagingIndex(index6) == true)  ||
			(SEGMENT.hasPagingIndex(index7) == true)  ||
			(SEGMENT.hasPagingIndex(index8) == true)  ||
			(SEGMENT.hasPagingIndex(index9) == true)) {
			printf("Failed SEGMENT.hasPagingIndex");
			exit(1);
		}

		printf("Set  FILE INDEX: %d\n", index0);
		//		SEGMENT.addPagingIndex(index0);
		SEGMENT.addPagingIndex(index0);

		if ((SEGMENT.hasPagingIndex(index0) == false) ||
			(SEGMENT.hasPagingIndex(index1) == true)  ||
			(SEGMENT.hasPagingIndex(index2) == true)  ||
			(SEGMENT.hasPagingIndex(index3) == true)  ||
			(SEGMENT.hasPagingIndex(index4) == true)  ||
			(SEGMENT.hasPagingIndex(index5) == true)  ||
			(SEGMENT.hasPagingIndex(index6) == true)  ||
			(SEGMENT.hasPagingIndex(index7) == true)  ||
			(SEGMENT.hasPagingIndex(index8) == true)  ||
			(SEGMENT.hasPagingIndex(index9) == true)) {
			printf("Failed SEGMENT.hasPagingIndex");
			exit(1);
		}

		printFileRange();

		calculateStats();

		boolean status = SEGMENT.validPagingRange(index1);
		if (status == true) {
			printf("Set  FILE INDEX: %d\n", index1);
			SEGMENT.addPagingIndex(index1);

		} else {
			printf("Failed %d\n", index1);
			exit(1);
		}

		if ((SEGMENT.hasPagingIndex(index0) == false) ||
			(SEGMENT.hasPagingIndex(index1) == false) ||
			(SEGMENT.hasPagingIndex(index2) == true)  ||
			(SEGMENT.hasPagingIndex(index3) == true)  ||
			(SEGMENT.hasPagingIndex(index4) == true)  ||
			(SEGMENT.hasPagingIndex(index5) == true)  ||
			(SEGMENT.hasPagingIndex(index6) == true)  ||
			(SEGMENT.hasPagingIndex(index7) == true)  ||
			(SEGMENT.hasPagingIndex(index8) == true)  ||
			(SEGMENT.hasPagingIndex(index9) == true)) {
			printf("Failed SEGMENT.hasPagingIndex");
			exit(1);
		}

		printFileRange();

		calculateStats();

		status = SEGMENT.validPagingRange(index2);
		if (status == false) {
			printf("Failed %d\n", index2);
			exit(1);

		} else {
			printf("Skip FILE INDEX: %d\n", index2);
		}

		status = SEGMENT.validPagingRange(index3);
		if (status == true) {
			printf("Set  FILE INDEX: %d\n", index3);
			SEGMENT.addPagingIndex(index3);
			SEGMENT.addPagingIndex(index3);
			SEGMENT.addPagingIndex(index3);

		} else {
			printf("Failed %d\n", index3);
			exit(1);
		}

		printFileRange();

		calculateStats();

		if ((SEGMENT.hasPagingIndex(index0) == false) ||
			(SEGMENT.hasPagingIndex(index1) == false) ||
			(SEGMENT.hasPagingIndex(index2) == true)  ||
			(SEGMENT.hasPagingIndex(index3) == false) ||
			(SEGMENT.hasPagingIndex(index4) == true)  ||
			(SEGMENT.hasPagingIndex(index5) == true)  ||
			(SEGMENT.hasPagingIndex(index6) == true)  ||
			(SEGMENT.hasPagingIndex(index7) == true)  ||
			(SEGMENT.hasPagingIndex(index8) == true)  ||
			(SEGMENT.hasPagingIndex(index9) == true)) {
			printf("Failed SEGMENT.hasPagingIndex");
			exit(1);
		}

		printf("Skip FILE INDEX: %d\n", index4);
		printf("Skip FILE INDEX: %d\n", index5);

		status = SEGMENT.validPagingRange(index6);
		if (status == true) {
			printf("Set  FILE INDEX: %d\n", index6);
			SEGMENT.addPagingIndex(index6);
			SEGMENT.addPagingIndex(index6);

		} else {
			printf("Failed %d\n", index6);
			exit(1);
		}

		printFileRange();

		calculateStats();

		status = SEGMENT.validPagingRange(index7);
		if (status == false) {
			printf("Failed %d\n", index7);
			exit(1);
		}

		printf("Skip FILE INDEX: %d\n", index7);

		if ((SEGMENT.hasPagingIndex(index0) == false) ||
			(SEGMENT.hasPagingIndex(index1) == false) ||
			(SEGMENT.hasPagingIndex(index2) == true)  ||
			(SEGMENT.hasPagingIndex(index3) == false) ||
			(SEGMENT.hasPagingIndex(index4) == true)  ||
			(SEGMENT.hasPagingIndex(index5) == true)  ||
			(SEGMENT.hasPagingIndex(index6) == false) ||
			(SEGMENT.hasPagingIndex(index7) == true)  ||
			(SEGMENT.hasPagingIndex(index8) == true)  ||
			(SEGMENT.hasPagingIndex(index9) == true)) {
			printf("Failed SEGMENT.hasPagingIndex");
			exit(1);
		}

		status = SEGMENT.validPagingRange(index8);
		if (status == true) {
			printf("Set  FILE INDEX: %d\n", index8);
			SEGMENT.addPagingIndex(index8);
			SEGMENT.addPagingIndex(index8);

		} else {
			printf("Failed %d\n", index8);
			exit(1);
		}

		if ((SEGMENT.hasPagingIndex(index0) == false) ||
			(SEGMENT.hasPagingIndex(index1) == false) ||
			(SEGMENT.hasPagingIndex(index2) == true)  ||
			(SEGMENT.hasPagingIndex(index3) == false) ||
			(SEGMENT.hasPagingIndex(index4) == true)  ||
			(SEGMENT.hasPagingIndex(index5) == true)  ||
			(SEGMENT.hasPagingIndex(index6) == false) ||
			(SEGMENT.hasPagingIndex(index7) == true)  ||
			(SEGMENT.hasPagingIndex(index8) == false) ||
			(SEGMENT.hasPagingIndex(index9) == true)) {
			printf("Failed SEGMENT.hasPagingIndex");
			exit(1);
		}

		printFileRange();

		calculateStats();

		status = SEGMENT.validPagingRange(index9);
		if (status == true) {
			printf("Failed %d\n", index9);
			exit(1);

		} else {
			SEGMENT.resetPagingIndexes();
		}
	}

	printf("\nSUCCESS\n\n");

	return 0;
}

void calculateStats(inttype vsize) {
	inttype files = 1;
	if (SEGMENT.getPagingRange() != 0) {
		ubytetype fileRange = SEGMENT.getPagingRange();
		for (int i = Properties::DEFAULT_SEGMENT_FILE_RANGE - 1; i >= 0; i--) {
			if (fileRange & (1 << i)) {
				files++;
			}
		}
	}

//	printf("  --> Stats files : %d\n", files);

	inttype block = vsize / files;

	longtype stat = 0;
	if (SEGMENT.getPagingRange() != 0) {
		ubytetype fileRange = SEGMENT.getPagingRange();
		for (int i = Properties::DEFAULT_SEGMENT_FILE_RANGE - 1; i >= 0; i--) {
			if (fileRange & (1 << i)) {
				ushorttype fileIndex = SEGMENT.getCurrentPagingIndex() - i - 1;
				stat = stats.get(fileIndex) + block;

//				printf("  --> Stats put index : %u, stat: %lld\n", fileIndex, stat);

				stats.put(fileIndex, stat);
			}
		}
	}

	// from above, place here for readability of output
	stat = stats.get(SEGMENT.getCurrentPagingIndex()) + block;
	stats.put(SEGMENT.getCurrentPagingIndex(), stat);
//	printf("  --> Stats put index : %u, stat: %lld\n", SEGMENT.getCurrentPagingIndex(), stat);

//	printf("\n");
}
