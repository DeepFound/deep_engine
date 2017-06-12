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
#include "cxx/lang/nbyte.h"
#include "cxx/lang/Memory.h"

#include "cxx/util/Logger.h"

using namespace cxx::lang;
using namespace cxx::util;


nbyte* allocateMB(int mb) {
	DEEP_LOG(INFO, OTHER, "Allocate %d MB\n", mb);

	return new nbyte(mb * 1024 * 1024);
}

void printStats() {
	ulongtype application = Memory::getApplicationAllocatedBytes();
	ulongtype process = Memory::getProcessAllocatedBytes();
	ulongtype available = Memory::getAvailableBytes();
	ulongtype totalMemory = Memory::getAvailableResources();
	ulongtype rssMemory   = Memory::getRSS();

	DEEP_LOG(INFO, OTHER, "MEMORY (application): %llu MB\n", (application/1024)/1024);
	DEEP_LOG(INFO, OTHER, "MEMORY (process):     %llu MB\n", (process/1024)/1024);
	DEEP_LOG(INFO, OTHER, "MEMORY (available):   %llu MB\n", (available/1024)/1024);
	DEEP_LOG(INFO, OTHER, "MEMORY (total):       %llu MB\n", (totalMemory/1024)/1024);
	DEEP_LOG(INFO, OTHER, "MEMORY (rss):         %llu bytes\n", rssMemory);

	DEEP_LOG(INFO, OTHER, "\n");
}

int main(int argc, char** argv) {

	cxx::util::Logger::enableLevel(cxx::util::Logger::INFO);
	cxx::util::Logger::enableLevel(cxx::util::Logger::DEBUG);
	cxx::util::Logger::enableLevel(cxx::util::Logger::WARN);
	cxx::util::Logger::enableLevel(cxx::util::Logger::ERROR);

	nbyte* data5 = allocateMB(5);

	printStats();

	Memory::dump();

	delete data5;

	DEEP_LOG(INFO, OTHER, "Free 5 MB\n");

	printStats();

	nbyte* data2 = allocateMB(2);

	printStats();

	nbyte* data10 = allocateMB(10);

	printStats();

	delete data10;

	DEEP_LOG(INFO, OTHER, "Free 10 MB\n");

	printStats();

	delete data2;

	DEEP_LOG(INFO, OTHER, "Free 2 MB\n");

	printStats();

	DEEP_LOG(INFO, OTHER, "Release Available Bytes\n");

	Memory::releaseAvailableBytes();

	printStats();

	Memory::dump();

	return 0;
}
