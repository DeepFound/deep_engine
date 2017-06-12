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
#ifndef CXX_LANG_MEMORY_H_
#define CXX_LANG_MEMORY_H_

#include <stdio.h>
#include <sys/types.h>

#include "cxx/lang/nbyte.h"
#include "cxx/lang/types.h"

#include "cxx/lang/Runtime.h"

#ifdef TCMALLOC
#include <malloc.h>
#include "malloc_extension.h"
#endif // TCMALLOC

#ifdef JEMALLOC
#include "jemalloc.h"
#endif

namespace cxx { namespace lang {

class Memory {

	private:
		Memory() {
		}

	public:
		static ulongtype getApplicationAllocatedBytes() {
			size_t allocatedBytes = 0;

			#ifdef TCMALLOC
			MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &allocatedBytes);
			#endif

			#ifdef JEMALLOC
			ulongtype epoch = 1;
			size_t sz = sizeof(ulongtype);
			mallctl("epoch", &epoch, &sz, &epoch, sizeof(ulongtype));

			sz = sizeof(size_t);
			mallctl("stats.allocated", &allocatedBytes, &sz, NULL, 0);
			#endif

			#ifdef SYSALLOC
			allocatedBytes = getRSS();
			#endif

			return allocatedBytes;
		}

		static ulongtype getProcessAllocatedBytes() {
			size_t allocatedBytes = 0;

			#ifdef TCMALLOC
			MallocExtension::instance()->GetNumericProperty("deep.current_allocated_bytes", &allocatedBytes);
			#endif

			#ifdef JEMALLOC
			ulongtype epoch = 1;
			size_t sz = sizeof(ulongtype);
			mallctl("epoch", &epoch, &sz, &epoch, sizeof(ulongtype));

			sz = sizeof(size_t);
			mallctl("stats.active", &allocatedBytes, &sz, NULL, 0);
			#endif

			#ifdef SYSALLOC
			allocatedBytes = getRSS();
			#endif

			return allocatedBytes;
		}

		static ulongtype getAvailableBytes() {
			size_t freeBytes = 0;
			size_t unMappedBytes = 0;

			#ifdef TCMALLOC
			MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes", &freeBytes);
			//MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_unmapped_bytes", &unMappedBytes);
			#endif

			return freeBytes + unMappedBytes;
		}

		static ulongtype getFreeListBytes() {
			size_t freeListBytes = 0;
			#ifdef TCMALLOC
			MallocExtension::instance()->GetNumericProperty("deep.freelist_bytes", &freeListBytes);
			#endif

			#ifdef JEMALLOC
			freeListBytes = getProcessAllocatedBytes() - getApplicationAllocatedBytes();
			#endif

			#ifdef SYSALLOC
			freeListBytes = getProcessAllocatedBytes() - getApplicationAllocatedBytes();
			#endif

			return freeListBytes;
		}

		static void releaseAvailableBytes() {
			#ifdef TCMALLOC
			MallocExtension::instance()->ReleaseFreeMemory();
			#endif

			#ifdef JEMALLOC
			mallctl("arenas.purge", NULL, 0, NULL, 0);
			#endif
		}

		static void getStats(nbyte* buffer) {
			#ifdef TCMALLOC
			MallocExtension::instance()->GetStats((bytearray) *buffer, buffer->length);
			#endif
		}

		static void free(void) {
			#ifdef TCMALLOC
			MallocExtension::instance()->MarkThreadIdle();
			#endif

			#ifdef JEMALLOC
			mallctl("thread.tcache.flush", NULL, 0, NULL, 0);
			#endif
		}

		static boolean checkManagement(void) {
			boolean status = true;

			#if defined TCMALLOC || JEMALLOC
			if (getApplicationAllocatedBytes() == 0) {
				status = false;
			}
			#endif

			return status;
		}

		static boolean hasManagement(void) {
			#if defined TCMALLOC || JEMALLOC
			return true;
			#else
			return false;
			#endif
		}

		static const char* getManagementName(void) {
			#ifdef TCMALLOC
			return "tcmalloc";
			#elif defined JEMALLOC
			return "jemalloc";
			#elif defined SYSALLOC
			return "system";
			#else
			return "NONE";
			#endif
		}

		static void dump(boolean verbose = false) {
			#ifdef TCMALLOC
				if (verbose == true) {
					nbyte buffer(12000);
					Memory::getStats(&buffer);
					printf("%s\n\n", (bytearray) buffer);

				} else {
					malloc_stats();
				}
			#endif // TCMALLOC

			#ifdef JEMALLOC
			malloc_stats_print(NULL, NULL, "gbla");
			#endif

			#ifdef SYSALLOC
			//malloc_stats();
			#endif
		}

	public:
		static ulongtype getAvailableResources() {
			ulongtype available = Runtime::getRuntime()->totalMemory();
			return available;
		}

		static ulongtype getRSS() {
			ulongtype rss = Runtime::getRuntime()->getRSS();
			return rss;
		}

		static ulongtype getPageSize() {
			ulongtype page_size = Runtime::getRuntime()->getPageSize();
			return page_size;
		}
};

} } // namespace

#endif /*CXX_LANG_MEMORY_H_*/
