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
#ifndef CXX_LANG_RUNTIME_H_
#define CXX_LANG_RUNTIME_H_

#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <sys/resource.h>

#include "cxx/lang/nbyte.h"
#include "cxx/lang/types.h"

namespace cxx { namespace lang {

class Runtime {

 private:
	Runtime() {
	}

	static Runtime* m_cRuntimeInstance;

 public:

	// Ensure we cleanup the global static instance of this Runtime Class.
	virtual ~Runtime() {
		if ((0 != Runtime::m_cRuntimeInstance) && (Runtime::m_cRuntimeInstance == this)) {
			Runtime::m_cRuntimeInstance = null;
		}
	}

	// Returns the singleton instance for the Runtime object.
	static Runtime* getRuntime() {
		if (null == Runtime::m_cRuntimeInstance) {
			Runtime::m_cRuntimeInstance = new Runtime();
		}

		return Runtime::m_cRuntimeInstance;
	}

	// Returns the number of processing cores.
	static inttype availableProcessors() {
		errno = 0;
		inttype procCount = sysconf(_SC_NPROCESSORS_ONLN);
		return procCount;
	}

	// Returns the amount of free memory in Bytes.
	static ulongtype freeMemory() {
		ulongtype pages     = sysconf(_SC_AVPHYS_PAGES);
		ulongtype page_size = getPageSize();
		return pages * page_size;
	}

	// Returns the total amount of memory in Bytes.
	static ulongtype totalMemory() {
		errno = 0;
		ulongtype pages     = sysconf(_SC_PHYS_PAGES);
		ulongtype page_size = getPageSize();
		return pages * page_size;
	}

	static ulongtype getPageSize() {
		errno = 0;
		ulongtype page_size = sysconf(_SC_PAGE_SIZE);
		return page_size;
	}

	static ulongtype getRSS() {
		struct rusage rss;
		memset(&rss, 0, sizeof(struct rusage));

		errno = 0;

		inttype status = getrusage(RUSAGE_SELF, &rss);
		if (-1 == status) {
			return 0;
		}

		return rss.ru_maxrss * 1024;
	}

}; // Runtime

Runtime* Runtime::m_cRuntimeInstance = null;

} } // namespace

#endif /*CXX_LANG_RUNTIME_H_*/
