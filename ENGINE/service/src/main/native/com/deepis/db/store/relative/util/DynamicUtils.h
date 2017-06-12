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
#ifndef CXX_LANG_DYNAMIC_RESOURCE_H_
#define CXX_LANG_DYNAMIC_RESOURCE_H_

#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <fstream>
#include <sys/types.h>
#include <sys/resource.h>

#include "cxx/lang/nbyte.h"
#include "cxx/lang/types.h"
#include "com/deepis/db/store/relative/core/Properties.h"

namespace cxx { namespace lang {

class DynamicUtils {

 private:
	DynamicUtils() {
	}

	static DynamicUtils* m_cDynamicResourceInstance;

 public:

	// XXX: Ensure we cleanup the global static instance of this DynamicResource Class.
	virtual ~DynamicUtils() {
		if ((null != DynamicUtils::m_cDynamicResourceInstance) && (DynamicUtils::m_cDynamicResourceInstance == this)) {
			DynamicUtils::m_cDynamicResourceInstance = null;
		}
	}

	// XXX: Returns the singleton instance for the DynamicResource object.
	static DynamicUtils* getInstance() {
		if (null == DynamicUtils::m_cDynamicResourceInstance) {
			DynamicUtils::m_cDynamicResourceInstance = new DynamicUtils();
		}

		return DynamicUtils::m_cDynamicResourceInstance;
	}

	inttype checkForProcessorChange(void) {
		inttype iCount = envOverride<inttype>("DEEP_CORES", Runtime::getRuntime()->availableProcessors());

		/* XXX: Function converts cores into a thread count that has a bounded range. */
		inttype availWorkThreads = iCount >> 1;

		if (Properties::DEFAULT_CACHE_WORK_THREADS_MIN > availWorkThreads) {
			availWorkThreads = Properties::DEFAULT_CACHE_WORK_THREADS_MIN;

		} else if (Properties::DEFAULT_CACHE_WORK_THREADS_MAX < availWorkThreads) {
			availWorkThreads = Properties::DEFAULT_CACHE_WORK_THREADS_MAX;
		}

		return availWorkThreads;
	}

	void setThreadChanges(inttype availWorkThreads) {
		inttype availReOrgThreads = availWorkThreads >> 1;

		DEEP_LOG(INFO, OTHER, "Dynamic Resource background tasks from %d to %d\n", Properties::getWorkThreads(), availWorkThreads);

		Properties::setWorkThreads(availWorkThreads);
		Properties::setReorgThreads(availReOrgThreads);
	}

	void checkForMemoryChange() {
		if (false == Properties::getDynamicResources()) {
			return;
		}

		ulongtype cacheSizeActual    = Properties::getCacheSizeActual();
		ulongtype allAvailableMemory = envOverride<ulongtype>("DEEP_CACHE", Memory::getAvailableResources());

		allAvailableMemory = allAvailableMemory * Properties::DEFAULT_CACHE_PERCENT_DEFAULT;

		/* XXX: If dynamic resources is configured. */
		if (cacheSizeActual < allAvailableMemory) {
			DEEP_LOG(INFO, CACHE, "Dynamic Resource memory from %llu Bytes to %llu Bytes\n", cacheSizeActual, allAvailableMemory);

			Properties::setCacheSize(allAvailableMemory);

		} else if (cacheSizeActual != allAvailableMemory) {
			/* XXX: Dynamic downscaling of memory is not currently supported. */
		}
	}

	static void setDynamicResources(bool dynamic) {
		if (dynamic == Properties::getDynamicResources()) {
			return;
		}

		Properties::setDynamicResources(dynamic);

		if (false == Properties::getDynamicResources()) {
			return;
		}
	}

	static bool getDynamicResources() {
		return Properties::getDynamicResources();
	}

 private:

	#ifdef DEEP_DEBUG
	FORCE_INLINE static bool checkForFile(char* pPath) {
		struct stat buffer;
		return (stat(pPath, &buffer) == 0);
	}
	#endif

	template<typename T>
	FORCE_INLINE static T envOverride(const char* pEnv, T iDefault) {
		T     iRetVal = iDefault;
		#ifdef DEEP_DEBUG
		char* pPath   = getenv(pEnv);

		if ((0 != pPath) && (false != checkForFile(pPath))) {
			ifstream valFile(pPath);
			valFile.exceptions(std::ifstream::badbit | std::ifstream::eofbit);
			
			try {
				valFile >> iRetVal;
			}
			catch (std::ifstream::failure e) {
				DEEP_LOG(ERROR, OTHER, "Failed to get value from %s -> %s\n", pEnv, pPath);
				iRetVal = iDefault;
			}
		}
		#endif
		return iRetVal;
	}

};

DynamicUtils* DynamicUtils::m_cDynamicResourceInstance = null;

} } // namespace

#endif /*CXX_LANG_DYNAMIC_RESOURCE_H_*/
