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
#include "cxx/lang/String.h"
#include "cxx/lang/System.h"
#include "cxx/lang/Thread.h"
#include "cxx/lang/Runnable.h"
#include "cxx/lang/ThreadLocal.h"

#include "cxx/util/Logger.h"
#include "cxx/util/concurrent/Synchronize.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent;

class TestThreadAndSync : public Synchronizable, public Runnable {

	public:
		TestThreadAndSync() {
			DEEP_LOG(INFO, OTHER, "Test Sync - construct\n");
		}

		virtual ~TestThreadAndSync() {
			DEEP_LOG(INFO, OTHER, "Test Sync - destruct\n");
		}

		virtual void run() {
			DEEP_LOG(INFO, OTHER, "Test Sync - run\n");
		}
};

int main(int argc, char** argv) {

	// XXX: default thread local deletes content
	ThreadLocal<String*> local;
	String* value = new String("Hello World");
	local.set(value);
	DEEP_LOG(INFO, OTHER, "LOCAL DATA: %s\n", local.get()->data());
	DEEP_LOG(INFO, OTHER, "\n");

	TestThreadAndSync testThreadAndSync;
	Thread testThread(&testThreadAndSync);
	DEEP_LOG(INFO, OTHER, "ABOUT TO START THREAD\n");
	testThread.start();
	Thread::sleep(1000);
	DEEP_LOG(INFO, OTHER, "\n");

	#ifdef DEEP_DEBUG
	try {
		DEEP_LOG(INFO, OTHER, "ABOUT TO WAIT 1.0 SECONDS out-side ( synchronized )\n");
		testThreadAndSync.wait(1000);

		DEEP_LOG(ERROR, OTHER, "FAILURE\n");
		exit(-1);

	} catch (Throwable t) {
		DEEP_LOG(INFO, OTHER, "SUCCESS\n");
	}
	#endif

	synchronized(testThreadAndSync) {
		DEEP_LOG(INFO, OTHER, "ABOUT TO WAIT 1.0 SECONDS\n");
		long long t1 = System::currentTimeMillis();
		testThreadAndSync.wait(1000);
		long long t2 = System::currentTimeMillis();
		DEEP_LOG(INFO, OTHER, "DONE: %lld\n", (t2-t1));

		DEEP_LOG(INFO, OTHER, "ABOUT TO WAIT 2.0 SECONDS\n");
		long long t3 = System::currentTimeMillis();
		testThreadAndSync.wait(2000);
		long long t4 = System::currentTimeMillis();
		DEEP_LOG(INFO, OTHER, "DONE: %lld\n", (t4-t3));

		DEEP_LOG(INFO, OTHER, "ABOUT TO WAIT 3.0 SECONDS\n");
		long long t5 = System::currentTimeMillis();
		testThreadAndSync.wait(3000);
		long long t6 = System::currentTimeMillis();
		DEEP_LOG(INFO, OTHER, "DONE: %lld\n", (t6-t5));
	}

	return 0;
}
