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
#ifndef CXX_UTIL_CONCURRENT_LOCKS_LOCK_H_
#define CXX_UTIL_CONCURRENT_LOCKS_LOCK_H_ 

#include <unistd.h>
#include <pthread.h>

#include "cxx/lang/Object.h"

#include "cxx/util/concurrent/Lockable.h"

using namespace cxx::lang;
using namespace cxx::util::concurrent;

namespace cxx { namespace util { namespace concurrent { namespace locks {

class Lock : public Object, public Lockable {

	private:
		static uinttype s_pause;
		static uinttype s_yield;

	public:
		static const uinttype SLEEP = 1000;

		static const uinttype VIRTUAL_PAUSE = 1000;
		static const uinttype VIRTUAL_YIELD = 10000;

		static const uinttype PHYSICAL_PAUSE = 10;
		static const uinttype PHYSICAL_YIELD = 1000;

	public:
		FORCE_INLINE static void setPause(uinttype pause) {
			s_pause = pause;
		}

		FORCE_INLINE static uinttype getPause(void) {
			return s_pause;
		}

		FORCE_INLINE static void setYield(uinttype yield) {
			s_yield = yield;
		}

		FORCE_INLINE static uinttype getYield(void) {
			return s_yield;
		}

		FORCE_INLINE static void schedule() {
			pthread_yield();
		}

		FORCE_INLINE static void yield(uinttype* state) {
			if ((++(*state) % s_pause) == 0) {

				/* XXX: schedule out, but wait a bit */
				if (*state == s_yield) {
					::usleep(SLEEP);
					*state = 1;

				/* XXX: schedule out, but come right back */
				} else {
					schedule();
				}

			} else {
				__asm volatile ("pause");
			}
		}

	public:
		virtual ~Lock() {
		}
};

} } } } // namespace

#endif /*CXX_UTIL_CONCURRENT_LOCKS_LOCK_H_*/
