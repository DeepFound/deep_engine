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
#ifndef CXX_UTIL_CONCURRENT_LOCKS_USERSPACELOCK_H_
#define CXX_UTIL_CONCURRENT_LOCKS_USERSPACELOCK_H_ 

#include <unistd.h>

#include "cxx/lang/RuntimeException.h"

#include "cxx/util/Logger.h"

#include "cxx/util/concurrent/locks/Lock.h"

namespace cxx { namespace util { namespace concurrent { namespace locks {

class UserSpaceLock /* : public Lock */ {

	private:
		#ifdef DEEP_DEBUG
		volatile bytetype m_lock;
		volatile pthread_t m_owner;
		#else
		volatile boolean m_lock;
		#endif

	public:
		inline UserSpaceLock(void):
			#ifdef DEEP_DEBUG
			// XXX: in some cases we allow unlocking from initial state, so don't error (see below)
			m_lock(-1),
			m_owner(0) {
			#else
			m_lock(false) {
			#endif
		}

		FORCE_INLINE void lock() {
			uinttype state = 1;
			#ifdef DEEP_DEBUG
			while (__sync_lock_test_and_set(&m_lock, 1) == 1) {
			#else
			while (__sync_lock_test_and_set(&m_lock, true)) {
			#endif
				Lock::yield(&state);
			}
			#ifdef DEEP_DEBUG
			m_owner = pthread_self();
			#endif
		}

		FORCE_INLINE boolean tryLock() {
			#ifdef DEEP_DEBUG
			if (__sync_lock_test_and_set(&m_lock, 1) == 1) {
				return false;

			} else {
				m_owner = pthread_self();
				return true;
			}
			#else
			return (__sync_lock_test_and_set(&m_lock, true) == 0);
			#endif
		}

		FORCE_INLINE void unlock() {
			// XXX: __sync_lock_release(&m_lock, false);

			#ifdef DEEP_DEBUG
			pthread_t owner = m_owner;
			bytetype lock = __sync_lock_test_and_set(&m_lock, 0);
			if ((lock == 1) && (pthread_equal(owner, pthread_self()) == 0)) {
				DEEP_LOG(ERROR, OTHER, "Invalid unlock, current thread %lu is not owner %lu\n", pthread_self(), owner);

				throw RuntimeException("Invalid unlock, current thread is not owner.");

			// XXX: in some cases we allow unlocking from initial state, so don't error here
			} else if ((lock == 0) /*|| (lock == -1)*/) { 
				DEEP_LOG(ERROR, OTHER, "Invalid unlock, already unlocked.\n");

				throw RuntimeException("Invalid unlock, already unlocked.");
			}
			#else
			(void) __sync_lock_test_and_set(&m_lock, false);
			#endif
		}
};

} } } } // namespace

#endif /*CXX_UTIL_CONCURRENT_LOCKS_USERSPACELOCK_H_*/
