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
#ifndef CXX_UTIL_CONCURRENT_LOCKS_REENTRANTLOCK_H_
#define CXX_UTIL_CONCURRENT_LOCKS_REENTRANTLOCK_H_ 

#include <pthread.h>

#include "cxx/util/concurrent/locks/Lock.h"

namespace cxx { namespace util { namespace concurrent { namespace locks {

class ReentrantLock : public Lock {

	private:
		pthread_mutex_t m_mutex;

	public:
		inline ReentrantLock(boolean reentrant = true) { 

			pthread_mutexattr_t attr;
			pthread_mutexattr_init(&attr);

			pthread_mutexattr_settype(&attr, reentrant ? PTHREAD_MUTEX_RECURSIVE : PTHREAD_MUTEX_NORMAL);
			pthread_mutex_init(&m_mutex, &attr);

			pthread_mutexattr_destroy(&attr);
		}

		virtual ~ReentrantLock(void) {
			pthread_mutex_destroy(&m_mutex);
		}

		FORCE_INLINE void lock() {
			pthread_mutex_lock(&m_mutex);
		}

		FORCE_INLINE boolean tryLock(void) {
			return (pthread_mutex_trylock(&m_mutex) == 0);
		}

		FORCE_INLINE void unlock() {
			pthread_mutex_unlock(&m_mutex);
		}

};

} } } } // namespace

#endif /*CXX_UTIL_CONCURRENT_LOCKS_REENTRANTLOCK_H_*/
