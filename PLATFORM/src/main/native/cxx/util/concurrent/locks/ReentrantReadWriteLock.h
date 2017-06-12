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
#ifndef CXX_UTIL_CONCURRENT_LOCKS_REENTRANTREADWRITELOCK_H_
#define CXX_UTIL_CONCURRENT_LOCKS_REENTRANTREADWRITELOCK_H_ 

#include <pthread.h>
#include <errno.h>

#include "cxx/lang/RuntimeException.h"
#include "cxx/util/Logger.h"
#include "cxx/util/concurrent/locks/ReadWriteLock.h"

namespace cxx { namespace util { namespace concurrent { namespace locks {

class ReentrantReadWriteLock : public ReadWriteLock {

	class ReadLock : public Lock {

		private:
			pthread_rwlock_t* m_rwlock;

		public:
			ReadLock(pthread_rwlock_t* rwlock):
				m_rwlock(rwlock) {
			}

			FORCE_INLINE void lock() {
				pthread_rwlock_rdlock(m_rwlock);
			}

			FORCE_INLINE boolean tryLock(void) {
				return (pthread_rwlock_tryrdlock(m_rwlock) == 0);
			}

			FORCE_INLINE void unlock() {
				pthread_rwlock_unlock(m_rwlock);
			}
	};

	class WriteLock : public Lock {

		private:
			pthread_t m_owner;
			pthread_rwlock_t* m_rwlock;

			uinttype m_count;

			FORCE_INLINE boolean isOwner() {
				pthread_t owner = pthread_self();
				return pthread_equal(m_owner, owner) != 0;
			}

			FORCE_INLINE void assertOwner() {
				#ifdef DEEP_DEBUG
				if (isOwner() == false) {
					DEEP_LOG(ERROR, OTHER, "The current thread does not own the write lock.\n");
					throw RuntimeException("The current thread does not own the write lock.");
				}
				#endif
			}

		public:
			WriteLock(pthread_rwlock_t* rwlock):
				m_owner((pthread_t)null), // TODO: use a more portable null
				m_rwlock(rwlock),
				m_count(0) {
			}

			FORCE_INLINE void lock() {
				switch (pthread_rwlock_wrlock(m_rwlock)) {
					case EDEADLK:
						assertOwner();
						++m_count;
						return;
					case 0:
						m_owner = pthread_self();
						++m_count;
						return;
					default:
						return;
				}
			}

			FORCE_INLINE boolean tryLock(void) {
				switch (pthread_rwlock_trywrlock(m_rwlock)) {
					case EDEADLK:
						assertOwner();
						++m_count;
						return true;
					case 0:
						m_owner = pthread_self();
						++m_count;
						return true;
					default:
						return false;
				}
			}

			// XXX: calling unlock() without holding the lock will corrupt the lock state
			FORCE_INLINE void unlock() {
				assertOwner();
				if (--m_count == 0) {
					m_owner = (pthread_t)null; // TODO: use a more portable null
					pthread_rwlock_unlock(m_rwlock);
				}
			}
	};

	private:
		pthread_rwlock_t m_rwlock;

		ReadLock m_rlock;
		WriteLock m_wlock;

	public:
		ReentrantReadWriteLock(void):
			m_rlock(&m_rwlock),
			m_wlock(&m_rwlock) {

			pthread_rwlock_init(&m_rwlock, null);
		}

		virtual ~ReentrantReadWriteLock(void) {
			pthread_rwlock_destroy(&m_rwlock);
		}

		virtual Lock* readLock(void) {
			return &m_rlock;
		}

		virtual Lock* writeLock(void) {
			return &m_wlock;
		}
};

} } } } // namespace

#endif /*CXX_UTIL_CONCURRENT_LOCKS_REENTRANTREADWRITELOCK_H_*/
