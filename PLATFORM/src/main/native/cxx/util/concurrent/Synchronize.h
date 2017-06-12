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
#ifndef CXX_LANG_SYNCHRONIZE_H_
#define CXX_LANG_SYNCHRONIZE_H_ 

#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/timeb.h>

#include "cxx/lang/Object.h"
#include "cxx/lang/Thread.h"

#ifdef DEEP_DEBUG
#include "cxx/lang/IllegalMonitorStateException.h"
#endif

#include "cxx/util/concurrent/Lockable.h"

using namespace cxx::lang;

namespace cxx { namespace util { namespace concurrent {

class Synchronizable : public Object, public Lockable {

	private:
		struct timeb m_tb;
		struct timespec m_ts;

		#ifdef DEEP_DEBUG
		pthread_t m_owner;
		#endif
		pthread_cond_t m_cond;
		pthread_mutex_t m_mutex;

	private:
		#ifdef DEEP_DEBUG
		FORCE_INLINE void validate() {
			if (m_owner != pthread_self()) {
				throw IllegalMonitorStateException(); 
			}
		}
		#endif

		FORCE_INLINE boolean tryLock(void) {
			// XXX: disallow by making private
			return false;
		}

	public:
		void lock() {
			pthread_mutex_lock(&m_mutex);
			#ifdef DEEP_DEBUG
			m_owner = pthread_self();
			#endif
		}

		void unlock() {
			#ifdef DEEP_DEBUG
				m_owner = -1;
			#endif

			pthread_mutex_unlock(&m_mutex);
		}

	public:
		Synchronizable(void) {
			#ifdef DEEP_DEBUG
				m_owner = -1;
			#endif

			pthread_cond_init(&m_cond, 0);

			pthread_mutexattr_t attr;
			pthread_mutexattr_init(&attr);

			pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
			pthread_mutex_init(&m_mutex, &attr);
		}

		virtual ~Synchronizable(void) {
			pthread_mutex_destroy(&m_mutex);
			pthread_cond_destroy(&m_cond);
		}

		FORCE_INLINE void notify(void) {
			#ifdef DEEP_DEBUG
			validate();
			#endif

			pthread_cond_signal(&m_cond);
		}

		FORCE_INLINE void notifyAll(void) {
			#ifdef DEEP_DEBUG
			validate();
			#endif

			pthread_cond_broadcast(&m_cond);
		}

		FORCE_INLINE boolean wait(void) {
			#ifdef DEEP_DEBUG
			validate();
			#endif

			const boolean ret = (pthread_cond_wait(&m_cond, &m_mutex) == 0);
				#ifdef DEEP_DEBUG
				if (ret == true) {
					m_owner = pthread_self();
				}
				#endif
			return ret;
		}

		FORCE_INLINE boolean wait(longtype timeout) {
			#ifdef DEEP_DEBUG
			validate();
			#endif

			#ifdef COM_DEEPIS_REALTIME_CLOCK
			clock_gettime(CLOCK_REALTIME, &m_ts);
			m_ts.tv_sec += timeout / 1000;
			m_ts.tv_nsec += (timeout % 1000) * 1000000;
			if (m_ts.tv_nsec > 1000000000) {
				m_ts.tv_nsec -= 1000000000;
				++m_ts.tv_sec;
			}
			#else
			ftime(&m_tb);
			m_tb.time += timeout / 1000;
			m_tb.millitm += timeout % 1000;
			if (m_tb.millitm > 999) {
				m_tb.millitm -= 1000;
				m_tb.time++;
			}
			m_ts.tv_sec = m_tb.time;
			m_ts.tv_nsec = m_tb.millitm * 1000000;
			#endif

			return (pthread_cond_timedwait(&m_cond, &m_mutex, &m_ts) == 0);
		}

	friend class StackBlock;
};

class SynchronizableSafe : public Object, public Lockable {

	private:
		struct timeb m_tb;
		struct timespec m_ts;

		#ifdef DEEP_DEBUG
		pthread_t m_owner;
		#endif
		pthread_cond_t m_cond;
		pthread_mutex_t m_mutex;

		static const int WAIT = 50;
		static const pthread_t TID_NONE = -1;

	private:
		#ifdef DEEP_DEBUG
		FORCE_INLINE void testAndSetOwner(void) {
			pthread_t tid = pthread_self();
			if (__sync_bool_compare_and_swap(&m_owner, TID_NONE, tid) == false) {
				throw IllegalMonitorStateException();
			}
		}

		FORCE_INLINE void testAndClearOwner(void) {
			pthread_t tid = pthread_self();
			if (__sync_bool_compare_and_swap(&m_owner, tid, TID_NONE) == false) {
				throw IllegalMonitorStateException();
			}
		}

		FORCE_INLINE void validate(void) {
			if (m_owner != pthread_self()) {
				throw IllegalMonitorStateException(); 
			}
		}
		#endif

		FORCE_INLINE boolean tryLock(void) {
			// XXX: disallow by making private
			return false;
		}

	public:
		void lock() {
			pthread_mutex_lock(&m_mutex);

			#ifdef DEEP_DEBUG
			testAndSetOwner();
			#endif
		}

		void unlock() {
			#ifdef DEEP_DEBUG
			testAndClearOwner();
			#endif

			pthread_mutex_unlock(&m_mutex);
		}

	public:
		SynchronizableSafe(void) {
			#ifdef DEEP_DEBUG
			m_owner = TID_NONE;
			#endif

			pthread_cond_init(&m_cond, 0);

			pthread_mutexattr_t attr;
			pthread_mutexattr_init(&attr);

			pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
			pthread_mutex_init(&m_mutex, &attr);
		}

		virtual ~SynchronizableSafe(void) {
			pthread_cond_destroy(&m_cond);
			pthread_mutex_destroy(&m_mutex);
		}

		virtual boolean safeToDestroy(void) = 0;

		FORCE_INLINE void ensureSafeToDestroy(void) {
			while(safeToDestroy() == false) {
				lock();
				{
					notifyAll();
				}
				unlock();

				Thread::sleep(WAIT);
			}
		}

		FORCE_INLINE void notify(void) {
			#ifdef DEEP_DEBUG
			validate();
			#endif

			pthread_cond_signal(&m_cond);
		}

		FORCE_INLINE void notifyAll() {
			#ifdef DEEP_DEBUG
			validate();
			#endif

			pthread_cond_broadcast(&m_cond);
		}

		FORCE_INLINE boolean wait(void) {
			#ifdef DEEP_DEBUG
			testAndClearOwner();
			#endif

			const inttype res = pthread_cond_wait(&m_cond, &m_mutex);

			#ifdef DEEP_DEBUG
			if (res == 0) {
				testAndSetOwner();
			}
			#endif

			return res == 0;
		}

		FORCE_INLINE boolean wait(longtype timeout) {
			#ifdef DEEP_DEBUG
			testAndClearOwner();
			#endif

			#ifdef COM_DEEPIS_REALTIME_CLOCK
			clock_gettime(CLOCK_REALTIME, &m_ts);
			m_ts.tv_sec += timeout / 1000;
			m_ts.tv_nsec += (timeout % 1000) * 1000000;
			if (m_ts.tv_nsec > 1000000000) {
				m_ts.tv_nsec -= 1000000000;
				++m_ts.tv_sec;
			}
			#else
			ftime(&m_tb);
			m_tb.time += timeout / 1000;
			m_tb.millitm += timeout % 1000;
			if (m_tb.millitm > 999) {
				m_tb.millitm -= 1000;
				m_tb.time++;
			}
			m_ts.tv_sec = m_tb.time;
			m_ts.tv_nsec = m_tb.millitm * 1000000;
			#endif

			const inttype res = pthread_cond_timedwait(&m_cond, &m_mutex, &m_ts);

			#ifdef DEEP_DEBUG
			if ((res == 0) || (res == ETIMEDOUT)) {
				testAndSetOwner();
			}
			#endif

			if (res == ETIMEDOUT) {
				unlock();
			}

			return res == 0;
		}

	friend class StackBlock;
	friend class AntiStackBlock;
};

class StackBlock {

	private:
		Lockable* m_lockable;
		volatile boolean m_locked;

	public:
		StackBlock(Lockable* lockable) :
			m_lockable(lockable),
			m_locked(true) {
			m_lockable->lock();
		}

		StackBlock(Lockable& lockable) :
			m_lockable(&lockable),
			m_locked(true) {
			m_lockable->lock();
		}

		~StackBlock() {
			m_lockable->unlock();
		}

		FORCE_INLINE void unlock() {
			m_locked = false;
		}

		FORCE_INLINE operator boolean(void) const {
			return m_locked;
		}

		friend class AntiStackBlock;
};

class AntiStackBlock {

	private:
		Lockable* m_lockable;
		volatile boolean m_locked;

	public:
		AntiStackBlock(StackBlock& sync) :
			m_lockable(sync.m_lockable),
			m_locked(false) {
			m_lockable->unlock();
		}

		~AntiStackBlock() {
			m_lockable->lock();
		}

		FORCE_INLINE void lock() {
			m_locked = true;
		}

		FORCE_INLINE operator boolean(void) const {
			return m_locked;
		}
};

#define SYNCHRONIZED(object) for (StackBlock object##_sync = object; object##_sync; object##_sync.unlock())
#define synchronized(object) SYNCHRONIZED(object)

#define UNSYNCHRONIZED(object) for (AntiStackBlock object##_unsync = object##_sync; object##_unsync == false; object##_unsync.lock())
#define unsynchronized(object) UNSYNCHRONIZED(object)

} } } // namespace

#endif /*CXX_LANG_SYNCHRONIZE_H_*/
