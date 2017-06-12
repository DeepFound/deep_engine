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
#ifndef CXX_UTIL_CONCURRENT_LOCKS_USERSPACEREADWRITELOCK_H_
#define CXX_UTIL_CONCURRENT_LOCKS_USERSPACEREADWRITELOCK_H_

#include "cxx/util/concurrent/locks/Lock.h"

namespace cxx { namespace util { namespace concurrent { namespace locks {

class UserSpaceReadWriteLock {
	private:
		static const ulongtype MAX_NEW_READERS = 100;

		enum WriteRequestMode {
			WRITE_IDLE = 0,
			WRITE_WAIT = 1,
			WRITE_TRY = 2
		};

		ulongtype m_readers;
		ulongtype m_readersWaiting;
		ulongtype m_readersUnlocking;
		ulongtype m_newReaders;
		uinttype m_writer;
		WriteRequestMode m_writeRequested;
		uinttype m_writerUnlocking;

		template<typename T>
		FORCE_INLINE static void inc(T* v) {
			__sync_add_and_fetch(v, 1);
		}

		template<typename T>
		FORCE_INLINE static T dec(T* v) {
			return __sync_sub_and_fetch(v, 1);
		}

		template<typename T>
		FORCE_INLINE static T get(T* v) {
			return __sync_fetch_and_or(v, 0);
		}

		template<typename T>
		FORCE_INLINE static T set(T* v, T val) {
			return __sync_lock_test_and_set(v, val);
		}

		template<typename T>
		FORCE_INLINE static boolean condSet(T* v, T oldval, T newval) {
			return __sync_bool_compare_and_swap(v, oldval, newval);
		}

	public:

		UserSpaceReadWriteLock() :
			m_readers(0),
			m_readersWaiting(0),
			m_readersUnlocking(0),
			m_newReaders(0),
			m_writer(false),
			m_writeRequested(WRITE_IDLE),
			m_writerUnlocking(false) {
		}

		virtual ~UserSpaceReadWriteLock() {
			// XXX: nothing to do
		}

		FORCE_INLINE void readLock() {
			uinttype state = 1;
			boolean waiting = false;
			while ((get(&m_writeRequested) != WRITE_IDLE) && (get(&m_writer) == false) && (get(&m_newReaders) >= MAX_NEW_READERS)) {
				Lock::yield(&state);
			}
			inc(&m_readers);
			// XXX: wait until there are no active writers
			while (get(&m_writer) == true) {
				if (waiting == false) {
					waiting = true;
					// XXX: indicate that we are waiting on a writer
					inc(&m_readersWaiting);
				}
				Lock::yield(&state);
			}
			if (waiting == true) {
				dec(&m_readersWaiting);
			}
			if ((get(&m_writeRequested) != WRITE_IDLE) && (get(&m_writer) == false)) {
				inc(&m_newReaders);
			}
		}

		FORCE_INLINE boolean tryReadLock() {
			if ((get(&m_writeRequested) != WRITE_IDLE) && (get(&m_writer) == false) && (get(&m_newReaders) >= MAX_NEW_READERS)) {
				return false;
			}
			inc(&m_readers);
			// XXX: wait until there are no active writers
			if (get(&m_writer) == true) {
				dec(&m_readers);
				if (get(&m_writeRequested) != WRITE_IDLE) {
					Lock::schedule();
				}
				return false;
			}
			if ((get(&m_writeRequested) != WRITE_IDLE) && (get(&m_writer) == false)) {
				inc(&m_newReaders);
			}
			return true;
		}

		FORCE_INLINE void readUnlock() {
			uinttype state = 1;
			inc(&m_readersUnlocking);
			// XXX: wait for all writers to finish unlock notification
			while (get(&m_writerUnlocking) == true) {
				dec(&m_readersUnlocking);
				Lock::yield(&state);
				inc(&m_readersUnlocking);
			}
			const ulongtype readers = dec(&m_readers);
			// XXX: give the writers a better chance to sneak in (notify the waiting writer of the lack of active readers)
			if ((readers == 0) && (get(&m_writeRequested) == WRITE_WAIT)) {
				set(&m_writer, (uinttype)true);
			}
			dec(&m_readersUnlocking);
		}

		FORCE_INLINE void writeLock() {
			uinttype state = 1;
			// XXX: only let one thread at a time into the write lock protocol
			while (condSet(&m_writeRequested, WRITE_IDLE, WRITE_WAIT) == false) {
				Lock::yield(&state);
			}
			// XXX: wait until there are no active readers (note that we get some help from readUnlock()) 
			while ((get(&m_readers) != 0) && (get(&m_writer) == false)) {
				Lock::yield(&state);
			}
			set(&m_writer, (uinttype)true);
			// XXX: wait until all readers are waiting (note that the get() order matters here)
			while (get(&m_readersWaiting) != get(&m_readers)) {
				Lock::yield(&state);
			}
			set(&m_newReaders, (ulongtype)0);
		}

		FORCE_INLINE boolean tryWriteLock() {
			// XXX: only let one thread at a time into the write lock protocol
			if (condSet(&m_writeRequested, WRITE_IDLE, WRITE_TRY) == false) {
				return false;
			}
			// XXX: wait until there are no active readers (note that we get some help from readUnlock()) 
			if ((get(&m_readers) != 0) && (get(&m_writer) == false)) {
				set(&m_writeRequested, WRITE_IDLE);
				return false;
			}
			set(&m_writer, (uinttype)true);
			// XXX: wait until all readers are waiting (note that the get() order matters here)
			if (get(&m_readersWaiting) != get(&m_readers)) {
				writeUnlock();
				return false;
			}
			return true;
		}

		FORCE_INLINE void writeUnlock() {
			uinttype state = 1;
			// XXX: wait for previous writer unlock notification
			while (condSet(&m_writerUnlocking, (uinttype)false, (uinttype)true) == false) {
				Lock::yield(&state);
			}
			// XXX: wait for all readers to finish the unlock notification
			if (get(&m_writeRequested) == WRITE_WAIT) {
				while (get(&m_readersUnlocking) != 0) {
					Lock::yield(&state);
				}
			}
			set(&m_writer, (uinttype)false);
			set(&m_writeRequested, WRITE_IDLE);
			set(&m_writerUnlocking, (uinttype)false);
		}
};

} } } } // namespace

#endif
