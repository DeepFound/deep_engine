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
#ifndef CXX_UTIL_CONCURRENT_LOCKS_LWUSERSPACEREADWRITELOCK_H_
#define CXX_UTIL_CONCURRENT_LOCKS_LWUSERSPACEREADWRITELOCK_H_

#include "cxx/util/concurrent/locks/Lock.h"

namespace cxx { namespace util { namespace concurrent { namespace locks {

template<typename N = ubytetype, N MAX = 0x7F>
class __attribute__((packed)) LwUserSpaceReadWriteLock {
	private:
		static const N RAW_MAX = MAX << 1;

		N m_readers;

		template<typename T>
		FORCE_INLINE static T decVal(T* v, T amt) {
			return __sync_sub_and_fetch(v, amt);
		}

		template<typename T>
		FORCE_INLINE static T get(T* v) {
			#if 0
			return __sync_fetch_and_or(v, 0);
			#else
			return *v;
			#endif
		}

		template<typename T>
		FORCE_INLINE static T set(T* v, T val) {
			return __sync_lock_test_and_set(v, val);
		}

		template<typename T>
		FORCE_INLINE static boolean condSet(T* v, T oldval, T newval) {
			return __sync_bool_compare_and_swap(v, oldval, newval);
		}

		FORCE_INLINE boolean consumeOne() {
			const N rawSample = get(&m_readers);
			const N sample = ((N)(rawSample >> 1)) & MAX;
			return (sample < MAX) && (condSet(&m_readers, rawSample, (N)(rawSample + 2)) == true);
		}

		FORCE_INLINE N consumeRemaining() {
			const N rawSample = get(&m_readers);
			if (condSet(&m_readers, rawSample, (N)(RAW_MAX | rawSample)) == true) {
				const N sample = ((N)(rawSample >> 1)) & MAX;
				return (MAX - sample);
			}
			return 0;
		}

		FORCE_INLINE boolean getWriteRequested() {
			const N rawSample = get(&m_readers);
			return (rawSample & 0x01) != 0;
		}

		FORCE_INLINE boolean setWriteRequested() {
			return (__sync_fetch_and_or(&m_readers, (N)0x01) & 0x01) == 0;
		}

		FORCE_INLINE void clearWriteRequested() {
			__sync_and_and_fetch(&m_readers, RAW_MAX);
		}

		FORCE_INLINE void setReaders(N readers) {
			set(&m_readers, (N)(readers << 1)); 
		}

		FORCE_INLINE void decReaders(N readers) {
			decVal(&m_readers, (N)(readers << 1));
		}

	public:

		FORCE_INLINE LwUserSpaceReadWriteLock() :
			m_readers(0) {
		}

		FORCE_INLINE ~LwUserSpaceReadWriteLock() {
			// XXX: nothing to do
		}

		FORCE_INLINE void readLock() {
			uinttype state = 1;
			// XXX: yield to writer
			while (getWriteRequested() == true) {
				Lock::yield(&state);
			}
			// XXX: wait for available read slot
			while (consumeOne() == false) {
				Lock::yield(&state);
			}
		}

		FORCE_INLINE boolean tryReadLock() {
			return (getWriteRequested() == false) && (consumeOne() == true);
		}

		FORCE_INLINE void readUnlock() {
			decReaders(1);
		}

		FORCE_INLINE void writeLock() {
			uinttype state = 1;
			// XXX: only let one thread at a time into the write lock protocol
			while (setWriteRequested() == false) {
				Lock::yield(&state);
			}
			// XXX: wait until there are no active readers
			N consumed = 0;
			while (true) {
				consumed += consumeRemaining();
				if (consumed == MAX) {
					break;
				}
				Lock::yield(&state);
			}
		}

		FORCE_INLINE boolean writeLock(const longtype timeout) {
			const longtype start = System::currentTimeMillis();
			uinttype state = 1;
			// XXX: only let one thread at a time into the write lock protocol
			while (setWriteRequested() == false) {
				if ((System::currentTimeMillis() - start) >= timeout) {
					return false;
				}
				Lock::yield(&state);
			}
			// XXX: wait until there are no active readers
			N consumed = 0;
			while (true) {
				consumed += consumeRemaining();
				if (consumed == MAX) {
					break;
				}
				if ((System::currentTimeMillis() - start) >= timeout) {
					decReaders(consumed);
					clearWriteRequested();
					return false;
				}
				Lock::yield(&state);
			}
			return true;
		}

		FORCE_INLINE boolean tryWriteLock() {
			// XXX: only let one thread at a time into the write lock protocol
			if (setWriteRequested() == false) {
				return false;
			}
			// XXX: ensure there are no active readers
			N consumed = consumeRemaining();
			if (consumed != MAX) {
				decReaders(consumed);
				clearWriteRequested();
				return false;
			}

			return true;
		}

		FORCE_INLINE void demote() {
			setReaders(1);
		}

		FORCE_INLINE void writeUnlock() {
			setReaders(0);
		}
};

} } } } // namespace

#endif
