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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_INFORMATION_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_INFORMATION_H_ 

#include "cxx/lang/types.h"

#ifdef DEEP_DEBUG
	#include "cxx/util/Logger.h"

	#include "com/deepis/db/store/relative/util/InvalidException.h"

	using namespace cxx::util;
#endif

#include "cxx/util/concurrent/locks/LwUserSpaceReadWriteLock.h"

#include "com/deepis/db/store/relative/util/ReferenceObject.h"

using namespace cxx::lang;
using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

struct StoryLine;
class Transaction;

struct __attribute__((packed)) StoryLock : public ReferenceObject<ushorttype,65533 /* -1 crud storyboard */> {

	private:
		ulongtype m_credentials;
		ubytetype m_storyDepth;
		boolean m_share:1;

		volatile boolean m_lock;

	public:
		StoryLock(void):
			m_credentials(0),
			m_storyDepth(0),
			m_share(false),
			m_lock(false) {

			CXX_LANG_MEMORY_DEBUG_INIT()
		}

		~StoryLock(void) {
			CXX_LANG_MEMORY_DEBUG_CLEAR()
		}

		FORCE_INLINE uinttype getIdentifier() const {
			return (uinttype) (m_credentials & 0xffffffff);
		}

		FORCE_INLINE uinttype getSequence(void) const {
			return (uinttype) (m_credentials >> 32);
		}

		FORCE_INLINE void setCredentials(uinttype identifier, uinttype sequence) {
			// XXX: future requirements might require atomic operations (return boolean)
			m_credentials = ((ulongtype) sequence << 32) | identifier;
		}

		FORCE_INLINE void setStoryDepth(inttype depth) {
			// XXX: internal max is ubytetype
			if (depth > UCHAR_MAX) {
				depth = UCHAR_MAX;
			}

			m_storyDepth = depth;
		}

		FORCE_INLINE inttype getStoryDepth() const {
			return m_storyDepth;
		}

		FORCE_INLINE void setShare(boolean flag) {
			m_share = flag;
		}

		FORCE_INLINE boolean getShare() const {
			return m_share;
		}

	private:
		FORCE_INLINE void lock() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);

			register int yield = 1;
			while (__sync_lock_test_and_set(&m_lock, true)) {
				if ((++yield % 10) == 0) {
					if (yield == 1000) {
						yield = 1;

						::usleep(1000);

					} else {
						pthread_yield();
					}

				} else {
					__asm volatile ("pause");
				}
			}
		}

		FORCE_INLINE boolean tryLock() {
			return (__sync_lock_test_and_set(&m_lock, true) == 0);
		}

		FORCE_INLINE void unlock() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);

			// XXX: __sync_lock_release(&m_lock, false);

			(void) __sync_lock_test_and_set(&m_lock, false);
		}

	public:

		FORCE_INLINE uinttype incrementSequence(void) {	
			// XXX: future requirements might require atomic operations
			setCredentials(getIdentifier(), getSequence() + 1);

			return getSequence();
		}

		FORCE_INLINE uinttype decrementSequence(void) {
			// XXX: future requirements might require atomic operations
			setCredentials(getIdentifier(), getSequence() - 1);

			return getSequence();
		}

		FORCE_INLINE void release(void) {
			// XXX: future requirements might require atomic operations
			m_credentials = 0;
		}

	public:
		friend struct StoryLine;
		friend class Transaction;
};

struct __attribute__((packed)) Information : public ReferenceObject<ubytetype,255> {

	public:
		enum Type {
			READ = 0,
			WRITE = 1,
			CMPRS = 2
		};

		static const uinttype OFFSET_NONE = 0xFFFFFFFF;

	private:
		Information* m_next;

		bytearray m_data;
		uinttype m_size;

		ushorttype m_fileIndex;
		uinttype m_filePosition;

		#ifdef DEEP_DEBUG
		#if 0
		ubytetype m_infoRef;
		#endif
		ubytetype m_rootedCount;
		boolean m_absolving;
		#endif

		volatile ulongtype m_viewpoint;

		ubytetype m_stateFlags;

	private:	
		union {
			volatile ulongtype m_indexed; //bitset
			volatile ulongtype m_rooted;  //bitset
		};

		shorttype m_level;

		union {
			uinttype m_CMPRS_MARK; /* XXX: CMPRS-marker */
			uinttype m_compressedOffset;
		};

	private:
		Information(const Information& info);
		Information& operator=(Information& info);

		FORCE_INLINE static uinttype sizeOf(Type t) {
			switch (t) {
				case READ:
				case WRITE:
				{
					Information i;
					return (uinttype) (((uchartype*)(&i.m_CMPRS_MARK)) - ((uchartype*)(&i)));
				}
				#if 0
				case READ:
				{
					#if 0
					Information i;
					return (uinttype) (((uchartype*)(&i.m_WRITE_MARK)) - ((uchartype*)(&i)));
					#else
					return sizeof(Information);
					#endif
				}
				#endif
				case CMPRS:
				{
					return sizeof(Information);
				}
				default:
				{
					#ifdef DEEP_DEBUG
					DEEP_LOG(ERROR, OTHER, "Invalid locking type: %d\n", t);

					throw InvalidException("Invalid Information type");
					#else
					return sizeof(Information); // XXX: requirement to not abort
					#endif
				}
			}
		}

	public:
		FORCE_INLINE static Information* newInfo(Type type) {
			uinttype size = 0;
			switch (type) {
				case WRITE:
				{
					static const uinttype s = sizeOf(type);
					size = s;
					break;
				}
				case READ:
				{
					static const uinttype s = sizeOf(type);
					size = s;
					break;
				}
				case CMPRS:
				{
					static const uinttype s = sizeOf(type);
					size = s;
					break;
				}
			}

			return new (malloc(size)) Information(type);
		}

		FORCE_INLINE static Information* newInfo(Type type, ushorttype index, uinttype position, inttype vsize) {
			uinttype size = 0;
			switch (type) {
				case WRITE:
				{
					static const uinttype s = sizeOf(type);
					size = s;
					break;
				}
				case READ:
				{
					static const uinttype s = sizeOf(type);
					size = s;
					break;
				}
				case CMPRS:
				{
					static const uinttype s = sizeOf(type);
					size = s;
					break;
				}
			}

			return new (malloc(size)) Information(type, index, position, vsize);
		}

		FORCE_INLINE static void nullInfo(Information& info, inttype size) {
			if (size < 0) {
				size = 1;
			}

			info.setSize(size);

			if (info.getData() != null) {
				info.freeData();
			}

			info.initData();
		}

		FORCE_INLINE static Information* nullInfo(inttype size, Type type = WRITE) {
			Information* info = newInfo(type);
			nullInfo(*info, size);
			return info;
		}

		#if 0
		FORCE_INLINE static Information* tackOnWriteInfo(Information* info, boolean lock) {
			#ifdef DEEP_DEBUG
			if (info->hasFields(Information::WRITE)) {
				DEEP_LOG(ERROR, OTHER, "Invalid tackOnWriteInfo: information has write fields\n");

				throw InvalidException("Invalid tackOnWriteInfo: information has write fields");
			}
			#endif

			Information* newinfo = Information::newInfo(Information::WRITE, info->getFileIndex(), info->getFilePosition(), info->getSize());

			if (info->getStoryLock() == null) {
				info->initStoryLock();
			}

			if (lock == true) info->lock();
			{
				newinfo->setStoryLock(info->getStoryLock());
				newinfo->setLevel(Information::LEVEL_COMMIT);
				newinfo->setDeleting(info->getDeleting());

				if (info->getData() != null) {
					nbyte tmpValue((const bytearray) info->getData(), info->getSize());

					bytearray data = (bytearray) malloc(tmpValue.length);
					memcpy(data, (voidarray) tmpValue, tmpValue.length);
					newinfo->setData(data);
				}

				Information* next = info->getNext();
				if (next != null) {
					newinfo->setNext(next);
				}

				info->setDeleting(true);
				info->setNext(newinfo);
			} 
			if (lock == true) info->unlock();

			return newinfo;
		}	
		#endif

		FORCE_INLINE static void freeInfo(Information* info) {
			info->~Information();
			free(info);
		}

	public:
		enum LevelState {
			LEVEL_ERASED = -4,
			LEVEL_ROLLED = -3,
			LEVEL_MERGED = -2,
			LEVEL_COMMIT = -1,
			LEVEL_ACTIVE = 0
		};

		enum TxState {
			TX_CLOSE = 0,
			TX_ABORT = 1
		};

		FORCE_INLINE void setIndexed(bytetype indexed, boolean flag) {
			#ifdef DEEP_DEBUG
			if (getLevel() != LEVEL_COMMIT) {
				DEEP_LOG(ERROR, OTHER, "Invalid indexing: information not committed\n");

				throw InvalidException("Invalid indexing: information not committed");
			}
			#if 0
			if (hasFields(WRITE) == false) {
				DEEP_LOG(ERROR, OTHER, "Invalid indexing: wrong information type\n");

				throw InvalidException("Invalid indexing: wrong information type");
			}
			#endif
			if ((indexed >= 64) || (indexed < 0)) {
				LOGGING_ERROR("Invalid indexing: index state overflow, %d\n", indexed);
				throw InvalidException("Invalid indexing: index state overflow");
			}
			#endif

			if (flag == true) {
				__sync_fetch_and_or(&m_indexed, (1L << indexed));

			} else {
				__sync_fetch_and_and(&m_indexed, ~(1L << indexed));
			}
		}

		FORCE_INLINE boolean getIndexed(bytetype indexed) const {
			#ifdef DEEP_DEBUG
			if (getLevel() != LEVEL_COMMIT) {
				DEEP_LOG(ERROR, OTHER, "Invalid indexing: information not committed\n");

				throw InvalidException("Invalid indexing: information not committed");
			}
			if ((indexed >= 64) || (indexed < 0)) {
				LOGGING_ERROR("Invalid indexing: index state overflow, %d\n", indexed);
				throw InvalidException("Invalid indexing: index state overflow");
			}
			#endif

			#if 0
			if (hasFields(WRITE) == true) {
				return (m_indexed & (1L << indexed));

			} else {
				return true;
			}
			#else
			return (m_indexed & (1L << indexed));
			#endif
		}

		#ifdef DEEP_DEBUG
		FORCE_INLINE void setAbsolving() {
			m_absolving = true;
		}
		#endif

		FORCE_INLINE void setRooted(bytetype rooted, boolean flag) {
			#ifdef DEEP_DEBUG
			if (hasFields(WRITE) == false) {
				DEEP_LOG(ERROR, OTHER, "Invalid rooted: wrong information type\n");

				throw InvalidException("Invalid rooted: wrong information type");
			}
			if ((rooted >= 64) || (rooted < 0)) {
				LOGGING_ERROR("Invalid rooted: rooted state overflow, %d\n", rooted);
				throw InvalidException("Invalid rooted: rooted state overflow");
			}
			#endif

			if (flag == true) {
				const ulongtype prevRooted = __sync_fetch_and_or(&m_rooted, (1L << rooted));
				const boolean wasRooted = (prevRooted & (1L << rooted));

				#ifdef DEEP_DEBUG
				if ((m_absolving == true) && (wasRooted == false)) {
					DEEP_LOG(ERROR, OTHER, "Invalid rooted: info being absolved\n");

					throw InvalidException("Invalid rooted: info being absolved");
				}
				#endif

				// XXX: check level after rooted (order matters) to determine rooted vs indexed
				if ((wasRooted == false) && (getLevel() == LEVEL_COMMIT)) {
					// XXX: if committed state changed, restore state of slot; note that this is safe because rooted state must be set under segment lock (so we couldn't be indexing for that index)
					__sync_fetch_and_and(&m_rooted, ~(1L << rooted));
				}

			} else {
				__sync_fetch_and_and(&m_rooted, ~(1L << rooted));
			}
		}

		FORCE_INLINE boolean getRooted(bytetype rooted) const {
			#ifdef DEEP_DEBUG
			if (hasFields(WRITE) == false) {
				DEEP_LOG(ERROR, OTHER, "Invalid rooted: wrong information type\n");

				throw InvalidException("Invalid rooted: wrong information type");
			}
			if ((rooted >= 64) || (rooted < 0)) {
				LOGGING_ERROR("Invalid rooted: rooted state overflow, %d\n", rooted);
				throw InvalidException("Invalid rooted: rooted state overflow");
			}
			#endif

			const boolean ret = (m_rooted & (1L << rooted));
			// XXX: check level after rooted (order matters) to determine rooted vs indexed
			if (getLevel() == LEVEL_COMMIT) {
				return false;
			}
			return ret;
		}

		FORCE_INLINE void resetIndexed(ulongtype indexed) {
			#ifdef DEEP_DEBUG
			if (hasFields(WRITE) == false) {
				DEEP_LOG(ERROR, OTHER, "Invalid indexing: wrong information type\n");

				throw InvalidException("Invalid indexing: wrong information type");
			}
			#endif
			m_indexed = indexed;
		}

	public:
		FORCE_INLINE uinttype getViewpoint() const {
			#if 0
			#ifdef DEEP_DEBUG
			if (getLevel() != LEVEL_COMMIT) {
				DEEP_LOG(ERROR, OTHER, "Invalid viewpoint: information not committed\n");

				throw InvalidException("Invalid viewpoint: information not committed");
			}
			#endif
			#endif

			return (uinttype) (m_viewpoint & 0xffffffff);
		}

		FORCE_INLINE uinttype getViewLimit() const {
			#if 0
			#ifdef DEEP_DEBUG
			if (getLevel() != LEVEL_COMMIT) {
				DEEP_LOG(ERROR, OTHER, "Invalid viewpoint: information not committed\n");

				throw InvalidException("Invalid viewpoint: information not committed");
			}
			#endif
			#endif

			return (uinttype) (m_viewpoint >> 32);
		}

		FORCE_INLINE void limitViewpoint(uinttype hi) {
			#ifdef DEEP_DEBUG
			if (getLevel() != LEVEL_COMMIT) {
				DEEP_LOG(ERROR, OTHER, "Invalid viewpoint: information not committed\n");

				throw InvalidException("Invalid viewpoint: information not committed");
			}
			#endif

			uinttype lo = (uinttype) (m_viewpoint & 0xffffffff);

			m_viewpoint = ((ulongtype) hi << 32) | lo;
		}

		FORCE_INLINE void linkViewpoint(uinttype viewpoint) {
			#ifdef DEEP_DEBUG
			if (getLevel() != LEVEL_COMMIT) {
				DEEP_LOG(ERROR, OTHER, "Invalid viewpoint: information not committed\n");

				throw InvalidException("Invalid viewpoint: information not committed");
			}
			#endif

			uinttype lo = (uinttype) (m_viewpoint & 0xffffffff);
			uinttype hi = (uinttype) (m_viewpoint >> 32);

			if ((lo == 0) || (lo > viewpoint)) {
				lo = viewpoint;
			}

			m_viewpoint = ((ulongtype) hi << 32) | lo;
		}

		FORCE_INLINE boolean hasViewpoint(uinttype viewpoint) const {
			#ifdef DEEP_DEBUG
			if (getLevel() != LEVEL_COMMIT) {
				DEEP_LOG(ERROR, OTHER, "Invalid viewpoint: information not committed\n");

				throw InvalidException("Invalid viewpoint: information not committed");
			}
			#endif

			uinttype lo = (uinttype) (m_viewpoint & 0xffffffff);
			uinttype hi = (uinttype) (m_viewpoint >> 32);
			return ((lo <= viewpoint) && (hi >= viewpoint));
		}

		FORCE_INLINE void release(boolean reset, StoryLock* slock) {
			if (reset == true) {
				m_indexed = 0; /* m_rooted = 0 */
				m_viewpoint = 0;
			}

			slock->release();
		}

		FORCE_INLINE void reset(uinttype lo, uinttype hi, boolean commit = false) {
			if (commit == true) {
				m_viewpoint = ((ulongtype) hi << 32) | lo;

			} else {
				m_viewpoint = 0;
			}
		}

	private:
		FORCE_INLINE Information(Type type = WRITE):
			m_next(null),
			m_data(null),
			m_size(0),
			m_fileIndex(0),
			m_filePosition(0),
			#ifdef DEEP_DEBUG
			#if 0
			m_infoRef(0),
			#endif
			m_rootedCount(0),
			m_absolving(false),
			#endif
			m_viewpoint(0),
			m_stateFlags(0),
			m_level(LEVEL_COMMIT) {

			if (hasFields(type, WRITE) == true) {
				m_indexed = 0; /* m_rooted = 0 */

				if (hasFields(type, CMPRS) == true) {
					m_compressedOffset = OFFSET_NONE;
				}
			} else { // if (hasFields(type, READ) ==  true)
				m_indexed = ULONG_MAX;
			}

			CXX_LANG_MEMORY_DEBUG_INIT()

			setType(type);
		}

		FORCE_INLINE Information(const Information* info):
			m_next(info->m_next),
			m_data(info->m_data),
			m_size(info->m_size),
			m_fileIndex(info->m_fileIndex),
			m_filePosition(info->m_filePosition),
			#ifdef DEEP_DEBUG
			#if 0
			m_infoRef(0),
			#endif
			m_rootedCount(0),
			m_absolving(false),
			#endif
			m_viewpoint(info->m_viewpoint),
			m_stateFlags(info->m_stateFlags),
			m_indexed(info->m_indexed), /* m_rooted(info->m_rooted) */
			m_level(info->m_level),
			m_compressedOffset(info->m_compressedOffset) {

			CXX_LANG_MEMORY_DEBUG_INIT()
		}

		FORCE_INLINE Information(Type type, ushorttype index, uinttype position, inttype size):
			m_next(null),
			m_data(null),
			m_size(size),
			m_fileIndex(index),
			m_filePosition(position),
			#ifdef DEEP_DEBUG
			#if 0
			m_infoRef(0),
			#endif
			m_rootedCount(0),
			m_absolving(false),
			#endif
			m_viewpoint(0),
			m_stateFlags(0),
			m_level(LEVEL_COMMIT) {

			CXX_LANG_MEMORY_DEBUG_INIT()

			if (hasFields(type, WRITE) == true) {
				m_indexed = 0; /* XXX: set individually in memory writes or bulk on disk reads - see stitching */

				if (hasFields(type, CMPRS) == true) {
					m_compressedOffset = OFFSET_NONE;
				}
			} else { // if (hasFields(type, READ) ==  true)
				m_indexed = ULONG_MAX;
			}

			setType(type);
		}

	public:
		FORCE_INLINE ~Information(void) {
			CXX_LANG_MEMORY_DEBUG_CLEAR()

			#ifdef DEEP_DEBUG
			if (m_next != null) {
				#if 0
				m_next->decInfoRef();
				#endif
			}
			#endif

			freeData(true /* destroying */);
		}

		#ifdef DEEP_DEBUG
		#if 0
		FORCE_INLINE void incInfoRef(void) {
			m_infoRef++;
		}

		FORCE_INLINE void decInfoRef(void) {
			m_infoRef--;
		}

		FORCE_INLINE ubytetype getInfoRef(void) const {
			return m_infoRef;
		}
		#endif

		FORCE_INLINE void incRooted(void) {
			if (__sync_add_and_fetch(&m_rootedCount, 1) == 0) {
				DEEP_LOG(ERROR, OTHER, "Information rooted count overflow\n");
				throw InvalidException("Information rooted count overflow");
			}
		}
	
		FORCE_INLINE void decRooted(void) {
			if (__sync_fetch_and_sub(&m_rootedCount, 1) == 0) {
				DEEP_LOG(ERROR, OTHER, "Information rooted count underflow\n");
				throw InvalidException("Information rooted count underflow");
			}
		}

		FORCE_INLINE ubytetype getRootedCount(void) const {
			return __sync_or_and_fetch(const_cast<ubytetype*>(&m_rootedCount), 0);
		}
		#endif

		FORCE_INLINE void setNext(Information* next) {
			CXX_LANG_MEMORY_DEBUG_TYPE_ASSERT(Information*, m_next);

			#ifdef DEEP_DEBUG
			if ((next != null) && (m_data == null)) {
				DEEP_LOG(ERROR, OTHER, "Invalid setNext: info has null data");

				throw InvalidException("Invalid setNext: info has null data");
			}
			#endif

			#ifdef DEEP_DEBUG
			#if 0
			if (m_next != null) {
				m_next->decInfoRef();
			}
			#endif
			#endif

			m_next = next;

			#ifdef DEEP_DEBUG
			#if 0
			if (m_next != null) {
				m_next->incInfoRef();
			}
			#endif
			#endif

			CXX_LANG_MEMORY_DEBUG_ASSERT(m_next);
		}

		#ifdef DEEP_DEBUG
		FORCE_INLINE Information* getNext(boolean testLock = true) const {
		#else
		FORCE_INLINE Information* getNext() const {
		#endif
			CXX_LANG_MEMORY_DEBUG_TYPE_ASSERT(Information*, m_next);

			#if 0
			#ifdef DEEP_DEBUG
			if ((testLock == true) && (m_next != null)) {
				if (getStoryLock() != m_next->getStoryLock()) {
					DEEP_LOG(ERROR, OTHER, "Invalid locking: story lock mismatch\n");

					throw InvalidException("Invalid locking: story lock mismatch");
				}	
			}	
			#endif
			#endif

			return m_next;
		}

		#ifdef CXX_LANG_MEMORY_DEBUG
		FORCE_INLINE void setNext(Information* next, boolean ignore) {
			if (ignore == false) {
				setNext(next);

			} else {

				#ifdef DEEP_DEBUG
				#if 0
				if (m_next != null) {
					m_next->decInfoRef();
				}
				#endif
				#endif

				m_next = next;

				#ifdef DEEP_DEBUG
				#if 0
				if (m_next != null) {
					m_next->incInfoRef();
				}
				#endif
				#endif
			}
		}
		#endif

		FORCE_INLINE void setRootedState(ulongtype state) {
			m_rooted = state;
		}

		FORCE_INLINE ulongtype getRootedState() const {
			return m_rooted;
		}

		FORCE_INLINE void setStateFlags(ubytetype stateFlags) {
			m_stateFlags = stateFlags;
		}

		FORCE_INLINE ubytetype getStateFlags() const {
			return m_stateFlags;
		}

		FORCE_INLINE void setType(Type type) {
			switch(type) {
				case READ:
					//m_stateFlags = m_stateFlags & ~0x80;
					break;
				case WRITE:
					m_stateFlags = m_stateFlags | 0x80;
					break;
				case CMPRS:
					m_stateFlags = m_stateFlags | 0x80;
					m_stateFlags = m_stateFlags | 0x40;
					break;
				default:
					#ifdef DEEP_DEBUG
					DEEP_LOG(ERROR, OTHER, "Invalid info type: %d\n", type);

					throw InvalidException("Invalid info type");
					#else
					break;
					#endif
			}
		}

		FORCE_INLINE Type getType(void) const {
			#ifdef DEEP_DEBUG
			if (((m_stateFlags & 0x40) != 0) && ((m_stateFlags & 0x80) == 0)) {
				DEEP_LOG(ERROR, OTHER, "Invalid info type: %d\n", m_stateFlags);

				throw InvalidException("Invalid info type");
			}
			#endif

			return ((m_stateFlags & 0x40) == 0) ? (((m_stateFlags & 0x80) == 0) ? READ : WRITE) : CMPRS;
		}

		FORCE_INLINE static boolean hasFields(Type t1, Type t2) {
			switch(t1) {
				case READ:
					return t2 == READ;
				case WRITE:
					return t2 != CMPRS;
				case CMPRS:
					return true;
				default:
					#ifdef DEEP_DEBUG
					DEEP_LOG(ERROR, OTHER, "Invalid info type: %d\n", t1);

					throw InvalidException("Invalid info type");
					#else
					return t1 == t2;
					#endif
			}
		}

		FORCE_INLINE boolean hasFields(Type t) const {
			return hasFields(getType(), t);
		}

		FORCE_INLINE void setMerged(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x08 : m_stateFlags & ~0x08;
		}

		FORCE_INLINE boolean getMerged() const {
			return (m_stateFlags & 0x08) != 0;
		}

		FORCE_INLINE void setExpunged(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x20 : m_stateFlags & ~0x20;
		}

		FORCE_INLINE boolean getExpunged() const {
			return (m_stateFlags & 0x20) != 0;
		}

		FORCE_INLINE void setDiverging(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x10 : m_stateFlags & ~0x10;
		}

		FORCE_INLINE boolean getDiverging() const {
			return (m_stateFlags & 0x10) != 0;
		}

		FORCE_INLINE void setUpdating(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x04 : m_stateFlags & ~0x04;
		}

		FORCE_INLINE boolean getUpdating() const {
			return (m_stateFlags & 0x04) != 0;
		}

		FORCE_INLINE void setCreating(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x02 : m_stateFlags & ~0x02;
		}

		FORCE_INLINE boolean getCreating() const {
			return (m_stateFlags & 0x02) != 0;
		}

		FORCE_INLINE void setDeleting(boolean flag) {
			m_stateFlags = flag ? m_stateFlags | 0x01 : m_stateFlags & ~0x01;
		}

		FORCE_INLINE boolean getDeleting() const {
			return (m_stateFlags & 0x01) != 0;
		}

		FORCE_INLINE void setMergeLevel(ushorttype level) {
			#ifdef DEEP_DEBUG
			if (getLevel() != LEVEL_MERGED) {
				DEEP_LOG(ERROR, OTHER, "Invalid level: information not merged\n");

				throw InvalidException("Invalid level: information not merged");
			}
			#endif

			m_level = level;
		}

		FORCE_INLINE ushorttype getMergeLevel() const {
			#ifdef DEEP_DEBUG
			if (getLevel() != LEVEL_MERGED) {
				DEEP_LOG(ERROR, OTHER, "Invalid level: information not merged\n");

				throw InvalidException("Invalid level: information not merged");
			}
			#endif

			return m_level;
		}

		FORCE_INLINE void setFileIndex(ushorttype fileIndex, boolean validate = true) {
			#ifdef DEEP_DEBUG
			if ((validate == true) && (getDeleting() == true) && (getLevel() == LEVEL_COMMIT)) {
				DEEP_LOG(ERROR, OTHER, "Invalid file: information not committed\n");

				throw InvalidException("Invalid file: information not committed");
			}
			#endif

			m_fileIndex = fileIndex;
		}

		FORCE_INLINE ushorttype getFileIndex() const {
			#ifdef DEEP_DEBUG
			if (getLevel() < LEVEL_COMMIT) {
				DEEP_LOG(ERROR, OTHER, "Invalid file: information not committed\n");

				throw InvalidException("Invalid file: information not committed");
			}
			#endif

			return m_fileIndex;
		}

		FORCE_INLINE void setFilePosition(uinttype position, boolean validate = true) {
			// XXX: allow setting when rolling information (i.e. don't validate)
			#if DEEP_DEBUG
			if ((validate == true) && (getDeleting() == true) && (getLevel() == LEVEL_COMMIT)) {
				DEEP_LOG(ERROR, OTHER, "Invalid positioning: information committed\n");

				throw InvalidException("Invalid positioning: information committed");
			}
			#endif

			m_filePosition = position;
		}

		FORCE_INLINE uinttype getFilePosition() const {
			return m_filePosition;
		}

		FORCE_INLINE void setSize(uinttype size) {
			m_size = size;
		}

		FORCE_INLINE uinttype getSize() const {
			return m_size;
		}

		FORCE_INLINE void setLevel(shorttype level) {
			if ((level == LEVEL_COMMIT) && (m_level != LEVEL_COMMIT)) {
				setRootedState(0L);
			} else if (level == LEVEL_MERGED) {
				setMerged(true);
				return;
			}
			m_level = level;
			setMerged(false);
		}

		FORCE_INLINE shorttype getLevel(void) const {
			if (getMerged() == true) {
				return LEVEL_MERGED;
			}
			if (hasFields(WRITE) == false) {
				return Information::LEVEL_COMMIT;

			} else {
				return m_level;
			}
		}

		FORCE_INLINE void setData(bytearray data) {
			#ifdef DEEP_DEBUG
			if (m_next != null) {
				DEEP_LOG(ERROR, OTHER, "Invalid setData: info has next");

				throw InvalidException("Invalid setData: info has next");
			}
			#endif

			m_data = data;
		}

		FORCE_INLINE bytearray getData(void) const {
			return m_data;
		}

		FORCE_INLINE void initData(void) {
			#ifdef DEEP_DEBUG
			if (m_next != null) {
				DEEP_LOG(ERROR, OTHER, "Invalid initData: info has next");

				throw InvalidException("Invalid initData: info has next");
			}
			#endif

			m_data = (bytearray) malloc(getSize());
		}

		FORCE_INLINE void freeData(boolean destroying = false) {
			#ifdef DEEP_DEBUG
			if ((destroying == false) && (m_next != null)) {
				DEEP_LOG(ERROR, OTHER, "Invalid freeData: info has next");

				throw InvalidException("Invalid freeData: info has next");
			}
			#endif

			free(m_data);
			m_data = null;
		}

		FORCE_INLINE void setCompressedOffset(uinttype off) {
			#ifdef DEEP_DEBUG
			if (hasFields(CMPRS) == false) {
				DEEP_LOG(ERROR, OTHER, "Invalid info type: %d\n", getType());

				throw InvalidException("Invalid info type");
			}
			#endif

			m_compressedOffset = off;
		}

		FORCE_INLINE uinttype getCompressedOffset(void) {
			if (hasFields(CMPRS) == false) {
				return OFFSET_NONE;
			}

			return m_compressedOffset;
		}

		FORCE_INLINE boolean getCompressed(void) const {
			return (hasFields(CMPRS) == true) && (m_compressedOffset != OFFSET_NONE);
		}

		FORCE_INLINE static ubytetype getInMemoryCompressedSize(const Information* info = null) {
			Type t = (info == null) ? Information::WRITE : info->getType();
			switch(t) {
				case READ:
				case WRITE:
					return 6;
				case CMPRS:
					return 11;
				default:
					#ifdef DEEP_DEBUG
					DEEP_LOG(ERROR, OTHER, "Invalid info type: %d\n", t);

					throw InvalidException("Invalid info type");
					#else
					return 11;
					#endif
			}
		}

		FORCE_INLINE void setBytes(bytearray data) {
			m_fileIndex = *((ushorttype*) data);
			data += sizeof(ushorttype);
			m_filePosition = *((uinttype*)data); 
			data += sizeof(uinttype);

			if (hasFields(WRITE) == true) {
				m_level = Information::LEVEL_COMMIT;
			}

			if (hasFields(CMPRS) == true) {
				m_compressedOffset = *((uinttype*) data);
			}
		}

		FORCE_INLINE bytearray getBytes() const {
			bytearray data = (bytearray) new char [getInMemoryCompressedSize(this)];
			ubytetype offset = 0;

			memcpy(data + offset, &m_fileIndex, sizeof(ushorttype));
			offset += sizeof(ushorttype);
			memcpy(data + offset, &m_filePosition, sizeof(uinttype)); 
			offset += sizeof(uinttype);

			if (hasFields(CMPRS) == true) {
				memcpy(data + offset, &m_compressedOffset, sizeof(uinttype));	
			}

			return data;
		}
};

struct __attribute__((packed)) StoryLine {
	private:
		Information* m_orginfo;
		StoryLock* m_slock;
		LwUserSpaceReadWriteLock<> m_lock;

	public:
		FORCE_INLINE static Information* tackOnWriteInfo(StoryLine* storyLine, Information* info, boolean lock) {
			#ifdef DEEP_DEBUG
			if (info->hasFields(Information::WRITE)) {
				DEEP_LOG(ERROR, OTHER, "Invalid tackOnWriteInfo: information has write fields\n");

				throw InvalidException("Invalid tackOnWriteInfo: information has write fields");
			}
			#endif

			Information* newinfo = Information::newInfo(Information::WRITE, info->getFileIndex(), info->getFilePosition(), info->getSize());

			if (lock == true) storyLine->lock();
			{
				newinfo->setLevel(Information::LEVEL_COMMIT);
				newinfo->setDeleting(info->getDeleting());

				if (info->getData() != null) {
					nbyte tmpValue((const bytearray) info->getData(), info->getSize());

					bytearray data = (bytearray) malloc(tmpValue.length);
					memcpy(data, (voidarray) tmpValue, tmpValue.length);
					newinfo->setData(data);
				}

				Information* next = info->getNext();
				if (next != null) {
					newinfo->setNext(next);
				}

				info->setDeleting(true);
				info->setNext(newinfo);
			} 
			if (lock == true) storyLine->unlock();

			return newinfo;
		}

		FORCE_INLINE StoryLock* initStoryLock(void) {
			if (m_slock == null) {
				StoryLock* slock  = new StoryLock(); // XXX: refcnt equals 1
				if (__sync_bool_compare_and_swap(&m_slock, null, slock) == false) {
					delete slock;
				}
			}
			return m_slock;
		}

	private:
		FORCE_INLINE void freeStoryLock(void) {
			StoryLock* slock = m_slock;
			m_slock = null;
			if ((slock != null) && (slock->decref() == 0)) {
				reassignmentLock();
				delete slock;
				reassignmentUnlock();
			}
		}

	public:
		FORCE_INLINE StoryLine(Information* info, StoryLock* lock = null) :
			m_orginfo(info),
			m_slock(lock),
			m_lock() {
			// XXX: nothing to do
		}

		FORCE_INLINE ~StoryLine() {
			freeStoryLock();
		}

		FORCE_INLINE Information* getRoot() const {
			return m_orginfo;
		}

		FORCE_INLINE void setRoot(Information* info) {
			m_orginfo = info;
		}

		FORCE_INLINE void lock() {
			RETRY:
			m_lock.readLock();
			StoryLock* slock = initStoryLock();
			slock->lock();
			if (slock != m_slock) {
				slock->unlock();
				m_lock.readUnlock();
				Thread::yield();
				goto RETRY;
			}
		}

		FORCE_INLINE boolean tryLock() {
			if (m_lock.tryReadLock() == false) {
				return false;
			}
			StoryLock* slock = initStoryLock();
			if (slock->tryLock() == false) {
				m_lock.readUnlock();
				return false;
			}
			if (slock != m_slock) {
				slock->unlock();
				m_lock.readUnlock();
				return false;
			}
			return true;
		}

		FORCE_INLINE void unlock() {
			m_slock->unlock();
			m_lock.readUnlock();
		}

		FORCE_INLINE void lock(StoryLock*& slock) {
			m_lock.readLock();
			if (slock != m_slock) {
				slock = initStoryLock();
			}
			RETRY:
			slock->lock();
			if (slock != m_slock) {
				slock->unlock();
				m_lock.readUnlock();
				Thread::yield();
				m_lock.readLock();
				slock = initStoryLock();
				goto RETRY;
			}
		}

		FORCE_INLINE boolean tryLock(StoryLock*& slock) {
			if (m_lock.tryReadLock() == false) {
				return false;
			}
			if (slock != m_slock) {
				slock = initStoryLock();
			}
			if (slock->tryLock() == false) {
				m_lock.readUnlock();
				return false;
			}
			if (slock != m_slock) {
				slock->unlock();
				slock = m_slock;
				m_lock.readUnlock();
				return false;
			}
			return true;
		}

		FORCE_INLINE void unlock(StoryLock* slock) {
			slock->unlock();
			m_lock.readUnlock();
		}

		FORCE_INLINE void reassignmentLock() {
			m_lock.writeLock();
		}

		FORCE_INLINE void reassignmentUnlock() {
			m_lock.writeUnlock();
		}

		FORCE_INLINE void setStoryLock(StoryLock* storyLock) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(m_slock);
			CXX_LANG_MEMORY_DEBUG_ASSERT(storyLock);

			m_slock = storyLock;

			CXX_LANG_MEMORY_DEBUG_ASSERT(m_slock);

			if (m_slock != null) {
				m_slock->incref();
			}
		}

		FORCE_INLINE StoryLock* getStoryLock(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(m_slock);

			return (StoryLock*)m_slock;
		}

		FORCE_INLINE boolean hasSameLock(StoryLine& storyLine) const {
			return (getStoryLock() == storyLine.getStoryLock());
		}

		FORCE_INLINE uinttype incrementLockSequence(void) {
			#ifdef DEEP_DEBUG
			if (getSharing() == false) {
				DEEP_LOG(ERROR, OTHER, "Invalid lock sequence: information not sharing\n");

				throw InvalidException("Invalid lock sequence: information not sharing");
			}
			#endif

			return getStoryLock()->incrementSequence();
		}

		FORCE_INLINE uinttype decrementLockSequence(void) {
			#ifdef DEEP_DEBUG
			if (getSharing() == false) {
				DEEP_LOG(ERROR, OTHER, "Invalid lock sequence: information not sharing\n");

				throw InvalidException("Invalid lock sequence: information not sharing");
			}
			#endif

			return getStoryLock()->decrementSequence();
		}

		FORCE_INLINE void release() {
			getStoryLock()->release();
		}

		FORCE_INLINE ushorttype getLockIdentifier() const {
			return (getStoryLock() != null) ? getStoryLock()->getIdentifier() : 0;
		}

		FORCE_INLINE void setLockCredentials(uinttype identifier, uinttype sequence) {
			#ifdef DEEP_DEBUG
			if (getStoryLock() == null) {
				DEEP_LOG(ERROR, OTHER, "Invalid locking: story lock not found\n");

				throw InvalidException("Invalid locking: story lock not found");
			}
			#endif

			getStoryLock()->setCredentials(identifier, sequence);
		}

		FORCE_INLINE uinttype getLockSequence(void) const {
			return (getStoryLock() != null) ? getStoryLock()->getSequence() : 0;
		}

		FORCE_INLINE void setStoryDepth(ubytetype depth) {
			#ifdef DEEP_DEBUG
			if (getStoryLock() == null) {
				DEEP_LOG(ERROR, OTHER, "Invalid locking: story lock not found\n");

				throw InvalidException("Invalid locking: story lock not found");
			}
			#endif

			getStoryLock()->setStoryDepth(depth);
		}

		FORCE_INLINE ubytetype getStoryDepth() const {
			return (getStoryLock() != null) ? getStoryLock()->getStoryDepth() : 0;
		}

		FORCE_INLINE void setSharing(boolean flag) {
			#ifdef DEEP_DEBUG
			if (getStoryLock() == null) {
				DEEP_LOG(ERROR, OTHER, "Invalid locking: story lock not found\n");

				throw InvalidException("Invalid locking: story lock not found");
			}
			#endif

			getStoryLock()->setShare(flag);
		}

		FORCE_INLINE boolean getSharing() const {
			return (getStoryLock() != null) ? getStoryLock()->getShare() : false;
		}
};

} } } } } } // namespace


#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_INFORMATION_H_*/
