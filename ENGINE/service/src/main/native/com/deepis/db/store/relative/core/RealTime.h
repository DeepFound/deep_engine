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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIME_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIME_H_

#include "cxx/nio/file/Files.h"

#include "cxx/util/concurrent/Synchronize.h"
#include "cxx/util/concurrent/atomic/AtomicLong.h"
#include "cxx/util/concurrent/atomic/AtomicInteger.h"
#include "cxx/util/concurrent/locks/UserSpaceReadWriteLock.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/Transaction.h"
#include "com/deepis/db/store/relative/core/Information.h"

#include "com/deepis/db/store/relative/core/RealTimeExtra.h"
#include "com/deepis/db/store/relative/core/RealTimeShare.h"
#include "com/deepis/db/store/relative/core/RealTimeLocality.h"

using namespace cxx::util;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::locks;
using namespace cxx::util::concurrent::atomic;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

struct PagedSummary;
struct PagedSummarySet;
struct XInfoRef;

class RealTime : public Synchronizable {

	public:
		static const shorttype O_CREATE = 0x01;
		static const shorttype O_DELETE = 0x02;
		static const shorttype O_STREAM = 0x04;
		static const shorttype O_MEMORY = 0x08;
		static const shorttype O_MEMORYCOMPRESS = 0x10;
		static const shorttype O_CLEANUP = 0x20;
		static const shorttype O_SINGULAR = 0x40;
		static const shorttype O_PREFETCH = 0x80;
		static const shorttype O_FIXEDKEY = 0x100;
		static const shorttype O_ROWSTORE = 0x200;
		static const shorttype O_KEYCOMPRESS = 0x400;
		static const shorttype O_VALUECOMPRESS = 0x800;
		static const shorttype O_STATICCONTEXT = 0x1000;

		enum ErrorCode {
			ERR_GENERAL = -1,
			ERR_SUCCESS = 0,
			ERR_DEADLOCK = 1,
			ERR_NOT_FOUND = 2,
			ERR_DUPLICATE = 3,
			ERR_TIMEOUT = 4
		};

		enum ReadOption {
			EXACT = 1,
			EXACT_OR_NEXT = 2,
			NEXT = 3,
			NEXT_MATCH = 4,
			EXACT_OR_PREVIOUS = 5,
			PREVIOUS = 6,
			FIRST = 7,
			LAST = 8
		};

		enum WriteOption {
			STANDARD = 0,
			EXISTING = 1,
			OVERRIDE = 2,
			RESERVED = 3,
			UNIQUE = 4
		};

		enum DeleteOption {
			DELETE_RETURN = 0,
			DELETE_IGNORE = 1,
			DELETE_POPULATED = 2
		};

		// XXX: may be previously defined in fcntl.h included by RandomAccessFile.h
		#ifdef LOCK_NONE
		#undef LOCK_NONE
		#endif
		#ifdef LOCK_READ
		#undef LOCK_READ
		#endif
		#ifdef LOCK_WRITE
		#undef LOCK_WRITE
		#endif

		enum LockOption {
			LOCK_NONE = 0,
			LOCK_READ = 1,
			LOCK_WRITE = 2,
			LOCK_ROLL = 3
		};

		enum FileOption {
			FILE_KEY = 0,
			FILE_VALUE = 1,
			FILE_PAIRED = 2
		};

		enum MountState {
			MOUNT_OPENING = 1,
			MOUNT_OPENED = 2,
			MOUNT_CLOSING = 3,
			MOUNT_CLOSED = 4,
			MOUNT_PAUSED = 5,
			MOUNT_INITIAL = 6,
			MOUNT_IGNORED = 7
		};

		enum OptimizeOption {
			OPTIMIZE_ONLINE_KEY = 0,
			OPTIMIZE_ONLINE_VALUE = 1,
			OPTIMIZE_ONLINE_PAIRED = 2,
			OPTIMIZE_OFFLINE_KEY = 3,
			OPTIMIZE_OFFLINE_VALUE = 4,
			OPTIMIZE_OFFLINE_PAIRED = 5
		};

		enum IndexOrientation {
			INDEX_ORIENTATION_ROW = 0,
			INDEX_ORIENTATION_COLUMN = 1
		};

		typedef RealTimeLocality Locality;

	public:
		RealTime(const char* filePath, const longtype options, const shorttype keySize, const inttype valueSize):
			m_share(filePath, options, keySize, valueSize),
			m_primaryIndex(null),
			m_cycleSize(0),
			m_cacheSize(0),
			m_identifier(0),
			m_organizationTime(-1),
			m_externalWorkTime(-1),
			m_finalizationTime(-1),
			m_internalCheckpointRequest(false),
			m_currentLrtLocality(Locality::LOCALITY_NONE),
			m_checkptLrtLocality(Locality::LOCALITY_NONE),
			m_checkptRequested(false),
			m_checkptRequestLock(),
			m_endwiseLrtLocality(Locality::LOCALITY_NONE),
			m_summaryLrtLocality(Locality::LOCALITY_NONE),
			m_recoveryEpoch(1),
			m_recoverLrtLocality(Locality::LOCALITY_NONE),
			m_recoverIrtLength(-1),
			m_recoverIrtIndex(-1),
			m_recoverSize(-1),
			m_destructor(false),
			m_associated(false),
			m_skipcheck(false),
			m_dynamic(false),
			m_indexValue(0),
			m_checkpointLock(),
			m_checkpointComplete(true),
			m_checkpointValid(true),
			m_clobberLock(),
			m_manualCheckpointSequence(0) {

			if ((m_share.getOptions() & O_FIXEDKEY) == O_FIXEDKEY) {
				// XXX: chartype is stored as two bytes
				m_share.setKeyProtocol((keySize == 1) ? 2 : keySize);
				m_share.setKeyBuffer(m_share.getKeyProtocol());

			} else {
				m_share.setKeyProtocol(-1);
				m_share.setKeyBuffer(keySize);
			}

			m_share.setValueAverage(valueSize);

			m_identifier = Transaction::addIdentifier(m_primaryIndex == null);
		}

		virtual ~RealTime(void) {
			setDestructor(true);

			Transaction::removeIdentifier(m_identifier, m_associated, m_indexValue);
		}

		FORCE_INLINE void setPrimaryIndex(RealTime* primary) {
			m_primaryIndex = primary;
		}

		FORCE_INLINE RealTimeShare* getShare(void) {
			return &m_share;
		}

		FORCE_INLINE void setEntrySize(longtype size) {
			m_entrySize.set(size);
		}

		FORCE_INLINE longtype getEntrySize(void) const {
			return m_entrySize.get();
		}

		FORCE_INLINE const longtype getCycleSize(void) const {
			return m_cycleSize;
		}

		FORCE_INLINE const longtype getCacheSize(void) const {
			return m_cacheSize;
		}

		FORCE_INLINE void setIdentifier(longtype identifier) {
			m_identifier = identifier;
		}

		FORCE_INLINE const longtype getIdentifier(void) const {
			return m_identifier;
		}

		FORCE_INLINE longtype getOrganizationTime(void) const {
			return m_organizationTime;
		}

		FORCE_INLINE longtype getExternalWorkTime(void) const {
			return m_externalWorkTime;
		}

		FORCE_INLINE const Locality& getCurrentLrtLocality(void) const {
			return m_currentLrtLocality;
		}

		FORCE_INLINE const Locality& getCheckptLrtLocality(void) const {
			return m_checkptLrtLocality;
		}

		FORCE_INLINE void setCheckptLrtLocality(const Locality& locality) {
			m_checkptLrtLocality = locality;
		}

		FORCE_INLINE const Locality& getEndwiseLrtLocality(void) const {
			return m_endwiseLrtLocality;
		}

		FORCE_INLINE const Locality& getSummaryLrtLocality(void) const {
			return m_summaryLrtLocality;
		}

		FORCE_INLINE const uinttype getRecoveryEpoch(void) const {
			return (m_primaryIndex == null) ? m_recoveryEpoch : m_primaryIndex->m_recoveryEpoch;
		}

		FORCE_INLINE void setRecoveryEpoch(const uinttype recoveryEpoch) {
			if (m_primaryIndex == null) {
				m_recoveryEpoch = recoveryEpoch;
			} else {
				m_primaryIndex->m_recoveryEpoch = recoveryEpoch;
			}
		}

		FORCE_INLINE const Locality& getRecoverLrtLocality(void) const {
			return m_recoverLrtLocality.isNone() == true ? getEndwiseLrtLocality() : m_recoverLrtLocality;
		}

		FORCE_INLINE const inttype getRecoverIrtIndex(void) const {
			return m_recoverIrtIndex;
		}

		FORCE_INLINE const longtype getRecoverIrtLength(void) const {
			return m_recoverIrtLength;
		}

		FORCE_INLINE void setDestructor(boolean flag) {
			m_destructor = flag;
		}

		FORCE_INLINE const boolean getDestructor(void) const {
			return m_destructor;
		}

		FORCE_INLINE void setAssociated(boolean flag) {
			m_associated = flag;
		}

		FORCE_INLINE const boolean getAssociated(void) const {
			return m_associated;
		}

		FORCE_INLINE void setDynamic(boolean dynamic) {
			m_dynamic = dynamic;
		}

		FORCE_INLINE boolean getDynamic(void) const {
			return m_dynamic;
		}

		FORCE_INLINE void setMount(MountState mount) {
			m_mount = mount;
		}

		FORCE_INLINE MountState getMount(void) const {
			return m_mount;
		}

		FORCE_INLINE const bytetype getIndexValue(void) const {
			return m_indexValue;
		} 

		FORCE_INLINE void setRecoverPosition(inttype iindex, longtype ilength, const RealTimeLocality& lrtLocality, longtype entrySize) {
			m_recoverIrtIndex = iindex;
			m_recoverIrtLength = ilength;
			m_recoverLrtLocality = lrtLocality;
			m_recoverSize = entrySize;
		}

		FORCE_INLINE void clearRecoverPosition(void) {
			return setRecoverPosition(-1, -1L, RealTimeLocality::LOCALITY_NONE, -1L);
		}

		// XXX: following methods are public, but should not be used externally
		//
			virtual void sizeStorage(longtype* total) = 0;

			// XXX: following methods drive cache management
			virtual void sizeCache(void) = 0;
			virtual inttype purgeCache(boolean index, boolean deep, boolean log) = 0;
			virtual void indexCache(boolean cycle, boolean* cont, boolean* reorg) = 0;

			// XXX: following methods drive error management
			virtual void recoverGenesis(void) = 0;
			virtual void recoverBegin(void) = 0;
			virtual void recoverComplete(longtype entries, const Locality& locality, longtype elapsed, boolean rebuild = false) = 0;

			// XXX: following methods drive thread management
			virtual ConcurrentObject* getThreadContext(void) = 0;
			virtual void removeThreadContext(ConcurrentObject* context) = 0;
			virtual void removeThreadContexts(BasicArray<ConcurrentObject*>* contexts) = 0;
			virtual void setContextKey(Transaction* tx, const bytearray bkey /* TODO, int keynum */) = 0;

			// XXX: following methods drive secondary management
			virtual boolean indexPrimary(RealTime* primary, bytetype index, boolean dynamic, boolean recovery) = 0;
			virtual boolean indexSecondary(Transaction* tx, RealTime* secondary) = 0;

			virtual void absolveInformation(Transaction* tx, const bytearray pkey, const Information* curinfo) = 0;
			virtual void rekeyInformation(Transaction* tx, const bytearray pkey, const Information* newinfo, const Information* oldinfo) = 0;
			virtual void ejectInformation(Transaction* tx, const bytearray pkey, const Information* oldinfo, const Information* newinfo, const Information* chkinfo, LockOption lock, boolean ignore) = 0;

			virtual XInfoRef* primaryInformation(Transaction* tx, const bytearray pkey, nbyte* value, LockOption lock, bytearray infoRefBuf, inttype* errcode = null) = 0;

			virtual boolean lockInformation(Transaction* tx, const bytearray pkey, XInfoRef& curinfoRef, LockOption lock, boolean primaryLocked, boolean stitch) = 0;
			virtual boolean lockInformation(Transaction* tx, const bytearray pkey, XInfoRef& newinfoRef, XInfoRef& oldinfoRef, LockOption lock, Conductor* conductor) = 0;

			virtual void abortInformation(Transaction* tx, const bytearray pkey, const Information* orginfo, XInfoRef& preinfoRef, const Information* curinfo) = 0;

			virtual Information* decomposeInformation(Transaction* tx, bytetype index, const Information* next, inttype* position, bytearray* pkey) = 0;

			#ifdef DEEP_IO_STATS
			virtual void ioStatistics(ulongtype* oR, ulongtype* oC, ulongtype* oU, ulongtype* oD, ulongtype* fV, ulongtype* fH, ulongtype* dV, ulongtype* dH) = 0;
			#endif

			virtual void seekStatistics(ulongtype* oT, ulongtype* oI, boolean reset) = 0;

			virtual longtype findSummaryPaging(MeasuredRandomAccessFile* iwfile, const RealTimeLocality& lastLrtLocality, const uinttype recoveryEpoch) = 0;
			virtual boolean initSummaryPaging(MeasuredRandomAccessFile* iwfile) = 0;

			virtual PagedSummarySet* getActiveSummaries() = 0;
			virtual void invalidateSummary(const RealTime::Locality& streamLocality) = 0;

			virtual void destroyFile(MeasuredRandomAccessFile* file) = 0;

			virtual void deleteSegmentMemory(void) = 0;
			virtual void setMemoryMode(boolean flag) = 0;

			#ifdef DEEP_DEBUG
			virtual Information* getRoot(Transaction* tx, const bytearray pkey, const bytearray data) = 0;
			#endif
		//

		virtual const ExtraStatistics* getExtraStats(void) = 0;
		virtual const char* getFilePath(void) const = 0;
		virtual String getDataDirectory(boolean* needsLink = null) const = 0;
		virtual String getIndexDirectory(boolean* needsLink = null) const = 0;
		virtual int getErrorCode(void) = 0;

		virtual inttype getLrtIndex(void) const = 0;
		virtual uinttype getLrtPosition(void) const = 0;

		FORCE_INLINE static boolean requestManualCheckpoint() {
			s_globalManualCheckpointSequence.getAndIncrement();
			return true;
		}

		FORCE_INLINE static longtype getGlobalManualCheckpointSequence() {
			return s_globalManualCheckpointSequence.get();
		}

		FORCE_INLINE longtype getManualCheckpointSequence() const {
			return m_manualCheckpointSequence;
		}

		FORCE_INLINE void syncManualCheckpointSequence() {
			m_manualCheckpointSequence = s_globalManualCheckpointSequence.get();
		}

		FORCE_INLINE void checkpointLock() {
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			rt->m_checkpointLock.lock();
		}

		FORCE_INLINE void checkpointUnlock() {
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			rt->m_checkpointLock.unlock();
		}

		FORCE_INLINE boolean getCheckpointComplete(boolean lock = true) {
			boolean ret;
			if (lock == true) checkpointLock();
			{
				ret = m_checkpointComplete;
			}
			if (lock == true) checkpointUnlock();
			return ret;
		}

		FORCE_INLINE boolean getCheckpointValid(boolean lock = true) {
			boolean ret;
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			if (lock == true) checkpointLock();
			{
				ret = rt->m_checkpointValid;
			}
			if (lock == true) checkpointUnlock();
			return ret;
		}

		FORCE_INLINE void setCheckpointValid(boolean valid, boolean lock = true) {
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			if (lock == true) checkpointLock();
			{
				rt->m_checkpointValid = valid;
			}
			if (lock == true) checkpointUnlock();
		}

		FORCE_INLINE boolean checkpointFullyComplete(boolean lock = true, uinttype* viewpoint = null) {
			RealTime* pri = m_primaryIndex == null ? this : m_primaryIndex;
			if (lock == true) checkpointLock();
			boolean ret = pri->m_checkpointComplete;
			if ((ret == false) && (viewpoint != null)) {
				*viewpoint = pri->m_endwiseLrtLocality.getViewpoint();
			}
			for (inttype i=0; (ret == true) && (i<pri->getShare()->getSecondaryList()->size()); ++i) {
				RealTime* rt = pri->getShare()->getSecondaryList()->get(i);
				if (rt == null) {
					// XXX: allow for dynamic add/remove secondary index
					continue;
				}
				if (rt->m_checkpointComplete == false) {
					ret = false;
					if (viewpoint != null) {
						// XXX: use checkpoint locality here instead of endwise (for secondaries only)
						*viewpoint = rt->m_checkptLrtLocality.getViewpoint();
					}
				}
			}
			if (lock == true) checkpointUnlock();
			return ret;
		}

		FORCE_INLINE uinttype getActiveCheckpoint() {
			RealTime* pri = m_primaryIndex == null ? this : m_primaryIndex;
			uinttype ret = 0;
			RETRY:
			checkpointLock();
			// XXX: wait until the checkpoint is selected
			if (pri->m_checkptRequested == true) {
				checkpointUnlock();
				Thread::yield();
				goto RETRY;
			}
			if (checkpointFullyComplete(false, &ret) == true) {
				ret = 0;
			}
			checkpointUnlock();
			return ret;
		}

		FORCE_INLINE void setCheckpointComplete(boolean complete) {
			m_checkpointComplete = complete;
		}

		FORCE_INLINE void clobberLock() {
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			rt->m_clobberLock.writeLock();
		}

		FORCE_INLINE boolean tryClobberLock() {
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			return rt->m_clobberLock.tryWriteLock();
		}

		FORCE_INLINE void clobberUnlock() {
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			rt->m_clobberLock.writeUnlock();
		}

		FORCE_INLINE void clobberReadLock() {
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			rt->m_clobberLock.readLock();
		}

		FORCE_INLINE boolean tryClobberReadLock() {
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			return rt->m_clobberLock.tryReadLock();
		}

		FORCE_INLINE void clobberReadUnlock() {
			RealTime* rt = m_primaryIndex == null ? this : m_primaryIndex;
			rt->m_clobberLock.readUnlock();
		}

		FORCE_INLINE void internalCheckpointRequest() {
			RealTime* pri = m_primaryIndex == null ? this : m_primaryIndex;
			boolean req = true;
			req = __sync_lock_test_and_set(&pri->m_internalCheckpointRequest, req);
		}

		FORCE_INLINE boolean getAndResetInternalCheckpointRequest() {
			RealTime* pri = m_primaryIndex == null ? this : m_primaryIndex;
			return __sync_bool_compare_and_swap(&pri->m_internalCheckpointRequest, true, false);
		}

	protected:
		RealTimeShare m_share;
		RealTime* m_primaryIndex;

		AtomicInteger m_reindexing;
		AtomicLong m_entrySize;

		longtype m_cycleSize;
		longtype m_cacheSize;
		longtype m_identifier;

		longtype m_organizationTime;
		longtype m_externalWorkTime;
		longtype m_finalizationTime;

		boolean m_internalCheckpointRequest;

		Locality m_currentLrtLocality;

		Locality m_checkptLrtLocality;
		boolean m_checkptRequested;
		UserSpaceReadWriteLock m_checkptRequestLock;

		Locality m_endwiseLrtLocality;
		Locality m_summaryLrtLocality;
		uinttype m_recoveryEpoch;

		Locality m_recoverLrtLocality;
		longtype m_recoverIrtLength;

		inttype m_recoverIrtIndex;
		longtype m_recoverSize;

		boolean m_destructor:1;
		boolean m_associated:1;
		boolean m_skipcheck:1;
		boolean m_dynamic:1;

		MountState m_mount;
		bytetype m_indexValue;

		UserSpaceLock m_checkpointLock;
		boolean m_checkpointComplete;
		boolean m_checkpointValid;

		UserSpaceReadWriteLock m_clobberLock;

	private:
		// XXX: manual checkpoint request is global across all tables
		static AtomicLong s_globalManualCheckpointSequence;
		longtype m_manualCheckpointSequence;

	public:
		friend struct PagedSummarySet;
};

AtomicLong RealTime::s_globalManualCheckpointSequence = AtomicLong();

} } } } } } // namespace

namespace cxx { namespace util {

template<>
class Comparator<RealTime*>  {
	public:
		FORCE_INLINE Comparator(void) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const RealTime* o1, const RealTime* o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const RealTime* o1, const RealTime* o2) const {
		#endif
			
			longtype s1 = o1->getCacheSize();
			longtype s2 = o2->getCacheSize();

			return (s1 < s2) ? -1 : ((s1 == s2) ? 0 : 1);
		}
};

} }

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIME_H_*/
