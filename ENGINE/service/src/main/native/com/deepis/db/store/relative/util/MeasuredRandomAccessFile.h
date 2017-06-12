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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMELOCALITY_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMELOCALITY_H_CMP_
#include "com/deepis/db/store/relative/core/RealTimeLocality.h"
#undef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMELOCALITY_H_CMP_
#include "com/deepis/db/store/relative/core/RealTimeLocality.h"
#endif

#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MEASUREDRANDOMACCESSFILE_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MEASUREDRANDOMACCESSFILE_H_

#include "cxx/lang/System.h"
#include "cxx/util/concurrent/Synchronize.h"
#include "cxx/util/concurrent/atomic/AtomicLong.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/util/MapFileSet.h"
#include "com/deepis/db/store/relative/util/MapFileUtil.h"
#include "com/deepis/db/store/relative/util/BufferedRandomAccessFile.h"

using namespace cxx::lang;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::atomic;
using namespace com::deepis::db::store::relative::core;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

class MeasuredRandomAccessFile : public BufferedRandomAccessFile {

	private:
		static AtomicLong s_syncCount;
		static Synchronizable s_syncEvent;
		static BasicArray<MeasuredRandomAccessFile*> s_syncFiles;

	public:
		FORCE_INLINE static void planSynchronizeGlobally(void) {
			s_syncCount.incrementAndGet();
		}

		FORCE_INLINE static void prepareSynchronizeGlobally(MeasuredRandomAccessFile* wfile) {
			synchronized(s_syncEvent) {
				if (wfile->m_syncable == false) {
					wfile->m_syncable = true;
					s_syncFiles.add(wfile, true /* resize */);
				}
			}
		}

		FORCE_INLINE static void prepareSynchronizeGlobally(MeasuredRandomAccessFile* lwfile, MeasuredRandomAccessFile* vwfile) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(lwfile);
			CXX_LANG_MEMORY_DEBUG_ASSERT(vwfile);
			synchronized(s_syncEvent) {
				if ((lwfile->m_syncable == false) || (vwfile->m_syncable == false)) {

					// XXX: due to performance, vwfile is synced 1st (typically larger)
					vwfile->m_syncable = true;
					s_syncFiles.add(vwfile, true /* resize */);

					// XXX: due to performance, lwfile synced 2nd (typically smaller)
					lwfile->m_syncable = true;
					s_syncFiles.add(lwfile, true /* resize */);
				}
			}
			CXX_LANG_MEMORY_DEBUG_ASSERT(lwfile);
			CXX_LANG_MEMORY_DEBUG_ASSERT(vwfile);
		}

		FORCE_INLINE static longtype performSynchronizeGlobally(boolean decrement = true /* for non-planned external invocation */, boolean holddown = false) {
			longtype start = 0;
			if (holddown == true) {
				start = System::currentTimeMillis();
			}

			synchronized(s_syncEvent) {
				if ((decrement == false) || (s_syncCount.decrementAndGet() == 0)) {
					for (inttype i = 0; i < s_syncFiles.size(); i++) {
						MeasuredRandomAccessFile* file = s_syncFiles.get(i);
						if (file == null) {
							continue;
						}
						CXX_LANG_MEMORY_DEBUG_ASSERT(file);
						if (file->syncPrepared() == true) {
							file->syncPerform();
						}
					}

					s_syncFiles.size(0);

					s_syncEvent.notifyAll();

				} else {
					s_syncEvent.wait();
				}
			}

			longtype stop = 0;
			if (holddown == true) {
				stop = System::currentTimeMillis();
			}

			return stop-start;
		}

		FORCE_INLINE static void syncAndCloseFiles(MeasuredRandomAccessFile* vwfile, MeasuredRandomAccessFile* lwfile) {
			synchronized(s_syncEvent) {
				vwfile->syncPerform(false);
				lwfile->syncPerform(false);

				vwfile->setOnline(false);
				lwfile->setOnline(false);
			}
		}

	private:
		MapFileUtil::FileType m_type;

		ulongtype m_optimizeCount;

		longtype m_objectCreationTime;

		ulongtype m_initialLength;
		RealTimeLocality m_initialLrtLocality;
		RealTimeLocality m_endwiseLrtLocality;
		volatile uinttype m_refcnt;

		doubletype m_percentFragmented;
		doubletype m_percentCompressed;

		ushorttype m_cycleCount;
		ushorttype m_reorgIgnoreCount;

		inttype m_secondaryIndex;
		inttype m_summaryState;
		longtype m_lastIrtLength;
		RealTimeLocality m_lastLrtLocality;
		longtype m_entrySize;
		longtype m_deadCount;
		longtype m_totalCount;
		uinttype m_recoveryEpoch;

		bytetype m_indexValue;
		longtype m_awaitingDeletionLength;

		boolean m_closureFile:1;
		boolean m_reorgComplete:1;

		boolean m_awaitingDeletion:1;
		boolean m_invalidSummary:1;

		// XXX: note: only set at runtime, not available on startup files
		boolean m_containsCheckpoint:1;

	public:
		FORCE_INLINE void syncPerform(boolean deactivate = true) {
			// XXX: m_syncable to false in fdsync
			fdsync();

			// XXX: not actively being used
			if ((deactivate == true) && (tryLock() == true)) {
				setActive(false);

				unlock();
			}
		}

		FORCE_INLINE boolean syncPrepared(void) {
			return m_syncable;
		}

	public:
		FORCE_INLINE uinttype ref(void) {
			return m_refcnt;
		}

		FORCE_INLINE uinttype incref(void) {
			return __sync_add_and_fetch(&m_refcnt, 1);
		}

		FORCE_INLINE uinttype decref(void) {
			return __sync_sub_and_fetch(&m_refcnt, 1);
		}

	public:
		MeasuredRandomAccessFile(const char* path, const char* mode, MapFileUtil::FileType type, inttype bufferSize = BUFFER_SIZE):
			BufferedRandomAccessFile(path, mode, bufferSize),
			m_type(type),
			m_optimizeCount(0),
			m_objectCreationTime(System::currentTimeMillis()),
			m_initialLength(0),
			m_initialLrtLocality(RealTimeLocality::LOCALITY_NONE),
			m_endwiseLrtLocality(RealTimeLocality::LOCALITY_NONE),
			m_refcnt(0),
			m_percentFragmented(0.0),
			m_percentCompressed(0.0),
			m_cycleCount(0),
			m_reorgIgnoreCount(0),
			m_secondaryIndex(-1),
			m_summaryState(-1),
			m_lastIrtLength(-1),
			m_lastLrtLocality(RealTimeLocality::LOCALITY_NONE),
			m_entrySize(-1),
			m_deadCount(0),
			m_totalCount(0),
			m_recoveryEpoch(0),
			m_indexValue(0),
			m_awaitingDeletionLength(0),
			m_closureFile(false),
			m_reorgComplete(false),
			m_awaitingDeletion(false),
			m_invalidSummary(false),
			m_containsCheckpoint(false) {
			CXX_LANG_MEMORY_DEBUG_INIT()

			if (Properties::getRangeSync() == true) {
				RandomAccessFile::setRangeSync((m_type == MapFileUtil::LRT) || (m_type == MapFileUtil::VRT) || (m_type == MapFileUtil::TRT));
				RandomAccessFile::setRangeBlock(Properties::DEFAULT_FILE_RANGE_SYNC_CHUNK);
				RandomAccessFile::setForceMaxRangeBlock(m_type == MapFileUtil::TRT);
			}
		}

		MeasuredRandomAccessFile(const File* file, const char* mode, MapFileUtil::FileType type, inttype bufferSize = BUFFER_SIZE):
			BufferedRandomAccessFile(file, mode, bufferSize),
			m_type(type),
			m_optimizeCount(0),
			m_objectCreationTime(System::currentTimeMillis()),
			m_initialLength(0),
			m_initialLrtLocality(RealTimeLocality::LOCALITY_NONE),
			m_endwiseLrtLocality(RealTimeLocality::LOCALITY_NONE),
			m_refcnt(0),
			m_percentFragmented(0.0),
			m_percentCompressed(0.0),
			m_cycleCount(0),
			m_reorgIgnoreCount(0),
			m_secondaryIndex(-1),
			m_summaryState(-1),
			m_lastIrtLength(-1),
			m_lastLrtLocality(RealTimeLocality::LOCALITY_NONE),
			m_entrySize(-1),
			m_deadCount(0),
			m_totalCount(0),
			m_recoveryEpoch(0),
			m_indexValue(0),
			m_awaitingDeletionLength(0),
			m_closureFile(false),
			m_reorgComplete(false),
			m_awaitingDeletion(false),
			m_invalidSummary(false), 
			m_containsCheckpoint(false) {
			CXX_LANG_MEMORY_DEBUG_INIT()

			if (Properties::getRangeSync() == true) {
				RandomAccessFile::setRangeSync((m_type == MapFileUtil::LRT) || (m_type == MapFileUtil::VRT) || (m_type == MapFileUtil::TRT));
				RandomAccessFile::setRangeBlock(Properties::DEFAULT_FILE_RANGE_SYNC_CHUNK);
				RandomAccessFile::setForceMaxRangeBlock(m_type == MapFileUtil::TRT);
			}
		}

		virtual ~MeasuredRandomAccessFile() {
			// XXX: due to semi-sync
			synchronized(s_syncEvent) {
				if (syncPrepared() == true) {
					syncPerform();
				}
				for (inttype i = 0; i < s_syncFiles.size(); ++i) {
					MeasuredRandomAccessFile* file = s_syncFiles.get(i);
					if (file == this) {
						s_syncFiles.set(i, null);
					}
				}
			}
		}

		FORCE_INLINE MapFileUtil::FileType getType() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_type;
		}

		FORCE_INLINE void setOptimizeCount(ulongtype count) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_optimizeCount = count;
		}

		FORCE_INLINE ulongtype getOptimizeCount(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_optimizeCount;
		}

		FORCE_INLINE void setObjectCreationTime(longtype time) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_objectCreationTime = time;
		}

		FORCE_INLINE longtype getObjectCreationTime(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_objectCreationTime;
		}

		FORCE_INLINE void setInitialLength(ulongtype length) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_initialLength = length;
		}

		FORCE_INLINE ulongtype getInitialLength(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_initialLength;
		}

		FORCE_INLINE void setInitialLrtLocality(const RealTimeLocality& locality) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_initialLrtLocality = locality;
			m_endwiseLrtLocality = locality;
		}

		FORCE_INLINE const RealTimeLocality& getInitialLrtLocality(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_initialLrtLocality;
		}

		FORCE_INLINE void setEndwiseLrtLocality(const RealTimeLocality& locality) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_endwiseLrtLocality = locality;
		}

		FORCE_INLINE const RealTimeLocality& getEndwiseLrtLocality(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_endwiseLrtLocality;
		}

		FORCE_INLINE void setClosureFile(boolean flag) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_closureFile = flag;
		}

		FORCE_INLINE boolean getClosureFile(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_closureFile;
		}

		FORCE_INLINE void incrementReorgIgnoreCount() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_reorgIgnoreCount++;
		}

		FORCE_INLINE void resetReorgIgnoreCount() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_reorgIgnoreCount = 0;
		}

		FORCE_INLINE ushorttype getReorgIgnoreCount() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_reorgIgnoreCount;
		}

		FORCE_INLINE void setCycleCount(ushorttype count) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_cycleCount = count;
		}

		FORCE_INLINE ushorttype getCycleCount(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_cycleCount;
		}

		FORCE_INLINE void setReorganizeComplete(boolean flag) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_reorgComplete = flag;
		}

		FORCE_INLINE boolean getReorganizeComplete(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_reorgComplete;
		}

		FORCE_INLINE void setIndexValue(const bytetype indexValue) {
			if (m_awaitingDeletion == false) {
				m_indexValue = indexValue;
			}
		}

		FORCE_INLINE const bytetype getIndexValue(void) const {
			return m_indexValue;
		}

		FORCE_INLINE void setAwaitingDeletion(const bytetype indexValue, boolean flag) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			const boolean wasAwaitingDeletion = m_awaitingDeletion;
			m_awaitingDeletion = flag;
			if ((flag == true) && (wasAwaitingDeletion == false)) {
				m_indexValue = indexValue;
				m_awaitingDeletionLength = length();
				if (m_awaitingDeletionLength < 0) {
					#ifdef DEEP_DEBUG
					DEEP_LOG(ERROR, OTHER, "Invalid file length (deferred deletion): %lld, %s\n", m_awaitingDeletionLength, getPath());
					throw InvalidException("Invalid file length");
					#else
					m_awaitingDeletionLength = 0;
					#endif
				}
			}
		}

		FORCE_INLINE boolean getAwaitingDeletion() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_awaitingDeletion;
		}

		FORCE_INLINE longtype getAwaitingDeletionLength() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_awaitingDeletionLength;
		}

		FORCE_INLINE void setInvalidSummary(boolean flag) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_invalidSummary = flag;
		}

		FORCE_INLINE boolean getInvalidSummary() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_invalidSummary;
		}

		FORCE_INLINE void setContainsCheckpoint(boolean flag) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_containsCheckpoint = flag;
		}

		FORCE_INLINE boolean getContainsCheckpoint() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_containsCheckpoint;
		}

		FORCE_INLINE virtual void write(int b) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			BufferedRandomAccessFile::write(b);
		}

		FORCE_INLINE virtual void write(const nbyte* bytes) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			BufferedRandomAccessFile::write(bytes);
		}

		FORCE_INLINE virtual void write(const nbyte* bytes, int offset, int length) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			BufferedRandomAccessFile::write(bytes, offset, length);
		}

		FORCE_INLINE void writeRaw(const nbyte* bytes, int offset, int length) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			BufferedRandomAccessFile::write(bytes, offset, length);
		}

		FORCE_INLINE void setPercentFragmented(doubletype value) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_percentFragmented = value;
		}

		FORCE_INLINE doubletype getPercentFragmented(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_percentFragmented;
		}

		FORCE_INLINE void setPercentCompressed(doubletype value) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_percentCompressed = value;
		}

		FORCE_INLINE doubletype getPercentCompressed(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_percentCompressed;
		}

		// XXX: see validateKeyPaging()
		FORCE_INLINE void setPagingState(inttype secondaryIndex, inttype summaryState, longtype lastIrtLength, const RealTimeLocality& lastLrtLocality) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_secondaryIndex = secondaryIndex;
			m_summaryState = summaryState;
			m_lastIrtLength = lastIrtLength;
			m_lastLrtLocality = lastLrtLocality;
		}

		FORCE_INLINE void clearPagingState() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			setPagingState(-1, -1, -1L, RealTimeLocality::LOCALITY_NONE);
		}

		FORCE_INLINE void setEntrySize(longtype entrySize) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_entrySize = entrySize;
		}

		FORCE_INLINE longtype getEntrySize() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_entrySize;
		}

		FORCE_INLINE void setDeadCount(uinttype count) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_deadCount = count;
		}

		FORCE_INLINE void incrementDeadCount(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_deadCount++;
		}

		FORCE_INLINE uinttype getDeadCount(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_deadCount;
		}

		FORCE_INLINE void setTotalCount(uinttype count) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_totalCount = count;
		}

		FORCE_INLINE void incrementTotalCount(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			m_totalCount++;
		}

		FORCE_INLINE uinttype getTotalCount(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_totalCount;
		}

		FORCE_INLINE uinttype getRecoveryEpoch(void) const {
			return m_recoveryEpoch;
		}

		FORCE_INLINE void setRecoveryEpoch(uinttype recoveryEpoch) {
			m_recoveryEpoch = recoveryEpoch;
		}

		FORCE_INLINE inttype getSecondaryIndex() const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_secondaryIndex;
		}

		FORCE_INLINE inttype getSummaryState(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_summaryState;
		}

		FORCE_INLINE longtype getLastIrtLength(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_lastIrtLength;
		}

		FORCE_INLINE const RealTimeLocality& getLastLrtLocality(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			return m_lastLrtLocality;
		}
};

AtomicLong MeasuredRandomAccessFile::s_syncCount(0);
Synchronizable MeasuredRandomAccessFile::s_syncEvent;
BasicArray<MeasuredRandomAccessFile*> MeasuredRandomAccessFile::s_syncFiles(Properties::DEFAULT_FILE_ARRAY, false);

} } } } } } // namespace

#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MEASUREDRANDOMACCESSFILE_CXX_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MEASUREDRANDOMACCESSFILE_CXX_

namespace cxx { namespace util {

using namespace com::deepis::db::store::relative::util;

template<>
class Comparator<MeasuredRandomAccessFile*>  {
	private:
		MapFileSet<MeasuredRandomAccessFile>* m_lrtWriteFileList;

		FORCE_INLINE inttype orderOf(inttype lrtFileIndex) const {
			/* TODO: this can be optimized to avoid the linear search */
			Iterator<MeasuredRandomAccessFile*>* iter = m_lrtWriteFileList->iterator();
			for (inttype i=0; iter->hasNext() == true; ++i) {
				MeasuredRandomAccessFile* lwfile = iter->next();
				if ((lwfile != null) && (lwfile->getFileIndex() == lrtFileIndex)) {
					Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
					return i;
				}
			}
			Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);

			/* XXX: not found */
			return -1;
		}

	public:
		FORCE_INLINE Comparator() :
			m_lrtWriteFileList(null) {
			// nothing to do
		}

		FORCE_INLINE Comparator(MapFileSet<MeasuredRandomAccessFile>* lrtWriteFileList) :
			m_lrtWriteFileList(lrtWriteFileList) {
			// nothing to do
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const MeasuredRandomAccessFile* o1, const MeasuredRandomAccessFile* o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const MeasuredRandomAccessFile* o1, const MeasuredRandomAccessFile* o2) const {
		#endif
			longtype delta = 0;
			{
				const inttype ord1 = orderOf(o1->getLastLrtLocality().getIndex());
				const inttype ord2 = orderOf(o2->getLastLrtLocality().getIndex());
				if ((ord1 != -1) && (ord2 != -1)) {
					delta = (inttype) (ord1 - ord2);
					if (delta != 0) {
						return -delta;
					}
				}
			}

			delta = o1->getLastLrtLocality().getIndex() - o2->getLastLrtLocality().getIndex();
			if (delta != 0) {
				return -delta;
			}

			delta = o1->getLastLrtLocality().getLength() - o2->getLastLrtLocality().getLength();
			if (delta != 0) {
				return -delta;
			}

			delta = o1->getFileCreationTime() - o2->getFileCreationTime();
			return -(delta != 0 ? delta : o1->getFileIndex() - o2->getFileIndex());
		}

		FORCE_INLINE int compare(const MeasuredRandomAccessFile* o1, const inttype lastLrtIndex, const longtype lastLrtLength) const {
			longtype delta = 0;
			{
				const inttype ord1 = orderOf(o1->getLastLrtLocality().getIndex());
				const inttype ord2 = orderOf(lastLrtIndex);
				if ((ord1 != -1) && (ord2 != -1)) {
					delta = (inttype) (ord1 - ord2);
					if (delta != 0) {
						return -delta;
					}
				}
			}

			delta = o1->getLastLrtLocality().getIndex() - lastLrtIndex;
			if (delta != 0) {
				return -delta;
			}

			delta = o1->getLastLrtLocality().getLength() - lastLrtLength;
			return -delta;
		}
};

} } // namespace

#endif

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MEASUREDRANDOMACCESSFILE_H_*/
