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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEMAP_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEMAP_H_ 

#include "cxx/lang/nbyte.h"
#include "cxx/util/PriorityQueue.h"
#include "cxx/util/QueueSet.h"
#include "cxx/util/TreeMap.h"
#include "cxx/util/concurrent/atomic/AtomicLong.h"
#include "com/deepis/db/store/relative/util/Versions.h"

#ifdef DEEP_USERLOCK
	#include "cxx/util/concurrent/locks/UserSpaceLock.h"
#else
	#include "cxx/util/concurrent/locks/ReentrantLock.h"
#endif

#include "cxx/io/File.h"

#include "com/deepis/db/store/relative/util/InvalidException.h"
#include "com/deepis/db/store/relative/util/LockableArrayList.h"
#include "com/deepis/db/store/relative/util/BufferedRandomAccessFile.h"
#include "com/deepis/db/store/relative/util/MeasuredRandomAccessFile.h"

#include "com/deepis/db/store/relative/core/Segment.h"
#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/Information.h"

#include "com/deepis/db/store/relative/core/RealTimeIndex.h"
#include "com/deepis/db/store/relative/core/RealTimeResource.h"
#include "com/deepis/db/store/relative/core/RealTimeContext.h"

#include "com/deepis/db/store/relative/core/RealTimeBuilder.h"
#include "com/deepis/db/store/relative/core/RealTimeConverter.h"
#include "com/deepis/db/store/relative/core/RealTimeSchema.h"
#include "com/deepis/db/store/relative/core/RealTimeCompress.h"
#include "com/deepis/db/store/relative/core/RealTimeSummary.h"
#include "com/deepis/db/store/relative/core/RealTimeTransaction.h"
#include "com/deepis/db/store/relative/core/RealTimeUtilities.h"

using namespace cxx::io;
using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent::locks;
using namespace cxx::util::concurrent::atomic;
using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template <typename K> class RealTimeIterator;
template <typename K> class RealTimeDiscover;
template <typename K> class RealTimeRecovery;
template <typename K> class RealTimeConductor;
template <typename K> class RealTimeUtilities;

template <typename K> struct RealTimeSchema_v1;
template <typename K> struct RealTimeAdaptive_v1;
template <typename K> struct RecoveryWorkspace_v1_0;
template <typename K> struct RecoveryWorkspace_v1_1;
template <int V, typename K> struct RealTimeProtocol_v1_0_0_0;
template <int V, typename K> struct RealTimeProtocol_v1_1_0_0;
template <int V, typename K> struct SegmentMetadata_v1_1;
template <int V, typename K> struct WriteKeyPaging;

// TODO: split RealTimeMap into Index and Map (i.e. RealTimeIndex, RealTimeMap) where Map derives from Index

template<typename K>
class RealTimeMap /* XXX: pseudo-map/tree interface */ : public RealTimeIndex {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;
	typedef InfoReference<K> InfoRef;
	typedef typename TreeMap<K,Segment<K>*>::TreeMapEntryIterator MapSegmentIterator;
	typedef typename TreeMap<K,Segment<K>*>::TreeMapEntrySet::EntrySetIterator MapSegmentEntrySetIterator;
	typedef typename SegTreeMap::TreeMapEntryIterator MapInformationEntryIterator;
	typedef typename SegTreeMap::TreeMapEntrySet::EntrySetIterator MapInformationEntrySetIterator;
	typedef typename TreeMap<K,boolean>::TreeMapEntrySet::EntrySetIterator MapBooleanEntrySetIterator;

	private:
		enum MapState {
			MAP_RUNNING = 1,
			MAP_INDXING = 2,
			MAP_RECOVER = 3,
			MAP_EXITING = 4,
			MAP_STOPPED = 5,
			MAP_INITIAL = 6
		};

		enum SummaryState {
			SUMMARY_ERROR = -1,
			SUMMARY_INTACT = 0,
			SUMMARY_IGNORE = 1,
			SUMMARY_EMPTY = 2
		};

		enum OrderedSegmentMode {
			MODE_INDEX = 0,
			MODE_CHECK = 1,
			MODE_REORG = 2
		};

		static const Comparator<K> COMPARATOR;
		static const KeyBuilder<K> KEY_BUILDER;
		static const SchemaBuilder<K> SCHEMA_BUILDER;

	public:
		class OrderedSegmentCmp : public Comparator<Segment<K>*> {
			private:
				const Comparator<K>* m_comparator;
			public:
				FORCE_INLINE OrderedSegmentCmp(const Comparator<K>* comparator) :
					m_comparator(comparator) {
				}

				FORCE_INLINE virtual ~OrderedSegmentCmp() {
					// XXX: nothing to do
				}

				#ifdef COM_DEEPIS_DB_CARDINALITY
				FORCE_INLINE int compare(const Segment<K>* o1, const Segment<K>* o2, inttype* pos = null) const {
					return m_comparator->compare(o1->getIndexOrderKey(), o2->getIndexOrderKey(), pos);
				#else
				FORCE_INLINE int compare(const Segment<K>* o1, const Segment<K>* o2) const {
					return m_comparator->compare(o1->getIndexOrderKey(), o2->getIndexOrderKey());
				#endif
				}
		};

	private:
		typedef QueueSet<Segment<K>*, PriorityQueue<Segment<K>*, OrderedSegmentCmp>, HashSet<Segment<K>*>, UserSpaceLock> OrderedSegmentList;

		MapState m_state;
		RealTimeResource m_resource;
		ExtraStatistics m_extraStats;

		const File& m_filePath;

		/* const */ boolean m_memoryMode;
		const boolean m_cleanupMode;
		const boolean m_singularMode;
		const boolean m_prefetchMode;
		const boolean m_rowStoreMode;
		/* const */ boolean m_keyCompressMode;
		/* const */ boolean m_valueCompressMode;
		/* const */ boolean m_memoryCompressMode;

		const Comparator<K>* m_comparator;
		const KeyBuilder<K>* m_keyBuilder;

		const SchemaBuilder<K>* m_schemaBuilder;

		const inttype m_irtHeader;
		const inttype m_irtBuffer;
		const inttype m_irtOffset;
		const inttype m_lrtOffset;

		uinttype m_resetIndex;
		uinttype m_resetFileRefCheck;
		inttype m_quantaDirty;
		inttype m_pagingIndex;
		inttype m_streamIndex;

		ulongtype m_optimizePaging;
		ulongtype m_optimizeStream;

		ulongtype m_activeKeyBlockSize;
		uinttype  m_activeKeyBlockCount;

		ulongtype m_valueCompressedBlockSize;
		uinttype  m_valueCompressedBlockCount;

		ulongtype m_incrementCount;
		ulongtype m_activeValueSize;
		ulongtype m_activeValueCount;
		ulongtype m_schemaCheckpoint;

		longtype m_statisticsFlushTime;

		AtomicLong m_purgeSize;
		AtomicLong m_coldPointRead;
		boolean m_coldPointReadLimitReached;

		boolean m_resetIterator;
		boolean m_threadIndexing;
		boolean m_hasReservation;
		boolean m_hasSecondaryMaps;

		boolean m_irtBuildReorganization;
		boolean m_irtForceReorganization;
		boolean m_vrtForceReorganization;

		#ifdef DEEP_USERLOCK
		UserSpaceLock m_reserveLock;
		#else
		ReentrantLock m_reserveLock;
		#endif

		RealTimeContext<K> m_threadContext;

		LockableArrayList<Segment<K>*> m_deletedSegmentList;
		OrderedSegmentCmp m_orderSegmentCmp;
		OrderedSegmentList m_orderSegmentList;
		OrderedSegmentMode m_orderSegmentMode;

		TreeMap<K,Segment<K>*> m_branchSegmentTreeMap;

		PagedSummarySet* m_summaries;
		boolean m_checkptTriggered;
		longtype m_fileCleanupTime;

		Comparator<Locality&>* m_localityCmp;

		ulongtype m_pendingCommits;

		bytetype m_compressionRatioKey;
		bytetype m_compressionRatioKeyValue;
		ubytetype m_failedPurgeCycles;

		RealTimeResource::Limit m_cycleCacheUsage;

		typename TreeMap<K,Segment<K>*>::TreeMapEntrySet m_orderSegmentSet;
		typename TreeMap<K,Segment<K>*>::TreeMapEntrySet m_purgeSegmentSet;
		typename SegTreeMap::TreeMapEntrySet m_fillInformationSet;
		typename SegTreeMap::TreeMapEntrySet m_indexInformationSet;
		typename SegTreeMap::TreeMapEntrySet m_purgeInformationSet;
		typename SegTreeMap::TreeMapEntrySet m_rolloverInformationSet;

	private:
		static longtype getCacheSize(longtype elements, shorttype keySize, inttype valSize);
		static inttype getBufferSize(shorttype keyOffset, shorttype keySize, shorttype segSize);

		FORCE_INLINE static Locality getLocality(BufferedRandomAccessFile* lwfile, RealTimeConductor<K>* conductor) {
			return Locality(lwfile->getFileIndex(), lwfile->BufferedRandomAccessFile::getFilePointer(), (conductor != null) ? (conductor->getTransaction()->getViewpoint() + 1 /* +1 due to view linking */) : Transaction::getNextViewpoint());
		}

		FORCE_INLINE Comparator<Locality&>* getLocalityCmp() {
			if (m_localityCmp == null) {
				RealTime* primary = (m_primaryIndex == null) ? this : m_primaryIndex;
				Comparator<Locality&>* localityCmp = new Comparator<Locality&>(primary->getShare()->getLrtWriteFileList());
				if (__sync_bool_compare_and_swap(&m_localityCmp, null, localityCmp) == false) {
					delete localityCmp;
				}
			}
			return m_localityCmp;
		}

	private:
		// XXX: following methods override RealTime
		//
			virtual void sizeStorage(longtype* total);

			// XXX: following methods drive cache management
			virtual void sizeCache(void);
			virtual inttype purgeCache(boolean index, boolean deep, boolean log);
			virtual void indexCache(boolean cycle, boolean* cont, boolean* reorg);

			// XXX: following methods drive error management
			virtual void recoverGenesis(void);
			virtual void recoverBegin(void);
			virtual void recoverComplete(longtype entries, const Locality& locality, longtype elapsed, boolean rebuild = false);

			// XXX: following methods drive thread management
			virtual ConcurrentObject* getThreadContext(void);
			virtual void removeThreadContext(ConcurrentObject* context);
			virtual void removeThreadContexts(BasicArray<ConcurrentObject*>* contexts);
			virtual void setContextKey(Transaction* tx, const bytearray bkey /* TODO, int keynum */);

			FORCE_INLINE ThreadContext<K>* getTransactionContext(Transaction* tx);
			FORCE_INLINE ThreadContext<K>* getConductorContext(Transaction* tx, RealTimeConductor<K>* conductor);

			// XXX: following methods drive secondary management
			virtual boolean indexPrimary(RealTime* primary, bytetype index, boolean dynamic, boolean recovery);
			virtual boolean indexSecondary(Transaction* tx, RealTime* secondary);

			virtual void absolveInformation(Transaction* tx, const bytearray pkey, const Information* curinfo);
			virtual void rekeyInformation(Transaction* tx, const bytearray pkey, const Information* newinfo, const Information* oldinfo);
			virtual void ejectInformation(Transaction* tx, const bytearray pkey, const Information* delinfo, const Information* curinfo, const Information* chkinfo, LockOption lock, boolean ignore);

			virtual XInfoRef* primaryInformation(Transaction* tx, const bytearray pkey, nbyte* value, LockOption lock, bytearray infoRefBuf, inttype* errcode = null);

			virtual boolean lockInformation(Transaction* tx, const bytearray pkey, XInfoRef& curinfoRef, LockOption lock, boolean primaryLocked, boolean stitch);
			virtual boolean lockInformation(Transaction* tx, const bytearray pkey, XInfoRef& newinfoRef, XInfoRef& oldinfoRef, LockOption lock, Conductor* conductor);

			virtual void abortInformation(Transaction* tx, const bytearray pkey, const Information* orginfo, XInfoRef& preinfoRef, const Information* curinfo);

			virtual Information* decomposeInformation(Transaction* tx, bytetype index, const Information* next, inttype* position, bytearray* pkey);

			#ifdef DEEP_IO_STATS
			virtual void ioStatistics(ulongtype* oR, ulongtype* oC, ulongtype* oU, ulongtype* oD, ulongtype* fV, ulongtype* fH, ulongtype* dV, ulongtype* dH);
			#endif

			virtual void seekStatistics(ulongtype* oT, ulongtype* oI, boolean reset);

			virtual longtype findSummaryPaging(MeasuredRandomAccessFile* iwfile, const RealTimeLocality& lastLrtLocality, const uinttype recoveryEpoch);
			virtual boolean initSummaryPaging(MeasuredRandomAccessFile* iwfile);

			virtual PagedSummarySet* getActiveSummaries();
			virtual void invalidateSummary(const RealTime::Locality& streamLocality);

			#ifdef DEEP_DEBUG
			virtual Information* getRoot(Transaction* tx, const bytearray pkey, const bytearray data);
			#endif
		//

		inline void initialize(void);

		inline void seedIncrement(void);
		inline void seedFinalFile(void);
		inline void seedValueAverage(void);
		inline ulongtype maximumKey(void);

		inline inttype getTotalSegments(void);
		inline inttype getActiveSegments(void);
		inline inttype getPurgedSegments(void);

		FORCE_INLINE uinttype getFileSize(boolean adaptive = true);

		FORCE_INLINE inttype getSegmentSize(boolean adaptive = true);

		FORCE_INLINE inttype getSegmentMinimumByteSize(void) {
			return Properties::DEFAULT_SEGMENT_BYTE_SIZE / 3;
		}

		FORCE_INLINE inttype getSegmentMinimumSize(boolean adaptive = true) {
			return getSegmentSize(adaptive) / 3;
		}

		FORCE_INLINE inttype getSegmentBufferedSize(boolean adaptive = true) {
			return getSegmentSize(adaptive) * 2;
		}

		inline boolean pendingCheckpoint(void);
		inline boolean toggleCheckpointMetrics();
		inline boolean selectCheckpointLocality(void);
		inline boolean toggleStreamingMetrics(boolean save, boolean checkpoint, boolean idle = false);
		inline void abortCheckpoint(const RealTimeLocality& streamLocality);

		inline void addStreamFile(boolean release);
		inline void addPagingFile(boolean release);
		inline virtual void destroyFile(MeasuredRandomAccessFile* file);

		inline void flushSegmentMemory(void);
		inline void deleteSegmentMemory(void);
		
		FORCE_INLINE virtual void setMemoryMode(boolean flag) {
			m_memoryMode = flag;
		}

		inline virtual void deleteCacheAndFileMemory(void);

		inline void abortRealTime(RealTimeConductor<K>* conductor);
		inline void commitRealTime(RealTimeConductor<K>* conductor);
		inline void rolloverRealTime(ThreadContext<K>* ctxt, Segment<K>* segment);

		inline void mergeCacheMemory(RealTimeConductor<K>* conductor);
		inline void abortCacheMemory(RealTimeConductor<K>* conductor);
		inline void commitCacheMemory(RealTimeConductor<K>* conductor);

		inline void rolloverSegment(ThreadContext<K>* ctxt, Segment<K>* segment);

		inline inttype getSecondaryCount(void);
		void addSecondary(RealTime* secondary, boolean hasPrimary, boolean dynamic);
		void removeSecondary(RealTime* secondary, /* boolean hadPrimary, */ boolean dynamic);

		inline inttype deleteSegments(void);
		inline inttype purgeSegments(BasicArray<Segment<K>*>* segments, ThreadContext<K>* ctxt, boolean index);
		inline inttype compressSegments(BasicArray<Segment<K>*>* segments, ThreadContext<K>* ctxt, boolean index, boolean growing);
		inline inttype orderSegments(ThreadContext<K>* ctxt, boolean reset, boolean timeout, boolean* cont, boolean* reorg, inttype* request = null, inttype* purge = null, uinttype viewpoint = 0, RealTimeSummary<K>* summaryWorkspace = null, IndexReport* indexReport = null);

		inline inttype indexCacheManagement(boolean* cont, boolean* reorg);
		inline inttype purgeCacheManagement(inttype active, boolean index, inttype* compressed, inttype* compressedDataPurge, PurgeReport& purgeReport);
		FORCE_INLINE boolean needsIndexing(Segment<K>* segment, const uinttype viewpoint, const boolean final, boolean reorg, boolean* summarize, boolean* backwardCheckpoint);

		inline uinttype onlineSummarizeSegments(ThreadContext<K>* ctxt, RealTimeSummary<K>& workspace);
		inline inttype onlineFinalizingSegments(ThreadContext<K>* ctxt);

		inline boolean reorganizeFiles(ThreadContext<K>* ctxt, boolean* cont, boolean valueReorganize, boolean valueCompression);

		FORCE_INLINE ushorttype pagingFileManagement(boolean lock, boolean create, const boolean needsClobberLock = true);
		FORCE_INLINE ushorttype streamFileManagement(boolean lock, boolean create);

		FORCE_INLINE boolean markSegment(ThreadContext<K>* ctxt, Segment<K>* segment, ushorttype curIndex);
		FORCE_INLINE void fillSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean values, boolean pace);

		FORCE_INLINE boolean scrubSegment(ThreadContext<K>* ctxt, Segment<K>* segment, ushorttype identifier);
		FORCE_INLINE boolean purgeSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean growing, boolean index, boolean semi, BasicArray<Segment<K>*>* purgeList, BasicArray<Segment<K>*>* compressList, PurgeReport& purgeReport);

		FORCE_INLINE boolean indexSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean rebuild, uinttype viewpoint = 0, boolean backwardCheckpoint = false, RealTimeSummary<K>* summaryWorkspace = null, IndexReport* indexReport = null);
		FORCE_INLINE Segment<K>* pageSegment(ThreadContext<K>* ctxt, Segment<K>* segment, inttype* indexed, boolean rebuild, uinttype viewpoint, boolean backwardCheckpoint, RealTimeSummary<K>* summaryWorkspace, IndexReport& indexReport);

		FORCE_INLINE void absolveRooted(Transaction* tx, const K key, const Information* curinfo);

		FORCE_INLINE void absolveSecondary(Transaction* tx, const K key, const Information* topinfo);
		FORCE_INLINE void rekeySecondary(Transaction* tx, const K key, const Information* newinfo, const Information* oldinfo);
		FORCE_INLINE void ejectSecondary(Transaction* tx, const K key, const Information* delinfo, const Information* curinfo, const Information* chkinfo, LockOption lock, boolean ignore = false);

		FORCE_INLINE boolean lockSecondary(Transaction* tx, const bytearray pkey, XInfoRef& curinfoRef, LockOption lock, boolean primaryLocked, boolean stitch);
		FORCE_INLINE boolean lockSecondary(Transaction* tx, const K key, XInfoRef& newinfo, XInfoRef& oldinfo, LockOption lock, Conductor* conductor);

		FORCE_INLINE void abortSecondary(Transaction* tx, const K key, const Information* orginfo, XInfoRef& preinfoRef, const Information* curinfo);

		FORCE_INLINE void readValue(ThreadContext<K>* ctxt, Segment<K>* segment);
		FORCE_INLINE void readValue(ThreadContext<K>* ctxt, Information* info, const K key);
		FORCE_INLINE void readIndex(ThreadContext<K>* ctxt, Segment<K>* segment, boolean fill, boolean pace);
		FORCE_INLINE Information* setupResult(ThreadContext<K>* ctxt, InfoRef& curinfoRef, nbyte* value, LockOption lock);

		FORCE_INLINE boolean trySetupSegment(ThreadContext<K>* ctxt, Segment<K>* segment);
		FORCE_INLINE boolean fillSetupSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean physical, boolean values);
		FORCE_INLINE boolean forceSetupSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean physical, boolean values, boolean force = true);

		FORCE_INLINE Segment<K>* initSegment(ThreadContext<K>* ctxt, const K key);
		FORCE_INLINE Segment<K>* firstSegment(ThreadContext<K>* ctxt, const K key, boolean create);
		FORCE_INLINE Segment<K>* getSegment(ThreadContext<K>* ctxt, const K key, boolean create, boolean fill = true, boolean forceSegmentLock = true, boolean forceContextLock = true, boolean* wasClosed = null, boolean openSegment = false);
		FORCE_INLINE Segment<K>* scanSegment(ThreadContext<K>* ctxt, const K key, boolean values);
		FORCE_INLINE Segment<K>* getNextSegment(ThreadContext<K>* ctxt, const K key, boolean values);
		FORCE_INLINE Segment<K>* getPreviousSegment(ThreadContext<K>* ctxt, const K key, boolean values);
		FORCE_INLINE Segment<K>* lastSegment(ThreadContext<K>* ctxt, const K key, boolean create);
		FORCE_INLINE const MapEntry<K,Segment<K>*>* scanEntry(ThreadContext<K>* ctxt, const K key, boolean values);

		FORCE_INLINE boolean first(ThreadContext<K>* ctxt, nbyte* value, K* retkey, boolean* again, LockOption lock);
		FORCE_INLINE boolean get(ThreadContext<K>* ctxt, const K key, nbyte* value, K* retkey, boolean* again, LockOption lock);
		FORCE_INLINE boolean getNext(ThreadContext<K>* ctxt, const K key, nbyte* value, K* retkey, boolean* again, LockOption lock, boolean match = false);
		FORCE_INLINE boolean getPrevious(ThreadContext<K>* ctxt, const K key, nbyte* value, K* retkey, boolean* again, LockOption lock);
		FORCE_INLINE boolean last(ThreadContext<K>* ctxt, nbyte* value, K* retkey, boolean* again, LockOption lock);

		FORCE_INLINE boolean checkCycleLock(Transaction* itx, Transaction*& otx, ushorttype id);
		FORCE_INLINE boolean checkAccessLock(Transaction* itx, InfoRef& orginfoRef, ErrorCode* code, LockOption lock);
		FORCE_INLINE boolean checkIsolateLock(ThreadContext<K>* ctxt, InfoRef& infoRef, Information*& info, ErrorCode* code, boolean* again, LockOption lock);

		FORCE_INLINE void updateReservationWatermark(const K key);
		FORCE_INLINE void copyValueFromInformation(const Information* info, nbyte* value);
		FORCE_INLINE void copyValueIntoInformation(Information* info, const nbyte* value);
		FORCE_INLINE void copyValueFromInformation(ThreadContext<K>* ctxt, Information* info, const K key);

		inline void failedSegment(Segment<K>* segment, const K key);
		inline void failedConductor(RealTimeConductor<K>* conductor, const Information* topinfo);

		FORCE_INLINE void mergeSegments(Segment<K>* segment, Segment<K>* nSegment);
		FORCE_INLINE Segment<K>* splitSegment(ThreadContext<K>* ctxt, Segment<K>* segment, K* firstKey, boolean lock, boolean recover = false /* TODO: why? */);
		FORCE_INLINE void reserveSegment(Conductor* conductor, ContextHandle<K>& handle, K key);

		FORCE_INLINE K rekeySegment(Segment<K>* segment, K newKey, boolean destroy, boolean lock);
		FORCE_INLINE void reseedSegment(Segment<K>* segment, K oldKey, boolean destroy, boolean lock);
		FORCE_INLINE void deleteSegment(Segment<K>* segment, K firstKey, boolean destroy, boolean lock, boolean remove = true);
		FORCE_INLINE ulongtype getActiveSize(ThreadContext<K>* ctxt, Segment<K>* segment);

		FORCE_INLINE SegMapEntry* updateInformation(ThreadContext<K>* ctxt, Segment<K>* segment, K key, Information* info, boolean enableCardinality);
		FORCE_INLINE SegMapEntry* addInformation(ThreadContext<K>* ctxt, ContextHandle<K>& handle, K key, Information* info, boolean enableCardinality);
		FORCE_INLINE Information* deleteInformation(ThreadContext<K>* ctxt, Segment<K>* segment, const K key, boolean persist);
		FORCE_INLINE void stitchInformationSafe(ThreadContext<K>* ctxt, Transaction* tx, XInfoRef& infoRef, InfoRef& curinfoRef, boolean primaryLocked, boolean stitch = true);
		FORCE_INLINE void stitchInformation(ThreadContext<K>* ctxt, Transaction* tx, XInfoRef& infoRef, InfoRef& curinfoRef);

		inline boolean isolateInformation(ThreadContext<K>* ctxt, Information*& info, const K dupkey);
		inline boolean restitchInformation(ThreadContext<K>* ctxt, const Information* oldinfo, XInfoRef& curinfoRef, LockOption lock, K oldkey);
		inline boolean duplicateInformation(ThreadContext<K>* ctxt, const bytearray pkey, Information* curinfo, Information* newinfo, const K dupkey);
		inline boolean duplicateInformation(ThreadContext<K>* ctxt, const bytearray pkey, const Information* newinfo, LockOption lock, K newkey, boolean* identical);

		FORCE_INLINE boolean isolatedStoryline(const Information* topinfo, const Information* curinfo);
		FORCE_INLINE Information* isolateCompressed(ThreadContext<K>* ctxt, Information* orginfo, Information* topinfo = null);	
		FORCE_INLINE Information* isolateCheckpoint(ThreadContext<K>* ctxt, Information* topinfo, uinttype viewpoint, const K key, boolean* divergent = null, boolean* ignore = null, boolean viewlimit = true);
		FORCE_INLINE Information* isolateInterspace(ThreadContext<K>* ctxt, Information* topinfo, Information::LevelState level, const K key, boolean* divergent);

		FORCE_INLINE Information* isolateInformationLocality(ThreadContext<K>* ctxt, const Information* topinfo); 
		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, Information* topinfo);
		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, boolean first);
		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, Information* curinfo);

		#ifdef DEEP_DEBUG
		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, uinttype viewpoint, const K key, boolean testStoryLock = true);
		#else
		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, uinttype viewpoint, const K key);
		#endif

		#ifdef DEEP_DEBUG
		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, Information::LevelState level, boolean testStoryLock = true);
		#else
		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, Information::LevelState level);
		#endif

		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, Information::LevelState level, const K key);

		#ifdef DEEP_DEBUG
		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, StoryLine& storyLine, Information* topinfo, Information::LevelState level, uinttype viewpoint, Transaction::Isolation mode, ushorttype id, const K key, boolean testStoryLock = true);
		#else
		FORCE_INLINE Information* isolateInformation(ThreadContext<K>* ctxt, StoryLine& storyLine, Information* topinfo, Information::LevelState level, uinttype viewpoint, Transaction::Isolation mode, ushorttype id, const K key);
		#endif

		#ifdef DEEP_DEBUG
		FORCE_INLINE boolean informationIsOld(const Information* info, const Information* curinfo, K key, ThreadContext<K>* ctxt = null, boolean read = false);
		FORCE_INLINE void validateStoryline(const Information* orginfo, const Information* info);
		#endif

		FORCE_INLINE Information* versionInformation(Transaction* tx, Information* topinfo, const K key, const nbyte* value, boolean compressed = false);

		FORCE_INLINE boolean putTransaction(const K key, const nbyte* value, WriteOption option, Transaction* tx, LockOption lock, uinttype position, ushorttype index, uinttype compressedOffset);
		FORCE_INLINE boolean removeTransaction(const K key, nbyte* value, DeleteOption option, Transaction* tx, LockOption lock, boolean forCompressedUpdate);

		#ifdef DEEP_SYNCHRONIZATION_GROUPING
		FORCE_INLINE void commit(RealTimeConductor<K>* conductor, bytetype mode) {
			if (mode == 1 /* durable-phase2 */) {
				commitCacheMemory(conductor);
				__sync_sub_and_fetch(&m_pendingCommits, 1);
				return;
			}

			/* if ((mode == 0 non-durable) || (mode == 2)) */ {
				m_checkptRequestLock.readLock();
				__sync_add_and_fetch(&m_pendingCommits, 1);

				if (m_memoryMode == false) {
					commitRealTime(conductor);
				}

				m_checkptRequestLock.readUnlock();

				if (mode == 0 /* non-durable */) {
					commitCacheMemory(conductor);
					__sync_sub_and_fetch(&m_pendingCommits, 1);
				}
			}
		#else
		FORCE_INLINE void commit(RealTimeConductor<K>* conductor) {
			m_checkptRequestLock.readLock();
			__sync_add_and_fetch(&m_pendingCommits, 1);

			if (m_memoryMode == false) {
				commitRealTime(conductor);
			}

			m_checkptRequestLock.readUnlock();

			commitCacheMemory(conductor);
			__sync_sub_and_fetch(&m_pendingCommits, 1);
		#endif
		}

		FORCE_INLINE void rollback(RealTimeConductor<K>* conductor) {
			if (m_memoryMode == false) {
				abortRealTime(conductor);
			}

			abortCacheMemory(conductor);
		}

	public:
		RealTimeMap(const char* filepath, const longtype options, const shorttype keySize, const inttype valueSize, const Comparator<K>* = &COMPARATOR, const KeyBuilder<K>* keyBuilder = &KEY_BUILDER, const SchemaBuilder<K>* schemaBuilder = &SCHEMA_BUILDER);

		virtual ~RealTimeMap();

		boolean get(const K key, nbyte* value, ReadOption option = EXACT, K* retkey = null, Transaction* tx = null, LockOption lock = LOCK_NONE, MapInformationEntryIterator* iterator = null, RealTimeCondition<K>* condition = null);
		boolean cursor(const K key, RealTimeIterator<K>* iterator /* XXX: not concurrent */, ReadOption option = EXACT, Transaction* tx = null, LockOption lock = LOCK_NONE);
		boolean contains(const K key, ReadOption option = EXACT, K* retkey = null, Transaction* tx = null, LockOption lock = LOCK_NONE);

		boolean put(const K key, const nbyte* value, WriteOption option = STANDARD, Transaction* tx = null, LockOption lock = LOCK_WRITE, uinttype position = 0, ushorttype index = 0, uinttype compressedOffset = Information::OFFSET_NONE);
		boolean remove(const K key, nbyte* value, DeleteOption option = DELETE_RETURN, Transaction* tx = null, LockOption lock = LOCK_WRITE, boolean forCompressedUpdate = false);

		boolean reserve(ulongtype offset, ulongtype block, ulongtype& first, ulongtype& reserved, Transaction* tx);

		longtype size(Transaction* tx = null, boolean recalculate = false /* XXX: used during recovery mode only */);

	public:
		FORCE_INLINE void markSchema(void);
		FORCE_INLINE void commitSchema(void);
		FORCE_INLINE void rollbackSchema(void);

		FORCE_INLINE boolean associate(RealTime* secondary, boolean hasPrimary, boolean dynamic = false) {
			addSecondary(secondary, hasPrimary, dynamic);
			return true;
		}

		FORCE_INLINE boolean disassociate(RealTime* secondary, boolean dynamic = false) {
			removeSecondary(secondary, /* hadPrimary, */ dynamic);
			return true;
		}

		FORCE_INLINE boolean associate(Transaction* tx) {
			#ifdef DEEP_DEBUG
			if (m_primaryIndex != null) {
				DEEP_LOG(ERROR, OTHER, "Invalid associate: secondary associate not supported, %s\n", getFilePath());

				throw InvalidException("Invalid begin: secondary associate not supported");
			}
			#endif

			Conductor* conductor = tx->getConductor(getIdentifier());
			if (conductor == null) {
				conductor = new RealTimeConductor<K>(tx, this);

				tx->initConductor(conductor, getIdentifier(), RealTimeResource::currentTimeMillis());
			}

			return true;
		}

	public:
		FORCE_INLINE void acquire(MapInformationEntryIterator* iterator) {
			// XXX: nothing to do
		}

		FORCE_INLINE void release(MapInformationEntryIterator* iterator) {
			Segment<K>* segment = (Segment<K>*) iterator->getContainer(false);
			if (segment != null) {
				iterator->resetContainer();
				segment->decref();
			}
		}

	public:
		longtype range(const K* skey, const K* ekey, Transaction* tx = null);

		boolean optimize(OptimizeOption optType, Transaction* tx = null);

		longtype length(FileOption fileType, Transaction* tx = null);

		void cardinality(longtype* stats, Transaction* tx = null, boolean recalculate = false);
	
		void unlock(Transaction* tx);

		// XXX: following methods do not invoke corresponding secondary maps (i.e. they need to be invoked manually)
		//
			boolean discover(boolean reserve = false, Transaction* tx = null);

			boolean mount(boolean reserve = false, Transaction* tx = null);
			boolean unmount(boolean destroy = true, Transaction* tx = null);

			boolean clobber(boolean final = false);
			boolean clear(boolean final = false);
		//

		boolean recover(boolean rebuild);

	public:
		FORCE_INLINE void setCompressionRatioKey(bytetype ratio) {
			m_compressionRatioKey = ratio;
		}

		FORCE_INLINE bytetype getCompressionRatioKey(void) const {
			return m_compressionRatioKey;
		}

		FORCE_INLINE void setCompressionRatioKeyValue(bytetype ratio) {
			m_compressionRatioKeyValue = ratio;
		}

		FORCE_INLINE bytetype getCompressionRatioKeyValue(void) const {
			return m_compressionRatioKeyValue;
		}

		FORCE_INLINE ubytetype getFailedPurgeCycles(void) const {
			return m_failedPurgeCycles;
		}

		FORCE_INLINE void setCycleCacheUsage(RealTimeResource::Limit limit) {
			m_cycleCacheUsage = limit;
		}

		FORCE_INLINE const RealTimeResource::Limit getCycleCacheUsage(void) const {
			return m_cycleCacheUsage;
		}

	public:
		virtual const ExtraStatistics* getExtraStats(void) {
			return &m_extraStats;
		}

		virtual const char* getFilePath(void) const {
			return m_filePath.getPath();
		}

		FORCE_INLINE virtual String getDataDirectory(boolean* needsLink) const {
			if (m_primaryIndex != null) {
				return m_primaryIndex->getDataDirectory(needsLink);
			}

			const File& dataDir = m_schemaBuilder->getDataDirectory();

			if (dataDir.String::length() == 0) {
				String filePath = getFilePath();
				const inttype index = filePath.lastIndexOf(File::separator);
				#ifdef DEEP_DEBUG
				if (index < 0) {
					DEEP_LOG(ERROR, OTHER, "Invalid file path, %s\n", getFilePath());

					throw InvalidException("Invalid file path");
				}
				#endif
				return filePath.substring(0, index+1);
			}

			if (needsLink != null) {
				*needsLink = true;
			}

			return dataDir;
		}

		FORCE_INLINE virtual String getIndexDirectory(boolean* needsLink) const {
			if (m_primaryIndex != null) {
				return m_primaryIndex->getIndexDirectory(needsLink);
			}

			const File& indexDir = m_schemaBuilder->getIndexDirectory();

			if (indexDir.String::length() == 0) {
				String filePath = getFilePath();
				const inttype index = filePath.lastIndexOf(File::separator);
				#ifdef DEEP_DEBUG
				if (index < 0) {
					DEEP_LOG(ERROR, OTHER, "Invalid file path, %s\n", getFilePath());

					throw InvalidException("Invalid file path");
				}
				#endif
				return filePath.substring(0, index+1);
			}

			if (needsLink != null) {
				*needsLink = true;
			}

			return indexDir;
		}

		virtual int getErrorCode(void) {
			int code = m_threadContext.getErrorCode();
			m_threadContext.setErrorCode(ERR_SUCCESS);
			return code;
		} 

		virtual K getErrorKey(void) {
			K key = m_threadContext.getErrorKey();
			m_threadContext.setErrorKey((K) Converter<K>::NULL_VALUE);
			return key;
		} 

		virtual inttype getLrtIndex(void) const {
			return m_schemaBuilder->getLrtIndex();
		}

		virtual uinttype getLrtPosition(void) const {
			return m_schemaBuilder->getLrtPosition();
		}

	private:
		typename RealTimeCondition<K>::ConditionResult checkCondition(ThreadContext<K>* ctxt, const K key) {
			typename RealTimeCondition<K>::ConditionResult result = RealTimeCondition<K>::CONDITION_OK;

			RealTimeCondition<K>* condition = ctxt->getCondition();
			if (condition != null) {
				result = condition->check(key);
			}

			return result;
		}

	public:
		FORCE_INLINE void verifySegmentKeyRange(Segment<K>* segment, K key) {
			const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::floorEntry(key);
			if (index->getValue() != segment) {
				KEY_TO_STRING(buffer1, key);

				DEEP_LOG(ERROR, OTHER, "Paging key segment mismatch: logical %p, actual %p, key %s, %s\n", index->getValue(), segment, buffer1, getFilePath());

				throw InvalidException("Paging key segment mismatch");
			}
		}

	// TODO: rework interface to remove the following relationships (i.e. get helper methods)
	//
		friend class RealTimeIterator<K>;
		friend class RealTimeDiscover<K>;
		friend class RealTimeRecovery<K>;
		friend class RealTimeConductor<K>;
		friend class RealTimeUtilities<K>;

		friend struct RealTimeSchema_v1<K>;
		friend struct RealTimeAdaptive_v1<K>;
		friend struct RealTimeSummary<K>;
		friend struct DynamicSummarization<K>;
		friend struct RecoveryWorkspace_v1_0<K>;
		friend struct RecoveryWorkspace_v1_1<K>;
		friend struct RealTimeProtocol_v1_0_0_0<CT_DATASTORE_PROTO_VER_1_0, K>;
		friend struct RealTimeProtocol_v1_0_0_0<CT_DATASTORE_PROTO_VER_1_1, K>;
		friend struct RealTimeProtocol_v1_0_0_0<CT_DATASTORE_PROTO_VER_1_2, K>;
		friend struct RealTimeProtocol_v1_0_0_0<CT_DATASTORE_PROTO_VER_1_3, K>;
		friend struct RealTimeProtocol_v1_1_0_0<CT_DATASTORE_PROTO_VER_1_0, K>;
		friend struct RealTimeProtocol_v1_1_0_0<CT_DATASTORE_PROTO_VER_1_1, K>;
		friend struct RealTimeProtocol_v1_1_0_0<CT_DATASTORE_PROTO_VER_1_2, K>;
		friend struct RealTimeProtocol_v1_1_0_0<CT_DATASTORE_PROTO_VER_1_3, K>;
		friend struct SegmentMetadata_v1_1<CT_DATASTORE_PROTO_VER_1_0, K>;
		friend struct SegmentMetadata_v1_1<CT_DATASTORE_PROTO_VER_1_1, K>;
		friend struct SegmentMetadata_v1_1<CT_DATASTORE_PROTO_VER_1_2, K>;
		friend struct SegmentMetadata_v1_1<CT_DATASTORE_PROTO_VER_1_3, K>;
		friend struct WriteKeyPaging<CT_DATASTORE_PROTO_VER_1_0, K>;
		friend struct WriteKeyPaging<CT_DATASTORE_PROTO_VER_1_1, K>;
		friend struct WriteKeyPaging<CT_DATASTORE_PROTO_VER_1_2, K>;
		friend struct WriteKeyPaging<CT_DATASTORE_PROTO_VER_1_3, K>;
	//
};

} } } } } } // namespace

// XXX: the following include contains logic split out to reduce overall header size
#include "com/deepis/db/store/relative/core/RealTimeTransaction.h"

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEMAP_H_*/
