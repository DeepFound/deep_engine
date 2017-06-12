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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEMAP_CXX_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEMAP_CXX_

#include "cxx/lang/Math.h"
#include "cxx/lang/System.h"

#include "cxx/util/Logger.h"
#include "cxx/util/Collections.h"
#include "cxx/util/PriorityQueue.h"

// XXX: includes required for templating dependencies
#include "cxx/util/TreeMap.cxx"
#include "cxx/util/TreeSet.cxx"
#include "cxx/util/HashMap.cxx"
#include "cxx/util/HashSet.cxx"
#include "cxx/util/concurrent/atomic/AtomicLong.h"

#include "com/deepis/db/store/relative/util/Versions.h"
#include "com/deepis/db/store/relative/util/MapFileUtil.h"
#include "com/deepis/db/store/relative/util/PermissionException.h"

#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeIterator.h"
#include "com/deepis/db/store/relative/core/RealTimeDiscover.h"

#include "com/deepis/db/store/relative/core/RealTimeSchema.h"
#include "com/deepis/db/store/relative/core/RealTimeVersion.h"
#include "com/deepis/db/store/relative/core/RealTimeAdaptive.h"
#include "com/deepis/db/store/relative/core/RealTimeRecovery.h"

/* XXX: Code legends (Information parameter meaning)
 *
 *	- info    (local working object)
 *	- topinfo (first object of a chapter)
 *	- endinfo (final object of a chapter)
 *	- curinfo (current object of a story)
 *	- oldinfo (removing object of a story)
 *	- preinfo (previous object of a story)
 *	- newinfo (new/next object of a story)
 *	- orginfo (original object of a story)
 *	- delinfo (deleted object from a story)
 *	- cmpinfo (compress object from a story)
 *	- chkinfo (checkpoint object from a story)
 */

// TODO: split RealTimeMap into Index and Map (i.e. RealTimeIndex, RealTimeMap) where Map derives from Index

using namespace com::deepis::db::store::relative::core;

template<typename K>
const Comparator<K> RealTimeMap<K>::COMPARATOR;

template<typename K>
const KeyBuilder<K> RealTimeMap<K>::KEY_BUILDER;

template<typename K>
const SchemaBuilder<K> RealTimeMap<K>::SCHEMA_BUILDER;

template<typename K>
longtype RealTimeMap<K>::getCacheSize(longtype elements, shorttype keySize, inttype valSize) {
	return elements * keySize * valSize;
}

template<typename K>
inttype RealTimeMap<K>::getBufferSize(shorttype keyOffset, shorttype keySize, shorttype segSize) {
	return segSize * (keySize + keyOffset);
}

template<typename K>
RealTimeMap<K>::RealTimeMap(const char* filepath, const longtype options, const shorttype keySize, const inttype valueSize, const Comparator<K>* comparator, const KeyBuilder<K>* keyBuilder, const SchemaBuilder<K>* schemaBuilder):
	RealTimeIndex(filepath, options, keySize, valueSize),

	m_filePath(m_share.getFilePath()),

	m_memoryMode((m_share.getOptions() & O_MEMORY) == O_MEMORY),
	m_cleanupMode((m_share.getOptions() & O_CLEANUP) == O_CLEANUP),
	m_singularMode((m_share.getOptions() & O_SINGULAR) == O_SINGULAR),
	m_prefetchMode((m_share.getOptions() & O_PREFETCH) == O_PREFETCH),
	m_rowStoreMode((m_share.getOptions() & O_ROWSTORE) == O_ROWSTORE),

	m_keyCompressMode((m_share.getOptions() & O_KEYCOMPRESS) == O_KEYCOMPRESS),
	m_valueCompressMode((m_share.getOptions() & O_VALUECOMPRESS) == O_VALUECOMPRESS),
	m_memoryCompressMode((m_share.getOptions() & O_MEMORYCOMPRESS) == O_MEMORYCOMPRESS),

	// XXX: the following attribute initialization order is important!
	//
	m_comparator(comparator),
	m_keyBuilder(keyBuilder),
	m_schemaBuilder(schemaBuilder),

	m_irtHeader(RealTimeVersion<K>::getIrtHeader(this)),
	m_irtBuffer(RealTimeVersion<K>::getBufferSize(this)),

	m_irtOffset(RealTimeVersion<K>::getIrtBlockSize(this)),
	m_lrtOffset(RealTimeVersion<K>::getLrtBlockSize(this)),
	//

	m_resetIterator(false),
	m_threadIndexing(false),
	m_hasReservation(false),
	m_hasSecondaryMaps(false),

	m_irtBuildReorganization(false),
	m_irtForceReorganization(false),
	m_vrtForceReorganization(false),

	#ifdef DEEP_USERLOCK
	// nothing to do
	#else
	m_reserveLock(false),
	#endif

	m_threadContext(Properties::getThreadLimit(), m_keyBuilder),

	m_deletedSegmentList(Properties::LIST_CAP, true),
	m_orderSegmentCmp(m_comparator),
	m_orderSegmentList((const PriorityQueue<Segment<K>*, OrderedSegmentCmp>&)PriorityQueue<Segment<K>*, OrderedSegmentCmp>(&m_orderSegmentCmp), (const HashSet<Segment<K>*>&)HashSet<Segment<K>*>(), (const UserSpaceLock&)UserSpaceLock()),
	m_orderSegmentMode(MODE_INDEX),

	m_branchSegmentTreeMap(m_comparator, Properties::DEFAULT_SEGMENT_BRANCH_ORDER),

	m_summaries(null),
	m_checkptTriggered(false),
	m_fileCleanupTime(0),

	m_localityCmp(null),

	m_pendingCommits(0),

	m_compressionRatioKey(CHAR_MIN),
	m_compressionRatioKeyValue(CHAR_MIN),
	m_failedPurgeCycles(0),
	m_cycleCacheUsage(RealTimeResource::IGNORE),

	// XXX: used to remove iterator allocation / deallocation (static relationships)
	m_orderSegmentSet(true),
	m_purgeSegmentSet(true),
	m_fillInformationSet(true),
	m_indexInformationSet(true),
	m_purgeInformationSet(true),
	m_rolloverInformationSet(true) {

	initialize();

	m_branchSegmentTreeMap.entrySet(&m_orderSegmentSet);
	m_branchSegmentTreeMap.entrySet(&m_purgeSegmentSet);

	if ((m_memoryMode == true) && (Properties::getDurable() == true)) {
		DEEP_LOG(ERROR, OTHER, "Invalid options: memory and durability, %s\n", getFilePath());

		throw InvalidException("Invalid options: memory and durability");
	}
}

template<typename K>
RealTimeMap<K>::~RealTimeMap(void) {

	if (m_primaryIndex != null) {
		for (int i = 0; i < m_primaryIndex->getShare()->getSecondaryList()->size(); i++) {
			if (m_primaryIndex->getShare()->getSecondaryList()->get(i) == this) {
				m_primaryIndex->getShare()->getSecondaryList()->set(i, null);
				break;
			}
		}

	} else if (m_associated == false) {
		for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
			RealTime* rt = m_share.getSecondaryList()->get(i);
			if (rt == null) {
				// XXX: allow for dynamic add/remove secondary index
				continue;
			}

			rt->indexPrimary(null, -1, false, false);
		}
	}

	if (m_state < MAP_EXITING) {
		unmount();
	}

	if (m_summaries != null) {
		Converter<PagedSummarySet*>::destroy(m_summaries);
	}

	if (m_localityCmp != null) {
		Converter<Comparator<Locality&>*>::destroy(m_localityCmp);
	}

	Iterator<MeasuredRandomAccessFile*>* iter = m_share.getIrtWriteFileList()->iterator();
	while (iter->hasNext() == true) {
		MeasuredRandomAccessFile* iwfile = iter->next();
		if (iwfile != null) {
			DEEP_LOG(DEBUG, CLSRE, "%llu | %s | %s, %s\n", iwfile->getInitialLength(), (const char*)iwfile->getInitialLrtLocality().toString(), (const char*)iwfile->getEndwiseLrtLocality().toString(), iwfile->getPath());
		}
		if ((iwfile != null) && (iwfile->getInitialLrtLocality() == iwfile->getEndwiseLrtLocality())) {
			const ulongtype initLen = iwfile->getInitialLength();
			const ulongtype curLen = iwfile->length();
			if (curLen > initLen) {
				if (initLen == 0) {
					DEEP_LOG(DEBUG, FINAL, "nothing changed; erase block, %s\n", iwfile->getPath());
					MapFileUtil::clobberFile(m_filePath, iwfile->getPath());

				} else {
					m_share.acquire(iwfile);
					{
						DEEP_LOG(DEBUG, FINAL, "nothing changed; truncate block (%llu => %llu), %s\n", curLen, initLen, iwfile->getPath());
						iwfile->BufferedRandomAccessFile::setLength(initLen);
					}
					m_share.release(iwfile);
				}
			}
		}
	}
	Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);

	deleteCacheAndFileMemory();

	if (m_primaryIndex != null) {
		m_share.setAwaitingDeletion(null, null);
	}
}

template<typename K>
void RealTimeMap<K>::sizeStorage(longtype* total) {

	if (m_primaryIndex == null) {
		*total += m_share.size<MeasuredRandomAccessFile>(m_share.getVrtWriteFileList());
		*total += m_share.size<MeasuredRandomAccessFile>(m_share.getLrtWriteFileList());
	}

	*total += m_share.size<MeasuredRandomAccessFile>(m_share.getIrtWriteFileList());
	#if 0
	*total += m_share.size<MeasuredRandomAccessFile>(m_share.getSrtWriteFileList());
	*total += m_share.size<MeasuredRandomAccessFile>(m_share.getXrtWriteFileList());
	#endif
}

template<typename K>
void RealTimeMap<K>::sizeCache(void) {

	RealTimeShare* share = (m_primaryIndex == null) ? &m_share : m_primaryIndex->getShare();

	if (share->getValueAverage() != -1) {
		m_cacheSize = getCacheSize(getActiveSegments(), m_share.getKeyBuffer(m_keyBuilder->isPrimitive()), share->getValueAverage());

	} else if (size() != 0) {
		m_cacheSize = getCacheSize(getActiveSegments(), m_share.getKeyBuffer(m_keyBuilder->isPrimitive()), 1 /* wait for discovery */);

	} else {
		m_cacheSize = 0;
	}
}

template<typename K>
inttype RealTimeMap<K>::purgeCache(boolean index, boolean deep, boolean log) {

	inttype purged = 0;
	inttype active = getActiveSegments();
	boolean growing = (size() > getCycleSize());

	active = /* purge down to this active */ RealTimeAdaptive_v1<K>::cacheMinimization(this, active, deep, growing);
	if (active > 0) {
		inttype compressed = 0;
		inttype compressedDataPurge = 0;
		PurgeReport purgeReport;

		purged = purgeCacheManagement(active, index /* on index thread */, &compressed, &compressedDataPurge, purgeReport);

		if ((purged != 0) || ((log == true) && (index == false))) {
			if (index == true) {
				DEEP_LOG(DEBUG, PURGE, "store: %s, index requested: %d, %c[%d;%dmachieved:%c[%d;%dm %d of %d\n", getFilePath(), active, 27,1,33,27,0, 25, purged, active);

			} else {
				DEEP_LOG(DEBUG, PURGE, "store: %s, purge requested: %d, %c[%d;%dmachieved:%c[%d;%dm %d of %d\n", getFilePath(), active, 27,1,33,27,0, 25, purged, active);
			}

			if (Properties::getLogOption(Properties::VERBOSE_PURGE) == true) {
				DEEP_LOG(DEBUG, PURGE, "store: %s, purge report: %s\n", getFilePath(), (const char*)purgeReport.toString());
			}
		}

		if (compressed > 0) {
			String cmprs;
			RealTimeCompress<K>::buildLoggingString(this, &cmprs, compressed);

			DEEP_LOG(DEBUG, CMPRS, "store: %s, %s\n", getFilePath(), cmprs.c_str());
		}

		if (compressedDataPurge != 0) {
			DEEP_LOG(DEBUG, PURGE, "store: %s, compressed data purged: %d\n", getFilePath(), compressedDataPurge);
		}

		if ((purged == 0) && (compressedDataPurge == 0)) {
			m_failedPurgeCycles++;
		} else {
			m_failedPurgeCycles = 0;
		}
	}

	return purged;
}

template<typename K>
void RealTimeMap<K>::indexCache(boolean cycle, boolean* cont, boolean* reorg) {

	if ((m_mount != MOUNT_OPENED) && (m_state != MAP_INDXING)) {
		DEEP_LOG(DEBUG, BREAK, "store: %s, bypass indexing due to mount state... %s\n", getFilePath(), (const char*)m_endwiseLrtLocality.toString());
		*cont = false;
		*reorg = false;
		return;
	}

	synchronized(this) {
		if ((m_state != MAP_RUNNING) && (m_state != MAP_INDXING) && (m_state != MAP_RECOVER)) {
			*cont = false;
			*reorg = false;
			return;
		}

		m_threadIndexing = true;
	}

	indexCacheManagement(cont, reorg);

	// XXX: cycle size used for purge optimization
	if (cycle == true) {
		m_cycleSize = size();
	}

	m_cycleCacheUsage = m_resource.getCacheUsage();

	synchronized(this) {
		m_threadIndexing = false;

		notifyAll();
	}
}

template<typename K>
ConcurrentObject* RealTimeMap<K>::getThreadContext(void) {
	return m_threadContext.getContext();
}

template<typename K>
void RealTimeMap<K>::removeThreadContext(ConcurrentObject* context) {

	// XXX: context might not have been setup/used
	if (context != null) {
		m_threadContext.removeContext((ThreadContext<K>*) context);
	}
}

template<typename K>
void RealTimeMap<K>::removeThreadContexts(BasicArray<ConcurrentObject*>* contexts) {

	// XXX: safe context lock: one writer / no readers on the branch tree
	m_threadContext.writeLock();
	{
		removeThreadContext(contexts->get(0));

		for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
			RealTime* rt = m_share.getSecondaryList()->get(i);
			if (rt == null) {
				// XXX: allow for dynamic add/remove secondary index
				continue;
			}

			if (contexts->size() > (i + 1)) {
				rt->removeThreadContext(contexts->get(i + 1));
			}
		}
	}
	m_threadContext.writeUnlock();
}

template<typename K>
void RealTimeMap<K>::setContextKey(Transaction* tx, const bytearray bkey /* TODO, int keynum */) {

	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();

	if (m_keyBuilder->isPrimitive() == true) {
		ctxt->setKey3((K) *((K*) bkey));

	} else {
		m_keyBuilder->fillKey(bkey, ctxt->getKey3());
	}
}

template<typename K>
ThreadContext<K>* RealTimeMap<K>::getTransactionContext(Transaction* tx) {

	Conductor* conductor = tx->getConductor(getIdentifier());

	ThreadContext<K>* ctxt = (ThreadContext<K>*) conductor->getContext(m_indexValue);
	if (ctxt == null) {
		ctxt = m_threadContext.getContext(((longtype) tx->getIdentifier()) | 0xFFFFFFFF00000000);
		conductor->index(m_indexValue, ctxt);
	}

	#ifdef CXX_LANG_MEMORY_DEBUG
	ctxt->setTransaction(tx, true);
	#else
	ctxt->setTransaction(tx);
	#endif

	return ctxt;
}

template<typename K>
ThreadContext<K>* RealTimeMap<K>::getConductorContext(Transaction* tx, RealTimeConductor<K>* conductor) {

	ThreadContext<K>* ctxt = (ThreadContext<K>*) conductor->getContext(m_indexValue);

	#ifdef CXX_LANG_MEMORY_DEBUG
	ctxt->setTransaction(tx, true);
	#else
	ctxt->setTransaction(tx);
	#endif

	return ctxt;
}

template<typename K>
void RealTimeMap<K>::recoverGenesis(void) {

	m_share.destroy<BufferedRandomAccessFile>(m_share.getIrtReadFileList());
	m_share.destroy<MeasuredRandomAccessFile>(m_share.getIrtWriteFileList());

	MapFileUtil::clobber(getFilePath(), false);

	m_recoverLrtLocality = Locality::LOCALITY_NONE;
	m_summaryLrtLocality = Locality::LOCALITY_NONE;
	m_endwiseLrtLocality = Locality::LOCALITY_NONE;
	m_checkptLrtLocality = Locality::LOCALITY_NONE;
	m_currentLrtLocality = Locality::LOCALITY_NONE;

	deleteSegmentMemory();

	seedFinalFile();
}

template<typename K>
void RealTimeMap<K>::recoverBegin() {
	m_state = MAP_RECOVER;

	if ((m_primaryIndex == null) && (m_hasSecondaryMaps == true)) {
		for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
			RealTime* rt = m_share.getSecondaryList()->get(i);
			if (rt == null) {
				// XXX: allow for dynamic add/remove secondary index
				continue;
			}

			rt->recoverBegin();
		}
	}
}

template<typename K>
void RealTimeMap<K>::recoverComplete(longtype entries, const Locality& locality, longtype elapsed, boolean rebuild) {

	if (Properties::getDebugEnabled(Properties::KEY_SECONDARY) == true) {
		if (m_primaryIndex != null) {
			size(null, true);
		}
	}

	m_entrySize.set(entries);

	m_recoverLrtLocality = Locality::LOCALITY_NONE;

	if (m_state == MAP_RECOVER) {
		m_endwiseLrtLocality = Locality::LOCALITY_NONE;

	} else {
		m_endwiseLrtLocality = locality;
	}
	m_summaryLrtLocality = m_endwiseLrtLocality;

	m_checkptLrtLocality = Locality::LOCALITY_NONE;
	m_currentLrtLocality = locality;

	seedIncrement();

	if (m_primaryIndex == null) {
		seedValueAverage();

		if (m_summaryLrtLocality.isNone() == false) {
			m_checkptLrtLocality = m_summaryLrtLocality;
		}
	}

	if (m_primaryIndex == null) {
		RealTimeRecovery<K>::repairStreamRanges(this);
		if ((Properties::getRecoveryRealign() == true) && (m_valueCompressMode == true) && (rebuild == true)) {
			RealTimeRecovery<K>::repairSegmentCompressionAlignment(this, m_threadContext.getContext());
		}
	}

	// XXX: seed cycle size used for purge optimization
	m_cycleSize = size();
	m_cycleCacheUsage = m_resource.getCacheUsage();

	m_organizationTime = System::currentTimeMillis();
	m_finalizationTime = m_organizationTime;

	if ((Properties::getCardinalityRecalculateRecovery() == true) && (m_state == MAP_RECOVER)) {
		longtype* stats = (longtype*) malloc(m_keyBuilder->getKeyParts() * sizeof(longtype));

		cardinality(stats, null /* tx */, true);

		free(stats);
	}

	m_state = MAP_RUNNING;

	if (rebuild == false) {
		DEEP_LOG(DEBUG, DCVRY, "store: %s, elapsed: %lld, segments: %d, size: %lld, mount finished... %p\n", getFilePath(), elapsed, getTotalSegments(), size(), this);

	} else {
		DEEP_LOG(INFO, DCVRY, "store: %s, elapsed: %lld, segments: %d, size: %lld, mount/recovery finished... %p\n", getFilePath(), elapsed, getTotalSegments(), size(), this);
	}
}

template<typename K>
boolean RealTimeMap<K>::indexPrimary(RealTime* primary, bytetype index, boolean dynamic, boolean recovery) {

	if (m_primaryIndex != null) {
		if (m_state < MAP_EXITING) {
			unmount();
		}
	}

	if (primary != null) {
		m_associated = true;
		m_indexValue = index;

	} else if (dynamic == true) {
		clobber();

		// XXX: see unmount due to secondary removal
		m_state = MAP_STOPPED;

		RandomAccessFile* swfile = m_primaryIndex->getShare()->getSrtWriteFileList()->last();
		m_share.acquire(swfile);
		{
			inttype lrtIndex = m_primaryIndex->getCurrentLrtLocality().getIndex();
			uinttype lrtPosition = m_primaryIndex->getCurrentLrtLocality().getLength();
			
			RealTimeSchema_v1<K>::remove(m_schemaBuilder, swfile, false, lrtIndex, lrtPosition);
		}
		m_share.release(swfile);
	}

	if (primary != null) {
		if (recovery == false) {
			// XXX: set primary dependent schema attributes
			// TODO: if we ever allow write during add index, this locality selection needs to be made concurrent
			((SchemaBuilder<K>*)m_schemaBuilder)->setLrtIndex(primary->getCurrentLrtLocality().getIndex());
			((SchemaBuilder<K>*)m_schemaBuilder)->setLrtPosition(primary->getCurrentLrtLocality().getLength());
		}

		m_share.setAwaitingDeletion(m_primaryIndex->getShare()->getAwaitingDeletion(), m_primaryIndex->getShare()->getDeferredStorageSizeAddr());

	} else {
		m_share.setAwaitingDeletion(null, null);
	}

	if ((dynamic == true) && (primary != null)) {
		m_state = MAP_INDXING;
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::indexSecondary(Transaction* tx, RealTime* secondary) {
	boolean success = true;

	if (tx != null) {
		tx->setDirty(true);
	}

	m_reindexing.incrementAndGet();

	ThreadContext<K>* ctxt = m_threadContext.getContext();
	CONTEXT_STACK_HANDLE(K,global,ctxt);

	ArrayList<Segment<K>*> indexSegmentList(Properties::LIST_CAP);

	PurgeReport purgeReport;

	RETRY:
	// XXX: safe context lock: multiple readers / no writer on the branch tree
	m_threadContext.readLock();
	{
		if (m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {
			typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);

			for (int i = 0; i < indexSegmentList.ArrayList<Segment<K>*>::size(); i++) {
				Segment<K>* segment = indexSegmentList.ArrayList<Segment<K>*>::get(i);
				segment->decref();
			}

			indexSegmentList.ArrayList<Segment<K>*>::clear();

			m_branchSegmentTreeMap.entrySet(&stackSegmentSet);
			MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
			while (segIter->MapSegmentEntrySetIterator::hasNext()) {
				MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
				Segment<K>* segment = segEntry->getValue();

				if (segment->getSummary() == true) {
					segment->incref();

					m_threadContext.readUnlock();

					if (forceSetupSegment(ctxt, segment, true /* physical */, true /* values */) == true) {
						segment->unlock();
					}

					goto RETRY;
				}

				segment->incref();

				indexSegmentList.ArrayList<Segment<K>*>::add(segment);
			}
		}
	}
	m_threadContext.readUnlock();

	typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

	for (int i = 0; (i < indexSegmentList.ArrayList<Segment<K>*>::size()) && (success == true); i++) {
		Segment<K>* segment = indexSegmentList.ArrayList<Segment<K>*>::get(i);

		if (forceSetupSegment(ctxt, segment, true /* physical */, true /* values */) == false) {
			continue;
		}

		CONTEXT_STACK_ASSIGN(segment,global);
		{
			segment->SegTreeMap::entrySet(&stackSegmentItemSet);

			MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
			while (infoIter->MapInformationEntrySetIterator::hasNext()) {
				SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();

				K key = infoEntry->getKey();

				Information* info = isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
				if ((info != null) && (info->getDeleting() == false)) {

					#if 0
					if (info->hasFields(Information::WRITE) == false) {
						Information* newinfo = Information::newInfo(Information::WRITE, info->getFileIndex(), info->getFilePosition(), info->getSize());

						if (info->getStoryLock() == null) {
							throw InvalidException("Not implemented"); //TODO: info->initStoryLock();
						}

						throw InvalidException("Not implemented"); // TODO: newinfo->setStoryLock(info->getStoryLock());
						newinfo->setLevel(Information::LEVEL_COMMIT);

						if (info->getData() != null) {
							nbyte tmpValue((const bytearray) info->getData(), info->getSize());
							copyValueIntoInformation(newinfo, &tmpValue);

						} else {
							readValue(ctxt, (Information*) info, key);

							newinfo->setData(info->getData());
							copyValueFromInformation(ctxt, newinfo, key);
						}

						Information* next = info->getNext();
						if (next != null) {
							newinfo->setNext(next);
						}

						info->setDeleting(true);
						info->setNext(newinfo);

						info = newinfo;

					} else {
						info->setIndexed(secondary->getIndexValue(), false);
					}
					#else
					info->setIndexed(secondary->getIndexValue(), false);
					#endif

					if (info->getData() == null) {
						copyValueFromInformation(ctxt, info, key);
					}

					bytearray pkey = Converter<K>::toData(key);

					InfoRef infoRef(m_indexValue, segment, infoEntry, info);
					InfoRef nullRef(m_indexValue, segment, infoEntry, null);
					if (secondary->lockInformation(tx, pkey, infoRef, nullRef, LOCK_WRITE, null /* conductor */) == false) {
						success = false;
						break;
					}

					// XXX: encourage more cache size for secondary indexing
					if (m_resource.getPurgeFlag() == true) {

						// TODO: lock when adding indexes is lockless
						info->freeData();
						segment->setBeenFilled(false);
					}
				}
			}

			// XXX: check whether cache pressure requires segment purging
			purgeSegment(ctxt, segment, false /* not growing */, false /* index */, false /* semi */, null /* purgeList */, null,  purgeReport);
		}
		CONTEXT_STACK_RELEASE(global);
	}

	m_reindexing.decrementAndGet();

	return success;
}

template<typename K>
void RealTimeMap<K>::absolveInformation(Transaction* tx, const bytearray pkey, const Information* curinfo) {

	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();

	K key;
	if (m_primaryIndex == null) {
		key = m_keyBuilder->fillKey(pkey, ctxt->getKey1());

	} else {
		key = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey1());
	}

	Segment<K>* segment = getSegment(ctxt, key, false, false /* don't fill, ok to be virtual */);
	if (segment != null) {
		CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

		const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(key);
		if (infoEntry != null) {
			if (infoEntry->getValue() == curinfo) {
				Information* delinfo = deleteInformation(ctxt, segment, key, false);

				#ifdef DEEP_DEBUG
				if (delinfo != curinfo) {
					DEEP_LOG(ERROR, OTHER, "Invalid absolve: information mismatch, %s\n", getFilePath());

					throw InvalidException("Invalid absolve: information mismatch");
				}
				#endif

				if (m_primaryIndex != null) {
					Converter<Information*>::destroy(delinfo);
				}
			}
		}
	}
}

template<typename K>
void RealTimeMap<K>::rekeyInformation(Transaction* tx, const bytearray pkey, const Information* newinfo, const Information* oldinfo) {

	#ifdef COM_DEEPIS_DB_REKEY
	if (m_keyBuilder->getRequiresRekey() == false) {
		return;
	}

	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();

	K newkey = m_keyBuilder->fillKey(pkey, newinfo->getData(), ctxt->getKey1());
	K oldkey = m_keyBuilder->fillKey(pkey, oldinfo->getData(), ctxt->getKey2());

	if ((m_comparator->compare(newkey, oldkey) == 0) && (m_keyBuilder->isEqual(newkey, oldkey) == false)) {
		Segment<K>* segment = getSegment(ctxt, oldkey, false, false /* don't fill, ok to be virtual */);
		CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

		rekeySegment(segment, newkey, true, true);
	}
	#endif
}

template<typename K>
void RealTimeMap<K>::ejectInformation(Transaction* tx, const bytearray pkey, const Information* delinfo, const Information* curinfo, const Information* chkinfo, LockOption lock, boolean ignore) {

	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();

	#ifdef DEEP_DEBUG
	if ((chkinfo != null) && (delinfo->getNext() != curinfo)) {
		DEEP_LOG(ERROR, OTHER, "Invalid eject: curinfo is not delinfo next, %s\n", getFilePath());

		throw InvalidException("Invalid eject: curinfo is not delinfo next");
	}
	#endif

	if (delinfo->getData() == null) {
		if (delinfo->hasFields(Information::WRITE) == false) {
			DEEP_LOG(ERROR, OTHER, "Invalid eject: root information of read type without data, %s\n", getFilePath());
		}

		readValue(ctxt, (Information*) delinfo, (K) Converter<K>::NULL_VALUE /* only for primary key */);
	}

	K delkey = m_keyBuilder->fillKey(pkey, delinfo->getData(), ctxt->getKey1());

	Segment<K>* segment = getSegment(ctxt, delkey, false, false /* don't fill, ok to be virtual */);
	if (segment != null) {
		CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

		const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(delkey);
		if (infoEntry != null) {

			// XXX: find/re-root matching information
			Information* orginfo = infoEntry->getValue();
			if (orginfo == delinfo) {

				Information* info = null;
				Information* next = (Information*) curinfo;

				while (next != null) {

					if ((next->getDeleting() == false) || ((next->getLevel() == Information::LEVEL_COMMIT) && (next->getViewpoint() != 0) && (Transaction::viewing(next->getViewLimit()) == true)) || (next == chkinfo)) {

						if (next->getData() == null) {
							if (next->hasFields(Information::WRITE) == false) {
								DEEP_LOG(ERROR, OTHER, "Invalid eject: next information of read type without data, %s\n", getFilePath());
							}

							readValue(ctxt, (Information*) next, infoEntry->getKey());
						}

						K nextkey = m_keyBuilder->fillKey(pkey, next->getData(), ctxt->getKey2());
						if (m_comparator->compare(delkey, nextkey) == 0) {
							info = next;
							break;
						} else if (next == chkinfo) {
							/// XXX: rooting past chkinfo, force rebuild for flux (checkpoint should not contain delkey)
							segment->setBeenAltered(true);
						}
					}

					if (isolatedStoryline(curinfo, delinfo) == true) {
						// XXX: curinfo is previous to deleted (putTransaction error case), walk storyline in reverse and stop at orginfo
						next = isolateInformation(ctxt, orginfo, next);
						if (next == orginfo) {
							break;
						}

					} else {
						// XXX: curinfo is after deleted info (deleteUnviewed case), walk storyline forward
						K nextkey = m_keyBuilder->fillKey(pkey, next->getData(), ctxt->getKey2());
						if (m_comparator->compare(delkey, nextkey) == 0) {
							RealTimeVersion<K>::statisticPagingDead(this, segment, next, false /* checkpoint */);
						}

						next = next->getNext();
						if (next == curinfo) {
							break;
						}
					}
				}

				RealTimeVersion<K>::statisticPagingDead(this, segment, delinfo, false /* checkpoint */);

				if (info == null) {
					K firstKey = segment->SegTreeMap::firstKey();

					K retkey = (K) Converter<K>::NULL_VALUE;

					#if 0
					#ifdef DEEP_DEBUG
					if (segment->getCardinalityEnabled() == false) {
						DEEP_LOG(ERROR, OTHER, "Invalid eject: segment cardinality off, %s\n", getFilePath());

						throw InvalidException("Invalid eject: segment cardinality off");
					}
					#endif
					#endif

					/* Information* delinfo = */ segment->SegTreeMap::remove(delkey, &retkey);

					if (segment->SegTreeMap::size() != 0) {
						if (retkey == firstKey) {
							reseedSegment(segment, retkey, true, true);

						} else {
							Converter<K>::destroy(retkey);
						}

						// FLUX: to be removed (see indexing)
						#if 1
						if (segment->getPaged() == true) {
							segment->setFragmentedKey(true);
							segment->setBeenAltered(true);
							segment->setDirty(true);
						}
						#endif

					} else {
						deleteSegment(segment, retkey, true, true);
					}

				} else {
					info->incref();

					((SegMapEntry*) infoEntry)->setValue(info, m_indexValue /* ctx */);
					segment->setDirty(true);
				}

				Converter<Information*>::destroy(orginfo);

			#ifdef DEEP_DEBUG
			} else if ((ignore == false) && (m_valueCompressMode == false)) {
				boolean found = false;
				Information* next = delinfo->getNext();

				// XXX: make sure orginfo is reachable from delinfo
				while ((next != null) && (next != delinfo)) {

					if (next == orginfo) {
						found = true;
						break;
					}

					if ((orginfo->getLevel() == Information::LEVEL_COMMIT) && (next->getLevel() == Information::LEVEL_COMMIT) && (next->getFileIndex() == orginfo->getFileIndex()) && (next->getFilePosition() == orginfo->getFilePosition())) {
						found = true;
						break;
					}

					next = next->getNext();
				}

				if (found == false) {
					DEEP_LOG(ERROR, OTHER, "Invalid eject: information not found, %s\n", getFilePath());

					throw InvalidException("Invalid eject: information not found");
				}
			#endif
			}

		#if 0
		#ifdef DEEP_DEBUG
		} else {
			DEEP_LOG(ERROR, OTHER, "Invalid eject: information not found, %s\n", getFilePath());

			throw InvalidException("Invalid eject: information not found");
		#endif
		#endif
		}
	}
}

template<typename K>
XInfoRef* RealTimeMap<K>::primaryInformation(Transaction* tx, const bytearray pkey, nbyte* value, LockOption lock, bytearray infoRefBuf, inttype* errcode) {

	// XXX: internal invocation is allowed to not specify a transaction (e.g. seedValueAverage)

	#ifdef DEEP_DEBUG
	if (m_primaryIndex != null) {
		DEEP_LOG(ERROR, OTHER, "Invalid call: primary to secondary invocation, %s\n", getFilePath());

		throw InvalidException("Invalid lock: primary to secondary invocation");
	}
	#endif

	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();

	K key = m_keyBuilder->fillKey(pkey, ctxt->getKey3());
	
	// TODO: investigate performce of flipping this back to false after cold start (ie not opening purged segments at runtime)
	boolean openSegment = RealTimeAdaptive_v1<K>::primaryInformationOpenSegment(this);
	boolean wasClosed = false;

	Segment<K>* segment = getSegment(ctxt, key, false /* create */, true /* fill */, false /* forceSegmentLock */, false /* forceContextLock */, &wasClosed, openSegment);
	CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

	if (segment == null) {
		if ((openSegment == false) && (wasClosed == true)) {
			m_coldPointRead.incrementAndGet();
		}

		// XXX: segment locked by another transaction (come around again)
		if (errcode != null) {
			*errcode = -1;
		}

		return null;
	}

	boolean localFill = false;
	if (segment->getBeenFilled() == false) {
		localFill = true;

		readValue(ctxt, segment);

		segment->setBeenFilled(true);
	}

	const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(key);
	if (infoEntry == null) {
		// XXX: segment does not have this particular key (i.e. issue with concurrency or storage)
		if (errcode != null) {
			*errcode = -2;
		}

		return null;
	}

	// XXX: the last committed information is the value to be copied
	Information* info = isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);

	if ((info != null) && (info->getDeleting() == false) && (info->getLevel() == Information::LEVEL_COMMIT) && (info->getData() != null)) {

		if (value != null) {
			if (((uinttype) value->length) < info->getSize() /* 24-bit unsigned int */) {
				value->realloc(info->getSize());
			}
			memcpy(*value, info->getData(), info->getSize());

		} else if ((lock != LOCK_NONE) && (tx != null)) {
			ErrorCode code = ERR_SUCCESS;
			InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
			if (checkAccessLock(tx, infoRef, &code, lock) == true) {
				// XXX: information locked by another transaction (come around again)
				CONTEXT_STACK_UNASSIGN(global);
				info = null;

			} else if (code != ERR_SUCCESS) {
				// XXX: information locked by another transaction (deadlock or timeout)
				CONTEXT_STACK_UNASSIGN(global);

				if (errcode != null) {
					*errcode = 1;
				}

				info = null;

			} else {
				info->incref();
			}

		} else {
			info->incref();
		}

	} else {
		// XXX: information locked by another transaction (deleted or active or did not load data)
		if (errcode != null) {
			if (info == null) {
				info = infoEntry->getValue();

				if (info != null) {
					DEEP_LOG(DEBUG, LPRIM, "store: %s, level: %d, state: %d\n", getFilePath(), info->getLevel(), info->getStateFlags());

					*errcode = 2;

				} else {
					DEEP_LOG(DEBUG, LPRIM, "store: %s, level: %d, state: %d\n", getFilePath(), -1, -1);

					*errcode = 3;
				}

			} else if (info->getLevel() != Information::LEVEL_COMMIT) {
				*errcode = 4;

			} else if (info->getDeleting() == true) {
				*errcode = 5;

			} else if (info->getData() == null) {
				*errcode = 7;

			} else {
				*errcode = 8;
			}
		}

		info = null;
	}

	if (info != null) {
		XInfoRef* ret = new (infoRefBuf) InfoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);

		// XXX: check whether cache pressure requires purging of locally filled segment
		if (localFill == true) {
			PurgeReport purgeReport;
			purgeSegment(ctxt, segment, false /* not growing */, false /* index */, false /* semi */, null /* purgeList */, null, purgeReport);
		}

		return ret;
	}

	return null;
}

template<typename K>
boolean RealTimeMap<K>::lockInformation(Transaction* tx, const bytearray pkey, XInfoRef& curinfoRef, LockOption lock, boolean primaryLocked, boolean stitch) {

	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();
	const Information* curinfo = curinfoRef.getInfo();

	K key;
	if ((m_primaryIndex == null) && (pkey != null)) {
		key = m_keyBuilder->fillKey(pkey, ctxt->getKey1());

	} else {
		key = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey1());
	}

	RETRY:
	Segment<K>* segment = getSegment(ctxt, key, false, true);
	if (segment != null) {
		CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

		const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(key);
		if (infoEntry != null) {
			ErrorCode code = ERR_SUCCESS;
			InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry);
			if (checkAccessLock(tx, infoRef, &code, lock) == true) {
				CONTEXT_STACK_UNASSIGN(global);
				goto RETRY;

			} else if (code != ERR_SUCCESS) {
				CONTEXT_STACK_UNASSIGN(global);
				return false;
			}

			if (m_primaryIndex == null) {
				primaryLocked = true;
			}
			stitchInformationSafe(ctxt, tx, curinfoRef, infoRef, primaryLocked, stitch);

			// XXX: ensure changes are active until paged out
			if (segment->getPaged() == true) {
				segment->setDirty(true);
			}

		#if 0
		#ifdef DEEP_DEBUG
		} else {
			DEEP_LOG(ERROR, OTHER, "Invalid lock: information not found, %s\n", getFilePath());

			throw InvalidException("Invalid lock: information not found");
		#endif
		#endif
		}
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::lockInformation(Transaction* tx, const bytearray pkey, XInfoRef& newinfoRef, XInfoRef& oldinfoRef, LockOption lock, Conductor* conductor) {

	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();
	const Information* newinfo = newinfoRef.getInfo();
	const Information* oldinfo = oldinfoRef.getInfo();

	RETRY:
	boolean identical = false;
	K newkey = m_keyBuilder->fillKey(pkey, newinfo->getData(), ctxt->getKey1());

	// XXX: if old key is diverging, ensure existing key information is locked
	if (oldinfo != null) {
		Information* curinfo = isolateInformation(ctxt, (Information*) oldinfo, (Information*) newinfo);
		if ((curinfo->getLevel() != Information::LEVEL_COMMIT) && (curinfo->getDeleting() == true)) {
			curinfo = isolateInformation(ctxt, (Information*) oldinfo, (Information*) curinfo);
		}

		K oldkey = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey2());

		identical = m_comparator->compare(newkey, oldkey) == 0;

		if (identical == false) {

			((Information*) newinfo)->setDiverging(true);

			// XXX: if new key is mirroring, ensure existing key information is locked
			if (getShare()->getHasPrimary() == false) {

				// XXX: lock mirrored information and check for storyboard duplication
				if (duplicateInformation(ctxt, pkey, newinfo, lock, newkey, &identical) == false) {
					return false;
				}
			}

			// XXX: lock old information and re-stitch storyline if needed
			XInfoRef& curinfoRef = newinfoRef;
			curinfoRef.setInfo(curinfo);
			if (restitchInformation(ctxt, oldinfo, curinfoRef, lock, oldkey) == false) {
				curinfoRef.setInfo((Information*)newinfo);
				return false;
			}
			curinfoRef.setInfo((Information*)newinfo);
		}
	}

	// XXX: if new key is mirroring, ensure existing key information is locked
	if ((identical == false) && (getShare()->getHasPrimary() == false) && (newinfo->getDiverging() == false)) {

		// XXX: lock mirrored information and check for storyboard duplication
		if (duplicateInformation(ctxt, pkey, newinfo, lock, newkey, &identical) == false) {
			return false;
		}
	}

	// XXX: if populating the segment is required, override when dynamically indexing (i.e. everything is unique and locked)
	boolean populate = (oldinfo != null) || (newinfo->getCreating() == false) || (getShare()->getHasPrimary() == false);
	boolean checkExisting = false;
	if (populate == true) {
		if (m_state == MAP_INDXING) {
			populate = false;
			checkExisting = true;
		}
	}

	// XXX: standard lookup (i.e. wildcard the primary prefix) for duplication checking
	Segment<K>* segment = getSegment(ctxt, newkey, true, populate);
	CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

	// XXX: the first key is allocated in ::getSegment
	if (populate == false) {
		populate = segment->getBeenCreated();
	}

	const SegMapEntry* infoEntry = ((populate == true) || (checkExisting == true)) ? segment->SegTreeMap::getEntry(newkey) : null;
	if ((infoEntry != null) && (infoEntry->getValue() != null)) {

		ErrorCode code = ERR_SUCCESS;
		InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry);
		if (checkAccessLock(tx, infoRef, &code, lock) == true) {
			CONTEXT_STACK_UNASSIGN(global);
			goto RETRY;

		} else if (code != ERR_SUCCESS) {
			CONTEXT_STACK_UNASSIGN(global);
			return false;
		}

		if ((infoEntry->getValue()->getLevel() == Information::LEVEL_COMMIT) && (infoEntry->getValue()->getDeleting() == false)) {
			if ((oldinfo != null) && (infoEntry->getValue() != oldinfo)) {
				InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry);
				stitchInformation(ctxt, null /* tx */, oldinfoRef, infoRef);
			}
		}

		if (infoEntry->getValue() != newinfo) {
			Information* topinfo = isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
			if (topinfo->getDeleting() == false) {

				if ((identical == false) && (getShare()->getHasPrimary() == false)) {
					Information* curinfo = isolateInformation(ctxt, topinfo, true);

					if (duplicateInformation(ctxt, pkey, curinfo, (Information*) newinfo, newkey) == true) {
						return false;
					}
				}

				if ((infoEntry->getValue() != newinfo) && (infoEntry->getValue()->getLevel() != Information::LEVEL_COMMIT)) {
					InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry);
					stitchInformation(ctxt, null /* tx */, newinfoRef, infoRef);
				}
			}
		}

		#ifdef COM_DEEPIS_DB_REKEY
		if ((m_keyBuilder->getRequiresRekey() == true) && (m_keyBuilder->isEqual(newkey, infoEntry->getKey()) == false)) {
			rekeySegment(segment, newkey, true, true);
		}
		#endif

	} else {
		newkey = (infoEntry != null) ? infoEntry->getKey() : m_keyBuilder->cloneKey(newkey);

		Information* info = (Information*) newinfo;
		info->incref();

		#if 0
		// XXX: implement secondary key space reserve (see ::reserve)
		if ((conductor != null) && (conductor->getReserved() == true)) {
			reserveSegment(conductor, CONTEXT_STACK_OBJECT(global), newkey);
		}
		#endif

		SegMapEntry* infoEntry = addInformation(ctxt, CONTEXT_STACK_OBJECT(global), newkey, info, true /* enableCardinality */);
		infoEntry->getStoryLine().setStoryLock(newinfoRef.getStoryLine().getStoryLock());

		// XXX: see paged segment dirty setting below
		segment = CONTEXT_STACK_OBJECT(global).getSegment();
	}

	// XXX: ensure changes are active until paged out
	if (segment->getPaged() == true) {
		segment->setDirty(true);
	}

	return true;
}

template<typename K>
void RealTimeMap<K>::abortInformation(Transaction* tx, const bytearray pkey, const Information* orginfo, XInfoRef& preinfoRef, const Information* curinfo) {

	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();
	Information* preinfo = preinfoRef.getInfo();

	K curkey = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey1());

	Segment<K>* segment = getSegment(ctxt, curkey, false, false /* don't fill, ok to be virtual */);
	if (segment != null) {
		CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

		const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(curkey);
		if (infoEntry != null) {
			if (infoEntry->getValue() == curinfo) {
				Information* next = (Information*) preinfo; preinfo = null;

				// XXX: information to reroot is the first merged at current level after latest committed
				while ((next != null) && (next->getLevel() != Information::LEVEL_COMMIT)) {

					if ((next->getLevel() == Information::LEVEL_MERGED) && (next->getMergeLevel() != tx->getLevel())) {
						break;
					}

					if (next->getDeleting() == false) {
						K prekey = m_keyBuilder->fillKey(pkey, next->getData(), ctxt->getKey2());
						if (m_comparator->compare(prekey, curkey) == 0) {
							preinfo = next;
						}
					}

					if (next == orginfo) {
						break;
					}

					// XXX: walk storyline in reverse
					next = isolateInformation(ctxt, (Information*) orginfo, next);
				}

				// XXX: reassign preinfo
				preinfoRef.setInfo(preinfo);

				if ((preinfo != null) && (isolatedStoryline((Information*) infoEntry->getValue(), (Information*) preinfo) == false)) {
					InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry);
					stitchInformation(ctxt, null /* tx */, preinfoRef, infoRef);
				}
			}
		}
	}
}

template<typename K>
Information* RealTimeMap<K>::decomposeInformation(Transaction* tx, bytetype index, const Information* next, inttype* position, bytearray* pkey) {

	RealTimeConductor<K>* conductor = (RealTimeConductor<K>*) tx->getConductor(getIdentifier());

	int i = 0;
	if (*position != -1) {
		i = *position;
		*pkey = null;
	}

	Information* curinfo = null;

	for (; i < conductor->size(); i++) {
		SegMapEntry* infoEntry = conductor->get(i);
		if (infoEntry->getValue() == next) {
			*pkey = Converter<K>::toData(infoEntry->getKey());
			*position = i;
			break;
		}

		if ((next == null) && (i == *position)) {
			*pkey = Converter<K>::toData(infoEntry->getKey());
			curinfo = infoEntry->getValue();
			break;
		}
	}

	return curinfo;
}

#ifdef DEEP_IO_STATS
template<typename K>
void RealTimeMap<K>::ioStatistics(ulongtype* oR, ulongtype* oC, ulongtype* oU, ulongtype* oD, ulongtype* fV, ulongtype* fH, ulongtype* dV, ulongtype* dH) {

	ConcurrentContainer<ThreadContext<K>*>* container = m_threadContext.getContainer();
	container->begin();

	for (int i = 0; i < container->size(); i++) {
		ThreadContext<K>* context = container->get(i);

		if (context->m_statsForFiles == false) {
			*oR += context->m_statsCount1;
			*oC += context->m_statsCount2;
			*oU += context->m_statsCount3;
			*oD += context->m_statsCount4;

		} else {
			*fV += context->m_statsCount1;
			*fH += context->m_statsCount2;
			*dV += context->m_statsCount3;
			*dH += context->m_statsCount4;
		}
	}

	container->end();
}
#endif

template<typename K>
void RealTimeMap<K>::seekStatistics(ulongtype* oT, ulongtype* oI, boolean reset) {

	m_share.getIrtReadFileList()->lock();
	{
		Iterator<BufferedRandomAccessFile*>* iter = m_share.getIrtReadFileList()->iterator();
		while (iter->hasNext() == true) {
			RandomAccessFile* file = iter->next();
			if ((file == null) || ((file->m_seekTotal == 0) && (file->m_seekInterval == 0))) {
				continue;
			}

			DEEP_LOG(DEBUG, SEEKS, "KEY: seek total: %12lld, seek interval: %12lld - %s\n", file->m_seekTotal, file->m_seekInterval, file->getPath());

			(*oT) += file->m_seekTotal;
			(*oI) += file->m_seekInterval;

			file->m_seekTotal += file->m_seekInterval;
			file->m_seekInterval = 0;

			if (reset == true) {
				file->m_seekTotal = 0;
			}
		}
		Converter<Iterator<BufferedRandomAccessFile*>*>::destroy(iter);
	}
	m_share.getIrtReadFileList()->unlock();

	// XXX: secondaries don't have vrt files
	if (m_primaryIndex != null) {
		return;
	}

	m_share.getVrtReadFileList()->lock();
	{
		Iterator<BufferedRandomAccessFile*>* iter = m_share.getVrtReadFileList()->iterator();
		while (iter->hasNext() == true) {
			BufferedRandomAccessFile* file = iter->next();
			if ((file == null) || ((file->m_seekTotal == 0) && (file->m_seekInterval == 0))) {
				continue;
			}

			DEEP_LOG(DEBUG, SEEKS, "VAL: seek total: %12lld, seek interval: %12lld - %s\n", file->m_seekTotal, file->m_seekInterval, file->getPath());

			(*oT) += file->m_seekTotal;
			(*oI) += file->m_seekInterval;

			file->m_seekTotal += file->m_seekInterval;
			file->m_seekInterval = 0;

			if (reset == true) {
				file->m_seekTotal = 0;
			}
		}
		Converter<Iterator<BufferedRandomAccessFile*>*>::destroy(iter);
	}
	m_share.getVrtReadFileList()->unlock();
}

template<typename K>
longtype RealTimeMap<K>::findSummaryPaging(MeasuredRandomAccessFile* iwfile, const RealTimeLocality& lastLrtLocality, const uinttype recoveryEpoch) {
	return RealTimeVersion<K>::findSummaryPaging(iwfile, this, lastLrtLocality, recoveryEpoch);
}

template<typename K>
boolean RealTimeMap<K>::initSummaryPaging(MeasuredRandomAccessFile* iwfile) {
	boolean res = false;
	// XXX: safe context lock: one writer / no readers on the branch tree
	m_threadContext.writeLock();
	{
		res = RealTimeVersion<K>::summaryRebuildKeyPaging(iwfile, this);
	}
	m_threadContext.writeUnlock();

	// FLUX
	m_pagingIndex = iwfile->getFileIndex();

	return res;
}

template<typename K>
PagedSummarySet* RealTimeMap<K>::getActiveSummaries() {
	if (m_summaries == null) {
		PagedSummarySet* summaries = new PagedSummarySet(this);
		if (__sync_bool_compare_and_swap(&m_summaries, null, summaries) == false) {
			delete summaries;
		}
	}
	return m_summaries;
}

template<typename K>
void RealTimeMap<K>::invalidateSummary(const RealTime::Locality& streamLocality) {
	PagedSummarySet* activeSummaries = getActiveSummaries();
	activeSummaries->lock();
	PagedSummary* summaryRef = activeSummaries->getSummaryRef(streamLocality);

	if (summaryRef == null) {
		activeSummaries->unlock();
		return;
	}

	Converter<PagedSummary*>::validate(summaryRef);
	activeSummaries->unlock();

	if (activeSummaries->isLastSummary(summaryRef) == true) {
		// XXX: re-terminate IRT
		MeasuredRandomAccessFile* iwfile = m_share.getIrtWriteFileList()->get(summaryRef->getPagingLocality().getIndex());
		if (iwfile != null) {
			m_share.acquire(iwfile);
			{
				Locality prev = m_summaryLrtLocality;
				m_summaryLrtLocality = streamLocality;
				RealTimeVersion<K>::terminatePaging(iwfile, this, false /* summarized */, getRecoveryEpoch(), true /* invalidate */);
				m_summaryLrtLocality = prev;
				iwfile->setOnline(false);
			}
			m_share.release(iwfile);
		}
	}
	activeSummaries->removeSummary(summaryRef);
	Converter<PagedSummary*>::destroy(summaryRef); /* XXX: get above does incref() */
}

template<typename K>
void RealTimeMap<K>::initialize(void) {

	m_state = MAP_INITIAL;
	m_mount = MOUNT_INITIAL;

	m_resetIndex = 0;
	m_quantaDirty = 0;
	m_pagingIndex = -1;
	m_streamIndex = -1;

	m_optimizePaging = 1;
	m_optimizeStream = 1;

	m_activeKeyBlockSize = 0;
	m_activeKeyBlockCount = 0;

	m_valueCompressedBlockSize = 0;	
	m_valueCompressedBlockCount = 0;

	m_incrementCount = 0;
	m_activeValueSize = 0;
	m_activeValueCount = 0;
	m_schemaCheckpoint = 0;

	m_statisticsFlushTime = 0;
	m_coldPointReadLimitReached = 0;

	m_organizationTime = System::currentTimeMillis();
	m_finalizationTime = m_organizationTime;
}

template<typename K>
void RealTimeMap<K>::seedIncrement(void) {

	if (m_hasReservation == true) {
		if (size() != 0) {
			// XXX: internal get needs to clear out transactions
			ThreadContext<K>* ctxt = m_threadContext.getContext();
			#ifdef CXX_LANG_MEMORY_DEBUG
			ctxt->setTransaction(null, true);
			#else
			ctxt->setTransaction(null);
			#endif

			K retkey = ctxt->getKey1();

			if (get((K) Converter<K>::NULL_VALUE, null, LAST, &retkey, null, LOCK_NONE) == true) {
				m_incrementCount = m_keyBuilder->getReservedKey(retkey);
			}
		}

		// TODO: fix cast
		const SchemaBuilder<K>* schemaBuilder = m_primaryIndex == null ? m_schemaBuilder : ((RealTimeMap<K>*) m_primaryIndex)->m_schemaBuilder;
		if (schemaBuilder->getAutoIncrementValue() > (m_incrementCount + 1)) {
			m_incrementCount = schemaBuilder->getAutoIncrementValue() - 1;
		}
	}
}

template<typename K>
void RealTimeMap<K>::seedFinalFile(void) {

	Iterator<MeasuredRandomAccessFile*>* lwiter = m_share.getLrtWriteFileList()->iterator();
	while(lwiter->hasNext() == true) {
		MeasuredRandomAccessFile* lwfile = lwiter->next();
		if (lwfile == null) {
			continue;
		}

		if ((m_streamIndex == -1) && (lwfile->getClosureFile() == true)) {
			if (lwfile->getProtocol() != Versions::getProtocolVersion()) {
				// XXX: m_streamIndex is set in addStreamFile
				addStreamFile(true);

			} else {
				m_streamIndex = lwfile->getFileIndex();
			}

			m_currentLrtLocality = Locality(m_streamIndex, lwfile->length(), Locality::VIEWPOINT_NONE);
		}
	}
	Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(lwiter);

	Iterator<MeasuredRandomAccessFile*>* iwiter = m_share.getIrtWriteFileList()->iterator();
	while(iwiter->hasNext() == true) {
		MeasuredRandomAccessFile* iwfile = iwiter->next();
		if (iwfile == null) {
			continue;
		}

		if ((m_pagingIndex == -1) && (iwfile->getClosureFile() == true)) {
			if (iwfile->getProtocol() != Versions::getProtocolVersion()) {
				// XXX: m_pagingIndex is set in addPagingFile
				addPagingFile(true);

			} else {
				m_pagingIndex = iwfile->getFileIndex();
			}
		}
	}
	Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iwiter);

	if (m_prefetchMode == true) {
		Iterator<BufferedRandomAccessFile*>* vriter = m_share.getVrtReadFileList()->iterator();
		while (vriter->hasNext() == true) {
			BufferedRandomAccessFile* vrfile = vriter->next();
			if ((vrfile != null) && (vrfile->length() > Properties::DEFAULT_FILE_FETCH_MIN)) {
				longtype start = System::currentTimeMillis();
				{
					m_share.acquire(vrfile);
					{
						vrfile->prefetch(Properties::DEFAULT_FILE_BUFFER, Properties::DEFAULT_FILE_FETCH_FACTOR);
						vrfile->BufferedRandomAccessFile::setPosition(0);
						vrfile->BufferedRandomAccessFile::setCursor(0);
					}
					m_share.release(vrfile);
				}
				longtype stop = System::currentTimeMillis();

				DEEP_LOG(DEBUG, FETCH, "block: %s, elapsed: %lld, length: %lld\n", vrfile->getPath(), (stop-start), vrfile->length());
			}
		}
		Converter<Iterator<BufferedRandomAccessFile*>*>::destroy(vriter);
	}

	if (m_share.getIrtWriteFileList()->size() == 0) {
		// XXX: m_pagingIndex is set in addPagingFile
		addPagingFile(true);
	}

	if (m_primaryIndex == null) {
		if (m_share.getLrtWriteFileList()->size() == 0) {
			// XXX: m_streamIndex is set in addStreamFile
			addStreamFile(true);
		}
	}
}

template<typename K>
void RealTimeMap<K>::seedValueAverage(void) {

	if ((m_primaryIndex == null) && (m_share.getValueAverage() == -1) && (size() != 0)) {
		// XXX: internal get needs to clear out transactions
		ThreadContext<K>* ctxt = m_threadContext.getContext();
		#ifdef CXX_LANG_MEMORY_DEBUG
		ctxt->setTransaction(null, true);
		#else
		ctxt->setTransaction(null);
		#endif

		nbyte value(1);
		K retkey = ctxt->getKey1();

		if (get((K) Converter<K>::NULL_VALUE, &value, LAST, &retkey, null, LOCK_NONE) == true) {
			m_share.setValueAverage(value.length);
		}
	}
}

template<typename K>
ulongtype RealTimeMap<K>::maximumKey(void) {

	ulongtype maxKey = 0;

	if (m_hasReservation == true) {
		// XXX: internal get needs to clear out transactions
		ThreadContext<K>* ctxt = m_threadContext.getContext();
		#ifdef CXX_LANG_MEMORY_DEBUG
		ctxt->setTransaction(null, true);
		#else
		ctxt->setTransaction(null);
		#endif

		K rKey = m_keyBuilder->newKey();

		boolean status = get((K) Converter<K>::NULL_VALUE, (nbyte*) null, RealTime::LAST, &rKey);
		if (status == true) {
			maxKey = m_keyBuilder->getReservedKey(rKey);
		}

		Converter<K>::destroy(rKey);
	}

	return maxKey;
}

template<typename K>
inttype RealTimeMap<K>::getTotalSegments(void) {
	return m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size();
}

template<typename K>
inttype RealTimeMap<K>::getActiveSegments(void) {
	return m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() - getPurgedSegments();
}

template<typename K>
inttype RealTimeMap<K>::getPurgedSegments(void) {
	// XXX: actual active vs actual purged - TODO: account for virtual (i.e. partially filled segments)
	return m_purgeSize.get();
}

template<typename K>
uinttype RealTimeMap<K>::getFileSize(boolean adaptive) {
	return RealTimeAdaptive_v1<K>::getFileInitialSize(this, adaptive);
}

template<typename K>
inttype RealTimeMap<K>::getSegmentSize(boolean adaptive) {
	return RealTimeAdaptive_v1<K>::getSegmentInitialSize(this, adaptive);
}

template<typename K>
boolean RealTimeMap<K>::pendingCheckpoint(void) {
	return m_checkpointComplete == false;
}

template<typename K>
boolean RealTimeMap<K>::toggleCheckpointMetrics() {

	boolean toggled = false;

	if (m_state != MAP_RUNNING) {
		return toggled;

	} else if ((m_irtForceReorganization == true) || (m_vrtForceReorganization == true)) {
		return toggled;
	}

	RealTimeShare* share = (m_primaryIndex == null) ? &m_share : m_primaryIndex->getShare();

	if (m_primaryIndex == null) {

		boolean doCheckpoint = false;
		boolean idle = false;

		if (Properties::getCheckpointMode() != Properties::CHECKPOINT_OFF) {
			if (Properties::getCheckpointMode() == Properties::CHECKPOINT_AUTO) {
				const longtype currentTime = System::currentTimeMillis();
				// XXX: auto checkpoint interval has expired
				doCheckpoint = (currentTime - m_finalizationTime) > (Properties::getAutomaticCheckpointInterval() * 1000);
				if ((doCheckpoint == true) && (m_checkptTriggered == false)) {
					m_checkptTriggered = true;
					DEEP_LOG(DEBUG, CHECK, "Automatic checkpoint interval triggered: %lld, %s\n", (currentTime - m_finalizationTime), getFilePath());
				}

			} else if (Properties::getCheckpointMode() == Properties::CHECKPOINT_MANUAL) {
				if (getManualCheckpointSequence() < getGlobalManualCheckpointSequence()) {
					syncManualCheckpointSequence();
					doCheckpoint = true;

					DEEP_LOG(DEBUG, CHECK, "Manual checkpoint triggered: %lld, %s\n", getManualCheckpointSequence(), getFilePath());
				}
			}

			// XXX: idle
			if ((doCheckpoint == false) && (RealTimeAdaptive_v1<K>::getSystemIdle(this) == true)) {
				doCheckpoint = true;
				idle = true;
			}
		}

		const boolean force = getAndResetInternalCheckpointRequest();
		if (force == true) {
			DEEP_LOG(DEBUG, CHECK, "Processing internal chekpoint request, %s\n", getFilePath());
			idle = false;
			doCheckpoint = true;
		}

		if (doCheckpoint == true) {
			share->getVrtWriteFileList()->lock();
			{
				// XXX: only checkpoint if something has changed
				if ((force == true) || (toggleStreamingMetrics(false /* save */, true /* checkpoint */, idle) == true)) {
					// XXX: make sure we have something to checkpoint on
					toggled = (m_currentLrtLocality.isNone() == false);

					// XXX: check to see whether secondaries have finished last checkpoint
					toggled = (toggled == true) && (checkpointFullyComplete() == true);

					// XXX: determine and set checkpoint locality
					toggled = (toggled == true) && (selectCheckpointLocality() == true);

					if (toggled == true) {
						m_organizationTime = System::currentTimeMillis();
						m_finalizationTime = m_organizationTime;
						m_checkptTriggered = false;
					}
				}
			}
			share->getVrtWriteFileList()->unlock();
		}

		if ((toggled == true) && (idle == true)) {
			DEEP_LOG(DEBUG, CHECK, "Processing internal idle checkpoint request, %s\n", getFilePath());
		}

	} else {
		// XXX: determine if there is a pending checkpoint for this index (the primary drives this); see selectCheckpointLocality()
		if (getCheckpointComplete() == false) {
			toggled = true;

			// XXX: lock order matters here
			share->getVrtWriteFileList()->lock();
			checkpointLock();
			{
				// XXX: derive endwise locality from checkpoint locality (since primary checkpoint/endwise may have moved on)
				m_endwiseLrtLocality = m_checkptLrtLocality;
			}
			checkpointUnlock();
			share->getVrtWriteFileList()->unlock();
		}
	}

	if (toggled == false) {
		// XXX: update endwise on regular indexing
		toggleStreamingMetrics(true /* save */, false /* checkpoint */);
	}

	return toggled;
}

template<typename K>
boolean RealTimeMap<K>::selectCheckpointLocality(void) {
	#ifdef DEEP_DEBUG
	if (m_primaryIndex != null) {
		DEEP_LOG(ERROR, CHECK, "unable to select checkpoint from secondary, %s\n", getFilePath());
		throw InvalidException("Unable to select checkpoint from secondary");
	}
	#endif

	// XXX: safely grab m_checkptRequestLock write lock (avoid lock cycle w/ committing transactions)
	if (m_checkptRequestLock.tryWriteLock() == false) {
		m_share.getVrtWriteFileList()->unlock();
		m_checkptRequestLock.writeLock();
		m_share.getVrtWriteFileList()->lock();
	}
	// XXX: mark our intention to checkpoint
	m_checkptRequested = true;
	inttype retry = 0;
	while (__sync_or_and_fetch(&m_pendingCommits, 0) != 0) {
		if (++retry >= Properties::DEFAULT_CACHE_CHECK_THREADS_RETRY) {
			DEEP_LOG(WARN, CHECK, "timeout waiting for checkpoint locality; store: %s, locality: %s\n", getFilePath(), (const char*)m_checkptLrtLocality.toString());
			// XXX: clear our intention to checkpoint
			m_checkptRequested = false;
			m_checkptRequestLock.writeUnlock();

			return false;
		}
		m_share.getVrtWriteFileList()->unlock();

		DEEP_LOG(DEBUG, CHECK, "store: %s, wait, locality: %s\n", getFilePath(), (const char*)m_checkptLrtLocality.toString());

		Thread::sleep(Properties::DEFAULT_CACHE_SLEEP);

		m_share.getVrtWriteFileList()->lock();
	}
	m_checkptLrtLocality = m_currentLrtLocality.updateViewpoint(Transaction::getNextViewpoint());
	m_checkptRequestLock.writeUnlock();

	// XXX: we've identified the checkpoint locality, so kick off the checkpoint on all indexes
	checkpointLock();
	{
		// XXX: nothing to checkpoint
		if (m_checkptLrtLocality.isNone() == true) {
			checkpointUnlock();
			// XXX: clear our intention to checkpoint
			m_checkptRequested = false;

			return false;
		}

		// XXX: bail out if any secondary is in the middle of a hot add (checkpoint would be useless)
		for (inttype i = 0; i < m_share.getSecondaryList()->size(); ++i) {
			RealTime* rt = m_share.getSecondaryList()->get(i);
			if (rt == null) {
				// XXX: allow for dynamic add/remove secondary index
				continue;
			}
			inttype secState = ((RealTimeMap<K>*)rt)->m_state;
			inttype secMount = ((RealTimeMap<K>*)rt)->m_mount;

			if ((secMount != MOUNT_OPENED) || (secState != MAP_RUNNING)) {
				checkpointUnlock();
				// XXX: clear our intention to checkpoint
				m_checkptRequested = false;

				return false;
			}
		}

		setCheckpointValid(true, false /* lock */);
		setCheckpointComplete(false);
		for (inttype i = 0; i < m_share.getSecondaryList()->size(); ++i) {
			RealTime* rt = m_share.getSecondaryList()->get(i);
			if (rt == null) {
				// XXX: allow for dynamic add/remove secondary index
				continue;
			}

			// XXX: trigger checkpoint on secondary
			rt->setCheckptLrtLocality(m_checkptLrtLocality);
			rt->setCheckpointComplete(false);
		}
		DEEP_LOG(DEBUG, CHECK, "store: %s, statistic, locality: %s\n", getFilePath(), (const char*)m_checkptLrtLocality.toString());
		m_endwiseLrtLocality = m_checkptLrtLocality;

		// XXX: clear our intention to checkpoint
		m_checkptRequested = false;
	}
	checkpointUnlock();

	return true;
}

template<typename K>
boolean RealTimeMap<K>::toggleStreamingMetrics(boolean save, boolean checkpoint, boolean idle) {

	#ifdef DEEP_DEBUG
	if ((checkpoint == true) && (save == true)) {
		DEEP_LOG(ERROR, CHECK, "Invalid save/checkpoint state, %s\n", getFilePath());
		throw InvalidException("Invalid save/checkpoint state");
	}
	if ((checkpoint == false) && (idle == true)) {
		DEEP_LOG(ERROR, CHECK, "Invalid checkpoint/idle state, %s\n", getFilePath());
		throw InvalidException("Invalid checkpoint/idle state");
	}
	#endif

	boolean toggled = false;

	if (m_primaryIndex == null) {
		// XXX: ensure we summarize final state on idle
		if (idle == true) {
			// XXX: only idle summarize/finalize if there's no regular indexing to be done
			const boolean needsRegularIndexing = getLocalityCmp()->compare(m_endwiseLrtLocality, m_currentLrtLocality) != 0;
			toggled = (needsRegularIndexing == false) && (m_currentLrtLocality.isNone() == false) && (getLocalityCmp()->compareNoViewpoint(m_summaryLrtLocality, m_currentLrtLocality) != 0);
		} else {
			if (checkpoint == true) {
				toggled = getLocalityCmp()->compareNoViewpoint(m_summaryLrtLocality, m_currentLrtLocality) != 0;
			} else {
				toggled = getLocalityCmp()->compareNoViewpoint(m_endwiseLrtLocality, m_currentLrtLocality) != 0;
			}
		}

		if ((toggled == true) && (save == true)) {
			m_endwiseLrtLocality = m_currentLrtLocality;
		}

	} else {
		#ifdef DEEP_DEBUG
		if (checkpoint == true) {
			DEEP_LOG(ERROR, CHECK, "Invalid checkpoint state, %s\n", getFilePath());
			throw InvalidException("Invalid checkpoint state");
		}
		#endif

		toggled = m_primaryIndex->getCurrentLrtLocality() != m_endwiseLrtLocality;

		if ((toggled == true) && (save == true)) {
			m_endwiseLrtLocality = m_primaryIndex->getCurrentLrtLocality();
		}
	}

	return toggled;
}

template<typename K>
void RealTimeMap<K>::abortCheckpoint(const RealTimeLocality& streamLocality) {
	BasicArray<RealTime*>* secondaries = null;
	if (m_primaryIndex == null) {
		secondaries = getShare()->getSecondaryList();

	} else {
		secondaries = m_primaryIndex->getShare()->getSecondaryList();
		m_primaryIndex->invalidateSummary(streamLocality);
	}

	for (inttype i=0; i<secondaries->size(); ++i) {
		RealTime* rt = secondaries->get(i);
		if ((rt == null) || (rt == this)) {
			// XXX: allow for dynamic add/remove secondary index
			continue;
		}
		rt->invalidateSummary(streamLocality);
	}
}

template<typename K>
void RealTimeMap<K>::addStreamFile(boolean release) {

	if (m_memoryMode == true) {
		return;
	}

	inttype fileIndex = 0;
	if (m_share.getLrtWriteFileList()->size() != 0) {
		fileIndex = m_share.getLrtWriteFileList()->getMaxIndex()+1;
		if (fileIndex > (ushorttype) Properties::DEFAULT_FILE_ARRAY) {
			boolean found = false;
			for (int i = 0; i < m_share.getLrtWriteFileList()->size(); i++) {
				if (m_share.getLrtWriteFileList()->get(i) == null) {
					found = true;
					fileIndex = i;
					break;
				}
			}

			if (found == false) {
				DEEP_LOG(ERROR, OTHER, "Maximum number of log files allocated: %s\n", getFilePath());

				throw InvalidException("Maximum number of log files allocated");
			}
		}
	}

	String sname = getFilePath();
	String dname = "." + File::separator;

	inttype index = sname.lastIndexOf(File::separator);
	if (index != -1) {
		dname = sname.substring(0, index + 1);
		sname = sname.substring(index + 1, sname.length() - 1);
	}

	char date[Properties::DEFAULT_FILE_NAME_SIZE];
	MapFileUtil::format(date, fileIndex, true, Properties::DEFAULT_FILE_NAME_SIZE);

	boolean needsLink = false;
	String path = getDataDirectory(&needsLink);
	String lrname = path + date + sname + MapFileUtil::FILE_SUFFIX_LRT;
	String vrname = path + date + sname + MapFileUtil::FILE_SUFFIX_VRT;
	String srname = path + date + sname + MapFileUtil::FILE_SUFFIX_SRT;
	String xrname = path + date + sname + MapFileUtil::FILE_SUFFIX_XRT;

	const time_t creationTime = MapFileUtil::fileCreationTime(date);

	MeasuredRandomAccessFile* lwfile = new MeasuredRandomAccessFile(lrname, "rw", MapFileUtil::LRT, Properties::DEFAULT_FILE_BUFFER);
	lwfile->setOptimizeCount(m_optimizeStream /* disguise between pre/post optimizing files */);
	lwfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
	lwfile->setFileIndex(fileIndex);
	lwfile->setFileCreationTime(creationTime);
	if (needsLink == true) {
		// TODO:
		Files::createSymbolicLink(dname + date + sname + MapFileUtil::FILE_SUFFIX_LRT, lrname);
	}

	MeasuredRandomAccessFile* vwfile = new MeasuredRandomAccessFile(vrname, "rw", MapFileUtil::VRT, Properties::DEFAULT_FILE_BUFFER);
	vwfile->setOptimizeCount(m_optimizeStream /* disguise between pre/post optimizing files */);
	vwfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
	vwfile->setFileIndex(fileIndex);
	vwfile->setFileCreationTime(creationTime);
	if (needsLink == true) {
		// TODO:
		Files::createSymbolicLink(dname + date + sname + MapFileUtil::FILE_SUFFIX_VRT, vrname);
	}

	BufferedRandomAccessFile* vrfile = new BufferedRandomAccessFile(vrname, "r", Properties::DEFAULT_FILE_BUFFER);
	vrfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
	vrfile->setWriter(vwfile);
	vrfile->setFileIndex(fileIndex);
	vrfile->setFileCreationTime(creationTime);

	RandomAccessFile* swfile = new RandomAccessFile(srname, "rw", false /* offline */);
	swfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
	swfile->setFileIndex(fileIndex);
	swfile->setFileCreationTime(creationTime);
	if (needsLink == true) {
		// TODO:
		Files::createSymbolicLink(dname + date + sname + MapFileUtil::FILE_SUFFIX_SRT, srname);
	}

	RandomAccessFile* srfile = new RandomAccessFile(srname, "r", false /* offline */);
	srfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
	srfile->setFileIndex(fileIndex);
	srfile->setFileCreationTime(creationTime);

	RandomAccessFile* xwfile = new RandomAccessFile(xrname, "rw", false /* offline */);
	xwfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
	xwfile->setFileIndex(fileIndex);
	xwfile->setFileCreationTime(creationTime);
	if (needsLink == true) {
		// TODO:
		Files::createSymbolicLink(dname + date + sname + MapFileUtil::FILE_SUFFIX_XRT, xrname);
	}

	RandomAccessFile* xrfile = new RandomAccessFile(xrname, "r", false /* offline */);
	xrfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
	xrfile->setFileIndex(fileIndex);
	xrfile->setFileCreationTime(creationTime);

	m_share.acquire(lwfile);
	m_share.acquire(vwfile);
	m_share.acquire(swfile);
	m_share.acquire(xwfile);

	MapFileUtil::version(lwfile, m_share.getKeySize() /* TODO: should be schema hash */, Properties::DEFAULT_FILE_HEADER);
	MapFileUtil::version(vwfile, m_share.getValueSize() /* TODO: should be schema hash */, Properties::DEFAULT_FILE_HEADER);

	ExtraStatistics_v1::version(xwfile);
	#if 0
	// XXX: Always print the activation key when we first make a table
	ExtraStatistics_v1::updateLicense(xwfile, Properties::getActivationKey(), Properties::getSystemUid(), true);
	#endif

	if (m_schemaBuilder->isTemporary() == false) {
		RealTimeSchema_v1<K>::write(m_schemaBuilder, swfile, true);

		for (inttype i = 0; i < m_share.getSecondaryList()->size(); ++i) {
			RealTime* rt = m_share.getSecondaryList()->get(i);
			if (rt == null) {
				// XXX: allow for dynamic add/remove secondary index
				continue;
			}

			// TODO: fix cast
			RealTimeSchema_v1<K>::write(((RealTimeMap<K>*) rt)->m_schemaBuilder, swfile, false);
		}
	}

	// XXX: lrt/vrt/srt/xrt write files are virtually locked (i.e. synchronized through commit threads)
	m_share.getLrtWriteFileList()->add(lwfile);
	m_share.getVrtWriteFileList()->add(vwfile);
	m_share.getSrtWriteFileList()->add(swfile);
	m_share.getXrtWriteFileList()->add(xwfile);

	m_share.getVrtReadFileList()->add(vrfile);
	m_share.getSrtReadFileList()->add(srfile);
	m_share.getXrtReadFileList()->add(xrfile);

	vwfile->setClosureFile(true);
	lwfile->setClosureFile(true);

	if (release == true) {
		m_share.release(vwfile);
		m_share.release(lwfile);

	} else {
		vwfile->unlock();
		lwfile->unlock();
	}

	m_share.release(swfile);
	m_share.release(xwfile);

	m_streamIndex = lwfile->getFileIndex();

	DEEP_LOG(DEBUG, F_VRT, "store: %s, %s - %d\n", getFilePath(), date, m_streamIndex);
}

template<typename K>
void RealTimeMap<K>::addPagingFile(boolean release) {

	if (m_memoryMode == true) {
		return;
	}

	inttype fileIndex = 0;
	if (m_share.getIrtWriteFileList()->size() != 0) {
		fileIndex = m_share.getIrtWriteFileList()->getMaxIndex()+1;
		if (fileIndex > (ushorttype) Properties::DEFAULT_FILE_ARRAY) {
			boolean found = false;
			for (int i = 0; i < m_share.getIrtWriteFileList()->size(); i++) {
				if (m_share.getIrtWriteFileList()->get(i) == null) {
					found = true;
					fileIndex = i;
					break;
				}
			}

			if (found == false) {
				DEEP_LOG(ERROR, OTHER, "Maximum number of index files allocated: %s\n", getFilePath());

				throw InvalidException("Maximum number of index files allocated");
			}
		}
	}

	String sname = getFilePath();
	String dname = "." + File::separator;

	inttype index = sname.lastIndexOf(File::separator);
	if (index != -1) {
		dname = sname.substring(0, index + 1);
		sname = sname.substring(index + 1, sname.length() - 1);
	}

	char date[Properties::DEFAULT_FILE_NAME_SIZE];
	MapFileUtil::format(date, fileIndex, true, Properties::DEFAULT_FILE_NAME_SIZE);

	boolean needsLink = false;
	String path = getIndexDirectory(&needsLink);
	String irtname = path + date + sname + MapFileUtil::FILE_SUFFIX_IRT;

	const time_t creationTime = MapFileUtil::fileCreationTime(date);

	MeasuredRandomAccessFile* iwfile = new MeasuredRandomAccessFile(irtname, "rw", MapFileUtil::IRT, m_irtBuffer);
	iwfile->setOptimizeCount(m_optimizePaging /* disguise between pre/post optimizing files */);
	iwfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
	iwfile->setFileIndex(fileIndex);
	iwfile->setFileCreationTime(creationTime);
	iwfile->setPagingState(m_indexValue, -1, -1L, RealTimeLocality::LOCALITY_NONE);
	if ((m_pagingIndex != -1) && (m_share.getIrtWriteFileList()->size() != 0)) {
		// XXX: treat a new IRT as an extension of the last one (locality-wise, to detect "useful" indexing)
		iwfile->setInitialLrtLocality(m_share.getIrtWriteFileList()->get(m_pagingIndex)->getInitialLrtLocality());
	}
	if (needsLink == true) {
		// TODO:
		Files::createSymbolicLink(dname + date + sname + MapFileUtil::FILE_SUFFIX_IRT, irtname);
	}

	BufferedRandomAccessFile* irfile = new BufferedRandomAccessFile(irtname, "r", m_irtBuffer);
	irfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
	irfile->setWriter(iwfile);
	irfile->setFileIndex(fileIndex);
	irfile->setFileCreationTime(creationTime);

	RandomAccessFile* swfile = null;
	// XXX: currently not needed
	#if 0
	RandomAccessFile* xwfile = null;
	#endif
	if (m_primaryIndex != null) {
		swfile = m_primaryIndex->getShare()->getSrtWriteFileList()->last();
		// XXX: currently not needed
		#if 0
		xwfile = m_primaryIndex->getShare()->getXrtWriteFileList()->last();
		#endif
	}

	m_share.acquire(iwfile);

	MapFileUtil::version(iwfile, m_share.getKeySize() /* TODO: should be schema hash */, Properties::DEFAULT_FILE_HEADER);

	if (m_schemaBuilder->isTemporary() == false) {
		if (swfile != null) {
			m_share.acquire(swfile);
			{
				RealTimeSchema_v1<K>::write(m_schemaBuilder, swfile, false);
			}
			m_share.release(swfile);
		}
	}

	// XXX: currently not needed
	#if 0
	if (xwfile != null) {
		m_share.acquire(xwfile);
		{
			ExtraStatistics_v1::write(xwfile, &m_extraStats, ...);
		}
		m_share.release(xwfile);
	}
	#endif

	m_share.getIrtWriteFileList()->add(iwfile);
	m_share.getIrtReadFileList()->add(irfile);

	iwfile->setClosureFile(true);

	if (release == true) {
		m_share.release(iwfile);

	} else {
		iwfile->unlock();
	}

	m_pagingIndex = iwfile->getFileIndex();

	DEEP_LOG(DEBUG, F_IRT, "store: %s, %s - %d\n", getFilePath(), date, m_pagingIndex);
}

template<typename K>
void RealTimeMap<K>::destroyFile(MeasuredRandomAccessFile* file) {
	struct Util {
		FORCE_INLINE static void collectLocalities(TreeSet<PagedSummary*>* impacted, BasicArray<Locality>& toInvalidate) {
			if (impacted == null) {
				return;
			}
			Iterator<PagedSummary*>* iter = impacted->iterator();
			while(iter->hasNext() == true) {
				PagedSummary* summaryRef = iter->next();
				Converter<PagedSummary*>::validate(summaryRef);
				toInvalidate.add(summaryRef->getStreamLocality(), true);
			}
			delete iter;
		}

		FORCE_INLINE static void invalidateSummaries(RealTime* primary, BasicArray<Locality>& toInvalidate) {
			for (inttype i=0; i < toInvalidate.size(); ++i) {
				const Locality& streamLocality = toInvalidate.get(i);
				primary->invalidateSummary(streamLocality);

				BasicArray<RealTime*>* secondaries = primary->getShare()->getSecondaryList();
				for (inttype i=0; i<secondaries->size(); ++i) {
					RealTime* rt = secondaries->get(i);
					if (rt == null) {
						// XXX: allow for dynamic add/remove secondary index
						continue;
					}
					rt->invalidateSummary(streamLocality);
				}
			}
		}
	};

	#ifdef DEEP_DEBUG
	if (tryClobberLock() == true) {
		DEEP_LOG(ERROR, OTHER, "Invalid lock state during destroyFile(), %s\n", getFilePath());
		throw InvalidException("Invalid lock state during destroyFile()");
	}
	#endif

	const inttype i = file->getFileIndex();
	RealTime* primary = (m_primaryIndex == null) ? this : this->m_primaryIndex;
	switch(file->getType()) {
		case MapFileUtil::VRT:
			{
				PagedSummarySet* activeSummaries = primary->getActiveSummaries();
				activeSummaries->lock();
				TreeSet<PagedSummary*>* impacted = activeSummaries->summariesForStreamFile(i);
				BasicArray<Locality> toInvalidate((impacted == null) ? 0 : impacted->size());
				Util::collectLocalities(impacted, toInvalidate);
				activeSummaries->unlock();
				Util::invalidateSummaries(primary, toInvalidate);

				RandomAccessFile* srfile = m_share.getSrtReadFileList()->get(i);
				RandomAccessFile* xrfile = m_share.getXrtReadFileList()->get(i);
				RandomAccessFile* swfile = m_share.getSrtWriteFileList()->get(i);
				RandomAccessFile* xwfile = m_share.getXrtWriteFileList()->get(i);

				BufferedRandomAccessFile* vrfile = m_share.getVrtReadFileList()->get(i);
				MeasuredRandomAccessFile* lwfile = m_share.getLrtWriteFileList()->get(i);
				MeasuredRandomAccessFile* vwfile = file;

				m_share.getSrtWriteFileList()->remove(i);
				m_share.getXrtWriteFileList()->remove(i);
				m_share.getSrtReadFileList()->remove(i);
				m_share.getXrtReadFileList()->remove(i);

				m_share.getVrtReadFileList()->remove(i);
				m_share.getLrtWriteFileList()->remove(i);
				m_share.getVrtWriteFileList()->remove(i);

				if (m_cleanupMode == false) {
					DEEP_LOG(DEBUG, FSMGT, "archive, block: %s, %c[%d;%dmcompleted%c[%d;%dm\n", vwfile->getPath(), 27,1,36,27,0, 25);

					String xrpath = xwfile->getPath();
					String xapath = xrpath.replaceLast(".xrt", ".xar");
					FileUtil::move(xrpath, xapath);

					String srpath = swfile->getPath();
					String sapath = srpath.replaceLast(".srt", ".sar");
					FileUtil::move(srpath, sapath);

					String lrpath = lwfile->getPath();
					String lapath = lrpath.replaceLast(".lrt", ".lar");
					FileUtil::move(lrpath, lapath);

					String vrpath = vwfile->getPath();
					String vapath = vrpath.replaceLast(".vrt", ".var");
					FileUtil::move(vrpath, vapath);

				} else {
					DEEP_LOG(DEBUG, FSMGT, "erase, block: %s, %c[%d;%dmcompleted%c[%d;%dm\n", vwfile->getPath(), 27,1,36,27,0, 25);
				}

				m_share.destroy(vrfile);
				m_share.destroy(srfile);
				m_share.destroy(xrfile);

				m_share.destroy(xwfile, m_cleanupMode /* clobber */);
				m_share.destroy(swfile, m_cleanupMode /* clobber */);
				m_share.destroy(lwfile, m_cleanupMode /* clobber */);
				m_share.destroy(vwfile, m_cleanupMode /* clobber */);
			}
			break;
		case MapFileUtil::IRT:
			{
				PagedSummarySet* activeSummaries = this->getActiveSummaries();
				activeSummaries->lock();
				TreeSet<PagedSummary*>* impacted = activeSummaries->summariesForPagingFile(i);
				BasicArray<Locality> toInvalidate((impacted == null) ? 0 : impacted->size());
				Util::collectLocalities(impacted, toInvalidate);
				activeSummaries->unlock();
				Util::invalidateSummaries(primary, toInvalidate);

				BufferedRandomAccessFile* irfile = m_share.getIrtReadFileList()->get(i);
				MeasuredRandomAccessFile* iwfile = file;

				DEEP_LOG(DEBUG, FSMGT, "erase, block: %s, %c[%d;%dmcompleted%c[%d;%dm\n", iwfile->getPath(), 27,1,36,27,0, 25);

				m_share.getIrtReadFileList()->remove(i);
				m_share.getIrtWriteFileList()->remove(i);

				m_share.destroy(irfile);
				m_share.destroy(iwfile, true /* clobber */);
			}
			break;
		default:
			#ifdef DEEP_DEBUG
			DEEP_LOG(ERROR, OTHER, "Invalid file deletion request, %s\n", file->getPath());
			throw InvalidException("Invalid file deletion request");
			#endif
			clobberUnlock();
			return;
	}
}

template<typename K>
void RealTimeMap<K>::flushSegmentMemory(void) {

	inttype indexed = 0;

	ThreadContext<K>* ctxt = m_threadContext.getContext();
	#ifdef CXX_LANG_MEMORY_DEBUG
	ctxt->setTransaction(null, true);
	#else
	ctxt->setTransaction(null);
	#endif

	longtype start = System::currentTimeMillis();
	{
		deleteSegments();

		if (m_memoryMode == false) {
			indexed = onlineFinalizingSegments(ctxt);
		}
	}
	longtype stop = System::currentTimeMillis();

	DEEP_LOG(DEBUG, CEASE, "store: %s, elapsed: %lld, indexed: %d\n", getFilePath(), (stop-start), indexed);
}

template<typename K>
void RealTimeMap<K>::deleteSegmentMemory(void) {

	inttype segments = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size();

	longtype start = System::currentTimeMillis();
	{
		m_orderSegmentList.clear();

		MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) m_orderSegmentSet.reset();
		while (segIter->MapSegmentEntrySetIterator::hasNext()) {
			const MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
			Segment<K>* segment = segEntry->getValue();

			// XXX: note that we are in memory mode during clobber
			if ((m_memoryMode == true) && (m_mount == MOUNT_CLOSING) && (m_primaryIndex == null)) {
				typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);
				segment->SegTreeMap::entrySet(&stackSegmentItemSet);
				MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();

				while (infoIter->MapInformationEntrySetIterator::hasNext()) {
					SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();

					Information* info = infoEntry->getValue();
					// XXX: root info will be cleared via TreeMap::clear below
					if (info->getNext() != null) {
						boolean dirty = false;
						RealTimeAdaptive_v1<K>::deleteUnviewed(this, m_threadContext.getContext(), infoEntry, segment, &dirty);
					}
				}
			}

			#if defined TCMALLOC || JEMALLOC
			segment->setDeleteKey(true);
			segment->setDeleteValue(true);
			m_resource.gc(segment);
			#else
			segment->SegTreeMap::clear(true /* delKey */, true /* delVal */);
			delete segment;
			#endif
		}

		m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::clear();
	}
	longtype stop = System::currentTimeMillis();

	m_entrySize.set(0);
	m_purgeSize.set(0);

	if (m_recoverLrtLocality.isNone() == true) {
		DEEP_LOG(DEBUG, CLEAR, "store: %s, elapsed: %lld, segments: %d\n", getFilePath(), (stop-start), segments);
	}
}

template<typename K>
void RealTimeMap<K>::deleteCacheAndFileMemory(void) {

	m_share.clearDeferred();
	m_share.destroy<MeasuredRandomAccessFile>(m_share.getVrtWriteFileList());
	m_share.destroy<MeasuredRandomAccessFile>(m_share.getLrtWriteFileList());
	m_share.destroy<MeasuredRandomAccessFile>(m_share.getIrtWriteFileList());
	m_share.destroy<BufferedRandomAccessFile>(m_share.getVrtReadFileList());
	m_share.destroy<BufferedRandomAccessFile>(m_share.getIrtReadFileList());
	m_share.destroy<RandomAccessFile>(m_share.getSrtWriteFileList());
	m_share.destroy<RandomAccessFile>(m_share.getSrtReadFileList());
	m_share.destroy<RandomAccessFile>(m_share.getXrtWriteFileList());
	m_share.destroy<RandomAccessFile>(m_share.getXrtReadFileList());

	deleteSegmentMemory();
}

template<typename K>
void RealTimeMap<K>::abortRealTime(RealTimeConductor<K>* conductor) {

	// XXX: nothing to do
}

template<typename K>
void RealTimeMap<K>::commitRealTime(RealTimeConductor<K>* conductor) {

	// XXX: bail out in recovery mode to avoid writing to disk a second time
	if (m_state == MAP_RECOVER) {
		return;
	}

	RealTimeVersion<K>::commitRealTime(this, conductor, false, false);
}

template<typename K>
void RealTimeMap<K>::rolloverRealTime(ThreadContext<K>* ctxt, Segment<K>* segment) {

	#ifdef DEEP_DEBUG
	if (m_state == MAP_RECOVER) {
		DEEP_LOG(ERROR, OTHER, "Rollover in recovery: %s\n", getFilePath());

		throw InvalidException("Rollover in recovery");
	}
	#endif

	if (segment->getSummary() == true) {
		#ifdef DEEP_DEBUG
		DEEP_LOG(ERROR, OTHER, "Rollover of summary segment, %s\n", getFilePath());
		throw InvalidException("Rollover of summary segment");
		#else
		return;
		#endif
	}

	rolloverSegment(ctxt, segment);
}

template<typename K>
void RealTimeMap<K>::mergeCacheMemory(RealTimeConductor<K>* conductor) {

	inttype curlevel = conductor->getTransaction()->getLevel();
	inttype prelevel = conductor->getTransaction()->getLevel() - 1;
	inttype mergedown = conductor->m_updateStats + conductor->m_deleteStats;

	for (int i = conductor->size() - 1; i >= 0; i--) {
		SegMapEntry* walkEntry = conductor->get(i);
		Information* preinfo = walkEntry->getValue();

		if (preinfo->getLevel() != prelevel) {
			if (preinfo->getLevel() == curlevel) {
				preinfo->setLevel(prelevel);
				continue;

			} else if (preinfo->getLevel() < Information::LEVEL_COMMIT) {
				if ((preinfo->getLevel() == Information::LEVEL_MERGED) && (preinfo->getMergeLevel() == curlevel)) {
					preinfo->setMergeLevel(prelevel);
				}

				continue;

			} else {
				break;
			}

		} else if (mergedown == 0) {
			break;
		}

		if (preinfo->getUpdating() == false) {

			Information* curinfo = preinfo->getNext();
			if (curinfo != null) {
				shorttype level = preinfo->getLevel();
				preinfo->setLevel(Information::LEVEL_MERGED);
				preinfo->setMergeLevel(level);

				mergedown--;
			}
		}

		if ((conductor->m_mergeOptimize > 0) && (--conductor->m_mergeOptimize == 0)) {
			break;
		}
	}
}

template<typename K>
void RealTimeMap<K>::abortCacheMemory(RealTimeConductor<K>* conductor) {

	Transaction* tx = conductor->getTransaction();

	const boolean secondaries = m_hasSecondaryMaps;

	inttype curlevel = conductor->getTransaction()->getLevel();

	ThreadContext<K>* ctxt = (ThreadContext<K>*) conductor->getContext(m_indexValue);
	CONTEXT_STACK_HANDLE(K,global,ctxt);

	for (int i = conductor->size() - 1; i >= 0; i--) {
		SegMapEntry* walkEntry = conductor->get(i);

		if (walkEntry->getValue()->getLevel() != curlevel) {
			continue;
		}

		K key = walkEntry->getKey();
		Information* endinfo = walkEntry->getValue();

		Segment<K>* segment = getSegment(ctxt, key, false, false /* don't fill, ok to be virtual */);
		CONTEXT_STACK_ASSIGN(segment,global);
		{
			Information* orginfo = endinfo->getNext();
			Information* topinfo = orginfo;
			if ((topinfo != null) && (topinfo->getLevel() == Information::LEVEL_COMMIT)) {
				topinfo = isolateInformation(ctxt, topinfo, Information::LEVEL_COMMIT);

				#if 0
				if (topinfo->hasFields(Information::WRITE) == false) {
					topinfo = Information::tackOnWriteInfo(topinfo, false /* lock */);
				}
				#endif

				topinfo->setIndexed(m_indexValue, false);
			}

			Information* preinfo = isolateInformation(ctxt, topinfo, endinfo);
			if (preinfo == null) {
				preinfo = endinfo;
			}

			if ((curlevel != 0) && (preinfo != endinfo) && (preinfo->getLevel() != Information::LEVEL_COMMIT)) {
				if (preinfo->getLevel() == Information::LEVEL_MERGED) {
					preinfo->setLevel(preinfo->getMergeLevel());
				}

				if (preinfo != orginfo) {
					preinfo->setUpdating(true);
					preinfo->setNext(orginfo);

				} else {
					preinfo->setUpdating(false);
					preinfo->setNext(null);
				}

			} else {
				preinfo->setUpdating(false);
				preinfo->setNext(null);

				if (curlevel == 0) {
					if ((topinfo != null) && (topinfo->getLevel() == Information::LEVEL_COMMIT)) {
						topinfo->setUpdating(false);
						topinfo->setNext(null);
					}

					#if 0
					if (orginfo != null) {
						orginfo->release(false);
					}
					#endif
				}
			}

			#ifdef COM_DEEPIS_DB_REKEY
			if (m_keyBuilder->getRequiresRekey() == true) {
				bytearray pkey = Converter<K>::toData(key);
				K newkey = m_keyBuilder->fillKey(pkey, preinfo->getData(), ctxt->getKey1());
				K oldkey = m_keyBuilder->fillKey(pkey, endinfo->getData(), ctxt->getKey2());

				if ((m_comparator->compare(newkey, oldkey) == 0) && (m_keyBuilder->isEqual(newkey, oldkey) == false)) {
					K delkey = key;
					key = rekeySegment(segment, newkey, true, true);

					for (int j = 0; j < conductor->size(); j++) {
						SegMapEntry* walkEntry = conductor->get(j);
						#ifdef CXX_LANG_MEMORY_DEBUG
						if (walkEntry->getKey(true) == delkey) {
							walkEntry->setKey(key, -1 /* ctx */, true);
						#else
						if (walkEntry->getKey() == delkey) {
							walkEntry->setKey(key, -1 /* ctx */);
						#endif
						}
					}
				}
			}
			#endif

			if ((secondaries == true) && (endinfo->getDeleting() == false)) {
				rekeySecondary(tx, key, preinfo, endinfo);

				InfoRef preinfoRef(m_indexValue, segment, walkEntry, preinfo);
				abortSecondary(tx, key, orginfo, preinfoRef, endinfo);
			}

			if (endinfo->getDeleting() == false) {
				endinfo->setLevel(Information::LEVEL_ROLLED);

			} else {
				endinfo->setLevel(Information::LEVEL_ERASED);
			}

			if (endinfo->getCreating() == true) {
				conductor->m_createStats--;

			} else if (endinfo->getDeleting() == true) {
				conductor->m_deleteStats--;

			} else {
				conductor->m_updateStats--;
			}
		}
		CONTEXT_STACK_RELEASE(global);
	}

	if (curlevel == 0) {
		for (int i = conductor->size() - 1; i >= 0; i--) {
			SegMapEntry* walkEntry = conductor->get(i);

			Information* info = walkEntry->getValue();

			#ifdef DEEP_DEBUG
			info->setAbsolving();
			#endif
			/* XXX: rooted states have been set */
			if (info->getRootedState() != 0) {
				absolveRooted(tx, walkEntry->getKey(), info);
			}

			conductor->clear(walkEntry, i);

			#ifdef DEEP_DEBUG
			if (info->getRootedCount() > 0) {
				bytearray pkey = Converter<K>::toData(walkEntry->getKey());
				const inttype indexes = (m_hasSecondaryMaps == true) ? m_share.getSecondaryList()->size()+1 : 1; 
				Information* roots[indexes];

				boolean found = false;
				for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
					RealTime* rt = m_share.getSecondaryList()->get(i);
					if (rt == null) {
						roots[i+1] = null;
						// XXX: allow for dynamic add/remove secondary index
						continue;
					}

					roots[i+1] = rt->getRoot(tx, pkey, info->getData());
					found |= (info == roots[i+1]);
				}
				roots[0] = getRoot(tx, pkey, info->getData());
				found |= (info == roots[0]);

				DEEP_LOG(ERROR, OTHER, "Invalid abort: info still rooted (count = %d, found = %s), %s\n", info->getRootedCount(), ((found == true) ? "true" : "false"), getFilePath());
				throw InvalidException("Invalid abort: info still rooted");
			}
			#endif

			Converter<Information*>::destroy(info);
		}

		conductor->dereferenceFiles();

	} else {
		conductor->m_mergeOptimize = -1;
	}

	RealTimeAdaptive_v1<K>::pace(this, tx);
}

#ifdef DEEP_DEBUG
template<typename K>
Information* RealTimeMap<K>::getRoot(Transaction* tx, const bytearray pkey, const bytearray data) {
	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();

	K key;
	if (m_primaryIndex == null) {
		key = m_keyBuilder->fillKey(pkey, ctxt->getKey1());

	} else {
		key = m_keyBuilder->fillKey(pkey, data, ctxt->getKey1());
	}

	Segment<K>* segment = getSegment(ctxt, key, false, false /* don't fill, ok to be virtual */);
	if (segment != null) {
		CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

		const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(key);
		if (infoEntry != null) {
			return infoEntry->getValue();
		}
	}
	return null;
}
#endif

template<typename K>
void RealTimeMap<K>::commitCacheMemory(RealTimeConductor<K>* conductor) {

	Transaction* tx = conductor->getTransaction();

	const boolean secondaries = m_hasSecondaryMaps;
	longtype commitViewpoint = tx->getViewpoint() + 1;

	ThreadContext<K>* ctxt = (ThreadContext<K>*) conductor->getContext(m_indexValue);
	CONTEXT_STACK_HANDLE(K,global,ctxt);

	for (int i = conductor->size() - 1; i >= 0; i--) {
		SegMapEntry* walkEntry = conductor->get(i);
		StoryLine& storyLine = walkEntry->getStoryLine();

		Information* endinfo = walkEntry->getValue();

		// XXX: ignore cleared deleted element (see conductor->clear below)
		if (endinfo == null) {
			continue;
		}

		// XXX: execute this after check endinfo for null due to memory debug
		K key = walkEntry->getKey();

		// XXX: clean up information that is "no longer" end of the storyline
		if (endinfo->getLevel() < Information::LEVEL_ACTIVE) {

			if (endinfo->getLevel() < Information::LEVEL_COMMIT) {
				if (endinfo->getLevel() == Information::LEVEL_ROLLED) {
 					if (endinfo->getRootedState() != 0) {
 						absolveRooted(tx, key, endinfo);
 					}
				}

				Converter<Information*>::destroy(endinfo);
			}

			continue;

		// XXX: ignore previously released storied information
		} else if (storyLine.getLockIdentifier() != tx->getIdentifier()) {
			continue;
		}

		if ((endinfo->getCreating() == true) && (endinfo->getUpdating() == false) && (endinfo->getDiverging() == false)) {

			// XXX: it is necessary to set the viewpoint prior to setting the level for isolateCheckpoint()
			commitViewpoint = Transaction::getCurrentViewpoint() + 1;
			endinfo->reset(commitViewpoint, 0, true);

			// XXX: note order is important here due to rolling concurrency
			endinfo->setNext(null /* end storyline */);
			endinfo->setLevel(Information::LEVEL_COMMIT);

			endinfo->setCreating(false);

			storyLine.release();

		} else if (endinfo->getDeleting() == true) {

			Segment<K>* segment = getSegment(ctxt, key, false, false /* don't fill, ok to be virtual */);
			CONTEXT_STACK_ASSIGN(segment,global);
			{
				Information* orginfo = endinfo->getNext();

				// XXX: executed insert and delete at the same level (i.e. no cycle in version information)
				if (orginfo == null) {
					orginfo = endinfo;
				}

				Information* topinfo = orginfo;
				if (topinfo != endinfo) {

					if (topinfo->getLevel() == Information::LEVEL_COMMIT) {
						topinfo = isolateInformation(ctxt, topinfo, Information::LEVEL_COMMIT);
					}

					// XXX: reset state values for viewpoint and storyline termination
					if (topinfo->getDeleting() == false) {
						uinttype viewpoint = topinfo->getViewpoint();
						topinfo->reset(viewpoint, Transaction::getCurrentViewpoint(), topinfo->getLevel() == Information::LEVEL_COMMIT);
						topinfo->setDeleting(true);
					}

					if (topinfo->getLevel() != Information::LEVEL_COMMIT) {
						conductor->clear(topinfo, i /* see null endinfo above */);

						topinfo->setLevel(Information::LEVEL_COMMIT);
						topinfo->setDiverging(false);
						topinfo->setUpdating(false);
						topinfo->setCreating(false);
					}

					Information* preinfo = topinfo->getNext();
					topinfo->setNext(null /* end storyline */);
					endinfo->setNext(null /* terminate storyline */);

					if (secondaries == true) {
						absolveSecondary(tx, key, preinfo);
					}
				}

				storyLine.release();

				if (orginfo != endinfo) {
					Converter<Information*>::destroy(endinfo);
				}
			}
			CONTEXT_STACK_RELEASE(global);

		} else /* if (endinfo->getUpdating() == true) */ {

			Segment<K>* segment = getSegment(ctxt, key, false, false /* don't fill, ok to be virtual */);
			CONTEXT_STACK_ASSIGN(segment,global);
			{
				Information* orginfo = endinfo->getNext();

				// XXX: executed insert and update at the same level (i.e. no cycle in version information)
				if (orginfo == null) {
					orginfo = endinfo;
				}

				Information* topinfo = orginfo;
				if (topinfo != endinfo) {

					if (topinfo->getLevel() == Information::LEVEL_COMMIT) {
						topinfo = isolateInformation(ctxt, topinfo, Information::LEVEL_COMMIT);
					}

					// XXX: reset state values for viewpoint and storyline termination
					if (topinfo->getDeleting() == false) {
						uinttype viewpoint = topinfo->getViewpoint();
						topinfo->reset(viewpoint, Transaction::getCurrentViewpoint(), topinfo->getLevel() == Information::LEVEL_COMMIT);
						topinfo->setDeleting(true);
					}

					if (topinfo->getLevel() != Information::LEVEL_COMMIT) {
						conductor->clear(topinfo, i /* see null endinfo above */);

						topinfo->setLevel(Information::LEVEL_COMMIT);
						topinfo->setDiverging(false);
						topinfo->setUpdating(false);
						topinfo->setCreating(false);
					}

					Information* preinfo = topinfo->getNext();
					endinfo->setNext(null /* end storyline */);
					topinfo->setNext(endinfo /* converge storyline */);

					if ((secondaries == true) && (endinfo->getDiverging() == true)) {
						absolveSecondary(tx, key, preinfo);
					}
				}

				// XXX: it is necessary to set the viewpoint prior to setting the level for isolateCheckpoint()
				commitViewpoint = Transaction::getCurrentViewpoint() + 1;
				endinfo->reset(commitViewpoint, 0, true);

				endinfo->setLevel(Information::LEVEL_COMMIT);
				endinfo->setDiverging(false);
				endinfo->setCreating(false);
				endinfo->setUpdating(false);

				storyLine.setStoryDepth(storyLine.getStoryDepth() + 1);
				storyLine.release();
			}
			CONTEXT_STACK_RELEASE(global);
		}
	}

	m_entrySize.addAndGet(conductor->m_createStats - conductor->m_deleteStats);
	m_extraStats.addUserSpaceSize(conductor->m_userSpaceSize);

	#ifdef DEEP_IO_STATS
	ctxt->m_statsCount2 += conductor->m_createStats; // XXX: CREATE OPERATIONS
	ctxt->m_statsCount4 += conductor->m_deleteStats; // XXX: DELETE OPERATIONS
	ctxt->m_statsCount3 += conductor->m_updateStats; // XXX: UPDATE OPERATIONS
	#endif

	conductor->dereferenceFiles();

	RealTimeAdaptive_v1<K>::pace(this, tx);
}

template<typename K>
void RealTimeMap<K>::rolloverSegment(ThreadContext<K>* ctxt, Segment<K>* segment) {
	Transaction* tx = ctxt->getTransaction();
	if (tx == null) {
		tx = Transaction::create();
		#ifdef CXX_LANG_MEMORY_DEBUG
		ctxt->setTransaction(tx, true);
		#else
		ctxt->setTransaction(tx);
		#endif
	}
	tx->setRoll(true);
	tx->setIsolation(Transaction::COMMITTED);
	tx->begin();
	associate(tx);

	boolean restart = false;
	TreeSet<K> rolled(m_comparator);

	#ifdef DEEP_DEBUG
	if (segment->getRolling() == true) {
		DEEP_LOG(ERROR, OTHER, "Invalid segment state for roll: already rolling, %s\n", getFilePath());
		throw InvalidException("Invalid segment state for roll: already rolling");
	}
	#endif

	segment->setRolling(true);

	if ((segment->getPurged() == true) || (segment->getVirtual() == true)) {
		fillSegment(ctxt, segment, false /* values */, false /* pace */);
	}

	RETRY:
	if (restart == true) {
		restart = false;
		segment->unlock(true /* force (overrides rolling "reentrancy") */);

		Thread::sleep((rand() % 10) + 50);

		segment->lock(true /* force (overrides rolling "reentrancy") */);
		segment->SegTreeMap::entrySet(&m_rolloverInformationSet);
		m_rolloverInformationSet.reset();
	} else {
		segment->SegTreeMap::entrySet(&m_rolloverInformationSet);
	}

	MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) m_rolloverInformationSet.iterator();
	while(infoIter->MapInformationEntrySetIterator::hasNext() == true) {
		SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();
		K key = infoEntry->getKey();
		Information* info = infoEntry->getValue();
		StoryLine& storyLine = infoEntry->getStoryLine();

		if (rolled.contains(key) == true) {
			continue;
		}

		ErrorCode code = ERR_SUCCESS;
		boolean again = false;
		InfoRef infoRef(m_indexValue, segment, infoEntry, info);
		const boolean wait = checkIsolateLock(ctxt, infoRef, info, &code, &again, RealTimeMap<K>::LOCK_ROLL);
		infoRef.setInfo(info);

		if ((again == true) || (wait == true)) {
			restart = true;
			continue;
		}

		switch(code) {
			case ERR_SUCCESS:
				break;
			case ERR_NOT_FOUND:
				/* XXX: deleted, no need to roll forward */
				continue;
			case ERR_TIMEOUT:
			case ERR_DEADLOCK:
				restart = true;
				{
					const ushorttype otxid = storyLine.getLockIdentifier();
					Transaction* otx = Transaction::getTransaction(otxid);
					if (otx != null) {
						segment->addLockOwner(otxid, storyLine.getLockSequence());
						otx->setRollWait(true);
						otx->decref();
					}
				}
				continue;
			default:
				#ifdef DEEP_DEBUG
				DEEP_LOG(ERROR, OTHER, "Invalid rollover: get failed, %s\n", getFilePath());
				throw InvalidException("Invalid rollover: get failed");
				#else
				continue;
				#endif
		}

		if (info->getData() == null) {
			const boolean compressed = info->getCompressed();
			if (compressed == true) {
				ctxt->setSegment(segment);
			}
			readValue(ctxt, info, key);
			if (compressed == true) {
				ctxt->setSegment(null);
			}
		}
		nbyte tmpValue(info->getData(), info->getSize());
		tx->setDirty(true);

		if (m_hasSecondaryMaps == true) {
			segment->unlock();
			lockSecondary(tx, Converter<K>::toData(key), infoRef, RealTimeMap<K>::LOCK_WRITE, false /* primaryLocked */, true /* stitch */);
			ctxt->setInformation(null);
			segment->lock();
		} else {
			ctxt->setInformation(null);
		}

		if (putTransaction(key, &tmpValue, RealTimeMap<K>::EXISTING, tx, RealTimeMap<K>::LOCK_ROLL, 0 /* position */, 0 /* index */, Information::OFFSET_NONE) == false) {
			restart = true;
			continue;
		}

		rolled.add(key);
	}

	if (restart == true) {
		rolled.clear();
		while (tx->getDirty()) {
			tx->rollback(tx->getLevel());
		}
		tx->releaseConductors(RealTimeResource::currentTimeMillis());
		if (tx->getLevel() < 0) {
			tx->begin();
		}
		associate(tx);

		goto RETRY;
	}

	#ifdef DEEP_DEBUG
	if (tx->getLevel() != 0) {
		DEEP_LOG(ERROR, OTHER, "Invalid TX level for roll: %d\n", tx->getLevel());
		throw InvalidException("Invalid TX level for roll");
	}
	#endif

	// XXX: the segment should not have a compressed offset once it has been rolled
	if (segment->getBeenRealigned() == true) {
		segment->setBeenRealigned(false);
	}

	#if 0
	// XXX: conductor may be out of order due to locking; it is only safe to sort roll transactions
	RealTimeConductor<K>* conductor = (RealTimeConductor<K>*)tx->getConductor(getIdentifier());
	conductor->sort();
	#endif

	tx->commit(tx->getLevel());

	// XXX: all data must have been read during rollover
	segment->setBeenFilled(true);

	// XXX: values have been relocated due to compress split/merge
	segment->setBeenRelocated(false);

	// XXX: ensure key rebuild is completed on shutdown
	segment->setBeenAltered(true);

	segment->setFragmentedValue(false);
	segment->resetStreamIndexes();

	segment->addStreamIndex(tx->getStreamIndex());
	if (m_valueCompressMode == true) {
		segment->setStreamPosition(tx->getStreamPosition());
	}

	segment->setRolling(false);
}

template<typename K>
inttype RealTimeMap<K>::getSecondaryCount(void) {

	inttype count = 0;
	for (inttype i = 0; i < m_share.getSecondaryList()->size(); i++) {
		if (m_share.getSecondaryList()->get(i) != null) {
			count++;
		}
	}

	return count;
}

template<typename K>
void RealTimeMap<K>::addSecondary(RealTime* secondary, boolean hasPrimary, boolean dynamic) {

	inttype index = -1;

	if (dynamic == true) {
		DEEP_LOG(INFO, INDEX, "store: %s, adding index\n", getFilePath());
	}

	// XXX: these fields must be set correctly as soon as primary has access to secondaries
	Transaction::removeIdentifier(secondary->getIdentifier(), secondary->getAssociated(), secondary->getIndexValue());
	secondary->setIdentifier(m_identifier);
	secondary->setPrimaryIndex(this);

	// XXX: safe context lock: one writer / no readers on the branch tree
	m_threadContext.writeLock();
	{
		if ((getSecondaryCount() + 1) > Properties::FIXED_INDEX_CAP) {
			m_threadContext.writeUnlock();

			DEEP_LOG(ERROR, OTHER, "associate: maximum capacity, %s\n", getFilePath());

			throw PermissionException("associate: maximum capacity");
		}

		for (inttype i = 0; i < m_share.getSecondaryList()->size(); i++) {
			if (m_share.getSecondaryList()->get(i) == null) {
				m_share.getSecondaryList()->set(i, secondary);
				index = i;
				break;
			}
		}

		if (index == -1) {
			index = m_share.getSecondaryList()->size();

			// XXX: turn this on to disable hot index add during a checkpoint
			#if 0
			RETRY:
			checkpointLock();
			{
				if (checkpointFullyComplete(false) == true) { 
					m_share.getSecondaryList()->add(secondary);

				} else {
					checkpointUnlock();
					Thread::sleep((rand() % 10) + 50);
					goto RETRY;
				}
			}
			checkpointUnlock();
			#else
			checkpointLock();
			{
				m_share.getSecondaryList()->add(secondary);
			}
			checkpointUnlock();
			#endif
		}

		secondary->getShare()->setHasPrimary(hasPrimary);
	}
	m_threadContext.writeUnlock();

	// XXX: secondary indexValue is +1 for primary
	secondary->setDynamic(dynamic);
	secondary->indexPrimary(this, index + 1 /* 1+ for primary */, dynamic, (m_state == MAP_RECOVER));

	m_hasSecondaryMaps = true;
}

template<typename K>
void RealTimeMap<K>::removeSecondary(RealTime* secondary, /* boolean hadPrimary, */ boolean dynamic) {

	DEEP_LOG(INFO, INDEX, "store: %s, removing index\n", getFilePath());

	// XXX: safe context lock: one writer / no readers on the branch tree
	m_threadContext.writeLock();
	{
		// XXX: secondary indexValue is +1 for primary
		m_share.getSecondaryList()->set(secondary->getIndexValue() - 1, null);

		m_hasSecondaryMaps = (getSecondaryCount() > 0);
	}
	m_threadContext.writeUnlock();

	secondary->indexPrimary(null, -1, dynamic, false /* recovery */);
}

template<typename K>
inttype RealTimeMap<K>::deleteSegments(void) {

	inttype deleted = m_deletedSegmentList.ArrayList<Segment<K>*>::size();
	if (deleted != 0) {
		m_deletedSegmentList.lock();
		{
			if (m_state == MAP_RUNNING) {
				for (int i = 0; i < m_deletedSegmentList.ArrayList<Segment<K>*>::size(); i++) {
					Segment<K>* segment = m_deletedSegmentList.ArrayList<Segment<K>*>::get(i);
					if (segment->ref() == 0) {
						m_deletedSegmentList.ArrayList<Segment<K>*>::remove(i);
						delete segment;
						i--;
					}
				}

			} else {
				m_deletedSegmentList.ArrayList<Segment<K>*>::clear();
			}
		}
		m_deletedSegmentList.unlock();
	}

	return deleted;
}

template<typename K>
inttype RealTimeMap<K>::purgeSegments(BasicArray<Segment<K>*>* segments, ThreadContext<K>* ctxt, boolean index) {

	PurgeReport purgeReport;
	inttype purged = segments->size();
	if (purged != 0) {
		for (int i = 0; i < purged; i++) {
			Segment<K>* segment = segments->get(i);
			segment->SegTreeMap::clear(true, true);
			delete segment;
		}
	}

	return purged;
}

template<typename K>
inttype RealTimeMap<K>::compressSegments(BasicArray<Segment<K>*>* segments, ThreadContext<K>* ctxt, boolean index, boolean growing) {

	PurgeReport purgeReport;
	inttype numSegments = segments->size();
	inttype compressed = 0;

	if (numSegments != 0) {
		for (int i = 0; i < numSegments; i++) {
			Segment<K>* segment = segments->get(i);
			if (segment->tryLock() == true) {

				
				if (RealTimeAdaptive_v1<K>::allowPurgeSegment(this, segment, index, growing, false /* semi */, purgeReport) == true) {
					if (RealTimeAdaptive_v1<K>::allowMemoryCompression(this, segment, true /* cmp */, false /* mem */, growing) == true) {
						boolean isVirtual = segment->getVirtual();

						segment->setVirtualSizeEnabled(false);
						segment->setCardinalityEnabled(false);

						RealTimeCompress<K>::compress(this, ctxt, segment, &m_share, m_keyBuilder);

						segment->setModification(segment->getModification() + 1);
						segment->setVirtualSizeEnabled(true);
						segment->setBeenFilled(false);

						if (segment->getDirty() == false) {
							segment->setPurged(true);
						}

						if (isVirtual == false) {
							m_purgeSize.incrementAndGet();
						}
					}
				}

				segment->unlock();
				compressed++;
			}
		}
	}

	return compressed;
}


template<typename K>
inttype RealTimeMap<K>::orderSegments(ThreadContext<K>* ctxt, boolean reset, boolean timeout, boolean* cont, boolean* reorg, inttype* request, inttype* purge, uinttype viewpoint, RealTimeSummary<K>* summaryWorkspace, IndexReport* indexReport) {

	IndexReport _indexReport;
	if (indexReport == null) {
		indexReport = &_indexReport;
	}
	PurgeReport& purgeReport = indexReport->purgeReport;

	inttype indexed = 0;
	inttype requestCount = 0;
	inttype purgeCount = 0;

	if (reset == true) {
		m_quantaDirty = 0;
	}

	if (m_orderSegmentList.size() != 0) {

		inttype compressed = 0;
		Segment<K>* segment = null;
		Segment<K>* gnSegment = null;
		BasicArray<Segment<K>*> dirtyList;
		boolean growing = (size() > getCycleSize());

		longtype start = RealTimeResource::currentTimeMillis();
		while ((m_orderSegmentList.size() != 0) && (m_state < MAP_EXITING) && (m_memoryMode == false)) {
			segment = m_orderSegmentList.get(0);
			if (segment != gnSegment) {
				if (gnSegment != null) {
					gnSegment->unlock();
					gnSegment = null;
				}

				segment->lock();
				// XXX: Membership must be checked again under segment lock; lock order is important to avoid deadlock
				if (m_orderSegmentList.contains(segment) == false) {
					segment->unlock();
					continue;
				}
			}
			// XXX: segment must be locked before remove
			m_orderSegmentList.remove(segment);

			// XXX: Make sure we need to index the segment
			boolean summarize = true;
			boolean backwardCheckpoint = false;
			if (needsIndexing(segment, viewpoint, false /* final */, *reorg, &summarize, &backwardCheckpoint) == false) {
				if ((summarize == true) && (summaryWorkspace != null)) {
					// XXX: even if we don't need to index the segment, we may need to summarize it
					summaryWorkspace->summarizeSegment(this, ctxt, segment->SegTreeMap::firstKey(), segment);
				}
				segment->unlock();
				continue;
			}

			requestCount++;
			boolean rebuild = RealTimeAdaptive_v1<K>::writeFragmentation(this, segment, viewpoint, growing);

			if ((m_primaryIndex == null) && (segment->getFragmentedValue() == true)) {

				// FLUX
				if (((viewpoint == 0) || (Properties::getCheckpointMode() == Properties::CHECKPOINT_OFF)) && (m_state != MAP_RECOVER)) {
					rolloverRealTime(ctxt, segment);
					rebuild = true;

				} else {
					dirtyList.add(segment, true);
				}
			}

			rebuild = ((rebuild == true) || (backwardCheckpoint == true));
			Segment<K>* nSegment = pageSegment(ctxt, segment, &indexed, rebuild, viewpoint, backwardCheckpoint, summaryWorkspace, *indexReport);

			if (backwardCheckpoint == true) {
				segment->setBeenAltered(true);
			}

			boolean split = false;
			if ((viewpoint == 0) && (segment->getDirty() == false) && (segment->getFragmentedKey() == false)) {

				// XXX: if a virtual segment was filled and the physical maximum is reached (i.e. 5x), at lease do one split
				if ((m_state != MAP_RECOVER) && (segment->getVirtual() == false) && (segment->getRolling() == false) && (RealTimeAdaptive_v1<K>::physicalMaximum(this, segment, 5) == true) && (segment->waitForReferences() == true)) {
					K firstKey = (K) Converter<K>::NULL_VALUE;
					/* Segment<K>* pSegment = */ splitSegment(ctxt, segment, &firstKey /* of pSegment */, false /* lock */);
					split = true;
				}
			}

			// XXX: don't purge and/or add split segments (which are now dirty) to the dirty list
			if (split == false) {

				if (purgeSegment(ctxt, segment, growing, true /* index */, (viewpoint != 0) && (Properties::getSemiPurge() == true) /* semi */, null /* purgeList */, null, purgeReport) == true) {
					purgeCount++;

					if (segment->getZipData() != null) {
						compressed++;
					}
				}

				if (segment->getBeenSurceased() == false) {
					if ((segment->getDirty() == true) || (segment->getFragmentedKey() == true)) {
						dirtyList.add(segment, true);
					}
				}
			}

			segment->unlock();
			gnSegment = nSegment;

			// XXX: don't pause after the above segment splitting
			if (split == false) {

				// FLUX
				// XXX: break out early if there is a pending checkpoint (only for secondaries)
				if ((Properties::getCheckpointMode() != Properties::CHECKPOINT_OFF) && (viewpoint == 0) && (m_primaryIndex != null) && (pendingCheckpoint() == true)) {
					start -= Properties::DEFAULT_CACHE_LIMIT;
					timeout = true;
				}

				longtype stop = RealTimeResource::currentTimeMillis();
				if (((timeout == true) && ((stop - start) > Properties::DEFAULT_CACHE_LIMIT)) || (m_mount != MOUNT_OPENED)) {
					if (viewpoint == 0) {
						DEEP_LOG(DEBUG, INDEX, "store: %s, elapsed: %lld, paused: %d\n", getFilePath(), (stop-start), m_orderSegmentList.size());
						for (int j = 0; (j < m_orderSegmentList.size()) && (m_state < MAP_EXITING); j++) {
							Segment<K>* qSegment = m_orderSegmentList.get(j);

							dirtyList.add(qSegment, true);
						}

					} else {
						DEEP_LOG(DEBUG, INDEX, "store: %s, elapsed: %lld, checkpoint aborted: %d\n", getFilePath(), (stop-start), m_orderSegmentList.size());
						setCheckpointValid(false);
					}

					*cont = true;
					break;

				} else if ((viewpoint != 0) && ((stop - start) > Properties::DEFAULT_CACHE_LIMIT) && (indexed > 0)) {
					DEEP_LOG(DEBUG, INDEX, "store: %s, elapsed: %lld, %c[%d;%dmcheckpoint indexed:%c[%d;%dm %d of %d\n", getFilePath(), (stop-start), 27,1,32,27,0, 25, indexed, m_orderSegmentList.size() + indexed);
					if (purge > 0) {
						DEEP_LOG(DEBUG, PURGE, "store: %s, %c[%d;%dmcheckpoint indexing achieved:%c[%d;%dm %d of %d\n", getFilePath(), 27,1,33,27,0, 25, purgeCount, requestCount);
					}
					indexed = 0;
					requestCount = 0;
					purgeCount = 0;
					start = RealTimeResource::currentTimeMillis();
				}
			}

			if (growing == false) {
				growing = (size() > getCycleSize());
			}
		}

		if (gnSegment != null) {
			gnSegment->unlock();
		}

		if ((dirtyList.size() != 0) && ((viewpoint != 0) || (m_skipcheck == true) || (m_quantaDirty++ < Properties::DEFAULT_CYCLE_QUANTA_LIMIT))) {
			for (int j = 0; j < dirtyList.size(); ++j) {
				m_orderSegmentList.add(dirtyList.get(j));
			}

		} else {
			m_orderSegmentList.clear();
			m_quantaDirty = -1;
		}

		if (compressed > 0) {
			String cmprs;
			RealTimeCompress<K>::buildLoggingString(this, &cmprs, compressed);

			DEEP_LOG(DEBUG, CMPRS, "store: %s, %s\n", getFilePath(), cmprs.c_str());
		}
	}

	if (request != null) {
		*request = requestCount;
	}

	if (purge != null) {
		*purge = purgeCount;
	}

	return indexed;
}

template<typename K>
boolean RealTimeMap<K>::needsIndexing(Segment<K>* segment, const uinttype viewpoint, const boolean final, boolean reorg, boolean* summarize, boolean* backwardCheckpoint) {
	const boolean checkpoint = viewpoint != Locality::VIEWPOINT_NONE;

	(*backwardCheckpoint) = ((Properties::getCheckpointMode() != Properties::CHECKPOINT_OFF) && (checkpoint == true) && (segment->getSummary() == false) && (getLocalityCmp()->compare(m_endwiseLrtLocality, segment->getIndexLocality()) < 0));

	const boolean forcing = segment->getBeenAltered() || segment->getBeenReseeded() || segment->getBeenRelocated() || (*backwardCheckpoint);
	const boolean reindex = (final == true) && (checkpoint == false) && (segment->getSummary() == false) && (segment->getViewpoint() == true);

	if (reindex == true) {
		return true;
	}

	if ((forcing == false) && (segment->getDirty() == false)) {

		// XXX: summary segments are manually indexed (discovery: account summary pending state)
		if (segment->getSummary() == true) {
			return false;
		}
		const boolean needsRoll = (segment->getFragmentedKey() == true) || (segment->getFragmentedValue() == true);

		if ((needsRoll == false) || (reorg == false)) {
			if ((segment->getPaged() == true) && ((needsRoll == false) || ((final == false) && (m_resource.getFragmented() == false)))) {
				return false;

			} else if (segment->getPurged() == true) {
				return false;

			}
		}
	}

	// XXX: During checkpoint indexing, do not skip deleted segments unless also surceasing (see mergeSegments())
	if (((segment->getBeenDeleted() == true) && ((checkpoint == false) || (segment->getBeenSurceased() == true))) || (segment->SegTreeMap::size() == 0)) {
		*summarize = false;
		return false;
	}

	if ((*backwardCheckpoint) == true) {
		// XXX: rebuild again after checkpoint indexing
		segment->setBeenAltered(true);
	}

	return true;
}

template<typename K>
inttype RealTimeMap<K>::indexCacheManagement(boolean* cont, boolean* reorg) {

	*cont = false;
	boolean reorganize = *reorg;
	*reorg = false; // see below

	// XXX: use for cache and fragmentation calculation
	m_activeValueSize = 0;
	m_activeKeyBlockSize = 0;
	m_activeKeyBlockCount = 0;
	m_activeValueCount = 0;

	if ((m_memoryMode == true) || (m_state >= MAP_EXITING)) {
		// XXX: no pending indexing, segments can be destroyed 
		if (m_orderSegmentList.size() == 0) {
			deleteSegments();
		}

		return 0;
	}

	ThreadContext<K>* ctxt = m_threadContext.getContext();
	#ifdef DEEP_IO_STATS
	ctxt->m_statsForFiles = true;
	#endif

	IndexReport indexReport;

	// XXX: periodically write out storage statistics (note: converting seconds to milli-seconds)
	if ((m_primaryIndex == null) && ((System::currentTimeMillis() - m_statisticsFlushTime) > (Properties::getStatisticsFlushInterval() * 1000))) {
		RandomAccessFile* xwfile = m_share.getXrtWriteFileList()->last();
		if (xwfile != null) {
			m_share.acquire(xwfile);
			{
				longtype total = 0;
				{
					sizeStorage(&total);

					for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
						RealTime* rt = m_share.getSecondaryList()->get(i);
						if (rt == null) {
							// XXX: allow for dynamic add/remove secondary index
							continue;
						}

						rt->sizeStorage(&total);
					}

					m_extraStats.setTotalSize(total);
				}

				ExtraStatistics_v1::updateSize(xwfile, &m_extraStats);
				#if 0
				//XXX: Update License info only if it has changed
				ExtraStatistics_v1::updateLicense(xwfile,Properties::getActivationKey(),Properties::getSystemUid(), Properties::getActivationKeyChange());
				#endif

				xwfile->setOnline(false);
			}
			m_share.release(xwfile);

		} else {
			DEEP_LOG(WARN, STATS, "store: %s, failed to find last xrt file\n", getFilePath());
		}

		m_statisticsFlushTime = System::currentTimeMillis();
	}

	if ((m_primaryIndex == null) && (System::currentTimeMillis() - m_fileCleanupTime) > (Properties::getFileCleanupInterval() * 1000)) {
		if (tryClobberLock() == true) {
			const inttype max = m_share.getAwaitingDeletion()->size();
			for (inttype i=0; (RealTimeAdaptive_v1<K>::clobberUnusedFiles(this) == true) && (i < max); ++i) {
				// XXX: clean up files
			}
			m_fileCleanupTime = System::currentTimeMillis();
			clobberUnlock();
		}
	}

	boolean checkpoint = false;
	// XXX: don't toggle until final shutdown or more resources are available
	if (((m_skipcheck == false) && (m_resource.getCacheUsage() < RealTimeResource::EXTREME)) || ((m_primaryIndex != null) && (getCheckpointComplete() == false))) {
		checkpoint = toggleCheckpointMetrics();
	}

	RealTimeSummary<K> summaryWorkspace(this, m_endwiseLrtLocality, checkpoint, true /* linked (summary back references) */);
	const boolean checkpointToggled = checkpoint;
	if (Properties::getCheckpointMode() == Properties::CHECKPOINT_OFF) {
		checkpoint = false;
	}

	inttype purge = 0;
	inttype indexRequest = 0;
	inttype indexAchieved = 0;
	ulongtype indexPotential = 0;

	// XXX: no pending indexing, collect more segments to index
	if ((m_orderSegmentList.size() == 0) || (checkpoint == true)) {

		// XXX: safe context lock: multiple readers / no writer on the branch tree
		m_threadContext.readLock();
		m_orderSegmentList.lock();
		{
			m_orderSegmentList.clear(false /* lock */);
			m_orderSegmentMode = (checkpoint == true) ? MODE_CHECK : MODE_INDEX;

			MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) m_orderSegmentSet.reset();
			while (segIter->MapSegmentEntrySetIterator::hasNext()) {
				const MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
				Segment<K>* segment = segEntry->getValue();
				segment->setIndexOrderKey(m_keyBuilder->cloneKey(segEntry->getKey()));
				m_orderSegmentList.add(segment, false /* lock */);

				boolean summarize = true;
				boolean backwardCheckpoint = false;
				if (needsIndexing(segment, Locality::VIEWPOINT_NONE, false /* final */, *reorg, &summarize, &backwardCheckpoint) == true) {
					++indexPotential;
				}
			}
		}
		m_orderSegmentList.unlock();
		m_threadContext.readUnlock();
	}

	longtype start = System::currentTimeMillis();
	{
		if ((m_skipcheck == false) && (Properties::getDynamicSummarization() == true) && (indexPotential >= Properties::DEFAULT_SEGMENT_INDEXING_MAX)) {
			DEEP_LOG(DEBUG, INDEX, "store: %s, indexing is not keeping up with ingestion: %llu\n", getFilePath(), indexPotential);

			m_skipcheck = true;
		}

		// FLUX
		if ((Properties::getCheckpointMode() == Properties::CHECKPOINT_OFF) && (checkpointToggled == true)) {
			DEEP_LOG(DEBUG, CHECK, "OFF; store: %s, executing, locality: %s\n", getFilePath(), (const char*)m_endwiseLrtLocality.toString());
			checkpoint = true;

		} else {
			if (checkpoint == true) {
				DEEP_LOG(DEBUG, CHECK, "store: %s, executing, locality: %s\n", getFilePath(), (const char*)m_endwiseLrtLocality.toString());
			}

			indexAchieved = orderSegments(ctxt, false /* reset */, (checkpoint == false) /* timeout */, cont, reorg, &indexRequest, &purge, (checkpoint == true) ? m_endwiseLrtLocality.getViewpoint() : Locality::VIEWPOINT_NONE, (checkpoint == true) ? &summaryWorkspace : null, &indexReport);
		}

		if ((m_skipcheck == true) && (indexPotential < Properties::DEFAULT_SEGMENT_INDEXING_MIN)) {
			DEEP_LOG(DEBUG, INDEX, "store: %s, indexing has caught up with ingestion: %llu\n", getFilePath(), indexPotential);

			m_skipcheck = false;
		}
	}
	longtype stop = System::currentTimeMillis();

	if (indexAchieved > 0) {
		DEEP_LOG(DEBUG, INDEX, "store: %s, elapsed: %lld, %c[%d;%dmindexed:%c[%d;%dm %d of %d\n", getFilePath(), (stop-start), 27,1,32,27,0, 25, indexAchieved, indexRequest);
		if (purge > 0) {
			DEEP_LOG(DEBUG, PURGE, "store: %s, %c[%d;%dmindexing achieved:%c[%d;%dm %d of %d\n", getFilePath(), 27,1,33,27,0, 25, purge, indexRequest);
		}

		if (Properties::getLogOption(Properties::VERBOSE_INDEX) == true) {
			DEEP_LOG(DEBUG, INDEX, "store: %s, index report: %s\n", getFilePath(), (const char*)indexReport.toString());
		}
	}

	// XXX: ordering segment list has been refreshed (i.e. stale)
	boolean refreshed = (m_quantaDirty == -1);
	if (refreshed == true) {
		m_quantaDirty = 0;
	}

	if (checkpoint == true) {
		const boolean summaryValid = getCheckpointValid();
		if (Properties::getCheckpointMode() != Properties::CHECKPOINT_OFF) {
			summaryWorkspace.indexSummary(this, ctxt);
		}
		m_summaryLrtLocality = m_endwiseLrtLocality;

		MeasuredRandomAccessFile* iwfile = m_share.getIrtWriteFileList()->get(m_pagingIndex);
		m_share.acquire(iwfile);
		{
			if (Properties::getCheckpointMode() != Properties::CHECKPOINT_OFF) {
				RealTimeVersion<K>::terminatePaging(iwfile, this, true /* summarized */, getRecoveryEpoch(), (summaryValid == false));
				iwfile->setContainsCheckpoint(true);
			}

			DEEP_LOG(DEBUG, CHECK, "block: %s, complete, length: %lld, [ %llu | %u | %u ], checkpoint %s: %s\n", iwfile->getPath(), iwfile->length(), summaryWorkspace.getSummarized(), summaryWorkspace.getIndexed(), summaryWorkspace.getReindexed(), (summaryValid == true) ? "summarized" : "aborted", (const char*)m_summaryLrtLocality.toString());
		}
		m_share.release(iwfile);

		Locality summaryLrtLocality = Locality::LOCALITY_NONE;
		checkpointLock();
		{
			setCheckpointComplete(true);

			// XXX: if necessary, invalidate checkpoint across indexes (when the checkpoint is fully complete)
			if (checkpointFullyComplete(false /* lock */) == true) {
				if (summaryValid == false) {
					summaryLrtLocality = summaryWorkspace.getPagedSummary()->getStreamLocality();
				}

				RealTime* pri = (m_primaryIndex == null) ? this : m_primaryIndex;
				DEEP_LOG(DEBUG, CHECK, "store: %s, fully complete, locality: %s\n", pri->getFilePath(), (const char*)m_endwiseLrtLocality.toString());
			}
		}
		checkpointUnlock();

		clobberLock();
		{
			if (summaryLrtLocality.isNone() == false) {
				abortCheckpoint(summaryLrtLocality);
			}

			if (summaryValid == true) {
				// XXX: add newly-paged summary to set of active summaries
				PagedSummary* summaryRef = summaryWorkspace.getPagedSummary();
				getActiveSummaries()->addSummary(summaryRef);
			}

			// XXX: attempt to clean up files freed up by the addition of a new checkpoint
			RealTimeAdaptive_v1<K>::clobberUnusedFiles(this);
		}
		clobberUnlock();
	}

	if (((m_state == MAP_RUNNING) || (m_state == MAP_RECOVER)) && (m_reindexing.get() == 0 /* no dynamic indexing */)) {

		// XXX: file reorganization (i.e. defragmentation and compression is resource intensive)
		if (m_resource.getCacheUsage() < RealTimeResource::EXTREME) {

			if ((reorganize == true) && (RealTimeAdaptive_v1<K>::reorganizeFiles(this, m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size(), m_orderSegmentList.size()))) {

				if ((m_primaryIndex == null) && (m_state == MAP_RUNNING)) {

					if ((m_share.getLastAverageKeyBlock() != 0) && (m_valueCompressMode == true)) {

						longtype averageKeyBlockTotalSize = m_share.getLastAverageKeyBlock();

						for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
							RealTime* rt = m_share.getSecondaryList()->get(i);
							if (rt == null) {
								// XXX: allow for dynamic add/remove secondary index
								continue;
							}

							uinttype lastAverageKeyBlock = rt->getShare()->getLastAverageKeyBlock();
							if (lastAverageKeyBlock == 0) {
								averageKeyBlockTotalSize = 0;
								break;
							}

							averageKeyBlockTotalSize += lastAverageKeyBlock;
						}

						if (averageKeyBlockTotalSize != 0) {
							m_share.setKeyBlockAverage(averageKeyBlockTotalSize);	
						}
					}

					*reorg = reorganizeFiles(ctxt, cont, true /* defrag values */, m_valueCompressMode);
				}

				*reorg = reorganizeFiles(ctxt, cont, false /* defrag keys */, false /* value compression */);
			}
		}
	}

	// XXX: indicate that some sort of index organziation has been achieved (see toggle checkpointing)
	if (indexAchieved != 0) {
		m_organizationTime = System::currentTimeMillis();
	}

	// XXX: statistics use for cache and fragmentation calculation
	if (m_activeValueCount != 0) {
		if (m_activeKeyBlockCount > 0) {
			m_share.setLastAverageKeyBlock((uinttype) (m_activeKeyBlockSize / m_activeKeyBlockCount));
		}
		m_share.setValueAverage((inttype) (m_activeValueSize / m_activeValueCount));
	}

	return indexAchieved;
}

template<typename K>
inttype RealTimeMap<K>::purgeCacheManagement(inttype active, boolean index, inttype* compressed, inttype* compressedDataPurge, PurgeReport& purgeReport) {

	if (m_memoryMode == true) {
		return 0;
	}

	inttype purged = 0;
	BasicArray<Segment<K>*> purgeList;
	BasicArray<Segment<K>*> compressList;
	boolean growing = (size() > getCycleSize());

	ThreadContext<K>* ctxt = m_threadContext.getContext();
	DynamicSummarization<K> DSWorkspace(this, index, &m_purgeSize);

	RELOCK:
	if (DSWorkspace.getRequiresContextWriteLock() == true) m_threadContext.writeLock(); else m_threadContext.readLock();
	{
		RELOOP:
		MapSegmentEntrySetIterator* segIter = null;

		if (index == true) {
			// XXX: pressure high, clear out potential reorganizations
			m_orderSegmentList.clear();

			segIter = (MapSegmentEntrySetIterator*) m_orderSegmentSet.reset();

		} else {
			segIter = (MapSegmentEntrySetIterator*) m_purgeSegmentSet.reset();
		}
		DSWorkspace.setSegmentIterator(segIter);

		if (ctxt->getPurgeSetup() == true) {
			ctxt->setPurgeSetup(false);

			K purgeCursor = ctxt->getPurgeCursor();
			ctxt->setPurgeCursor((K) Converter<K>::NULL_VALUE);

			if (m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {
				segIter->reset(purgeCursor);
			}

			Converter<K>::destroy(purgeCursor);
		}

		while (segIter->MapSegmentEntrySetIterator::hasNext() && (m_state < MAP_EXITING)) {
			MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
			Segment<K>* segment = segEntry->getValue();

			// XXX: user disabling of dynamic summarization will take place after this cycle
			if ((Properties::getDynamicSummarization() == true) || (DSWorkspace.getRequiresContextWriteLock() == true)) {
				boolean final = (segIter->MapSegmentEntrySetIterator::hasNext() == false);
				typename DynamicSummarization<K>::Action action = DSWorkspace.processEntry(segEntry, final);

				if (action == DynamicSummarization<K>::DONE) {
					break;
				}

				switch (action) {
					case DynamicSummarization<K>::RELOCK:
						m_threadContext.readUnlock();
						goto RELOCK;
					case DynamicSummarization<K>::RELOOP:
						{
							const MapEntry<K,Segment<K>*>* sumEntry = DSWorkspace.getLoopStartEntry();	
							const MapEntry<K,Segment<K>*>* lowerEntry = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::lowerEntry(sumEntry->getKey());
							if (lowerEntry != null) {
								ctxt->setPurgeSetup(true);
								ctxt->setPurgeCursor(m_keyBuilder->cloneKey(lowerEntry->getKey()));
							}
							goto RELOOP;
						}
					case DynamicSummarization<K>::IGNORE:
						break;
					case DynamicSummarization<K>::CONTINUE:
						continue;
					default:
						throw InvalidException("Invalid dynamic summarization action");
				}
			}

			if (((segment->getSummary() == true) || (segment->getPaged() == false)) || ((segment->getPurged() == true) && (RealTimeAdaptive_v1<K>::purgeSegmentCompressedData(this, segment) == false))) {
				continue;
			}

			if (segment->tryLock() == true) {

				// XXX: check whether cache pressure requires segment purging
				if (purgeSegment(ctxt, segment, growing, index, false /* semi */, &purgeList, &compressList, purgeReport) == true) {
					if (segment->getZipData() != null) {
						(*compressed)++;
					}

				} else if (RealTimeAdaptive_v1<K>::purgeSegmentCompressedData(this, segment) == true) {
					segment->freeZipData();
					(*compressedDataPurge)++;
				}

				segment->unlock();
			}

			if (growing == false) {
				growing = (size() > getCycleSize());
			}

			if (growing == false) {
				boolean force = m_resource.getCacheUsage() == RealTimeResource::EXTREME;
				if (force == false) {
					force = (m_resource.getFragmented() == true);
				}

				if ((force == false) && (getActiveSegments() < (active - 1) /* allowed active */)) {

					// XXX: round robin the purging cycle (i.e. even distribution across segments)
					if (segIter->MapSegmentEntrySetIterator::hasNext() == false) {
						ctxt->setPurgeSetup(true);
						ctxt->setPurgeCursor(m_keyBuilder->cloneKey(segEntry->getKey()));
					}

					break;
				}
			}
		}

		#ifdef DEEP_DEBUG
		if (DSWorkspace.partialSummarization() == true) {
			DEEP_LOG(ERROR, OTHER, "Invalid dynamic summarization: partial summarization, %s\n", getFilePath());

			throw InvalidException("Invalid dynamic summarization: partial summarization");
		}
		#endif
	}

	if (DSWorkspace.getRequiresContextWriteLock() == true) m_threadContext.writeUnlock(); else m_threadContext.readUnlock();

	purged = purgeSegments(&purgeList, ctxt, index);
	(*compressed) = compressSegments(&compressList, ctxt, index, growing);

	return purged + (*compressed);
}

template<typename K>
uinttype RealTimeMap<K>::onlineSummarizeSegments(ThreadContext<K>* ctxt, RealTimeSummary<K>& workspace) {

	m_threadContext.readLock();
	if (m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {

		MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) m_orderSegmentSet.reset();

		while (segIter->MapSegmentEntrySetIterator::hasNext()) {
			const MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
			Segment<K>* segment = segEntry->getValue();

			// XXX: account for segment in summary
			workspace.summarizeSegment(this, ctxt, segEntry->getKey(), segment);
		}
	}
	m_threadContext.readUnlock();

	// XXX: flush last summary segment, if necessary (may be empty)
	workspace.indexSummary(this, ctxt);

	DEEP_LOG(DEBUG, SUMIZ, "store: %s, segments: %d, reindex: %d, indexed: %d, \n", getFilePath(), getTotalSegments(), workspace.getReindexed(), workspace.getIndexed());

	return workspace.getReindexed() + workspace.getIndexed();
}

template<typename K>
inttype RealTimeMap<K>::onlineFinalizingSegments(ThreadContext<K>* ctxt) {

	PurgeReport purgeReport;

	RealTime* pri = (m_primaryIndex == null) ? this : m_primaryIndex;

	// XXX: mark any outstanding checkpoint as "complete"
	Locality checkptLrtLocality = Locality::LOCALITY_NONE;
	checkpointLock();
	{
		const boolean checkpointComplete = getCheckpointComplete(false /* lock */);
		setCheckpointComplete(true);
		if ((getCheckpointValid(false /* lock */) == true) && (checkpointComplete == false) && (getLocalityCmp()->compareNoViewpoint(getCheckptLrtLocality(), ((m_primaryIndex == null) ? this : m_primaryIndex)->getCurrentLrtLocality()) != 0)) {
			setCheckpointValid(false, false /* lock */);
			checkptLrtLocality = getCheckptLrtLocality();
		}
	}
	checkpointUnlock();

	if (checkptLrtLocality.isNone() == false) {
		clobberLock();
		{
			abortCheckpoint(checkptLrtLocality);
		}
		clobberUnlock();
	}

	inttype indexed = -1;

	// XXX: see segment reseed and delete during indexing
	RETRY:
	m_resetIterator = false;

	MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) m_orderSegmentSet.reset();
	while (segIter->MapSegmentEntrySetIterator::hasNext()) {
		const MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
		Segment<K>* segment = segEntry->getValue();
		segment->lock();

		boolean summarize = true; // XXX: ignored here (unlike regular indexing), as we will summarize everything in onlineSummarizeSegments() below
		boolean backwardCheckpoint = false;
		if (needsIndexing(segment, Locality::VIEWPOINT_NONE, true /* final */, false /* reorg */, &summarize, &backwardCheckpoint) == false) {
			segment->unlock();
			continue;
		}

		if ((m_primaryIndex == null) && ((segment->getBeenRelocated() == true) || (segment->getFragmentedValue() == true))) {
			rolloverRealTime(ctxt, segment);
		}

		const boolean rebuild = segment->getBeenAltered() || segment->getBeenReseeded() || segment->getFragmentedKey();

		if (indexSegment(ctxt, segment, rebuild, Locality::VIEWPOINT_NONE) == true) {
			indexed++;
		}

		// XXX: check whether cache pressure requires segment purging
		purgeSegment(ctxt, segment, false /* not growing */, false /* index */, false /* semi */, null /* purgeList */, null, purgeReport);

		segment->unlock();

		// XXX: indexing segments might delete and/or reseed first keys
		if (m_resetIterator == true) {
			goto RETRY;
		}
	}

	const boolean toggled = toggleStreamingMetrics(true /* save */, false /* checkpoint */);

	// XXX: if things have been changed then re-summarize and re-terminate
	MeasuredRandomAccessFile* iwfile = m_share.getIrtWriteFileList()->get(m_pagingIndex);
	if ((toggled == true) || (iwfile->getInitialLrtLocality() != iwfile->getEndwiseLrtLocality()) || (iwfile->getInitialLrtLocality() != pri->getCurrentLrtLocality())) {
		RealTimeSummary<K> summaryWorkspace(this, m_endwiseLrtLocality, false /* checkpoint */, false /* linked (summary back references) */);
		onlineSummarizeSegments(ctxt, summaryWorkspace);
		m_summaryLrtLocality = RealTimeLocality(m_endwiseLrtLocality, System::currentTimeMillis());

		// XXX: m_pagingIndex may increase during summarization
		MeasuredRandomAccessFile* iwfile = m_share.getIrtWriteFileList()->get(m_pagingIndex);
		m_share.acquire(iwfile);
		{
			RealTimeVersion<K>::terminatePaging(iwfile, this, true /* summarized */, getRecoveryEpoch());

			DEEP_LOG(DEBUG, FINAL, "block: %s, length: %lld, [ %llu | %u | %u ], summarized: %s\n", iwfile->getPath(), iwfile->length(), summaryWorkspace.getSummarized(), summaryWorkspace.getIndexed(), summaryWorkspace.getReindexed(), (const char*)m_summaryLrtLocality.toString());
		}
		m_share.release(iwfile);

		// XXX: add newly-paged summary to set of active summaries
		clobberLock();
		{
			PagedSummary* summaryRef = summaryWorkspace.getPagedSummary();
			if (getActiveSummaries()->contains(summaryRef->getStreamLocality()) == false) {
				getActiveSummaries()->addSummary(summaryRef);
			}
		}
		clobberUnlock();

	} else {
		DEEP_LOG(DEBUG, FINAL, "skipped block: %s, length: %lld\n", iwfile->getPath(), iwfile->length());
	}

	return indexed;
}

template<typename K>
boolean RealTimeMap<K>::reorganizeFiles(ThreadContext<K>* ctxt, boolean* cont, boolean valueReorganize, boolean valueCompression) {

	inttype reorg = 0;
	HashMap<ushorttype,uinttype> deadStats;
	HashMap<ushorttype,uinttype> totalStats;
	HashMap<ushorttype,doubletype> compressStats;

	const ulongtype optimizeCount = (valueReorganize == true) ? m_optimizeStream : m_optimizePaging;
	const boolean force = ((valueReorganize == true) && m_vrtForceReorganization) || ((valueReorganize == false) && m_irtForceReorganization);

	if (valueReorganize == true) {
		RealTimeAdaptive_v1<K>::fragmentationStatistics(this, m_share.getVrtWriteFileList(), &deadStats, true /* use dead count */);
		RealTimeAdaptive_v1<K>::fragmentationStatistics(this, m_share.getVrtWriteFileList(), &totalStats, false /* use dead count */);

	} else {
		RealTimeAdaptive_v1<K>::fragmentationStatistics(this, m_share.getIrtWriteFileList(), &deadStats, true /* use dead count */);
		RealTimeAdaptive_v1<K>::fragmentationStatistics(this, m_share.getIrtWriteFileList(), &totalStats, false /* use dead count */);
	}

	if (valueReorganize == true) {
		RealTimeAdaptive_v1<K>::compressionStatistics(this, MapFileUtil::VRT, &compressStats);
	} else {
		RealTimeAdaptive_v1<K>::compressionStatistics(this, MapFileUtil::IRT, &compressStats);
	}

	// XXX: the following file creation is required to be performed on the index (i.e. no file locking)
	if (m_irtBuildReorganization == true) {
		pagingFileManagement(true /* lock */, true /* create file */);
		m_irtBuildReorganization = false;
	}

	clobberReadLock();
	MapFileSet<MeasuredRandomAccessFile> files(*(valueReorganize ? m_share.getVrtWriteFileList() : m_share.getIrtWriteFileList()));
	Iterator<MeasuredRandomAccessFile*>* iter = files.iterator();
	while ((m_mount == MOUNT_OPENED) && (m_resource.getCacheUsage() < RealTimeResource::IMMENSE) && (iter->hasNext() == true)) {

		// XXX: null write (empty index == place holder)
		MeasuredRandomAccessFile* wfile = iter->next();
		if (wfile == null) {
			continue;

		} else if (wfile->getAwaitingDeletion() == true) {
			continue;

		// XXX: referenced until information is committed to main memory
		} else if (wfile->ref() != 0 /* when using large transactions */) {
			continue;

		} else if ((force == false) && (wfile->length() < (getFileSize() / 25.0))) {
			continue;

		} else if ((force == true) && (wfile->getOptimizeCount() == optimizeCount)) {
			continue;
		}

		// XXX: signal to reorg that only file references should be checked (for possible file removal, ie not statistics based defrag)
		boolean checkFileReferences = false;

		if (RealTimeAdaptive_v1<K>::driveReorganization(this, wfile, valueCompression, optimizeCount, &totalStats, &deadStats, &compressStats, &checkFileReferences) == true) {
			reorg++;

			RETRY:
			// XXX: bail out if we're only checking file references and we find a reference
			boolean fileRefBailout = false;

			// XXX: safe context lock: multiple readers / no writer on the branch tree
			m_threadContext.readLock();
			m_orderSegmentList.lock();
			{
				m_orderSegmentList.clear(false /* lock */);
				m_orderSegmentMode = MODE_REORG;

				MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) m_orderSegmentSet.reset();
				while (segIter->MapSegmentEntrySetIterator::hasNext()) {
					MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
					Segment<K>* segment = segEntry->getValue();

					if (segment->getSummary() == true) {
						// XXX: we can check file references on summaries directly
						if (checkFileReferences == false) {
							segment->incref();

							m_orderSegmentList.unlock();
							m_threadContext.readUnlock();

							if (forceSetupSegment(ctxt, segment, true /* physical */, false /* values */) == true) {
								segment->unlock();
							}

							goto RETRY;
						}
					}

					if (valueReorganize == true) {
						if (segment->hasStreamIndex(wfile->getFileIndex()) == false) {
							continue;
						}
			
						if (checkFileReferences == false) {
							segment->setFragmentedValue(true);
							segment->setFragmentedKey(true);

						} else {
							fileRefBailout = true;
							break;
						}

					} else {
						if (segment->hasPagingIndex(wfile->getFileIndex()) == false) {
							continue;
						}

						if (checkFileReferences == false) {
							segment->setFragmentedKey(true);

						} else {
							fileRefBailout = true;
							break;
						}
					}

					if (checkFileReferences == false) {
						segment->setIndexOrderKey(m_keyBuilder->cloneKey(segEntry->getKey()));
						m_orderSegmentList.add(segment, false /* lock */);
					}
				}

			}
			m_orderSegmentList.unlock();
			m_threadContext.readUnlock();

			if (fileRefBailout == true) {
				wfile->setCycleCount(0);
				wfile->setReorganizeComplete(false);
				continue;
			}

			if (checkFileReferences == true) {
				#ifdef DEEP_DEBUG	
				DEEP_LOG(ERROR, OTHER, "Invalid reorg: found unreferenced file, %s\n", wfile->getPath());
				throw InvalidException("Invalid reord: found unreferenced file");
				#else
				DEEP_LOG(WARN, OTHER, "Invalid reorg: found unreferenced file, %s\n", wfile->getPath());
				#endif
			}

			inttype request = m_orderSegmentList.size();
			inttype indexed = 0;

			longtype start = System::currentTimeMillis();
			{
				if (m_orderSegmentList.size() != 0) {
					if (valueReorganize == true) {
						if (wfile->getFileIndex() == m_streamIndex) {
							streamFileManagement(true /* lock */, true /* create file */);
						}

					} else {
						if (wfile->getFileIndex() == m_pagingIndex) {
							pagingFileManagement(true /* lock */, true /* create file */, false /* needsClobberLock */);
						}
					}

					boolean reorgFlag = true;
					indexed = orderSegments(ctxt, true /* reset */, true /* timeout */, cont, &reorgFlag);
				}
			}
			longtype stop = System::currentTimeMillis();
			m_organizationTime = stop;

			if (m_orderSegmentList.size() != 0) {
				DEEP_LOG(DEBUG, REORG, "block: %s, %c[%d;%dmpartially:%c[%d;%dm i[%d of %d], t[%lld]\n", wfile->getPath(), 27,1,35,27,0, 25, indexed, request, (stop-start));

				// XXX: must break from reorganizaton (see: m_orderSegmentList.clear above)
				wfile->setCycleCount(0);
				break;

			} else {
				DEEP_LOG(DEBUG, REORG, "block: %s, %c[%d;%dmcomplete:%c[%d;%dm i[%d of %d], t[%lld]\n", wfile->getPath(), 27,1,35,27,0, 25, indexed, request, (stop-start));
			}

			if ((request > 0) && (wfile->getCycleCount() < Properties::DEFAULT_CYCLE_LIMIT /* allow indexing to settle by cycle limit */)) {
				wfile->setCycleCount(wfile->getCycleCount() + 1);
				continue;
			}

			// XXX: update xrt with new compression percentages
			if ((m_valueCompressMode == true) && (valueReorganize == true)) {
				RealTimeAdaptive_v1<K>::writeValueStatistics(this, false /* force */);
			}

			wfile->setReorganizeComplete(true);

		} else {
			wfile->setCycleCount(0);
			wfile->setReorganizeComplete(false);
		}
	}
	Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);

	if ((reorg > 0) && (m_mount == MOUNT_OPENED)) {
		iter = files.iterator();
		while (iter->hasNext() == true) {
			MeasuredRandomAccessFile* wfile = iter->next();

			// XXX: null write (empty index == place holder)
			if (wfile == null) {
				continue;
			}

			if (wfile->getReorganizeComplete() == false) {
				continue;
			}

			if (wfile->getAwaitingDeletion() == true) {
				DEEP_LOG(DEBUG, FSMGT, "block: %s - deferred\n", wfile->getPath());
				continue;
			}

			if (wfile->getActive() == true) {
				DEEP_LOG(WARN, FSMGT, "block: %s - still active %d\n", wfile->getPath(), wfile->syncPrepared());
				continue;
			}

			if (valueReorganize == true) {
				if (wfile->getFileIndex() == m_streamIndex) {
					DEEP_LOG(WARN, FSMGT, "block: %s - still streaming, invalid destroy\n", wfile->getPath());

					//#ifdef DEEP_DEBUG
					#if 0
					throw InvalidException("Invalid destroy: file still streaming");
					#else
					streamFileManagement(true /* lock */, true /* create file */);
					#endif
				}

			} else {
				if (wfile->getFileIndex() == m_pagingIndex) {
					DEEP_LOG(WARN, FSMGT, "block: %s - still paging, invalid destroy\n", wfile->getPath());

					//#ifdef DEEP_DEBUG
					#if 0
					throw InvalidException("Invalid destroy: file still paging");
					#else
					pagingFileManagement(true /* lock */, true /* create file */, false /* needsClobberLock */);
					#endif
				}
			}

			reorg--;

			if (RealTimeAdaptive_v1<K>::allowFileClobber(this, wfile, false /* needsLock */) == false) {
				DEEP_LOG(DEBUG, FSMGT, "erase, deferred block: %s\n", wfile->getPath());
				m_share.deferredDestroy(getIndexValue(), wfile);
				continue;
			}

			destroyFile(wfile);
		}
		Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
	}
	clobberReadUnlock();

	if (reorg == 0) {
		if (valueReorganize == true) {
			if (m_vrtForceReorganization == true) {
				// XXX: after a forced value reorg, clear active summaries and let the old files go away
				clobberLock();
				BasicArray<RealTime*>* secondaries = getShare()->getSecondaryList();
				for (inttype i=0; i<secondaries->size(); ++i) {
					RealTime* rt = secondaries->get(i);
					if (rt == null) {
						// XXX: allow for dynamic add/remove secondary index
						continue;
					}

					rt->getActiveSummaries()->clear();
				}
				getActiveSummaries()->clear();

				// XXX: attempt to clean up files freed up by the reorg
				RealTimeAdaptive_v1<K>::clobberUnusedFiles(this);
				clobberUnlock();
			}
			m_vrtForceReorganization = false;

		} else {
			m_irtForceReorganization = false;
		}
	}

	return (reorg != 0);
}

template<typename K>
ushorttype RealTimeMap<K>::pagingFileManagement(boolean lock, boolean create, const boolean needsClobberLock) {

	ushorttype fileIndex = m_pagingIndex;

	MeasuredRandomAccessFile* iwfile = m_share.getIrtWriteFileList()->get(fileIndex);
	if ((create == true) || (RealTimeAdaptive_v1<K>::needsAddPagingFile(this, iwfile) == true)) {
		if (lock == true) m_share.getIrtWriteFileList()->lock();
		{
			iwfile = m_share.getIrtWriteFileList()->get(m_pagingIndex);
			if ((create == true) || (RealTimeAdaptive_v1<K>::needsAddPagingFile(this, iwfile) == true)) {

				boolean clobberLock = (needsClobberLock == false);
				if (needsClobberLock == true) clobberLock = tryClobberLock();
				const boolean summaryInvalid = (clobberLock == true) ? (getActiveSummaries()->contains(m_summaryLrtLocality) == false) : false;
				if ((needsClobberLock == true) && (clobberLock == true)) clobberUnlock();

				m_share.acquire(iwfile);
				{
					RealTimeVersion<K>::terminatePaging(iwfile, this, false /* summarized */, getRecoveryEpoch(), summaryInvalid);

					iwfile->setOnline(false);
				}
				m_share.release(iwfile);

				// XXX: m_pagingIndex is set in addPagingFile
				addPagingFile(false);
			}
		}
		if (lock == true) m_share.getIrtWriteFileList()->unlock();
	}

	return m_pagingIndex;
}

template<typename K>
ushorttype RealTimeMap<K>::streamFileManagement(boolean lock, boolean create) {

	ushorttype fileIndex = m_streamIndex;

	MeasuredRandomAccessFile* vwfile = m_share.getVrtWriteFileList()->get(fileIndex);
	/* MeasuredRandomAccessFile* lwfile = m_share.getLrtWriteFileList()->get(fileIndex); */
	if ((create == true) || (vwfile->BufferedRandomAccessFile::getFilePointer() > getFileSize()) /* || (lwfile->BufferedRandomAccessFile::getFilePointer() > getFileSize()) */) {
		if (lock == true) m_share.getVrtWriteFileList()->lock();
		{
			vwfile = m_share.getVrtWriteFileList()->get(m_streamIndex);
			/* lwfile = m_share.getLrtWriteFileList()->get(m_streamIndex); */
			if ((create == true) || (vwfile->BufferedRandomAccessFile::getFilePointer() > getFileSize()) /* || (lwfile->BufferedRandomAccessFile::getFilePointer() > getFileSize()) */) {

				MeasuredRandomAccessFile* lwfile = m_share.getLrtWriteFileList()->get(m_streamIndex);

				// XXX: don't offline until durable sync has been completed
				if (lwfile->syncPrepared() == false) {

					m_share.acquire(lwfile);
					m_share.acquire(vwfile);
					{
						lwfile->setOnline(false);
						vwfile->setOnline(false);
					}
					m_share.release(vwfile);
					m_share.release(lwfile);
				}

				RandomAccessFile* xwfile = m_share.getXrtWriteFileList()->last();
				if (xwfile != null) {
					m_share.acquire(xwfile);
					{
						longtype total = 0;
						{
							sizeStorage(&total);

							for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
								RealTime* rt = m_share.getSecondaryList()->get(i);
								if (rt == null) {
									// XXX: allow for dynamic add/remove secondary index
									continue;
								}

								rt->sizeStorage(&total);
							}

							m_extraStats.setTotalSize(total);
						}

						ExtraStatistics_v1::updateSize(xwfile, &m_extraStats, true /* force */);

						xwfile->setOnline(false);
					}
					m_share.release(xwfile);

				} else {
					DEEP_LOG(WARN, STATS, "store: %s, failed to find last xrt file\n", getFilePath());
				}

				// XXX: m_streamIndex is set in addStreamFile
				addStreamFile(false);
			}

			if (m_schemaBuilder->isTemporary() == false) {
				RandomAccessFile* swfile = m_share.getSrtWriteFileList()->last();
				m_share.acquire(swfile);
				{
					for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
						RealTime* rt = m_share.getSecondaryList()->get(i);
						if (rt == null) {
							// XXX: allow for dynamic add/remove secondary index
							continue;
						}

						// TODO: fix cast
						RealTimeSchema_v1<K>::write(((RealTimeMap<K>*) rt)->m_schemaBuilder, swfile, false);
					}

					swfile->setOnline(false);
				}
				m_share.release(swfile);
			}
		}
		if (lock == true) m_share.getVrtWriteFileList()->unlock();
	}

	return m_streamIndex;
}

template<typename K>
boolean RealTimeMap<K>::markSegment(ThreadContext<K>* ctxt, Segment<K>* segment, ushorttype cindex) {
	boolean rollover = false;

	if ((m_state == MAP_RECOVER) || (segment->getStreamed() == false)) {
		segment->addStreamIndex(cindex);

	} else {
		segment->addStreamIndex(cindex);

		if (segment->getStreamRangeOverflow(false /* includeOld */) == true) {
			rolloverRealTime(ctxt, segment);
			rollover = true;
		}
	}

	#ifdef DEEP_DEBUG
	if ((rollover == false) && (segment->hasStreamIndex(cindex) == false) && (segment->getStreamRangeOverflow(false /* includeOld */) == false)) {
		DEEP_LOG(ERROR, OTHER, "Invalid mark: index not set %d, %s\n", cindex, getFilePath());

		throw InvalidException("Invalid mark: index not set");
	}
	#endif

	return rollover;
}

template<typename K>
void RealTimeMap<K>::fillSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean values, boolean pace) {

	boolean summary = segment->getSummary();

	if (((m_memoryCompressMode == false) || (segment->getZipData() == null)) && (m_memoryMode == false)) {

		if (summary == false) {

			// XXX: setCardinalityEnabled / setVirtualSizeEnabled 
			segment->RealTimeTypes<K>::SegTreeMap::setStatisticsEnabled(false);
			{
				readIndex(ctxt, segment, values, pace);
			
				// XXX: puts on any virtual segment can cause incorrect cardinality, fix that up here since we have all the keys
				segment->recalculateCardinality();
			}
			segment->RealTimeTypes<K>::SegTreeMap::setStatisticsEnabled(true);
		}

	// XXX: Could be purged but not zipped eg on restart
	} else if ((m_memoryCompressMode == true) && (segment->getPurged() == true) && (segment->getZipData() != null)) {

		// XXX: setCardinalityEnabled / setVirtualSizeEnabled 
		segment->RealTimeTypes<K>::SegTreeMap::setStatisticsEnabled(false);
		{
			RealTimeCompress<K>::decompress(this, segment, &m_share, (KeyBuilder<K>*)m_keyBuilder);
		}
		segment->RealTimeTypes<K>::SegTreeMap::setStatisticsEnabled(true);
	}

	if (summary == true) {
		Segment<K>* firstSegment = segment;
		BasicArray<voidptr> array(firstSegment->SegTreeMap::size());

		typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);
		firstSegment->SegTreeMap::entrySet(&stackSegmentItemSet);
		MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
		while (infoIter->MapInformationEntrySetIterator::hasNext()) {
			SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();
			array.add(infoEntry->getValue());
		}

		// XXX: remove first key since it will be re-created below
		K initFirstKey = firstSegment->SegTreeMap::firstKey();
		firstSegment->SegTreeMap::remove(initFirstKey);
		firstSegment->SegTreeMap::clear(true, false);

		for (int i = 0; i < array.size(); i++) {
			Information* info = (Information*) array.get(i);

			if (i > 0) {
				segment = new Segment<K>(m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER, Versions::GET_PROTOCOL_CURRENT(), m_keyBuilder->getKeyParts());
				segment->setMapContext(m_indexValue);
				segment->lock();

			} else {
				segment = firstSegment;
				segment->resetStreamIndexes();
				segment->resetPagingIndexes();
				segment->resetFlags();
			}

			array.set(i, segment);

			m_share.getIrtReadFileList()->lock();
			{
				BufferedRandomAccessFile* irfile = m_share.getIrtReadFileList()->get(info->getFileIndex());
				m_share.acquire(irfile);
				{
					// XXX: setCardinalityEnabled / setVirtualSizeEnabled 
					segment->RealTimeTypes<K>::SegTreeMap::setStatisticsEnabled(false);

					segment->addPagingIndex(info->getFileIndex());
					segment->setPagingPosition(info->getFilePosition());

					RealTimeVersion<K>::readPagingHeader(irfile, this, segment, ctxt);

					// XXX: leave cardinality off
					segment->setVirtualSizeEnabled(true);
				}
				m_share.release(irfile);
			}
			m_share.getIrtReadFileList()->unlock();

			Converter<Information*>::destroy(info);

			// XXX: safe context lock: one writer / no readers on the branch tree
			m_threadContext.writeLock();
			{
				if (firstSegment == segment) {
					m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::remove(initFirstKey);
					Converter<K>::destroy(initFirstKey);
				}

				m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(segment->SegTreeMap::firstKey(), segment);
			}
			m_threadContext.writeUnlock();

			// XXX: account for more purged segments (actual active vs actual purged)
			m_purgeSize.incrementAndGet();

			segment->setPaged(true);
			segment->setPurged(true);
		}

		for (int i = 1; i < array.size(); i++) {
			Segment<K>* segment = (Segment<K>*) array.get(i);
			segment->unlock();
		}

	} else {
		segment->setPurged(false);
		segment->setVirtual(false);
		segment->setCardinalityEnabled(true);
	}

	m_purgeSize.decrementAndGet();
}

template<typename K>
boolean RealTimeMap<K>::scrubSegment(ThreadContext<K>* ctxt, Segment<K>* segment, ushorttype identifier) {

	segment->setModification(0);

	typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

	segment->SegTreeMap::entrySet(&stackSegmentItemSet);
	MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
	if (infoIter->MapInformationEntrySetIterator::hasNext()) {
		infoIter->MapInformationEntrySetIterator::next();
	}

	while (infoIter->MapInformationEntrySetIterator::hasNext()) {
		SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();

		K key = infoEntry->getKey();

		Information* info = isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT, key);
		if ((info != null) && (info->getLevel() == Information::LEVEL_COMMIT) && (info->getDeleting() == false)) {

			if (info->getData() == null) {
				continue;
			}

			info->freeData();
			segment->setBeenFilled(false);
		}
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::purgeSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean growing, boolean index, boolean semi, BasicArray<Segment<K>*>* purgeList, BasicArray<Segment<K>*>* compressList, PurgeReport& purgeReport) {

	if (RealTimeAdaptive_v1<K>::allowPurgeSegment(this, segment, index, growing, semi, purgeReport) == false) {
		return false;
	}

	boolean compress = RealTimeAdaptive_v1<K>::allowMemoryCompression(this, segment, m_memoryCompressMode, m_memoryMode, growing);

	if ((compress == false) || (compressList == null)) {
		segment->setVirtualSizeEnabled(false);
		segment->setCardinalityEnabled(false);
	}

	boolean isVirtual = segment->getVirtual();

	if (segment->getDirty() == true) {

		#ifdef DEEP_DEBUG
		if (semi == false) {
			DEEP_LOG(ERROR, OTHER, "Invalid purge: semi purge not requested, %s\n", getFilePath());

			throw InvalidException("Invalid purge: semi purge not requested");
		}
		#endif

		typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

		segment->SegTreeMap::entrySet(&stackSegmentItemSet);
		MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();

		for (inttype i = 0; infoIter->MapInformationEntrySetIterator::hasNext(); i++) {
			SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();
			StoryLine& storyLine = infoEntry->getStoryLine();

			Information* info = infoEntry->getValue();

			// XXX: secondary segment could be 'viewed' do to first key deletion awaiting eject, filling segment is invalid, so we cannot purge it
			if ((i == 0) && ((info->getDeleting() == true) || (info->getNext() != null))) {
				return false;
			}

			if ((i != 0) && (info->getNext() == null) && (info->getLevel() == Information::LEVEL_COMMIT)) {

				// XXX: not yet indexed
				if (info->getIndexed(m_indexValue) == false) {
					continue;
				}

				// XXX: deleted infos singal to indexing that a rebuild is needed so keep them around
				if (info->getDeleting() == true) {
					continue;
				}

				// XXX: viewed by some transaction 
				if ((info->getViewpoint() != 0) && (Transaction::viewing(info->getViewLimit()) == true)) {
					continue;
				}

				// XXX: locked by some transaction
				if ((storyLine.getSharing() == true) || (Transaction::lockable(storyLine, 0, false) == false)) {
					continue;
				}

				// XXX: note setting virtual state
				segment->setVirtual(true);

				K key = infoEntry->getKey();
				infoIter->MapInformationEntrySetIterator::remove();

				Converter<K>::destroy(key);
				Converter<Information*>::destroy(info);
			}
		}

	} else {

		if (compress == false) {
			K firstKey = segment->SegTreeMap::firstKey();
			Information* firstValue = segment->SegTreeMap::remove(firstKey);

			// XXX: turn this on to validate secondary segment purge is valid
			#if 0
			#ifdef DEEP_DEBUG
			if (m_primaryIndex != null) {
				typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

				segment->SegTreeMap::entrySet(&stackSegmentItemSet);
				MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();

				for (inttype i = 0; infoIter->MapInformationEntrySetIterator::hasNext(); i++) {
					SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();

					Information* info = infoEntry->getValue();
					if ((info->getDeleting() == true) || (info->getNext() != null)) {
						DEEP_LOG(ERROR, OTHER, "Invalid purge: info state, %s\n", getFilePath());

						throw InvalidException("Invalid purge: info state");
					}
				}
			}
			#endif
			#endif

			// XXX: deferring deletion allows the purge/index threads to quickly unlock the branch and segment trees
			if ((purgeList != null) && (m_resource.getCacheUsage() < RealTimeResource::EXTREME)) {
				Segment<K>* delSegment = new Segment<K>(m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER, Versions::GET_PROTOCOL_CURRENT(), m_keyBuilder->getKeyParts());
				delSegment->setMapContext(-1);
				segment->transfer(delSegment);

				purgeList->add(delSegment, true);

			} else {
				segment->SegTreeMap::clear(true, true);
			}

			segment->SegTreeMap::put(firstKey, firstValue);


		} else {
			if (compressList != null) {
				compressList->add(segment, true);
			} else {
				RealTimeCompress<K>::compress(this, ctxt, segment, &m_share, m_keyBuilder);
			}
		}
	}

	if ((compress == false) || (compressList == null)) {
		segment->setModification(segment->getModification() + 1);
		segment->setVirtualSizeEnabled(true);
		segment->setBeenFilled(false);

		if (segment->getDirty() == false) {
			segment->setPurged(true);
		}

		if (isVirtual == false) {
			m_purgeSize.incrementAndGet();
		}
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::indexSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean rebuild, uinttype viewpoint, boolean backwardCheckpoint, RealTimeSummary<K>* summaryWorkspace, IndexReport* indexReport) {

	IndexReport _indexReport;
	if (indexReport == null) {
		indexReport = &_indexReport;
	}

	boolean written = false;
	K summaryFirstKey = (K) Converter<K>::NULL_VALUE;
	ushorttype fileIndex = pagingFileManagement(true /* lock */, false /* create */, (m_orderSegmentMode != MODE_REORG) /* needsClobberLock */);

	if ((rebuild == false) && (segment->getCurrentPagingIndex() != fileIndex)) {
		// XXX: paging file indexes increase until wrapping
		rebuild = (segment->validPagingRange(fileIndex) == false);
	}

	MeasuredRandomAccessFile* iwfile = m_share.getIrtWriteFileList()->get(fileIndex);
	m_share.acquire(iwfile);
	{
		iwfile->setNofityFlushed(true);

		#ifdef DEEP_IO_STATS
			#ifdef DEEP_DEBUG
			const ubytetype fileRange = segment->getPagingRange(m_state == MAP_RECOVER /* ignore overflow */);
			#else
			const ubytetype fileRange = segment->getPagingRange();
			#endif
		const ubytetype fragmentCount = segment->getFragmentCount();
		#endif

		segment->setBeenSurceased(false);

		const boolean wasAltered = segment->getBeenAltered();
		segment->setBeenAltered(false);

		boolean early = false;
		written = RealTimeVersion<K>::writeKeyPaging(iwfile, this, ctxt, segment, rebuild, viewpoint, summaryFirstKey, early, *indexReport) != 0;
		if (written == true) {
			if (segment->getPaged() == false) {

				segment->addPagingIndex(iwfile->getFileIndex());
				segment->setPaged(true);

				#ifdef DEEP_IO_STATS
				ctxt->m_statsCount3 += fragmentCount; // XXX: VERTICAL DEFRAGMENT
				if (fileRange != 0) {
					for (int i = Properties::DEFAULT_SEGMENT_FILE_RANGE - 1; i >= 0; i--) {
						if (fileRange & (1 << i)) {
							ctxt->m_statsCount4++; // XXX: HORIZONTAL DEFRAGMENT
						}
					}
				}
				#endif

			} else if (segment->hasPagingIndex(iwfile->getFileIndex()) == false) {

				segment->addPagingIndex(iwfile->getFileIndex());

				#ifdef DEEP_IO_STATS
				ctxt->m_statsCount2++; // XXX: HORIZONTAL FRAGMENT
				#endif
			}

			#ifdef DEEP_IO_STATS
			ctxt->m_statsCount1++; // XXX: VERTICAL FRAGMENT
			#endif

			// XXX: this might change with dynamic summarization
			if (segment->getSummary() == false) {
				segment->incrementFragmentCount();

				if ((viewpoint == 0) || (backwardCheckpoint == false)) {
					segment->setFragmentedKey(false);
					if (viewpoint == 0) {
						segment->setBeenReseeded(false);
					}
				}
			}

			iwfile->setEndwiseLrtLocality(m_primaryIndex == null ? getCurrentLrtLocality() : m_primaryIndex->getCurrentLrtLocality());

		} else {
			segment->setBeenAltered(wasAltered || segment->getBeenAltered());

			if ((viewpoint == 0) && (early == false)) {
				segment->setFragmentedKey(false);
			}

			#if 0
			if ((m_mount == MOUNT_CLOSING) && ((segment->getDirty() == true) || (segment->getBeenAltered() == true))) {
				DEEP_LOG(ERROR, OTHER, "Invalid unmount paging: segment %p dirty %d altered %d , %s\n", segment, segment->getDirty(), segment->getBeenAltered(), getFilePath());

				throw InvalidException("Invalid unmount paging: segment dirty or altered");
			}
			#endif
		}

		iwfile->setNofityFlushed(false);
	}
	m_share.release(iwfile);

	if (summaryWorkspace != null) {
		// XXX: note that summarizeSegment() may call indexSegment() (but without a SummaryWorkspace, so recursion is bounded)
		summaryWorkspace->summarizeSegment(this, ctxt, (written == true) ? summaryFirstKey : segment->SegTreeMap::firstKey(), segment);
		segment->setBeenSurceased(false);

		// XXX; old stream references for checkpointing have been summarized and can be removed from segment
		segment->clearOldStreamReferences();
	}

	return written;
}

template<typename K>
Segment<K>* RealTimeMap<K>::pageSegment(ThreadContext<K>* ctxt, Segment<K>* segment, inttype* indexed, boolean rebuild, uinttype viewpoint, boolean backwardCheckpoint, RealTimeSummary<K>* summaryWorkspace, IndexReport& indexReport) {

	RETRY:
	Segment<K>* nSegment = null;
	// XXX: safe context lock: multiple readers / no writer on the branch tree
	m_threadContext.readLock();
	{
		const MapEntry<K,Segment<K>*>* higherEntry = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::higherEntry(segment->SegTreeMap::firstKey());
		if (higherEntry != null) {
			nSegment = higherEntry->getValue();

			if (nSegment->tryLock() == false) {
				nSegment = null;

			} else if ((nSegment->getBeenDeleted() == true) || (nSegment->SegTreeMap::size() == 0)) {
				nSegment->unlock();
				nSegment = null;
			}
		}
	}
	m_threadContext.readUnlock();

	if (nSegment != null) {

		boolean merge = RealTimeAdaptive_v1<K>::mergeFragmentation(this, segment, nSegment);
		if (merge == true) {

			#if 0 /* Properties::getMemoryStreaming() */
			if (segment->getVirtual() == true) {
				fillSegment(ctxt, segment, segment->getValuesCompressed() /* values */, false);
			}

			if (nSegment->getVirtual() == true) {
				fillSegment(ctxt, nSegment, nSegment->getValuesCompressed() /* values */, false);
			}
			#endif

			if ((segment->getValuesCompressed() == true) && (segment->getBeenFilled() == false)) {
				readValue(ctxt, segment);

				segment->setBeenFilled(true);
			}

			if ((nSegment->getValuesCompressed() == true) && (nSegment->getBeenFilled() == false)) {
				readValue(ctxt, nSegment);

				nSegment->setBeenFilled(true);
			}

			// XXX: safe context lock: one writer / no readers on the branch tree
			m_threadContext.writeLock();
			{
				mergeSegments(segment, nSegment);
			}
			m_threadContext.writeUnlock();

			rebuild = true;
			goto RETRY;
		}

		nSegment->unlock();
		nSegment = null;
	}

	// XXX: paging is due to dirty state, following will clear this state if indexing completes
	{
		if (indexSegment(ctxt, segment, rebuild, viewpoint, backwardCheckpoint, summaryWorkspace, &indexReport) == true) {
			(*indexed)++;
		}

		// XXX: may need to reindex segment during checkpointing in order to purge it
		if ((viewpoint != 0) && (RealTimeAdaptive_v1<K>::reindexCheckpoint(this, segment) == true)) {
			indexSegment(ctxt, segment, segment->getBeenAltered() /* rebuild */, 0 /* viewpoint */, false /* backwardCheckpoint */, null /* summaryWorkspace */, &indexReport);
		}
	}

	return nSegment;
}

template<typename K>
void RealTimeMap<K>::absolveRooted(Transaction* tx, const K key, const Information* curinfo) {

	bytearray pkey = Converter<K>::toData(key);

	for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
		RealTime* rt = m_share.getSecondaryList()->get(i);
		if (rt == null) {
			// XXX: allow for dynamic add/remove secondary index
			continue;
		}

		if (curinfo->getRooted(i + 1 /* secondary->m_indexValue */) == false) {
			continue;
		}

		rt->absolveInformation(tx, pkey, curinfo);
	}

	if (curinfo->getRooted(m_indexValue) == true) {
		RealTimeMap<K>::absolveInformation(tx, pkey, curinfo);
	}
}

template<typename K>
void RealTimeMap<K>::absolveSecondary(Transaction* tx, const K key, const Information* topinfo) {

	bytearray pkey = Converter<K>::toData(key);

	Information* next = (Information*) topinfo;
	while (next != null) {
		if (next->getUpdating() == true) {
			break;
		}

		Information* temp = next;
		next = next->getNext();
		temp->setNext(null);

		// XXX: MERGED overrides deleting
		if ((temp->getLevel() == Information::LEVEL_MERGED) || (temp->getDeleting() == false)) {
			for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
				RealTime* rt = m_share.getSecondaryList()->get(i);
				if (rt == null) {
					// XXX: allow for dynamic add/remove secondary index
					continue;
				}

				// XXX: all MERGED infos are effectively surrogates
				if ((temp->getLevel() != Information::LEVEL_MERGED) && (temp->getRooted(i + 1 /* secondary->m_indexValue */) == false)) {
					continue;
				}

				rt->absolveInformation(tx, pkey, temp);
			}
		}
	}
}

template<typename K>
void RealTimeMap<K>::rekeySecondary(Transaction* tx, const K key, const Information* newinfo, const Information* oldinfo) {

	bytearray pkey = Converter<K>::toData(key);

	for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
		RealTime* rt = m_share.getSecondaryList()->get(i);
		if (rt == null) {
			// XXX: allow for dynamic add/remove secondary index
			continue;
		}

		rt->rekeyInformation(tx, pkey, newinfo, oldinfo);
	}
}

template<typename K>
void RealTimeMap<K>::ejectSecondary(Transaction* tx, const K key, const Information* delinfo, const Information* curinfo, const Information* chkinfo, LockOption lock, boolean ignore) {

	bytearray pkey = Converter<K>::toData(key);

	for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
		RealTime* rt = m_share.getSecondaryList()->get(i);
		if (rt == null) {
			// XXX: allow for dynamic add/remove secondary index
			continue;
		}

		rt->ejectInformation(tx, pkey, delinfo, curinfo, chkinfo, lock, ignore);
	}
}

template<typename K>
boolean RealTimeMap<K>::lockSecondary(Transaction* tx, const bytearray pkey, XInfoRef& curinfoRef, LockOption lock, boolean primaryLocked, boolean stitch) {

	RealTimeShare* share = (m_primaryIndex == null) ? &m_share : m_primaryIndex->getShare();
	for (int i = 0; i < share->getSecondaryList()->size(); i++) {
		RealTime* rt = share->getSecondaryList()->get(i);
		if (share->getSecondaryList()->get(i) == this) {
			continue;
		}

		if (rt == null) {
			// XXX: allow for dynamic add/remove secondary index
			continue;
		}

		if (rt->getMount() == RealTime::MOUNT_IGNORED) {
			// XXX: ignore future secondary during replay
			continue;
		}

		if (rt->lockInformation(tx, pkey, curinfoRef, lock, primaryLocked, stitch) == false) {
			return false;
		}
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::lockSecondary(Transaction* tx, const K key, XInfoRef& newinfoRef, XInfoRef& oldinfoRef, LockOption lock, Conductor* conductor) {

	#ifdef DEEP_DEBUG
	if (m_primaryIndex != null) {
		DEEP_LOG(ERROR, OTHER, "Invalid lock secondary: called on secondary, %s\n", getFilePath());

		throw InvalidException("Invalid lock secondary: called on secondary");
	}
	#endif

	bytearray pkey = Converter<K>::toData(key);

	/* XXX: assume newinfo exists and has access to its data
	if ((newinfo != null) && (newinfo->getData() == null)) {
		ThreadContext<K>* = getTransactionContext(tx);

		copyValueFromInformation(ctxt, (Information*) newinfo, key);
	}
	*/

	#ifdef DEEP_DEBUG
	if ((oldinfoRef.getInfo() != null) && (oldinfoRef.getInfo()->getData() == null)) {
		DEEP_LOG(ERROR, OTHER, "Invalid lockSecondary: oldinfo has null data");

		throw InvalidException("Invalid lockSecondary: oldinfo has null data");
	}
	#endif

	for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
		RealTime* rt = m_share.getSecondaryList()->get(i);
		if (rt == null) {
			// XXX: allow for dynamic add/remove secondary index
			continue;
		}

		if (rt->getMount() == RealTime::MOUNT_IGNORED) {
			// XXX: ignore future secondary during replay
			continue;
		}

		if (rt->lockInformation(tx, pkey, newinfoRef, oldinfoRef, lock, conductor) == false) {
			return false;
		}
	}

	return true;
}

template<typename K>
void RealTimeMap<K>::abortSecondary(Transaction* tx, const K key, const Information* orginfo, XInfoRef& preinfoRef, const Information* curinfo) {

	bytearray pkey = Converter<K>::toData(key);

	if ((orginfo != null) && (orginfo->getData() == null)) {
		ThreadContext<K>* ctxt = getTransactionContext(tx);

		readValue(ctxt, (Information*) orginfo, key);
	}

	for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
		RealTime* rt = m_share.getSecondaryList()->get(i);
		if (rt == null) {
			// XXX: allow for dynamic add/remove secondary index
			continue;
		}

		rt->abortInformation(tx, pkey, orginfo, preinfoRef, curinfo);
	}
}

template<typename K>
void RealTimeMap<K>::readValue(ThreadContext<K>* ctxt, Segment<K>* segment) {

	RealTimeVersion<K>::readInfoValue(this, ctxt, segment);
}

template<typename K>
void RealTimeMap<K>::readValue(ThreadContext<K>* ctxt, Information* info, const K key) {

	#ifdef DEEP_DEBUG
	if (info->getData() != null) {
		DEEP_LOG(ERROR, OTHER, "Invalid read: existing info data, %s\n", getFilePath());

		throw InvalidException("Invalid read: existing info data");
	}
	#endif

 	// XXX: fill compressed segments as blocks since we need to read the entire block anyway
	if ((info->getCompressed() == true) && (m_primaryIndex == null)) {
		Segment<K>* segment = ctxt->getSegment();
		if (segment == null) {
			DEEP_LOG(ERROR, OTHER, "Invalid read: segment not found, %s\n", getFilePath());

			throw InvalidException("Invalid read: segment not found");
		}

		if (segment->getBeenFilled() == false) {
			readValue(ctxt, segment);

			segment->setBeenFilled(true);
		}

	#ifdef DEEP_COMPRESS_PRIMARY_READ
	} else if ((m_primaryIndex != null) && (info->getCompressed() == true) && (RealTimeAdaptive_v1<K>::readInfoValueFromPrimary(this, info) == true)) {
		if (m_keyBuilder->getPrimaryKey(key, ctxt->getKeyBuffer(), ctxt->getKeyOffset(), ctxt->getKeyLength()) == true) {
			inttype errcode = 0;

			nbyte tmpValue(info->getSize());
			InfoRefBuf infoRefBuf;
			XInfoRef* priinfoRef = m_primaryIndex->primaryInformation(ctxt->getTransaction(), ctxt->getKeyBuffer(), &tmpValue, LOCK_NONE, infoRefBuf, &errcode);
			if (priinfoRef == null) {
				DEEP_LOG(ERROR, READC, ",store: %s, read value - primary lookup deadlock: %d, %p\n", getFilePath(), errcode, info);

				throw InvalidException("Invalid read: compression could not get primary information");
			}
			Converter<XInfoRef*>::destroy(priinfoRef);

			if (info->getData() == null) {
				info->initData();
				memcpy(info->getData(), tmpValue, info->getSize());
			}

		} else {
			DEEP_LOG(ERROR, OTHER, "Invalid read: compresion could not get primary key, %s\n", getFilePath());

			throw InvalidException("Invalid read: compression could not get primary key");
		}

	} else {
	#else
	} else {
	#endif
		info->initData();
		nbyte tmpValue((const bytearray) info->getData(), info->getSize());

		RealTimeVersion<K>::readInfoValue(this, ctxt, info, &tmpValue);
	}

	#ifdef DEEP_DEBUG
	if (info->getData() == null) {
		DEEP_LOG(ERROR, OTHER, "Invalid read: info data not found, %s\n", getFilePath());

		throw InvalidException("Invalid read: info data not found");
	}
	#endif
}

template<typename K>
void RealTimeMap<K>::readIndex(ThreadContext<K>* ctxt, Segment<K>* segment, boolean fill, boolean pace) {

	boolean modification = false;
	boolean compression = false;

	if ((m_valueCompressMode == true) && (segment->getFragmentedValue() == true)) {
		compression = true;

	} else {
		modification = (m_reindexing.get() > 0) || segment->getFragmentedValue() || (Properties::getMemoryAnalytics() == false);
	}

	RealTimeVersion<K>::readKeyPaging(this, segment, ctxt, modification, compression);

	// XXX: for now only primary(s) bulk load values
	if (m_primaryIndex == null) {

		// XXX: typically non-growing information is scanned
		if ((fill == true) && (getCycleSize() != 0)) {
			fill = (size() == getCycleSize());
		}

		// XXX: only fill segment if there is no memory pressure or during dynamic alter
		if (((fill == true) && (m_resource.getPurgeFlag() == false)) || (m_reindexing.get() > 0)) {
			readValue(ctxt, segment);

			segment->setBeenFilled(true);
		}
	}

	// XXX: for CompressionTest unit test
	#ifndef DEEP_FORCE_MEMORY_COMPRESSION
	if ((pace == true) && (m_resource.getPaceFlag() == true)) {
		m_resource.pace(size() > getCycleSize(), false);
	}
	#endif

	if (Properties::getDebugEnabled(Properties::KEY_RANGE) == true) {
		typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

		segment->SegTreeMap::entrySet(&stackSegmentItemSet);
		MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
		for (int i = 0; infoIter->MapInformationEntrySetIterator::hasNext(); i++) {
			SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();

			m_threadContext.readLock();
			{
				verifySegmentKeyRange(segment, infoEntry->getKey());
			}
			m_threadContext.readUnlock();
		}
	}
}

template<typename K>
Information* RealTimeMap<K>::setupResult(ThreadContext<K>* ctxt, InfoRef& curinfoRef, nbyte* value, LockOption lock) {

	Segment<K>* segment = curinfoRef.segment;
	const SegMapEntry* infoEntry = curinfoRef.infoEntry;
	Information* curinfo = curinfoRef.getInfo();

	#ifdef DEEP_IO_STATS
	ctxt->m_statsCount1++; // XXX: READ OPERATIONS
	#endif

	#ifdef DEEP_COMPRESS_PRIMARY_READ
	if ((m_primaryIndex != null) && (curinfo->getData() == null) && (curinfo->getCompressed() == true) && (RealTimeAdaptive_v1<K>::readInfoValueFromPrimary(this, curinfo) == true)) {
		if (m_keyBuilder->getPrimaryKey(infoEntry->getKey(), ctxt->getKeyBuffer(), ctxt->getKeyOffset(), ctxt->getKeyLength()) == true) {
			inttype errcode = 0;

			if (lock != LOCK_NONE) {
				Information* info = null;
				InfoRefBuf infoRefBuf;
				XInfoRef* priinfoRef = m_primaryIndex->primaryInformation(ctxt->getTransaction(), ctxt->getKeyBuffer(), null, lock, infoRefBuf, &errcode);
				if (priinfoRef != null) {
					info = priinfoRef->getInfo();
				}
				if (info != null) {
					curinfo = info;

					InfoRef& infoRef = curinfoRef;
					stitchInformationSafe(ctxt, null /* tx */, *priinfoRef, infoRef, false /* primaryLocked */);
					Converter<XInfoRef*>::destroy(priinfoRef);

					// XXX: information was referenced in ::primaryInformation
					Converter<Information*>::destroy(curinfo);

				} else {
					if (priinfoRef != null) {
						Converter<XInfoRef*>::destroy(priinfoRef);
					}
					DEEP_LOG(DEBUG, READC, "store: %s, setup value - primary lookup deadlock: %d\n", getFilePath(), errcode);
					return null;
				}

			} else {
				InfoRefBuf infoRefBuf;
				XInfoRef* priinfoRef = m_primaryIndex->primaryInformation(ctxt->getTransaction(), ctxt->getKeyBuffer(), value, lock, infoRefBuf, &errcode);
				if (priinfoRef != null) {
					Converter<XInfoRef*>::destroy(priinfoRef);
				}

				if (errcode != 0) {
					DEEP_LOG(DEBUG, READC, "store: %s, setup value - primary lookup deadlock: %d\n", getFilePath(), errcode);
					return null;
				}

				curinfo->initData();
				memcpy(curinfo->getData(), *value, curinfo->getSize());
			}

		} else {
			DEEP_LOG(ERROR, OTHER, "Invalid setup: compression could not get primary key, %s\n", getFilePath());

			throw InvalidException("Invalid setup: compression could not get primary key");
		}
	}
	#endif

	if ((m_primaryIndex != null) && (curinfo->getData() == null) && (curinfoRef.getStoryLine().getSharing() == false) && (RealTimeAdaptive_v1<K>::readInfoValueFromPrimary(this, curinfo) == true)) {
		if (m_keyBuilder->getPrimaryKey(infoEntry->getKey(), ctxt->getKeyBuffer(), ctxt->getKeyOffset(), ctxt->getKeyLength()) == true) {
			Information* info = null;
			InfoRefBuf infoRefBuf;
			XInfoRef* priinfoRef = m_primaryIndex->primaryInformation(ctxt->getTransaction(), ctxt->getKeyBuffer(), null, lock, infoRefBuf);
			if (priinfoRef != null) {
				info = priinfoRef->getInfo();
			}
			if (info != null) {
				curinfo = info;

				boolean ignore = false;
				InfoRef& infoRef = curinfoRef;
				stitchInformationSafe(ctxt, null /* tx */, *priinfoRef, infoRef, false /* primaryLocked */);
				Converter<XInfoRef*>::destroy(priinfoRef);

				if (ignore == false) {
					// XXX: information was referenced in ::primaryInformation
					Converter<Information*>::destroy(curinfo);
				}
			} else {
				if (priinfoRef != null) {
					Converter<XInfoRef*>::destroy(priinfoRef);
				}
			}
		}
	}

	// XXX: reading information with value compression might require a segment context
	ctxt->setSegment(segment);
	{
		if (((uinttype) value->length) < curinfo->getSize() /* 24-bit unsigned int */) {
			value->realloc(curinfo->getSize());
		}

		if (curinfo->getData() == null) {
			readValue(ctxt, curinfo, infoEntry->getKey());
		}

		memcpy(*value, curinfo->getData(), curinfo->getSize());
	}
	ctxt->setSegment(null);

	return curinfo;
}

template<typename K>
boolean RealTimeMap<K>::trySetupSegment(ThreadContext<K>* ctxt, Segment<K>* segment) {

	if (segment->tryLock() == false) {
		segment->incref();
		return false;
	}

	if (segment->getSummary() == true) {
		segment->incref();
		segment->unlock();
		return false;
	}

	if (segment->getPurged() == true) {
		segment->incref();
		segment->unlock();
		return false;
	}

	if (segment->getVirtual() == true) {
		segment->incref();
		segment->unlock();
		return false;
	}

	/* XXX: no need to check state due to context read/write locking
	if (segment->getBeenDeleted() == true) {
		segment->unlock();
		return false;
	}
	*/

	return true;
}

template<typename K>
boolean RealTimeMap<K>::fillSetupSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean physical, boolean values) {

	if (segment->getSummary() == true) {

		fillSegment(ctxt, segment, false /* values */, false /* pace */);

		segment->unlock();

		// XXX: force a re-get after expanding this summary segment
		return false;

	} else if (segment->getPurged() == true) {

		if ((physical == true) || ((segment->getZipData() != null) && (m_memoryCompressMode == true))) {
			fillSegment(ctxt, segment, values, RealTimeAdaptive_v1<K>::doPace(this, ctxt->getTransaction()));

		} else {
			segment->setPurged(false);
			segment->setVirtual(true);
		}

	} else if ((segment->getVirtual() == true) && (physical == true)) {

		fillSegment(ctxt, segment, values, RealTimeAdaptive_v1<K>::doPace(this, ctxt->getTransaction()));
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::forceSetupSegment(ThreadContext<K>* ctxt, Segment<K>* segment, boolean physical, boolean values, boolean force) {

	if (force == true /* default case */) {
		segment->lock();

	} else if (segment->tryLock() == false) {
		segment->decref();
		return false;
	}

	if (segment->getBeenDeleted() == true) {
		segment->unlock();
		segment->decref();
		return false;
	}

	segment->decref();

	return fillSetupSegment(ctxt, segment, physical, values);
}

template<typename K>
Segment<K>* RealTimeMap<K>::initSegment(ThreadContext<K>* ctxt, const K key) {

	K newkey = m_keyBuilder->cloneKey(key);

	Segment<K>* segment = new Segment<K>(m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER, Versions::GET_PROTOCOL_CURRENT(), m_keyBuilder->getKeyParts());
	segment->setMapContext(m_indexValue);

	segment->setCardinalityEnabled(true);
	segment->SegTreeMap::put(newkey, null);

	segment->setBeenCreated(true);
	segment->lock();

	const MapEntry<K,Segment<K>*>* higherEntry = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::higherEntry(key);
	if (higherEntry != null) {
		Segment<K>* nSegment = higherEntry->getValue();
		if (nSegment->tryLock() == false) {
			nSegment = null;

		} else if ((nSegment->getBeenDeleted() == true) || (nSegment->SegTreeMap::size() == 0)) {
			nSegment->unlock();
			nSegment = null;
		}

		if (nSegment != null) {
			if (RealTimeAdaptive_v1<K>::mergeFragmentation(this, segment, nSegment) == true) {
				mergeSegments(segment, nSegment);

			} else {
				nSegment->unlock();
			}
		}
	}

	m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(newkey, segment);

	return segment;
}

template<typename K>
Segment<K>* RealTimeMap<K>::firstSegment(ThreadContext<K>* ctxt, const K key, boolean create) {

	Segment<K>* segment = null;

	RETRY:
	// XXX: safe context lock: multiple readers / no writer on the branch tree
	m_threadContext.readLock();
	{
		const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::firstEntry();
		if (index != null) {
			segment = index->getValue();

			if (trySetupSegment(ctxt, segment) == true) {
				m_threadContext.readUnlock();
				return segment;
			}
		}
	}
	m_threadContext.readUnlock();

	if (segment != null) {
		if (forceSetupSegment(ctxt, segment, true /* physical */, false /* values */) == false) {
			segment = null;
			goto RETRY;
		}

		// XXX: safe context lock: multiple readers / no writer on the branch tree
		m_threadContext.readLock();
		{
			const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::firstEntry();
			if ((index != null) && (segment != index->getValue())) {
				m_threadContext.readUnlock();
				segment->unlock();
				segment = null;
				goto RETRY;

			} else if (index == null) {
				segment->unlock();
				segment = null;
			}
		}
		m_threadContext.readUnlock();
	}

	if ((segment == null) && (create == true)) {
		// XXX: safe context lock: one writer / no readers on the branch tree
		m_threadContext.writeLock();
		{
			const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::firstEntry();
			if (index == null) {
				segment = initSegment(ctxt, key);
			}
		}
		m_threadContext.writeUnlock();

		if (segment == null) {
			goto RETRY;
		}
	}

	return segment;
}

template<typename K>
Segment<K>* RealTimeMap<K>::getSegment(ThreadContext<K>* ctxt, const K key, boolean create, boolean fill, boolean forceSegmentLock, boolean forceContextLock, boolean* wasClosed, boolean openSegment) {

	Segment<K>* segment = null;

	RETRY:

	// XXX: safe context lock: multiple readers / no writer on the branch tree
	#if 1
	if (m_threadContext.tryReadLock() == false) {
		if (forceContextLock == true) {
			m_threadContext.readLock();

		} else {
			return null;
		}
	}
	#else
	m_threadContext.readLock();
	#endif
	{
		const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::floorEntry(key);
		if (index != null) {
			segment = index->getValue();

			if (trySetupSegment(ctxt, segment) == true) {
				m_threadContext.readUnlock();
				return segment;
			}
		}
	}
	m_threadContext.readUnlock();

	if (segment != null) {
		if ((wasClosed != null) && ((segment->getSummary() == true) || (segment->getPurged() == true))) {
			(*wasClosed) = true;
		}
		boolean retrySegment = (openSegment == true) && ((segment->getSummary() == true) || (segment->getPurged() == true));

		// XXX: primaries call on to secondaries for locking, however ::primaryInformation does this in reverse
		if ((forceSegmentLock == false) && (m_valueCompressMode == false) && (retrySegment == false)) {
			segment->decref();
			return null;
		}

		if (forceSetupSegment(ctxt, segment, fill /* physical */, false /* values */, forceSegmentLock) == false) {

			// XXX: should only be here if value compression is turned on (see force early exit above)
			if (forceSegmentLock == false) {
				return null;
			}

			segment = null;
			goto RETRY;
		}

		// XXX: safe context lock: multiple readers / no writer on the branch tree
		#if 1
		if (m_threadContext.tryReadLock() == false) {
			if (forceContextLock == true) {
				m_threadContext.readLock();

			} else {
				segment->unlock();
				return null;
			}
		}
		#else
		m_threadContext.readLock();
		#endif
		{
			const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::floorEntry(key);
			if ((index != null) && (segment != index->getValue())) {
				m_threadContext.readUnlock();
				segment->unlock();
				segment = null;
				goto RETRY;

			} else if (index == null) {
				segment->unlock();
				segment = null;
			}
		}
		m_threadContext.readUnlock();
	}

	if ((segment == null) && (create == true)) {
		// XXX: safe context lock: one writer / no readers on the branch tree
		m_threadContext.writeLock();
		{
			const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::floorEntry(key);
			if (index == null) {
				segment = initSegment(ctxt, key);
			}
		}
		m_threadContext.writeUnlock();

		if (segment == null) {
			goto RETRY;
		}
	}

	return segment;
}

template<typename K>
Segment<K>* RealTimeMap<K>::scanSegment(ThreadContext<K>* ctxt, const K key, boolean values) {
	const MapEntry<K,Segment<K>*>* entry = scanEntry(ctxt, key, values);
	
	return (entry != null) ? entry->getValue() : null;
}

template<typename K>
Segment<K>* RealTimeMap<K>::getNextSegment(ThreadContext<K>* ctxt, const K key, boolean values) {

	RETRY:
	// XXX: entry needs to be re-gotten on any retry below (e.g. deleting segment)
	const MapEntry<K,Segment<K>*>* entry = scanEntry(ctxt, key, values);
	Segment<K>* segment = (entry != null) ? entry->getValue() : null;

	if ((segment == null) || (m_comparator->compare(segment->SegTreeMap::lastKey(), key) <= 0)) {

		// XXX: safe context lock: multiple readers / no writer on the branch tree
		m_threadContext.readLock();
		{
			// XXX: entry is only valid while segment is locked (see segment unlock below)
			K curkey = (entry != null) ? entry->getKey() : key;

			const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::higherEntry(curkey);

			if (segment != null) {
				segment->unlock();
			}

			if (index != null) {
				segment = index->getValue();

				if (trySetupSegment(ctxt, segment) == true) {
					m_threadContext.readUnlock();
					return segment;
				}

			} else if (segment != null) {
				segment->incref();
			}
		}
		m_threadContext.readUnlock();

		if (segment != null) {
			if (forceSetupSegment(ctxt, segment, true /* physical */, values) == false) {
				segment = null;
				goto RETRY;
			}
		}
	}

	return segment;
}

template<typename K>
Segment<K>* RealTimeMap<K>::getPreviousSegment(ThreadContext<K>* ctxt, const K key, boolean values) {

	RETRY:
	// XXX: entry needs to be re-gotten on any retry below (e.g. deleting segment)
	const MapEntry<K,Segment<K>*>* entry = scanEntry(ctxt, key, values);
	Segment<K>* segment = (entry != null) ? entry->getValue() : null;

	if ((segment != null) && (m_comparator->compare(segment->SegTreeMap::firstKey(), key) == 0)) {

		// XXX: safe context lock: multiple readers / no writer on the branch tree
		m_threadContext.readLock();
		{
			// XXX: entry is only valid while segment is locked (see segment unlock below)
			K curkey = (entry != null) ? entry->getKey() : key;

			const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::lowerEntry(curkey);

			if (segment != null) {
				segment->unlock();
			}

			if (index != null) {
				segment = index->getValue();

				if (trySetupSegment(ctxt, segment) == true) {
					m_threadContext.readUnlock();
					return segment;
				}

			} else if (segment != null) {
				segment->incref();
			}
		}
		m_threadContext.readUnlock();

		if (segment != null) {
			if (forceSetupSegment(ctxt, segment, true /* physical */, values) == false) {
				segment = null;
				goto RETRY;
			}
		}
	}

	return segment;
}

template<typename K>
Segment<K>* RealTimeMap<K>::lastSegment(ThreadContext<K>* ctxt, const K key, boolean create) {

	Segment<K>* segment = null;

	RETRY:
	// XXX: safe context lock: multiple readers / no writer on the branch tree
	m_threadContext.readLock();
	{
		const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::lastEntry();
		if (index != null) {
			segment = index->getValue();

			if (trySetupSegment(ctxt, segment) == true) {
				m_threadContext.readUnlock();
				return segment;
			}
		}
	}
	m_threadContext.readUnlock();

	if (segment != null) {
		if (forceSetupSegment(ctxt, segment, true /* physical */, false /* values */) == false) {
			segment = null;
			goto RETRY;
		}

		// XXX: safe context lock: multiple readers / no writer on the branch tree
		m_threadContext.readLock();
		{
			const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::lastEntry();
			if ((index != null) && (segment != index->getValue())) {
				m_threadContext.readUnlock();
				segment->unlock();
				segment = null;
				goto RETRY;

			} else if (index == null) {
				segment->unlock();
				segment = null;
			}
		}
		m_threadContext.readUnlock();
	}

	if ((segment == null) && (create == true)) {
		// XXX: safe context lock: one writer / no readers on the branch tree
		m_threadContext.writeLock();
		{
			const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::lastEntry();
			if (index == null) {
				segment = initSegment(ctxt, key);
			}
		}
		m_threadContext.writeUnlock();

		if (segment == null) {
			goto RETRY;
		}
	}

	return segment;
}

template<typename K>
const MapEntry<K,Segment<K>*>* RealTimeMap<K>::scanEntry(ThreadContext<K>* ctxt, const K key, boolean values) {

	const MapEntry<K,Segment<K>*>* entry = null;

	RETRY:
	// XXX: safe context lock: multiple readers / no writer on the branch tree
	m_threadContext.readLock();
	{
		entry = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::floorEntry(key);
		if (entry != null) {
			if (trySetupSegment(ctxt, entry->getValue()) == true) {
				m_threadContext.readUnlock();
				return entry;
			}
		}
	}
	m_threadContext.readUnlock();

	if (entry != null) {
		Segment<K>* segment = entry->getValue();

		if (forceSetupSegment(ctxt, segment, true /* physical */, values) == false) {
			entry = null;
			goto RETRY;
		}

		// XXX: safe context lock: multiple readers / no writer on the branch tree
		m_threadContext.readLock();
		{
			entry = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::floorEntry(key);
			if ((entry != null) && (segment != entry->getValue())) {
				m_threadContext.readUnlock();
				segment->unlock();
				entry = null;
				goto RETRY;

			} else if (entry == null) {
				segment->unlock();
			}
		}
		m_threadContext.readUnlock();
	}

	return entry;
}

template<typename K>
boolean RealTimeMap<K>::first(ThreadContext<K>* ctxt, nbyte* value, K* retkey, boolean* again, LockOption lock) {
	boolean result = false;

	RETRY:
	Segment<K>* segment = firstSegment(ctxt, (K) Converter<K>::NULL_VALUE, false);
	if (segment != null) {
		const SegMapEntry* infoEntry = segment->SegTreeMap::firstEntry();
		if (infoEntry != null) {
			Information* info = infoEntry->getValue();

			typename RealTimeCondition<K>::ConditionResult condition = checkCondition(ctxt, infoEntry->getKey());

			if (condition == RealTimeCondition<K>::CONDITION_OK) {
				ErrorCode code = ERR_SUCCESS;
				InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
				if (checkIsolateLock(ctxt, infoRef, info, &code, again, lock) == true) {
					goto RETRY;

				} else if (code == ERR_NOT_FOUND) {
					*again = true;

				} else if (code != ERR_SUCCESS) {
					return false;
				}

				m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

				if (*again == false) {

					if (value != null /* null for key only */) {

						InfoRef infoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info);
						info = setupResult(ctxt, infoRef, value, lock);
						if (info == null) {
							segment->unlock();

							Thread::sleep((rand() % 10) + 50);

							goto RETRY;
						}
					}

					// XXX: stash away for primary/secondary storyline re-stitching
					if (lock != LOCK_NONE) {
						ctxt->setInformation(null);
						ctxt->setInformation(new (ctxt->getInfoRefBuf()) InfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info));
					}
				}

				result = true;

			} else if (condition == RealTimeCondition<K>::CONDITION_NEXT) {
				m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

				*again = true;

				result = true;
			}
		}

		segment->unlock();
	}

	return result;
}

template<typename K>
boolean RealTimeMap<K>::get(ThreadContext<K>* ctxt, const K key, nbyte* value, K* retkey, boolean* again, LockOption lock) {
	boolean result = false;

	RETRY:
	Segment<K>* segment = (value == null) ? getSegment(ctxt, key, false, true) : scanSegment(ctxt, key, true);
	if (segment != null) {
		const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(key);
		if (infoEntry != null) {
			Information* info = infoEntry->getValue();

			typename RealTimeCondition<K>::ConditionResult condition = checkCondition(ctxt, infoEntry->getKey());

			if (condition == RealTimeCondition<K>::CONDITION_OK) {
				ErrorCode code = ERR_SUCCESS;
				InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
				if (checkIsolateLock(ctxt, infoRef, info, &code, again, lock) == true) {
					goto RETRY;

				} else if (code == ERR_NOT_FOUND) {
					*again = false;

				} else if (code != ERR_SUCCESS) {
					return false;
				}

				if (code == ERR_SUCCESS) {
					m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

					if (*again == false) {

						if (m_state == MAP_RECOVER) {
							Transaction* tx = ctxt->getTransaction();
							tx->setLastFileIndex(info->getFileIndex());
						} 	
					
						if (value != null /* null for key only */) {

							InfoRef infoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info);
							info = setupResult(ctxt, infoRef, value, lock);
							if (info == null) {
								segment->unlock();

								Thread::sleep((rand() % 10) + 50);

								goto RETRY;
							}
						}

						// XXX: stash away for primary/secondary storyline re-stitching
						if (lock != LOCK_NONE) {
							ctxt->setInformation(null);
							ctxt->setInformation(new (ctxt->getInfoRefBuf()) InfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info));
						}
					}

					result = true;
				}

			} else if (condition == RealTimeCondition<K>::CONDITION_NEXT) {
				m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

				*again = true;

				result = true;
			}
		}

		segment->unlock();
	}

	return result;
}

template<typename K>
boolean RealTimeMap<K>::getNext(ThreadContext<K>* ctxt, const K key, nbyte* value, K* retkey, boolean* again, LockOption lock, boolean match) {
	boolean result = false;

	RETRY:
	K curkey = key;
	MapInformationEntryIterator* iterator = ctxt->getIterator();
	if (iterator != null) {
		Segment<K>* segment = (Segment<K>*) iterator->getContainer(true);
		if ((segment != null) && (segment->tryLock() == true)) {

			// XXX: check whether this iterator is still in sync with the segment
			if (iterator->getModification() == segment->getModification()) {
				const SegMapEntry* infoEntry = iterator->MapInformationEntryIterator::next();
				if (infoEntry != null) {
					Information* info = infoEntry->getValue();

					if ((match == false) || (m_keyBuilder->isMatch(m_comparator, infoEntry->getKey(), curkey) == true)) {
						typename RealTimeCondition<K>::ConditionResult condition = checkCondition(ctxt, infoEntry->getKey());
						if (condition == RealTimeCondition<K>::CONDITION_OK) {
							ErrorCode code = ERR_SUCCESS;
							InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
							if (checkIsolateLock(ctxt, infoRef, info, &code, again, lock) == true) {
								iterator->resetContainer();
								segment->decref();
								goto RETRY;

							} else if (code == ERR_NOT_FOUND) {
								*again = true;

							} else if (code != ERR_SUCCESS) {
								iterator->resetContainer();
								segment->decref();
								return false;
							}

							m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

							if (match == true) {
								m_keyBuilder->setMatch(curkey, *retkey);
							}

							if (*again == false) {

								if (value != null /* null for key only */) {

									InfoRef infoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info);
									info = setupResult(ctxt, infoRef, value, lock);
									if (info == null) {
										segment->unlock();

										iterator->resetContainer();
										segment->decref();

										Thread::sleep((rand() % 10) + 50);

										goto RETRY;
									}
								}

								// XXX: stash away for primary/secondary storyline re-stitching
								if (lock != LOCK_NONE) {
									ctxt->setInformation(null);
									ctxt->setInformation(new (ctxt->getInfoRefBuf()) InfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info));
								}
							}

							result = true;

						} else if (condition == RealTimeCondition<K>::CONDITION_NEXT) {
							m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

							if (match == true) {
								m_keyBuilder->setMatch(curkey, *retkey);
							}

							*again = true;
							result = true;
						}
	
					} else {
						segment->unlock();

						iterator->resetContainer();
						segment->decref();

						return false;
					}
				}
			}

			segment->unlock();

			if (result == true) {
				return result;
			}

			iterator->resetContainer();
			segment->decref();
		}
	}

	Segment<K>* segment = getNextSegment(ctxt, curkey, (value != null));
	if (segment != null) {
		const SegMapEntry* infoEntry = segment->SegTreeMap::higherEntry(curkey);
		if (infoEntry != null) {
			Information* info = infoEntry->getValue();

			if ((match == false) || (m_keyBuilder->isMatch(m_comparator, infoEntry->getKey(), curkey) == true)) {
				typename RealTimeCondition<K>::ConditionResult condition = checkCondition(ctxt, infoEntry->getKey());
				if (condition == RealTimeCondition<K>::CONDITION_OK) {
					ErrorCode code = ERR_SUCCESS;
					InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
					if (checkIsolateLock(ctxt, infoRef, info, &code, again, lock) == true) {
						goto RETRY;

					} else if (code == ERR_NOT_FOUND) {
						*again = true;

					} else if (code != ERR_SUCCESS) {
						return false;
					}

					m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

					if (match == true) {
						m_keyBuilder->setMatch(curkey, *retkey);
					}

					if (*again == false) {

						if (value != null /* null for key only */) {

							InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
							info = setupResult(ctxt, infoRef, value, lock);
							if (info == null) {
								segment->unlock();

								Thread::sleep((rand() % 10) + 50);

								goto RETRY;
							}
						}

						// XXX: stash away for primary/secondary storyline re-stitching
						if (lock != LOCK_NONE) {
							ctxt->setInformation(null);
							ctxt->setInformation(new (ctxt->getInfoRefBuf()) InfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info));
						}
					}

					result = true;

				} else if (condition == RealTimeCondition<K>::CONDITION_NEXT) {
					m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

					if (match == true) {
						m_keyBuilder->setMatch(curkey, *retkey);
					}

					*again = true;
					result = true;
				}

				if (result == true) {
					if (iterator != null) {
						// XXX: getContainer(true) might have return null, ensure segment is released
						release(iterator);

						segment->iterator(infoEntry->getKey(), iterator, false);
						segment->incref();

						iterator->MapInformationEntryIterator::next();
					}
				}
			}
		}

		segment->unlock();
	}

	return result;
}

template<typename K>
boolean RealTimeMap<K>::getPrevious(ThreadContext<K>* ctxt, const K key, nbyte* value, K* retkey, boolean* again, LockOption lock) {
	boolean result = false;

	RETRY:
	K curkey = key;
	MapInformationEntryIterator* iterator = ctxt->getIterator();
	if (iterator != null) {
		Segment<K>* segment = (Segment<K>*) iterator->getContainer(true);
		if ((segment != null) && (segment->tryLock() == true)) {

			// XXX: check whether this iterator is still in sync with the segment
			if (iterator->getModification() == segment->getModification()) {
				const SegMapEntry* infoEntry = iterator->MapInformationEntryIterator::previous();
				if (infoEntry != null) {
					Information* info = infoEntry->getValue();

					typename RealTimeCondition<K>::ConditionResult condition = checkCondition(ctxt, infoEntry->getKey());
					if (condition == RealTimeCondition<K>::CONDITION_OK) {
						ErrorCode code = ERR_SUCCESS;
						InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
						if (checkIsolateLock(ctxt, infoRef, info, &code, again, lock) == true) {
							iterator->resetContainer();
							segment->decref();
							goto RETRY;

						} else if (code == ERR_NOT_FOUND) {
							*again = true;

						} else if (code != ERR_SUCCESS) {
							iterator->resetContainer();
							segment->decref();
							return false;
						}

						m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

						if (*again == false) {

							if (value != null /* null for key only */) {

								InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
								info = setupResult(ctxt, infoRef, value, lock);
								if (info == null) {
									segment->unlock();

									iterator->resetContainer();
									segment->decref();

									Thread::sleep((rand() % 10) + 50);

									goto RETRY;
								}
							}

							// XXX: stash away for primary/secondary storyline re-stitching
							if (lock != LOCK_NONE) {
								ctxt->setInformation(null);
								ctxt->setInformation(new (ctxt->getInfoRefBuf()) InfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info));
							}
						}

						result = true;

					} else if (condition == RealTimeCondition<K>::CONDITION_NEXT) {
						m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

						*again = true;

						result = true;
					}
				}
			}

			segment->unlock();

			if (result == true) {
				return result;
			}

			iterator->resetContainer();
			segment->decref();
		}
	}

	Segment<K>* segment = getPreviousSegment(ctxt, curkey, (value != null));
	if (segment != null) {
		const SegMapEntry* infoEntry = segment->SegTreeMap::lowerEntry(curkey);
		if (infoEntry != null) {
			Information* info = infoEntry->getValue();

			typename RealTimeCondition<K>::ConditionResult condition = checkCondition(ctxt, infoEntry->getKey());
			if (condition == RealTimeCondition<K>::CONDITION_OK) {
				ErrorCode code = ERR_SUCCESS;
				InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
				if (checkIsolateLock(ctxt, infoRef, info, &code, again, lock) == true) {
					goto RETRY;

				} else if (code == ERR_NOT_FOUND) {
					*again = true;

				} else if (code != ERR_SUCCESS) {
					return false;
				}

				m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

				if (*again == false) {

					if (value != null /* null for key only */) {

						InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
						info = setupResult(ctxt, infoRef, value, lock);
						if (info == null) {
							segment->unlock();

							Thread::sleep((rand() % 10) + 50);

							goto RETRY;
						}
					}

					// XXX: stash away for primary/secondary storyline re-stitching
					if (lock != LOCK_NONE) {
						ctxt->setInformation(null);
						ctxt->setInformation(new (ctxt->getInfoRefBuf()) InfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info));
					}
				}

				result = true;

			} else if (condition == RealTimeCondition<K>::CONDITION_NEXT) {
				m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

				*again = true;

				result = true;
			}

			if (result == true) {
				if (iterator != null) {
					// XXX: getContainer(true) might have return null, ensure segment is released
					release(iterator);

					segment->iterator(infoEntry->getKey(), iterator, false);
					segment->incref();
				}
			}
		}

		segment->unlock();
	}

	return result;
}

template<typename K>
boolean RealTimeMap<K>::last(ThreadContext<K>* ctxt, nbyte* value, K* retkey, boolean* again, LockOption lock) {
	boolean result = false;

	RETRY:
	Segment<K>* segment = lastSegment(ctxt, (K) Converter<K>::NULL_VALUE, false);
	if (segment != null) {
		const SegMapEntry* infoEntry = segment->SegTreeMap::lastEntry();
		if (infoEntry != null) {
			Information* info = infoEntry->getValue();

			typename RealTimeCondition<K>::ConditionResult condition = checkCondition(ctxt, infoEntry->getKey());

			if (condition == RealTimeCondition<K>::CONDITION_OK) {
				ErrorCode code = ERR_SUCCESS;
				InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
				if (checkIsolateLock(ctxt, infoRef, info, &code, again, lock) == true) {
					goto RETRY;

				} else if (code == ERR_NOT_FOUND) {
					*again = true;

				} else if (code != ERR_SUCCESS) {
					return false;
				}

				m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

				if (*again == false) {

					if (value != null /* null for key only */) {

						InfoRef infoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, info);
						info = setupResult(ctxt, infoRef, value, lock);
						if (info == null) {
							segment->unlock();

							Thread::sleep((rand() % 10) + 50);

							goto RETRY;
						}
					}

					// XXX: stash away for primary/secondary storyline re-stitching
					if (lock != LOCK_NONE) {
						ctxt->setInformation(null);
						ctxt->setInformation(new (ctxt->getInfoRefBuf()) InfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, info));
					}
				}

				result = true;

			} else if (condition == RealTimeCondition<K>::CONDITION_NEXT) {
				m_keyBuilder->copyKey(infoEntry->getKey(), retkey);

				*again = true;

				result = true;
			}
		}

		segment->unlock();
	}

	return result;
}

template<typename K>
boolean RealTimeMap<K>::checkCycleLock(Transaction* itx, Transaction*& otx, ushorttype id) {
	boolean deadlock = false;

	itx->setWaitIdentifier(id);

	CXX_LANG_MEMORY_DEBUG_ASSERT(itx);

	otx = Transaction::getTransaction(itx->getWaitIdentifier());
	if (otx != null) {
		Transaction* ntx = otx;
		ntx->incref();

		inttype iter = 0;
		for (; (ntx != null) && (iter < Properties::DEFAULT_TRANSACTION_INFINITE); iter++) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(ntx);

			if (ntx->getWaitIdentifier() == 0) {
				break;

			} else if (ntx->getWaitIdentifier() == itx->getIdentifier()) {
				deadlock = true;
				break;
			}

			Transaction* ptx = ntx;
			ntx = Transaction::getTransaction(ntx->getWaitIdentifier());
			ptx->decref();
		}

		if (ntx != null) {
			ntx->decref();
		}

		if (iter == Properties::DEFAULT_TRANSACTION_INFINITE) {
			deadlock = true;
		}
	}

	if (deadlock == true) {
		itx->setWaitIdentifier(0);
	}

	return deadlock;
}

template<typename K>
boolean RealTimeMap<K>::checkAccessLock(Transaction* itx, InfoRef& orginfoRef, ErrorCode* code, LockOption lock) {
	
	Segment<K>* segment = orginfoRef.segment;
	StoryLine& storyLine = orginfoRef.getStoryLine();
	boolean wait = true;

	storyLine.lock();
	{
		boolean sharing = storyLine.getSharing();
		boolean lockable = Transaction::lockable(storyLine, itx->getIdentifier(), lock == LOCK_READ);

		if (lock == LOCK_READ) {

			if (sharing == false) {

				if ((lockable == true) && (storyLine.getLockIdentifier() != itx->getIdentifier())) {

					if (itx->getReadLockSet()->HashSet<StoryLock*>::remove(storyLine.getStoryLock()) == true) {
						//#ifdef DEEP_READ_STORYLOCK_REFERENCING
						storyLine.getStoryLock()->decref();
						//#endif
					}

					storyLine.setSharing(true);
					storyLine.release();

					// XXX: keep segment from being purged
					segment->setDirty(true);

					sharing = true;
				}
			}

			if (sharing == true) {

				if (itx->getReadLockSet()->HashSet<StoryLock*>::add(storyLine.getStoryLock()) == true) {
					//#ifdef DEEP_READ_STORYLOCK_REFERENCING
					storyLine.getStoryLock()->incref();
					//#endif

					itx->setDirty(true);

					if (storyLine.incrementLockSequence() == 1) {
						storyLine.setLockCredentials(itx->getIdentifier(), storyLine.getLockSequence());
					}
				}

				wait = false;

			} else if (storyLine.getLockIdentifier() == itx->getIdentifier()) {
				wait = false;
			}

		} else /* if ((lock == LOCK_WRITE) || (lock == LOCK_NONE)) */ {

			if (sharing == false) {

				if (lockable == true) {
					storyLine.setLockCredentials(itx->getIdentifier(), itx->getSequence());

					// XXX: keep segment from being purged
					segment->setDirty(true);

					wait = false;
				}

			// XXX: read lock "re-ownership" is lockless (see Transaction::reassign)
			} else if (storyLine.getLockSequence() == 0) {

				if (itx->getReadLockSet()->HashSet<StoryLock*>::remove(storyLine.getStoryLock()) == true) {
					//#ifdef DEEP_READ_STORYLOCK_REFERENCING
					storyLine.getStoryLock()->decref();
					//#endif
				}

				storyLine.setSharing(false);
				storyLine.setLockCredentials(itx->getIdentifier(), itx->getSequence());

				// XXX: keep segment from being purged
				segment->setDirty(true);

				wait = false;

			} else if (storyLine.getLockSequence() == 1) {

				// XXX: promote "read lock" to "write lock" if transaction is the sole owner
				if (storyLine.getLockIdentifier() == itx->getIdentifier()) {

					if (itx->getReadLockSet()->HashSet<StoryLock*>::remove(storyLine.getStoryLock()) == true) {
						//#ifdef DEEP_READ_STORYLOCK_REFERENCING
						storyLine.getStoryLock()->decref();
						//#endif
					}

					storyLine.setSharing(false);
					storyLine.setLockCredentials(itx->getIdentifier(), itx->getSequence());

					// XXX: keep segment from being purged
					segment->setDirty(true);

					wait = false;

				// XXX: read lock "dead-ownership" is lockless (see Transaction::reassign)
				} else if (Transaction::getTransaction(storyLine.getLockIdentifier(), false /* no ref */) == null) {

					if (itx->getReadLockSet()->HashSet<StoryLock*>::remove(storyLine.getStoryLock()) == true) {
						//#ifdef DEEP_READ_STORYLOCK_REFERENCING
						storyLine.getStoryLock()->decref();
						//#endif
					}

					storyLine.setSharing(false);
					storyLine.setLockCredentials(itx->getIdentifier(), itx->getSequence());

					// XXX: keep segment from being purged
					segment->setDirty(true);

					wait = false;
				}
			}
		}
	}
	storyLine.unlock();

	// XXX: information has been successfully acquired for either READ or WRITE lock
	if (wait == false) {
		return false;
	}

	Transaction* otx = null;

	if (checkCycleLock(itx, otx, storyLine.getLockIdentifier()) == true) {
		segment->unlock();

		if (itx->getRetry() > Properties::DEFAULT_TRANSACTION_RETRY) {
			m_threadContext.setErrorCode(ERR_DEADLOCK);
			*code = ERR_DEADLOCK;

			itx->release();
			if (otx != null) otx->decref();
			return false;

		} else {
			Thread::sleep((rand() % 10) + 50);

			itx->setRetry(itx->getRetry() + 1);
			if (otx != null) otx->decref();
			return true;
		}

	} else if (otx == null) {
		segment->unlock();

		itx->release();
		return true;
	}

	otx->lock();
	{
		// XXX: ensure lock has not changed hands after cycle lock check
		if (storyLine.getLockIdentifier() != otx->getIdentifier()) {
			segment->unlock();

			itx->release();
			otx->unlock();
			otx->decref();
			return true;
		}

		otx->setWaitFlag(true);

		segment->unlock();

		const longtype timeout = (itx->getRoll() == true) ? 10 : (longtype) Properties::getTransactionTimeout();
		if (otx->wait(timeout) == false) {
			m_threadContext.setErrorCode(ERR_TIMEOUT);
			*code = ERR_TIMEOUT;

			itx->release();
			otx->decref();
			return false;
		}
	}
	otx->unlock();

	itx->release();
	otx->decref();
	return true;
}

template<typename K>
boolean RealTimeMap<K>::checkIsolateLock(ThreadContext<K>* ctxt, InfoRef& infoRef, Information*& info, ErrorCode* code, boolean* again, LockOption lock) {

	Segment<K>* segment = infoRef.segment;
	const K key = infoRef.infoEntry->getKey();
	StoryLine& storyLine = infoRef.getStoryLine();

	// XXX: identify the origin
	Information* orginfo = info;

	Transaction* itx = ctxt->getTransaction();
	if (itx == null) {
		// XXX: due to internal requests (i.e. seedValueAverage) without transactions and compression (i.e. artificial storyline)
		info = isolateInformation(ctxt, info, Information::LEVEL_COMMIT);
		return false;
	}

	// XXX: needs retry to wait for roll (no row locks held on segment; priority given to roll)
	if ((lock != LOCK_ROLL) && (segment->getRolling() == true) && (segment->hasRowsLockedBy(itx->getIdentifier(), itx->getSequence()) == false)) {
		segment->unlock();
		if (itx->getRetry() > Properties::DEFAULT_TRANSACTION_RETRY) {
			m_threadContext.setErrorCode(ERR_DEADLOCK);
			*code = ERR_DEADLOCK;

			itx->release();
			return false;

		} else {
			Thread::sleep((rand() % 10) + 50);

			itx->setRetry(itx->getRetry() + 1);
			return true;
		}
	}

	if (lock == LOCK_ROLL) {
		lock = LOCK_WRITE;
	}

	if ((lock == LOCK_NONE) && (itx->getIsolation() == Transaction::SERIALIZABLE)) {
		lock = LOCK_READ;
	}

	if (lock != LOCK_NONE) {
		if (checkAccessLock(itx, infoRef, code, lock) == true) {
			return true;

		} else if (*code != ERR_SUCCESS) {
			return false;
		}
	}

	#ifdef DEEP_DEBUG
	boolean testStoryLock = true;
	if (lock == LOCK_NONE) {
		testStoryLock = false;
	}
	#endif

	// XXX: isolate to the current set, to be assigned below (i.e. Information*&);
	#ifdef DEEP_DEBUG
	Information* topinfo = isolateInformation(ctxt, storyLine, orginfo, Information::LEVEL_COMMIT, itx->getViewpoint(), itx->getIsolation(), itx->getIdentifier(), key, testStoryLock);
	#else
	Information* topinfo = isolateInformation(ctxt, storyLine, orginfo, Information::LEVEL_COMMIT, itx->getViewpoint(), itx->getIsolation(), itx->getIdentifier(), key);
	#endif

	// XXX: isolate to a transactional mode
	if (topinfo != null) {

		// TODO: lock to ensure correct storyline consistency (i.e. walking level active)
		if ((itx->getIsolation() == Transaction::UNCOMMITTED) && (topinfo->getNext() != null)) {
			#ifdef DEEP_DEBUG
			topinfo = isolateInformation(ctxt, topinfo->getNext(), Information::LEVEL_ACTIVE, testStoryLock);
			#else
			topinfo = isolateInformation(ctxt, topinfo->getNext(), Information::LEVEL_ACTIVE);
			#endif
		}

		// XXX: isolate to a repeatable set, to be assigned below (i.e. Information*&);
		if (topinfo->getDeleting() == true) {

			// XXX: owning the information needs to go through the storyboard below
			if (storyLine.getLockIdentifier() != itx->getIdentifier()) {

				if (itx->getIsolation() == Transaction::REPEATABLE /* || SNAPSHOT */) {
					// XXX: don't forget the assignment (i.e. Information*&);
					#ifdef DEEP_DEBUG
					info = isolateInformation(ctxt, orginfo, itx->getViewpoint(), key, testStoryLock);
					#else
					info = isolateInformation(ctxt, orginfo, itx->getViewpoint(), key);
					#endif
					if (info == null) {
						*code = ERR_NOT_FOUND;
						*again = true;
					}

				} else {
					*code = ERR_NOT_FOUND;
					*again = true;
				}

				return false;
			}

		} else if (topinfo->getCreating() == true) /* i.e. read-uncommitted */ {
			*again = (itx->getIsolation() != Transaction::UNCOMMITTED);
		}

		// XXX: don't forget the assignment (i.e. Information*&);
		info = topinfo;

		if (storyLine.getLockIdentifier() == itx->getIdentifier()) {
			boolean ignore = false;

			if (info->getNext() != null) {
				info = isolateInformation(ctxt, info, true);

				if (m_primaryIndex != null) {

					// XXX: can't modify actual branch segment's first key state
					K cloneKey = m_keyBuilder->cloneKey(key);
					{
						m_keyBuilder->setIgnorePrimary(cloneKey, true);

						ignore = isolateInformation(ctxt, info, cloneKey);
					}
					Converter<K>::destroy(cloneKey);
				}
			}

			if ((ignore == true) || (info->getLevel() < Information::LEVEL_COMMIT)) {
				*code = ERR_NOT_FOUND;

			} else if (info->getDeleting() == true) {
				*code = ERR_NOT_FOUND;

			} else {
				*again = false;
			}

		} else if (*again == false) {

			if ((itx->getIsolation() == Transaction::REPEATABLE /* || SNAPSHOT */) && (info->getViewpoint() > itx->getViewpoint())) {
				#ifdef DEEP_DEBUG
				info = isolateInformation(ctxt, orginfo, itx->getViewpoint(), key, testStoryLock);
				#else
				info = isolateInformation(ctxt, orginfo, itx->getViewpoint(), key);
				#endif
				if (info == null) {
					*code = ERR_NOT_FOUND;
					*again = true;
				}

			} else if ((itx->getIsolation() < Transaction::UNCOMMITTED) && (info->getLevel() != Information::LEVEL_COMMIT)) {
				*code = ERR_NOT_FOUND;
				*again = true;
			}
		}

	} else {
		*code = ERR_NOT_FOUND;
		*again = true;
	}

	return false;
}

template<typename K>
void RealTimeMap<K>::updateReservationWatermark(const K key) {

	if (m_hasReservation == true) {
		if (m_keyBuilder->getReservedKey(key) > m_incrementCount) {
			m_reserveLock.lock();
			{
				ulongtype count = m_keyBuilder->getReservedKey(key);
				if (count > m_incrementCount) {
					m_incrementCount = count;

					// TODO: fix cast
					const SchemaBuilder<K>* schemaBuilder = ((m_primaryIndex == null) ? m_schemaBuilder : ((RealTimeMap<K>*) m_primaryIndex)->m_schemaBuilder);
					((SchemaBuilder<K>*) schemaBuilder)->setAutoIncrementValue(m_incrementCount + 1);
				}
			}
			m_reserveLock.unlock();
		}
	}
}

template<typename K>
void RealTimeMap<K>::copyValueFromInformation(const Information* info, nbyte* value) {

	if (((uinttype) value->length) != info->getSize() /* 24-bit unsigned int */) {
		value->realloc(info->getSize());
	}

	memcpy(*value, info->getData(), info->getSize());
}

template<typename K>
void RealTimeMap<K>::copyValueIntoInformation(Information* info, const nbyte* value) {

	if (info->getData() != null) {
		if (((uinttype) value->length) > info->getSize() /* 24-bit unsigned int */) {
			info->setData((bytearray) realloc(info->getData(), value->length));
		}

		memcpy(info->getData(), *value, value->length);

	} else {
		bytearray data = (bytearray) malloc(value->length);
		memcpy(data, (voidarray) *value, value->length);
		info->setData(data);
	}

	info->setSize(value->length /* 24-bit unsigned int */);
}

template<typename K>
void RealTimeMap<K>::copyValueFromInformation(ThreadContext<K>* ctxt, Information* info, const K key) {

	if (info->getData() == null) {
		readValue(ctxt, info, key);

	} else {
		bytearray data = info->getData();
		info->initData();
		memcpy(info->getData(), data, info->getSize());
	}
}

template<typename K>
void RealTimeMap<K>::failedSegment(Segment<K>* segment, const K key) {

	// XXX: might have been created with value being null (see: getSegment)
	if (segment->SegTreeMap::get(key) == null) {
		deleteInformation(null, segment, key, false);
	}
}

template<typename K>
void RealTimeMap<K>::failedConductor(RealTimeConductor<K>* conductor, const Information* topinfo) {
	SegMapEntry* infoEntry = conductor->get(conductor->size() - 1);

	#ifdef DEEP_DEBUG
	if (topinfo == infoEntry->getValue()) {
		DEEP_LOG(ERROR, OTHER, "Invalid conductor state: no begin found, %s\n", getFilePath());

		throw InvalidException("Invalid conductor state: no begin found");
	}
	#endif

	if ((topinfo != null) && (topinfo->getNext() == infoEntry->getValue())) {
		((Information*) topinfo)->setNext(null);
	}

	infoEntry->getValue()->setDeleting(true);

	conductor->remove(conductor->size() - 1);

	/* XXX: keys are shared and not owned by the conductor
	Converter<K>::destroy(infoEntry->getKey());
	*/

	Converter<Information*>::destroy(infoEntry->getValue());
	delete infoEntry;
}

template<typename K>
void RealTimeMap<K>::mergeSegments(Segment<K>* segment, Segment<K>* nSegment) {

	#ifdef DEEP_DEBUG
	if ((segment->getRolling() == true) || (nSegment->getRolling() == true)) {
		DEEP_LOG(ERROR, OTHER, "Invalid merge: rolling segment, %s\n", getFilePath());
		throw InvalidException("Invalid merge: rolling segment");
	}
	#endif

	#if 0
	#ifdef DEEP_DEBUG
	if ((segment->getBeenSurceased() == true) && (nSegment->getBeenSurceased() == true)) {
		DEEP_LOG(ERROR, OTHER, "Invalid merge: only surceased segments, %s\n", getFilePath());
		throw InvalidException("Invalid merge: only surceased segments");
	}
	#endif
	#endif

	segment->setBeenSurceased(false);
	nSegment->setBeenSurceased(false);

	// PERF (TODO): move merging execution outside of m_threadContext.writeLock
	{
		typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

		nSegment->SegTreeMap::entrySet(&stackSegmentItemSet);
		MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
		while (infoIter->MapInformationEntrySetIterator::hasNext()) {
			SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();
			K key = infoEntry->getKey();
			Information* value = infoEntry->getValue();
			infoEntry->setValueIgnoreRooted(null, m_indexValue /* ctx */);
			SegMapEntry* minfoEntry = null;
			segment->SegTreeMap::add(key, value, &minfoEntry);
			minfoEntry->getStoryLine().setStoryLock(infoEntry->getStoryLine().getStoryLock());
			#ifdef DEEP_DEBUG
			value->decRooted();
			#endif
		}
	}

	#ifdef DEEP_DEBUG
	if (m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::remove(nSegment->SegTreeMap::firstKey()) != nSegment) {
		DEEP_LOG(ERROR, OTHER, "Invalid merge: segment not found, %s\n", getFilePath());

		throw InvalidException("Invalid merge: segment not found");
	}
	#else
	m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::remove(nSegment->SegTreeMap::firstKey());
	#endif

	if ((segment->getValuesCompressed() == true) || (segment->getBeenRelocated() == true) || (nSegment->getValuesCompressed() == true) || (nSegment->getBeenRelocated() == true)) {
		// XXX: ensure values are relocated at lease on shutdown
		segment->setStreamPosition(0);
		segment->setBeenRelocated(true);
		segment->setFragmentedValue(true);
	}

	segment->setFragmentedValue(segment->getFragmentedValue() || nSegment->getFragmentedValue());

	// XXX: set state on segment for completeness, however rebuild flag drives it below
	segment->setFragmentedKey(true);
	segment->setBeenAltered(true);
	segment->setDirty(true);

	nSegment->setBeenDeleted(true);
	nSegment->setModification(0);
	nSegment->setMapContext(-1);

	m_orderSegmentList.lock();
	if ((m_orderSegmentMode != MODE_INDEX) && (m_orderSegmentList.contains(segment, false /* lock */) == true)) {
		m_orderSegmentList.remove(nSegment, false /* lock */);
	}
	m_orderSegmentList.unlock();

	nSegment->unlock();

	m_deletedSegmentList.lock();
	{
		m_deletedSegmentList.ArrayList<Segment<K>*>::add(nSegment);
	}
	m_deletedSegmentList.unlock();
}

template<typename K>
Segment<K>* RealTimeMap<K>::splitSegment(ThreadContext<K>* ctxt, Segment<K>* segment, K* firstKey, boolean lock, boolean recover) {

	#ifdef DEEP_DEBUG
	if (segment->getRolling() == true) {
		DEEP_LOG(ERROR, OTHER, "Invalid split: rolling segment, %s\n", getFilePath());
		throw InvalidException("Invalid split: rolling segment");
	}
	#endif

	if ((segment->getValuesCompressed() == true) && (segment->getBeenFilled() == false)) {
		readValue(ctxt, segment);

		segment->setBeenFilled(true);
	}

	const boolean streamRangeOverflow = segment->getStreamRangeOverflow();
	if (streamRangeOverflow == true) {
		segment->resetStreamIndexes();
	}

	Segment<K>* pSegment = new Segment<K>(m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER, Versions::GET_PROTOCOL_CURRENT(), m_keyBuilder->getKeyParts());
	pSegment->setMapContext(m_indexValue);
	if (lock == true) {
		pSegment->lock();
	}

	// PERF: ensure splitting is execution outside of m_threadContext.writeLock
	{
		inttype middle = (segment->SegTreeMap::size() + 1) / 2;

		typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

		segment->SegTreeMap::entrySet(&stackSegmentItemSet);
		MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
		for (int i = 0; infoIter->MapInformationEntrySetIterator::hasNext(); i++) {
			SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();
			Information* curinfo = null;
			if (streamRangeOverflow == true) {
				// XXX: isolate to latest on disk
				curinfo = isolateInformationLocality(ctxt, infoEntry->getValue());
				#if 0
				// TODO: checkpoint indexing may cause stream range overflow in running system
				#ifdef DEEP_DEBUG
				if ((curinfo == null) || ((curinfo->getLevel() != Information::LEVEL_COMMIT) && (curinfo->getFilePosition() == 0))) {
					DEEP_LOG(ERROR, OTHER, "Invalid info state, Information not on disk\n");
					throw InvalidException("Invalid info state, Information not on disk");
				}
				#endif
				#else
				if ((curinfo != null) && (curinfo->getLevel() != Information::LEVEL_COMMIT) && (curinfo->getFilePosition() == 0)) {
					curinfo = null;
				}
				#endif
			}

			if (i >= middle) {
				K key = infoEntry->getKey();
				Information* value = infoEntry->getValue();
				StoryLock* slock = infoEntry->getStoryLine().getStoryLock();
				infoEntry->setValueIgnoreRooted(null, m_indexValue /* ctx */);

				SegMapEntry* pinfoEntry = null;
				pSegment->SegTreeMap::add(key, value, &pinfoEntry);
				pinfoEntry->getStoryLine().setStoryLock(slock);
				infoIter->MapInformationEntrySetIterator::remove();

				#ifdef DEEP_DEBUG
				value->decRooted();
				#endif
				if ((streamRangeOverflow == true) && (curinfo != null)) {
					pSegment->addStreamIndex(curinfo->getFileIndex());
				}

			} else if ((streamRangeOverflow == true) && (curinfo != null)) {
				segment->addStreamIndex(curinfo->getFileIndex());
			}
		}
	}
	// XXX: for proper dead count statistics
	pSegment->addPagingIndexes(segment->getPagingIndexes());

	// XXX: ensure values are relocated at lease on shutdown
	if ((segment->getValuesCompressed() == true) || (segment->getBeenRelocated() == true)) {

		if (recover == false) {
			segment->setStreamPosition(0);
			segment->setBeenRelocated(true);
			segment->setFragmentedValue(true);
		}

		pSegment->setBeenRelocated(true);
	}

	pSegment->setFragmentedKey(true);
	pSegment->setBeenAltered(true);
	pSegment->setDirty(true);

	segment->setFragmentedKey(true);
	segment->setBeenAltered(true);
	segment->setDirty(true);

	*firstKey = pSegment->SegTreeMap::firstKey();

	// XXX: safe context lock: one writer / no readers on the branch tree
	if (recover == false) m_threadContext.writeLock();
	{
		pSegment->setFragmentedValue(segment->getFragmentedValue());

		if ((streamRangeOverflow == false) && (recover == false)) {
			pSegment->addStreamReferences(segment->getStreamReferences());
		}

		m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(*firstKey, pSegment);
	}
	if (recover == false) m_threadContext.writeUnlock();

	return pSegment;
}

template<typename K>
void RealTimeMap<K>::reserveSegment(Conductor* conductor, ContextHandle<K>& handle, K key) {
	Segment<K>* segment = handle.getSegment();

	// XXX: create new segment for first reserved key (if segment not already new)
	if ((m_hasReservation == true) && (segment->getBeenCreated() == false) && (m_keyBuilder->getReservedKey(key) == conductor->getReservedKey())) {

		if (conductor->getReservedBlock() > (ulongtype) getSegmentMinimumSize(false /* adaptive */)) {
			// XXX: safe context lock: one writer / no readers on the branch tree
			m_threadContext.writeLock();
			{
				// XXX: due to above's mininum size reserve block check, ensure incoming request is still valid
				inttype compare = m_comparator->compare(key, segment->SegTreeMap::lastKey());
				if (compare >= 0) {
					segment = new Segment<K>(m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER, Versions::GET_PROTOCOL_CURRENT(), m_keyBuilder->getKeyParts());
					segment->setMapContext(m_indexValue);
					segment->lock();

					m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(key, segment);

					handle.assign(segment);
				}
			}
			m_threadContext.writeUnlock();
		}
	}
}

template<typename K>
K RealTimeMap<K>::rekeySegment(Segment<K>* segment, K newKey, boolean destroy, boolean lock) {

	const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(newKey);

	K oldKey = infoEntry->getKey();
	K copyKey = m_keyBuilder->cloneKey(newKey);
	K firstKey = segment->SegTreeMap::firstKey();

	((SegMapEntry*) infoEntry)->setKey(copyKey, m_indexValue /* ctx */);

	if (firstKey == oldKey) {

		// XXX: safe context lock: one writer / no readers on the branch tree
		if (lock == true) m_threadContext.writeLock();
		{
			m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(copyKey, segment);
		}
		if (lock == true) m_threadContext.writeUnlock();
	}

	Converter<K>::destroy(oldKey);

	return copyKey;
}

template<typename K>
void RealTimeMap<K>::reseedSegment(Segment<K>* segment, K oldKey, boolean destroy, boolean lock) {

	K firstKey = segment->SegTreeMap::firstKey();

	// XXX: safe context lock: one writer / no readers on the branch tree
	if (lock == true) m_threadContext.writeLock();
	{
		m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::remove(oldKey);
		m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(firstKey, segment);
	}
	if (lock == true) m_threadContext.writeUnlock();

	if (destroy == true) {
		Converter<K>::destroy(oldKey);
	}

	segment->setBeenReseeded(true);
}

template<typename K>
void RealTimeMap<K>::deleteSegment(Segment<K>* segment, K firstKey, boolean destroy, boolean lock, boolean remove) {

	// XXX: safe context lock: one writer / no readers on the branch tree
	if (lock == true) m_threadContext.writeLock();
	{
		if (remove == true) {
			#ifdef DEEP_DEBUG
			if (m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::remove(firstKey) != segment) {
				DEEP_LOG(ERROR, OTHER, "Invalid delete: segment not found, %s\n", getFilePath());

				throw InvalidException("Invalid delete: segment not found");
			}
			#else
			m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::remove(firstKey);
			#endif
		}

		segment->setBeenDeleted(true);
		segment->setModification(0);

		if (destroy == true) {
			Converter<K>::destroy(firstKey);
		}

		m_deletedSegmentList.lock();
		{
			m_deletedSegmentList.ArrayList<Segment<K>*>::add(segment);
		}
		m_deletedSegmentList.unlock();
	}
	if (lock == true) m_threadContext.writeUnlock();
}

template<typename K>
ulongtype RealTimeMap<K>::getActiveSize(ThreadContext<K>* ctxt, Segment<K>* segment) {
	ulongtype count = 0L;

	typedef typename SegTreeMap::TreeMapEntrySet::EntrySetIterator MapInformationEntrySetIterator;
	typename SegTreeMap::TreeMapEntrySet segmentInfoSet(true);
	segment->SegTreeMap::entrySet(&segmentInfoSet);
	MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) segmentInfoSet.iterator();

	for (int i = 0; infoIter->MapInformationEntrySetIterator::hasNext(); ++i) {
		SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();

		K key = infoEntry->getKey();
		Information* info = infoEntry->getValue();

		Information* endinfo = isolateInformation(ctxt, info, Information::LEVEL_COMMIT, key);
		if ((endinfo != null) && (endinfo->getDeleting() == false)) {
			++count;
		}
	}

	return count;
}

template<typename K>
typename RealTimeTypes<K>::SegMapEntry* RealTimeMap<K>::updateInformation(ThreadContext<K>* ctxt, Segment<K>* segment, K key, Information* info, boolean enableCardinality) {

	// XXX: note the importance of this logic throughout key space allocation
	updateReservationWatermark(key);

	K retkey = key;
	
	boolean cardinalityBefore = segment->getCardinalityEnabled();
	segment->setCardinalityEnabled(enableCardinality);

	SegMapEntry* retEntry = null;
	boolean existing = false;
	Information* oldinfo = segment->SegTreeMap::put(key, info, &retkey, &existing, &retEntry);

	#ifdef DEEP_DEBUG
	if ((enableCardinality == true) && (oldinfo != null)) {
		if (ctxt->getTransaction()->getConductor(getIdentifier())->getUniqueChecks() == true) {
			DEEP_LOG(ERROR, OTHER, "Update Information: unexpected duplicate key, %s\n", getFilePath());

			throw InvalidException("Update Information: unexpected duplicate key");

		} else {
			DEEP_LOG(WARN, OTHER, "Update Information: duplicate key with unique checks off, %s\n", getFilePath());
		}
	}
	#endif

	segment->setCardinalityEnabled(cardinalityBefore);

	if ((oldinfo != null) && (oldinfo->getLevel() == Information::LEVEL_COMMIT)) {

		// XXX: non-primitive first keys are shared across branch and segment trees
		if (m_keyBuilder->isPrimitive() == false) {
			K firstKey = segment->SegTreeMap::firstKey();

			// XXX: only need to update transactionally committed segment first keys
			if (firstKey == key) {

				// XXX: safe context lock: one writer / no readers on the branch tree
				m_threadContext.writeLock();
				{
					m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::remove(retkey);
					m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(firstKey, segment);
				}
				m_threadContext.writeUnlock();
			}

			Converter<K>::destroy(retkey);
		}

		Converter<Information*>::destroy(oldinfo);
	}

	#ifdef DEEP_DEBUG
	if ((segment->SegTreeMap::size() > Properties::DEFAULT_SEGMENT_MAXIMUM_SIZE) || (segment->vsize() > Properties::DEFAULT_SEGMENT_MAXIMUM_SIZE)) {
		DEEP_LOG(ERROR, OTHER, "Invalid capacity: segment size: %d / %d, %s\n", segment->SegTreeMap::size(), segment->vsize(), getFilePath());

		throw InvalidException("Invalid capacity: segment size");
	}
	#endif

	// XXX: the first key is allocated in ::getSegment
	if (segment->getBeenCreated() == true) {
		segment->setBeenCreated(false);
	}

	return retEntry;
}

template<typename K>
typename RealTimeTypes<K>::SegMapEntry* RealTimeMap<K>::addInformation(ThreadContext<K>* ctxt, ContextHandle<K>& handle, K key, Information* info, boolean enableCardinality) {

	boolean split = false;
	Segment<K>* segment = handle.getSegment();

	if ((segment->getVirtual() == true) && (RealTimeAdaptive_v1<K>::virtualMaximum(this, segment) == true)) {

		// XXX: virtual state implies the segment is purged
		fillSegment(ctxt, segment, segment->getValuesCompressed() /* values */, RealTimeAdaptive_v1<K>::doPace(this, ctxt->getTransaction()));

		split = true;

	} else if ((segment->getVirtual() == false) && (RealTimeAdaptive_v1<K>::physicalMaximum(this, segment) == true)) {

		#ifdef DEEP_DEBUG
		if (segment->getPurged() == true) {
			DEEP_LOG(ERROR, OTHER, "Invalid segment state in addInformation: purged and physicalMaximum, %s\n", getFilePath());

			throw InvalidException("Invalid segment state in addInformation: purged and physicalMaximum");
		}
		#endif

		split = true;

	} else if (RealTimeAdaptive_v1<K>::byteMaximum(this, segment) == true) {

		if ((segment->getVirtual() == true) || (segment->getPurged() == true)) {
			fillSegment(ctxt, segment, segment->getValuesCompressed() /* values */, RealTimeAdaptive_v1<K>::doPace(this, ctxt->getTransaction()));
		}

		split = true;
	}

	if ((split == true) && (segment->getRolling() == false)) {
		inttype compare = m_comparator->compare(key, segment->SegTreeMap::lastKey());
		if (compare == 0) {
			// nothing to do

		} else if (compare > 0) {
			segment = new Segment<K>(m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER, Versions::GET_PROTOCOL_CURRENT(),m_keyBuilder->getKeyParts());
			segment->setMapContext(m_indexValue);
			segment->lock();

			// XXX: safe context lock: one writer / no readers on the branch tree
			m_threadContext.writeLock();
			{
				m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(key, segment);

				handle.assign(segment);
			}
			m_threadContext.writeUnlock();

		} else if ((segment->SegTreeMap::containsKey(key) == false) && (segment->waitForReferences() == true)) {

			K firstKey = (K) Converter<K>::NULL_VALUE;
			Segment<K>* pSegment = splitSegment(ctxt, segment, &firstKey /* of pSegment */, true /* lock */);

			// XXX: ensure to add the other half to an active index cycle
			{
				m_orderSegmentList.lock();
				if ((m_orderSegmentMode != MODE_INDEX) && (m_orderSegmentList.contains(segment, false /* lock */) == true)) {
					pSegment->setIndexOrderKey(m_keyBuilder->cloneKey(pSegment->SegTreeMap::firstKey()));
					m_orderSegmentList.add(pSegment, false /* lock */);
				}
				m_orderSegmentList.unlock();
			}

			// XXX: see handle.assign for segment unlock when swapped
			boolean swap = m_comparator->compare(key, firstKey) > 0;
			if (swap == true) {
				segment = pSegment;
				handle.assign(segment);

			} else {
				pSegment->unlock();
			}
		}
	}

	return updateInformation(ctxt, segment, key, info, enableCardinality);
}

template<typename K>
Information* RealTimeMap<K>::deleteInformation(ThreadContext<K>* ctxt, Segment<K>* segment, const K key, boolean persist) {

	if (persist == true) {
		if (/*(segment->getPurged() == true) ||*/ (segment->getVirtual() == true)) {
			fillSegment(ctxt, segment, false /* values */, RealTimeAdaptive_v1<K>::doPace(this, ctxt->getTransaction()));
		}
	}

	K retkey = (K) Converter<K>::NULL_VALUE;
	K firstKey = segment->SegTreeMap::firstKey();

	// XXX: 1/3 filled segments will be merged into other segments via the index cycle

	boolean cardinalityBefore = segment->getCardinalityEnabled();
	segment->setCardinalityEnabled(true);

	Information* delinfo = segment->SegTreeMap::remove(key, &retkey);

	segment->setCardinalityEnabled(cardinalityBefore);
	
	if (segment->SegTreeMap::size() != 0) {
		if (retkey == firstKey) /* value or pointer match */ {
			reseedSegment(segment, retkey, true, true);

		} else {
			Converter<K>::destroy(retkey);
		}

		// XXX: ensure changes are active until paged out
		if ((segment->getPaged() == true) && (persist == true)) {
			segment->setFragmentedKey(true);
			segment->setBeenAltered(true);
			segment->setDirty(true);
		}

	} else {
		deleteSegment(segment, retkey, true, true);
	}

	return delinfo;
}

template<typename K>
void RealTimeMap<K>::stitchInformationSafe(ThreadContext<K>* ctxt, Transaction* tx, XInfoRef& infoRef, InfoRef& curinfoRef, boolean primaryLocked, boolean stitch) {

	Information* info = infoRef.getInfo();
	StoryLine& storyLine = infoRef.getStoryLine();
	
	SegMapEntry* infoEntry = curinfoRef.infoEntry;

	// XXX: don't allow primary to roll (append WRITE) or remove info during this stitch
	boolean lock = (primaryLocked == false) && (((m_valueCompressMode == false) && (info->hasFields(Information::WRITE) == false)) || ((m_valueCompressMode == true) && info->hasFields(Information::CMPRS) == false));

	if (lock == true) storyLine.lock();
	{
		if ((stitch == true) && (isolatedStoryline((Information*) infoEntry->getValue(), (Information*) info) == false)) {

			storyLine.initStoryLock();
			stitchInformation(ctxt, tx, infoRef, curinfoRef);
		}
	}
	if (lock == true) storyLine.unlock();
}

template<typename K>
void RealTimeMap<K>::stitchInformation(ThreadContext<K>* ctxt, Transaction* tx, XInfoRef& infoRef, InfoRef& curinfoRef) {
	Information* info = infoRef.getInfo();

	Segment<K>* segment = curinfoRef.segment;
	SegMapEntry* infoEntry = curinfoRef.infoEntry;
	Information* curinfo = curinfoRef.infoEntry->getValue();
	StoryLine& curStoryLine = curinfoRef.getStoryLine();
	StoryLock* curlock = curStoryLine.getStoryLock();

	#ifdef DEEP_DEBUG
	if (informationIsOld(info, curinfo, (K) Converter<K>::NULL_VALUE) == true) {
		DEEP_LOG(ERROR, OTHER, "Invalid stitch: info is older than current info, %s\n", getFilePath());

		throw InvalidException("Invalid stitch: information is older than current info");
	}
	#endif

	#ifdef DEEP_DEBUG
	if (infoRef.getStoryLine().getStoryLock() == null) {
		DEEP_LOG(ERROR, OTHER, "Invalid stitch: null story lock, %s\n", getFilePath());

		throw InvalidException("Invalid stitch: null story lock");
	}
	#endif

	if ((curlock != null) && (curStoryLine.hasSameLock(infoRef.getStoryLine()) == false)) {

		curStoryLine.lock(curlock /* XXX: may be reassigned */);

		// XXX: removed shared lock will either be replaced or upgraded in checkAccessLock
		if ((tx != null) && (curStoryLine.getSharing() == true)) {
			if (tx->getIdentifier() == curlock->getIdentifier()) {
				tx->reassign(curlock);
			}

			if (tx->getReadLockSet()->HashSet<StoryLock*>::remove(curlock) == true) {
				//#ifdef DEEP_READ_STORYLOCK_REFERENCING
				curlock->decref();
				//#endif
			}
		}

		curStoryLine.setStoryLock(infoRef.getStoryLine().getStoryLock());

		// XXX: not needed, here for completeness
		curStoryLine.unlock(curlock);

		if (curlock->decref() == 0) {
			curStoryLine.reassignmentLock();
			delete curlock;
			curStoryLine.reassignmentUnlock();
		}
	}

	curinfo = isolateInformation(ctxt, curinfo, Information::LEVEL_COMMIT); 
	boolean stitched = false;

	if ((curinfo->getLevel() == Information::LEVEL_COMMIT) && ((curinfo->getIndexed(m_indexValue) == false)) && (isolatedStoryline(info, curinfo) == false)) {

		// XXX: updating secondary to latest committed within active storyline
		if (isolatedStoryline(infoEntry->getValue(), info) == true) {
			#ifdef DEEP_DEBUG
			if ((m_primaryIndex == null) || (info->getLevel() != Information::LEVEL_COMMIT)) {
				DEEP_LOG(ERROR, OTHER, "Invalid stitch: info in curinfo storyline, %s\n", getFilePath());

				throw InvalidException("Invalid stitch: info in curinfo storyline ");
			}
			#endif
	
			info->setIndexed(m_indexValue, false);

		} else {

			Information* topinfo = isolateInformation(ctxt, info, Information::LEVEL_COMMIT);
			if (topinfo->hasFields(Information::WRITE) == true) {
				topinfo->setIndexed(m_indexValue, false);

			} else {

				#ifdef DEEP_DEBUG
				if ((topinfo->getLevel() != Information::LEVEL_COMMIT) || (topinfo->getNext() != null)) {
					DEEP_LOG(ERROR, OTHER, "Invalid stitch: existing story line, %s\n", getFilePath());

					throw InvalidException("Invalid stitch: existing story line");
				}
				#endif

				#ifdef DEEP_DEBUG
				if ((curinfo->getFileIndex() != topinfo->getFileIndex()) || (curinfo->getFilePosition() != topinfo->getFilePosition())) {
					DEEP_LOG(ERROR, OTHER, "Invalid stitch: curinfo locality does not match topinfo locality, %s\n", getFilePath());

					throw InvalidException("Invalid stitch: curinfo locality odes not match topinfo locality");
				}
				#endif

				#ifdef DEEP_DEBUG
				if (informationIsOld(curinfo, topinfo, (K) Converter<K>::NULL_VALUE) == true) {
					DEEP_LOG(ERROR, OTHER, "Invalid stitch: curinfo is older than topinfo, %s\n", getFilePath());

					throw InvalidException("Invalid stitch: curinfo is older than topinfo");
				}
				#endif

				if (curinfo == infoEntry->getValue()) {
					if ((topinfo->getLevel() == Information::LEVEL_COMMIT) && (curinfo->getLevel() == Information::LEVEL_COMMIT)) {
						topinfo->setDeleting(true);
					}

					topinfo->setNext(curinfo);
					stitched = true;

				} else {
					// XXX: stitching this here will cause invalid graph-like storyline, force rebuild
					segment->setBeenAltered(true);
				}
			}
		}
	}

	info->incref();

	// XXX: statement order is important due to memory debug validation
	{
		Information* delinfo = infoEntry->getValue();
		RealTimeVersion<K>::statisticPagingDead(this, segment, delinfo, false /* checkpoint */);

		((SegMapEntry*) infoEntry)->setValue(info, m_indexValue /* ctx */);

		if (stitched == false) {
			Converter<Information*>::destroy(delinfo);
		}
	}
}

template<typename K>
boolean RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, Information*& info, const K dupkey) {

	inttype pos = -1;
	boolean ignore = true;
	bytearray retkey = null;
	Information* curinfo = info;

	Transaction* tx = ctxt->getTransaction();

	do {
		m_primaryIndex->decomposeInformation(tx, m_indexValue, curinfo, &pos, &retkey);

		// XXX: information not in working set (i.e. not operated on in this transaction)
		if (retkey == null) {
			#ifdef DEEP_DEBUG
			if (pos != -1) {
				DEEP_LOG(ERROR, OTHER, "Invalid isolation: information not found, %s\n", getFilePath());

				throw InvalidException("Invalid isolation: information not found");
			}
			#endif
			break;
		}

		if (curinfo->getDeleting() == false) {
			K curkey = m_keyBuilder->fillKey(retkey, curinfo->getData(), ctxt->getKey3());
			if (m_comparator->compare(curkey, dupkey) == 0) {
				ignore = (curinfo != info);
				info = curinfo;
			}
		}

		curinfo = m_primaryIndex->decomposeInformation(tx, m_indexValue, null, &(++pos), &retkey);
		if (curinfo != null) {
			curinfo = isolateInformation(ctxt, curinfo, true);
		}

	} while ((curinfo != null) && (curinfo->getLevel() > Information::LEVEL_COMMIT));

	return ignore;
}

template<typename K>
boolean RealTimeMap<K>::restitchInformation(ThreadContext<K>* ctxt, const Information* oldinfo, XInfoRef& curinfoRef, LockOption lock, K oldkey) {

	Transaction* tx = ctxt->getTransaction();
	const Information* curinfo = curinfoRef.getInfo();

	RETRY:
	Segment<K>* segment = getSegment(ctxt, oldkey, false, true /* TODO: allow to be virtual, add if not found */);
	CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

	const SegMapEntry* infoEntry = (segment != null) ? segment->SegTreeMap::getEntry(oldkey) : null;
	if (infoEntry != null) {
		ErrorCode code = ERR_SUCCESS;
		InfoRef infoRef(m_indexValue, segment, (SegMapEntry*)infoEntry);
		if (checkAccessLock(tx, infoRef, &code, lock) == true) {
			CONTEXT_STACK_UNASSIGN(global);
			goto RETRY;

		} else if (code != ERR_SUCCESS) {
			CONTEXT_STACK_UNASSIGN(global);
			return false;
		}

		if ((infoEntry->getValue()->getLevel() == Information::LEVEL_COMMIT) && (infoEntry->getValue()->getDeleting() == false)) {
			if ((infoEntry->getValue() != curinfo) && (curinfo->getLevel() != Information::LEVEL_COMMIT)) {
				curinfo = isolateInformation(ctxt, (Information*) oldinfo, Information::LEVEL_COMMIT, oldkey);
			}

			if (infoEntry->getValue() != curinfo) {
				stitchInformation(ctxt, tx, curinfoRef, infoRef);

				// XXX: ensure changes are active until paged out
				if (segment->getPaged() == true) {
					segment->setDirty(true);
				}
			}
		}

	#if 0
	#ifdef DEEP_DEBUG
	} else {
		DEEP_LOG(ERROR, OTHER, "Invalid lock: information not found, %s\n", getFilePath());

		throw InvalidException("Invalid lock: information not found");
	#endif
	#endif
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::duplicateInformation(ThreadContext<K>* ctxt, const bytearray pkey, Information* curinfo, Information* newinfo, const K dupkey) {

	inttype pos = -1;
	bytearray retkey = null;
	Transaction* tx = ctxt->getTransaction();

	if (curinfo->getData() == null) {
		#ifdef DEEP_COMPRESS_PRIMARY_READ
		if (curinfo->getCompressed() == true) {
			tx->setFillFlag(true);
			return true;
		}	
		#endif

		readValue(ctxt, (Information*) curinfo, dupkey);
	}

	do {
		m_primaryIndex->decomposeInformation(tx, m_indexValue, curinfo, &pos, &retkey);

		// XXX: information not in working set (i.e. not operated on in this transaction)
		if (retkey == null) {
			#ifdef DEEP_DEBUG
			if (pos != -1) {
				DEEP_LOG(ERROR, OTHER, "Invalid isolation: information not found, %s\n", getFilePath());

				throw InvalidException("Invalid isolation: information not found");
			}
			#endif

			K ckey = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey3());
			if (m_comparator->compare(ckey, dupkey) == 0) {
				m_threadContext.setErrorKey(ckey);
				m_threadContext.setErrorCode(ERR_DUPLICATE);
				return true;
			}

			break;
		}

		if ((newinfo != curinfo) && (curinfo->getDeleting() == false) && (curinfo->getLevel() >= Information::LEVEL_COMMIT)) {
			#ifdef DEEP_DEBUG
			if (curinfo->getLevel() == Information::LEVEL_COMMIT) {
				DEEP_LOG(ERROR, OTHER, "Invalid isolation: level commit\n");

				throw InvalidException("Invalid isolation: level commit");
			}
			#endif
			
			K ckey = m_keyBuilder->fillKey(retkey, curinfo->getData(), ctxt->getKey3());
			if (m_comparator->compare(ckey, dupkey) == 0) {
				m_threadContext.setErrorKey(ckey);
				m_threadContext.setErrorCode(ERR_DUPLICATE);
				return true;
			}
		}

		curinfo = m_primaryIndex->decomposeInformation(tx, m_indexValue, null, &(++pos), &retkey);
		if (curinfo != null) {
			curinfo = isolateInformation(ctxt, curinfo, true);
		}

	} while (curinfo != null);

	return false;
}

template<typename K>
boolean RealTimeMap<K>::duplicateInformation(ThreadContext<K>* ctxt, const bytearray pkey, const Information* newinfo, LockOption lock, K newkey, boolean* identical) {

	boolean lower = false;
	Segment<K>* segment = null;

	Transaction* tx = ctxt->getTransaction();

	m_keyBuilder->setIgnorePrimary(newkey, true);

	RESTART:
	m_threadContext.readLock();
	{
		const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::lowerEntry(newkey /* ignoring primary */);
		if (index != null) {
			segment = index->getValue();
			if (segment->tryLock() == false) {
				m_threadContext.readUnlock();
				goto RESTART;
			}

			lower = true;

		} else {
			index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::floorEntry(newkey /* ignoring primary */);
			if (index != null) {
				segment = index->getValue();
				if (segment->tryLock() == false) {
					m_threadContext.readUnlock();
					goto RESTART;
				}
			}
		}
	}
	m_threadContext.readUnlock();

	NEXTSEG:
	if (segment != null) {
		CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

		if (fillSetupSegment(ctxt, segment, true /* physical */, false /* values */) == false) {
			// XXX: filled a summary segment, start again
			goto RESTART;
		}

		const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(newkey /* ignoring primary */);
		if (infoEntry != null) {
			const SegMapEntry* floorEntry = segment->SegTreeMap::floorEntry(newkey /* ignoring primary */);
			if (floorEntry != null) {
				infoEntry = floorEntry;
			}
		}

		MIRROR:
		if (infoEntry != null) {
			#if 1
			ErrorCode code = ERR_SUCCESS;
			InfoRef infoRef(m_indexValue, segment, (SegMapEntry*)infoEntry);
			if (checkAccessLock(tx, infoRef, &code, lock) == true) {
				CONTEXT_STACK_UNASSIGN(global);
				goto RESTART;

			} else if (code != ERR_SUCCESS) {
				m_keyBuilder->setIgnorePrimary(newkey, false);
				CONTEXT_STACK_UNASSIGN(global);
				return false;
			}
			#endif

			Information* topinfo = isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
			if (topinfo->getDeleting() == false) {
				Information* curinfo = isolateInformation(ctxt, topinfo, true);

				if (duplicateInformation(ctxt, pkey, curinfo, (Information*) newinfo, newkey /* ignoring primary */) == true) {

					#ifdef DEEP_COMPRESS_PRIMARY_READ
					if (tx->getFillFlag() == true) {
						if (m_keyBuilder->getPrimaryKey(infoEntry->getKey(), ctxt->getKeyBuffer(), ctxt->getKeyOffset(), ctxt->getKeyLength()) == true) {
							// XXX: see primary invocation putTransaction
							m_primaryIndex->setContextKey(tx, ctxt->getKeyBuffer());

						} else {
							DEEP_LOG(ERROR, OTHER, "Invalid read: duplicate compression could not get primary key, %s\n", getFilePath());

							throw InvalidException("Invalid read: duplicate compression could not get primary key");
						}
					}
					#endif

					m_keyBuilder->setIgnorePrimary(newkey, false);
					return false;
				}
			}

			// XXX: stash away for potential next segment lookup
			const SegMapEntry* prevEntry = infoEntry;

			infoEntry = segment->SegTreeMap::higherEntry(infoEntry->getKey());
			if (infoEntry != null) {
				if (m_comparator->compare(infoEntry->getKey(), newkey /* ignoring primary */) == 0) {
					// XXX: continue to walk next element (i.e. proverbial key space mirroring)
					goto MIRROR;
				}

			} else {
				m_threadContext.readLock();
				{
					const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::higherEntry(prevEntry->getKey());
					if ((index != null) && (index->getValue() != segment) ) {
						m_threadContext.readUnlock();

						CONTEXT_STACK_RELEASE(global);
						segment = index->getValue();
						if (segment->tryLock() == false) {
							goto RESTART;
						}

						goto NEXTSEG;
					}
				}
				m_threadContext.readUnlock();
			}

			*identical = true;

		} else if (lower == true) {
			lower = false;

			m_threadContext.readLock();
			{
				const MapEntry<K,Segment<K>*>* index = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::floorEntry(newkey);
				if ((index != null) && (index->getValue() != segment) ) {
					m_threadContext.readUnlock();

					CONTEXT_STACK_RELEASE(global);
					segment = index->getValue();
					if (segment->tryLock() == false) {
						goto RESTART;
					}

					goto NEXTSEG;
				}
			}
			m_threadContext.readUnlock();
		}
	}

	m_keyBuilder->setIgnorePrimary(newkey, false);

	return true;
}

template<typename K>
boolean RealTimeMap<K>::isolatedStoryline(const Information* topinfo, const Information* curinfo) {

	if (topinfo == curinfo) {
		return true;
	}

	boolean found = (curinfo == null);

	const Information* next = topinfo;
	while (next != null) {
		if (next->getUpdating() == true) {
			break;
		}

		if (next->getNext() == curinfo) {
			found = true;
			break;
		}

		next = next->getNext();
	}

	return found;
}

template<typename K>
Information* RealTimeMap<K>::isolateCompressed(ThreadContext<K>* ctxt, Information* orginfo, Information* topinfo) {

	Information* curinfo = null;

	Information* next = orginfo;
	while (next != null) {
		// XXX: never want compressed / pending commit infos (see RealTimeProtocol::rolloverRealTime)
		if ((next->getCompressed() == true) && (next->getLevel() == Information::LEVEL_COMMIT)) {
			curinfo = next;
		}

		if ((next->getUpdating() == true) || (next == topinfo)) {
			break;
		}

		next = next->getNext();
	}

	return curinfo;
}

template<typename K>
Information* RealTimeMap<K>::isolateCheckpoint(ThreadContext<K>* ctxt, Information* topinfo, uinttype viewpoint, const K key, boolean* divergent, boolean* ignore, boolean viewlimit) {

	Information* curinfo = null;
	Information* next = topinfo;

	while (next != null) {

		if (next->getLevel() != Information::LEVEL_COMMIT) {
			break;
		}

		if ((ignore != null) && (*ignore == true)) {
			if ((next->getViewpoint() >= viewpoint) && (next->getIndexed(m_indexValue) == true)) {
				*ignore = false;
			}
		}

		// XXX: only checkpoint written information
		if (next->getViewpoint() <= viewpoint) {
			if ((curinfo == null) || (next->getViewpoint() >= curinfo->getViewpoint())) {
				curinfo = next;
			}
		}
		
		next = next->getNext();

		#ifdef DEEP_DEBUG
		if (next == topinfo) {
			DEEP_LOG(ERROR, OTHER, "Invalid storyline: isolateCheckpoint looping storyline\n");

			throw InvalidException("Invalid storyline: isolateCheckpoint looping storyline");
		}
		#endif
	}

	// XXX: this info is an actual delete, not an update delete, should not be a part of the checkpoint
	if (curinfo != null) {
		if ((curinfo->getDeleting() == true) && ((curinfo->getNext() == null) || (curinfo->getNext()->getLevel() != Information::LEVEL_COMMIT)) && (curinfo->getViewLimit() == 0)) {
			if (ignore != null) {
				*ignore = false;
			}
			if (viewlimit == true) {
				curinfo = null;
			}
		}
	}

	if (curinfo != null) {
		const uinttype limit = curinfo->getViewLimit();
		if ((limit > 0) && (viewpoint > limit)) {
			if (ignore != null) {
				*ignore = false;
			}
			if (viewlimit == true) {
				curinfo = null;
			}
		}
	}

	if ((m_primaryIndex != null) && (curinfo != null) && (curinfo != topinfo)) {

		bytearray pkey = null;
		if (m_keyBuilder->hasHiddenKey() == true) {
			pkey = m_keyBuilder->getHiddenKey(key);
		}

		if (curinfo->getData() == null) {
			readValue(ctxt, (Information*) curinfo, key);
		}

		K curkey = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey3());
		if (m_comparator->compare(curkey, key) != 0) {
			if (divergent != null) {
				*divergent = true;
			}
			if (ignore != null) {
				*ignore = false;
			}
			curinfo = null;
		}
	}

	return curinfo;
}

template<typename K>
Information* RealTimeMap<K>::isolateInterspace(ThreadContext<K>* ctxt, Information* topinfo, Information::LevelState level, const K key, boolean* divergent) {

	Information* curinfo = topinfo;

	Information* next = topinfo;
	while (next != null) {

		if (next->getLevel() != level) {
			break;
		}

		if (next->getUpdating() == true) {
			break;
		}

		curinfo = next;
		next = next->getNext();

		#ifdef DEEP_DEBUG
		if (next == topinfo) {
			DEEP_LOG(ERROR, OTHER, "Invalid storyline: isolateInterspace looping storyline\n");

			throw InvalidException("Invalid storyline: isolateInterspace looping storyline");
		}
		#endif
	}

	if ((m_primaryIndex != null) && (curinfo != null) && (curinfo != topinfo)) {

		bytearray pkey = null;
		if (m_keyBuilder->hasHiddenKey() == true) {
			pkey = m_keyBuilder->getHiddenKey(key);
		}

		if (curinfo->getData() == null) {
			readValue(ctxt, (Information*) curinfo, key);
		}

		K curkey = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey3());
		if (m_comparator->compare(curkey, key) != 0) {

			// XXX: check for committed not divergence of one prior to curinfo
			if ((divergent != null) && (*divergent == false)) {

				next = topinfo;
				while (next != curinfo) {
					if (next->getIndexed(m_indexValue) == true) {

						if (next->getData() == null) {
							readValue(ctxt, (Information*) next, key);
						}

						curkey = m_keyBuilder->fillKey(pkey, next->getData(), ctxt->getKey3());
						if (m_comparator->compare(curkey, key) == 0) {
							*divergent = true;
							break;
						}
					}

					next = next->getNext();
				}
			}

			curinfo = null;
		}
	}

	return curinfo;
}

template<typename K>
Information* RealTimeMap<K>::isolateInformationLocality(ThreadContext<K>* ctxt, const Information* topinfo) {

	const Information* curinfo = null;
	const Information* next = topinfo;

	while (next != null) {

		boolean committedToDisk = (next->getLevel() != Information::LEVEL_COMMIT) && (next->getFilePosition() != 0);
		if ((next->getLevel() == Information::LEVEL_COMMIT) || (committedToDisk == true)) {
			curinfo = next;
		}

		if (next->getUpdating() == true) {
			break;
		}

		next = next->getNext();
	}

	return (Information*) curinfo;
}

template<typename K>
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, Information* topinfo) {

	Information* curinfo = null;

	Information* next = topinfo;
	while (next != null) {

		if (next->getLevel() > Information::LEVEL_COMMIT) {
			curinfo = next;
		}

		if (next->getUpdating() == true) {
			break;
		}

		next = next->getNext();
	}

	return curinfo;
}

template<typename K>
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, boolean first) {

	Information* curinfo = isolateInformation(ctxt, topinfo);

	if ((curinfo == null) && (first == true)) {
		curinfo = topinfo;
	}

	return curinfo;
}

template<typename K>
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, Information* curinfo) {

	Information* next = topinfo;
	while (next != null) {
		topinfo = next;

		if (next->getUpdating() == true) {
			break;
		}

		if (next->getNext() == curinfo) {
			break;
		}

		next = next->getNext();
	}

	return topinfo;
}

template<typename K>
#ifdef DEEP_DEBUG
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, uinttype viewpoint, const K key, boolean testStoryLock) {
#else
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, uinttype viewpoint, const K key) {
#endif

	Information* curinfo = null;

	Information* next = topinfo;
	while (next != null) {

		if (next->getLevel() != Information::LEVEL_COMMIT) {
			break;
		}

		if (next->hasViewpoint(viewpoint) == true) {
			curinfo = next;
		}

		if (next->getUpdating() == true) {
			break;
		}

		#ifdef DEEP_DEBUG
		next = next->getNext(testStoryLock);
		#else
		next = next->getNext();
		#endif
	}

	if ((m_primaryIndex != null) && (curinfo != null) && (curinfo != topinfo)) {

		bytearray pkey = null;
		if (m_keyBuilder->hasHiddenKey() == true) {
			pkey = m_keyBuilder->getHiddenKey(key);
		}

		if (curinfo->getData() == null) {
			readValue(ctxt, (Information*) curinfo, key);
		}

		K curkey = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey3());
		if (m_comparator->compare(curkey, key) != 0) {
			curinfo = null;
		}
	}

	return curinfo;
}

template<typename K>
#ifdef DEEP_DEBUG
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, Information::LevelState level, boolean testStoryLock) {
#else
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, Information::LevelState level) {
#endif

	Information* curinfo = topinfo;

	Information* next = topinfo;
	while (next != null) {

		if (next->getLevel() != level) {
			break;
		}

		if (next->getUpdating() == true) {
			break;
		}

		curinfo = next;

		#ifdef DEEP_DEBUG
		next = next->getNext(testStoryLock);
		#else
		next = next->getNext();
		#endif
	}

	return curinfo;
}

template<typename K>
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, Information* topinfo, Information::LevelState level, const K key) {

	Information* curinfo = topinfo;

	Information* next = topinfo;
	while (next != null) {

		if (next->getLevel() != level) {
			break;
		}

		if (next->getUpdating() == true) {
			break;
		}

		curinfo = next;
		next = next->getNext();
	}

	if ((m_primaryIndex != null) && (curinfo != null) && (curinfo != topinfo)) {

		bytearray pkey = null;
		if (m_keyBuilder->hasHiddenKey() == true) {
			pkey = m_keyBuilder->getHiddenKey(key);
		}

		if (curinfo->getData() == null) {
			readValue(ctxt, (Information*) curinfo, key);
		}

		K curkey = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey3());
		if (m_comparator->compare(curkey, key) != 0) {
			curinfo = null;
		}
	}

	return curinfo;
}

template<typename K>
#ifdef DEEP_DEBUG 
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, StoryLine& storyLine, Information* topinfo, Information::LevelState level, uinttype viewpoint, Transaction::Isolation mode, ushorttype id, const K key, boolean testStoryLock) {
#else
Information* RealTimeMap<K>::isolateInformation(ThreadContext<K>* ctxt, StoryLine& storyLine, Information* topinfo, Information::LevelState level, uinttype viewpoint, Transaction::Isolation mode, ushorttype id, const K key) {
#endif

	Information* curinfo = topinfo;

	Information* next = topinfo;
	while (next != null) {

		if (next->getLevel() != level) {
			// XXX: include non-committed information when owned
			if ((m_primaryIndex != null) && (storyLine.getLockIdentifier() == id)) { // TODO: DN
				curinfo = next;

			} else {
				break;
			}
		}

		if (next->getUpdating() == true) {
			break;
		}

		if ((mode == Transaction::REPEATABLE) && (storyLine.getLockIdentifier() != id) && (next->getViewpoint() > viewpoint)) {
			break;
		}

		curinfo = next;

		#ifdef DEEP_DEBUG
		next = next->getNext(testStoryLock);
		#else
		next = next->getNext();
		#endif
	}

	if ((m_primaryIndex != null) && (curinfo != null) && (curinfo != topinfo)) {

		bytearray pkey = null;
		if (m_keyBuilder->hasHiddenKey() == true) {
			pkey = m_keyBuilder->getHiddenKey(key);
		}

		if (curinfo->getData() == null) {
			readValue(ctxt, (Information*) curinfo, key);
		}

		K curkey = m_keyBuilder->fillKey(pkey, curinfo->getData(), ctxt->getKey3());
		if (m_comparator->compare(curkey, key) != 0) {
			curinfo = null;
		}
	}

	return curinfo;
}

template<typename K>
Information* RealTimeMap<K>::versionInformation(Transaction* tx, Information* preinfo, const K key, const nbyte* value, boolean compressed) {

	Information* curinfo = preinfo;

	if ((m_hasSecondaryMaps == true) || (preinfo->getLevel() != tx->getLevel())) {
		curinfo = Information::newInfo((compressed == false) ? Information::WRITE : Information::CMPRS);
		curinfo->setLevel(tx->getLevel());
		curinfo->setDiverging(preinfo->getDiverging());

		preinfo->setUpdating(false);

		if (preinfo->getData() == null) {
			ThreadContext<K>* ctxt = getTransactionContext(tx);
			readValue(ctxt, preinfo, key);
		}

		preinfo->setNext(curinfo);

	#if 0
	} else if ((preinfo->getCompressed() == false) && (compressed == true)) {
		DEEP_LOG(ERROR, OTHER, "Update non-compressed info with compressed info\n");

		throw InvalidException("Update non-compressed info with compressed info");
	#endif

	/* XXX: potential insert and update/delete at the same level (i.e. no cycle in version information)
	} else if (preinfo->getNext() == null) {
	*/
	}

	if (value != null) {
		copyValueIntoInformation(curinfo, value);
	}

	return curinfo;
}

#ifdef DEEP_DEBUG 
template<typename K>
boolean RealTimeMap<K>::informationIsOld(const Information* info, const Information* curinfo, K key, ThreadContext<K>* ctxt, boolean read) {
	if (read == true) {
		// XXX: if segment being read was virtual, existing info may have a storyline
		info = (const Information*) isolateInformation(ctxt, (Information*) info, Information::LEVEL_COMMIT, key);
		if (info == null) {
			return false;
		}
	}

	boolean infoIsOld = false;
	if ((m_state != MAP_RECOVER) && (info->getLevel() == Information::LEVEL_COMMIT) && (curinfo->getLevel() == Information::LEVEL_COMMIT)) {
		RealTimeShare* share = (m_primaryIndex == null) ? getShare() : m_primaryIndex->getShare();

		MeasuredRandomAccessFile* infoFile = share->getVrtWriteFileList()->get(info->getFileIndex());
		MeasuredRandomAccessFile* curFile = share->getVrtWriteFileList()->get(curinfo->getFileIndex());

		// XXX: on read side, curinfo is from previous delta, we may have moved passed it's file index, which is valid. 
		if ((curFile == null) && (read == true)) {
			return false;
		}

		time_t infoFileCreationTime = infoFile->MeasuredRandomAccessFile::getFileCreationTime();
		time_t curFileCreationTime = curFile->MeasuredRandomAccessFile::getFileCreationTime();

		infoIsOld = (infoFileCreationTime < curFileCreationTime);
		if (infoIsOld == false) {
			#if 0
			infoIsOld = (infoFileCreationTime == curFileCreationTime) && (info->getFilePosition() < curinfo->getFilePosition());
			#else
			infoIsOld = getLocalityCmp()->compare(RealTimeLocality(info->getFileIndex(), info->getFilePosition(), RealTimeLocality::VIEWPOINT_NONE), RealTimeLocality(curinfo->getFileIndex(), curinfo->getFilePosition(), RealTimeLocality::VIEWPOINT_NONE)) < 0;
			#endif
		}
	}
	return infoIsOld;
}
#endif

#ifdef DEEP_DEBUG 
template<typename K>
void RealTimeMap<K>::validateStoryline(const Information* orginfo, const Information* info) {

	const Information* next = orginfo;
	boolean found = false;

	// XXX: info is reachable from orginfo
	while (next != null) {
		if (next == info) {
			found = true;
			break;
		}

		next = next->getNext();

		if (next == orginfo) {
			break;
		}
	}

	if (found == false) {
		DEEP_LOG(ERROR, OTHER, "Storyline violation: info not reachable from orginfo, %s\n", getFilePath());
		throw InvalidException("Storyline violation: info not reachable from orginfo");
	}

	// XXX: storyline ends with null or updating
	next = orginfo;
	const Information* prev = null;
	while (true) {
		if (next == null) {
			break;
		}

		if (next->getUpdating() == true) {
			break;
		}

		prev = next;
		next = next->getNext();

		if (next == orginfo) {
			DEEP_LOG(ERROR, OTHER, "Storyline violation: not terminated with null or updating, %s\n", getFilePath());
			throw InvalidException("Storyline violation: not terminated with null or updating");
		}
	}

	if (next == null) {
		if ((prev != null) && (prev->getLevel() != Information::LEVEL_COMMIT)) {
			DEEP_LOG(ERROR, OTHER, "Storyline violation: uncommitted null termination, %s\n", getFilePath());
			throw InvalidException("Storyline violation: uncommitted null termination");
		}
	}

	if ((next != null) && (next->getUpdating() == true)) {
		if (next->getNext() != orginfo) {
			DEEP_LOG(ERROR, OTHER, "Storyline violation: orginfo not start of story, %s\n", getFilePath());
			throw InvalidException("Storyline violation: orginfo not start of story");
		}
	}

	// XXX: committed infos after non committed 
	next = orginfo;
	boolean uncommitted = false;

	while (next != null) {
		shorttype level = next->getLevel();
		if (level >= Information::LEVEL_ACTIVE) {
			uncommitted = true;
		}

		if ((level == Information::LEVEL_COMMIT) && (uncommitted == true)) {
			DEEP_LOG(ERROR, OTHER, "Storyline violation: committed info after uncommitted, %s\n", getFilePath());
			throw InvalidException("Storyline violation: committed info after uncommitted");
		}

		next = next->getNext();
		if (next == orginfo) {
			break;
		}
	}
}
#endif

template<typename K>
boolean RealTimeMap<K>::putTransaction(const K key, const nbyte* value, WriteOption option, Transaction* tx, LockOption lock, uinttype position, ushorttype index, uinttype compressedOffset) {

	RealTimeConductor<K>* conductor = (RealTimeConductor<K>*) tx->getConductor(getIdentifier());
	ThreadContext<K>* ctxt = getConductorContext(tx, conductor);

	tx->setDirty(true);

	const boolean rolling = (lock == LOCK_ROLL);
	if (rolling == true) {
		lock = LOCK_NONE;
	}

	RETRY:
	const boolean compressed = (compressedOffset != Information::OFFSET_NONE) || (m_valueCompressMode == true);

	#ifdef DEEP_COMPRESS_PRIMARY_READ
	// XXX: secondary may require fill of primary with this key for duplicate check
	if (tx->getFillFlag() == true) {
		tx->setFillFlag(false);

		// XXX: see secondary invocation setContextKey
		K fillKey = ctxt->getKey3();

		Segment<K>* segment = getSegment(ctxt, fillKey, true, true /*, TODO(due to deadlock protection): false */);
		CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

		if (segment != null) {
			DEEP_LOG(DEBUG, READC, "put retry for secondary fill: %d, %p, %s\n", tx->getIdentifier(), segment, getFilePath());

			if (segment->getBeenFilled() == false) {
				readValue(ctxt, segment);

				segment->setBeenFilled(true);
			}

			const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(fillKey);
			Information* topinfo = isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
			if (topinfo != null) {
				ErrorCode code = ERR_SUCCESS;
				InfoRef infoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, topinfo);
				if (checkAccessLock(tx, infoRef, &code, LOCK_WRITE) == true) {
					CONTEXT_STACK_UNASSIGN(global);

					tx->setFillFlag(true);
					goto RETRY;

				} else if (code != ERR_SUCCESS) {
					CONTEXT_STACK_UNASSIGN(global);
					return false;
				}

				InfoRef topinfoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, topinfo);
				if (lockSecondary(tx, Converter<K>::toData(infoEntry->getKey()), topinfoRef, LOCK_WRITE, true /* primaryLocked */, true) == false) {
					DEEP_LOG(ERROR, READC, "put retry for secondary fill, lock failed: %d, %p, %s\n", tx->getIdentifier(), segment, getFilePath());
					return false;
				}

			#if 0
			} else {
				DEEP_LOG(ERROR, READC, "put retry for secondary fill (info not found): %d, %p, %s\n", tx->getIdentifier(), null, getFilePath());
				return false;
			#endif
			}

		#if 0
		} else {
			DEEP_LOG(ERROR, READC, "put retry for secondary fill (segment not found): %d, %p, %s\n", tx->getIdentifier(), null, getFilePath());
			return false;
		#endif
		}
	}
	#endif
	
	K newkey = key;
	Information* newinfo;
	Information* topinfo = null;

	Segment<K>* segment = getSegment(ctxt, key, true, (option != RESERVED) /* don't fill if reserved, ok to be virtual */);
	CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

	// XXX: note RESERVED put may dupe within same transaction, so we must lookup entry, but filling the segment is not necessary since
	// any duplicate must be uncommitted and within same transaction's reserved block. 

	const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(key);
	if (infoEntry != null) {

		// XXX: initial segment construction might not yet have an associated information
		if (infoEntry->getValue() != null) {

			// XXX: information might have been locked via an external ::get with LOCK_WRITE
			if (lock != LOCK_NONE) {
				ErrorCode code = ERR_SUCCESS;
				InfoRef infoRef(m_indexValue, segment, (SegMapEntry*)infoEntry);
				if (checkAccessLock(tx, infoRef, &code, lock) == true) {
					CONTEXT_STACK_UNASSIGN(global);
					goto RETRY;

				} else if (code != ERR_SUCCESS) {
					CONTEXT_STACK_UNASSIGN(global);
					return false;
				}
			}

			// XXX: if storyline depth is getting to large, inline clean up (i.e. not on index cycle)
			if ((infoEntry->getValue()->getLevel() == Information::LEVEL_COMMIT) && (infoEntry->getValue()->getDeleting() == true)) {

				if (infoEntry->getStoryLine().getStoryDepth() > Properties::DEFAULT_STORYLINE_MAXIMUM) {
					boolean dirty;
					Transaction::minimum();
					RealTimeAdaptive_v1<K>::deleteUnviewed(this, ctxt, (SegMapEntry*) infoEntry, segment, &dirty, true /* inline */);
				}
			}

			topinfo = isolateInformation(ctxt, infoEntry->getValue() /* XXX: re-get due to deleteUnviewed */, Information::LEVEL_COMMIT);
		}

		newkey = infoEntry->getKey();

		#ifdef COM_DEEPIS_DB_REKEY
		if ((m_keyBuilder->getRequiresRekey() == true) && (m_keyBuilder->isEqual(newkey, key) == false)) {
			K delkey = newkey;
			newkey = rekeySegment(segment, key, true, true);

			for (int j = 0; j < conductor->size(); j++) {
				SegMapEntry* walkEntry = conductor->get(j);
				#ifdef CXX_LANG_MEMORY_DEBUG
				if (walkEntry->getKey(true) == delkey) {
					walkEntry->setKey(newkey, -1 /* ctx */, true);
				#else
				if (walkEntry->getKey() == delkey) {
					walkEntry->setKey(newkey, -1 /* ctx */);
				#endif
				}
			}
		}
		#endif

	// XXX: if segment is new init, but we are RESERVED, key has been cloned in initSegment already
	} else if ((option == RESERVED) && (segment->SegTreeMap::size() == 1)) {

		if (m_comparator->compare(key, segment->SegTreeMap::firstKey()) == 0) {
			newkey = segment->SegTreeMap::firstKey();
		}
	}

	Information* trxinfo = isolateInformation(ctxt, topinfo);
	if (trxinfo != null) {
		newinfo = trxinfo;

		if (newinfo->getDeleting() == true) {
			if (option == EXISTING) {
				m_threadContext.setErrorCode(ERR_NOT_FOUND);
				return false;
			}

			newinfo = versionInformation(tx, trxinfo, key, value, compressed);
			// XXX: ensure states at identical levels
			newinfo->setCreating(true);
			newinfo->setDeleting(false);

		// XXX: a transaction may attempt duplicate insert within a single reserved block
		} else if (option >= RESERVED) {
			m_threadContext.setErrorCode(ERR_DUPLICATE);
			return false;

		} else {
			newinfo = versionInformation(tx, trxinfo, key, value, compressed);
		}

		if (position != 0) {
			newinfo->setFileIndex(index);
			newinfo->setFilePosition(position);

			if (compressed == true) {
				newinfo->setCompressedOffset(compressedOffset);
			}

			segment->addStreamIndex(index);
		}

		if (newinfo != trxinfo) {
			newinfo->setUpdating(true);
			newinfo->setNext(infoEntry->getValue());

			#ifdef DEEP_DEBUG
			conductor->add(infoEntry->getStoryLine().getStoryLock(), newkey, newinfo, tx);
			#else
			conductor->add(infoEntry->getStoryLine().getStoryLock(), newkey, newinfo);
			#endif

			if (trxinfo->getLevel() != Information::LEVEL_COMMIT) {
				conductor->m_mergeOptimize = -1;
			}
		}

		if (m_hasSecondaryMaps == true) {

			// XXX: avoid holding primary/secondary segment locks at the same time
			if (segment->getPaged() == true) {
				segment->setDirty(true);
			}

			InfoRef topinfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, topinfo);
			InfoRef newinfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, newinfo);

			CONTEXT_STACK_RELEASE(global);
			segment = null;

			if (lockSecondary(tx, newkey, newinfoRef, topinfoRef, lock, conductor) == false) {
				ejectSecondary(tx, newkey, newinfo, topinfo, null, RealTimeMap<K>::LOCK_WRITE, true /* ignore debug */);

				failedConductor(conductor, trxinfo);

				// XXX: restitch storyline if disconnected during versioning
				if (infoEntry->getValue() != trxinfo) {
					trxinfo->setUpdating(true);
					trxinfo->setNext(infoEntry->getValue());
				}

				#ifdef DEEP_COMPRESS_PRIMARY_READ
				if (tx->getFillFlag() == true) {
					goto RETRY;

				} else {
					return false;
				}
				#else
				return false;
				#endif
			}
		}

	} else {
		if ((topinfo != null) && (topinfo->getLevel() > Information::LEVEL_MERGED)) {
			#ifdef DEEP_DEBUG 
			if ((option == RESERVED) && (topinfo->getDeleting() == false)) {
				DEEP_LOG(ERROR, OTHER, "Invalid reservation: duplicate key, %s\n", getFilePath());

				throw InvalidException("Invalid reservation: duplicate key");
			}
			#endif

			if ((option > RESERVED) && (topinfo->getDeleting() == false)) {
				m_threadContext.setErrorCode(ERR_DUPLICATE);
				return false;
			}

			newinfo = versionInformation(tx, topinfo, key, value, compressed);
			// XXX: ensure states at identical levels
			newinfo->setCreating(false);
			newinfo->setDeleting(false);

			// XXX: simulate "new" information due to active viewing of deleted information
			if ((topinfo->getLevel() == Information::LEVEL_COMMIT) && (topinfo->getDeleting() == true)) {
				newinfo->setCreating(true);
			}

			newinfo->setUpdating(true);
			newinfo->setNext(infoEntry->getValue());

		} else {
			if (option == EXISTING) {
				failedSegment(segment, key);
				m_threadContext.setErrorCode(ERR_NOT_FOUND);
				return false;
			}

			newinfo = Information::newInfo((compressed == false) ? Information::WRITE : Information::CMPRS);
			newinfo->setCreating(true);
			newinfo->setLevel(tx->getLevel());

			copyValueIntoInformation(newinfo, value);

			// XXX: newkey might have been allocated in getSegment (i.e. first key)
			if (key == newkey) {
				newkey = m_keyBuilder->cloneKey(key);
			}

			if (conductor->getReserved() == true) {
				reserveSegment(conductor, CONTEXT_STACK_OBJECT(global), newkey);
			}

			infoEntry = addInformation(ctxt, CONTEXT_STACK_OBJECT(global), newkey, newinfo, (topinfo == null) /* enableCardinality */);
			infoEntry->getStoryLine().initStoryLock();
			infoEntry->getStoryLine().setLockCredentials(tx->getIdentifier(), tx->getSequence());

			// XXX: see paged segment dirty setting below
			segment = CONTEXT_STACK_OBJECT(global).getSegment();
		}

		if (position != 0) {
			newinfo->setFileIndex(index);
			newinfo->setFilePosition(position);

			if (compressed == true) {
				newinfo->setCompressedOffset(compressedOffset);
			}

			segment->addStreamIndex(index);
		}

		#ifdef DEEP_DEBUG
		conductor->add(infoEntry->getStoryLine().getStoryLock(), newkey, newinfo, tx);
		#else
		conductor->add(infoEntry->getStoryLine().getStoryLock(), newkey, newinfo);
		#endif

		if (m_hasSecondaryMaps == true) {

			// XXX: avoid holding primary/secondary segment locks at the same time
			if (segment->getPaged() == true) {
				segment->setDirty(true);
			}

			InfoRef topinfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, topinfo);
			InfoRef newinfoRef(m_indexValue, segment, (SegMapEntry*)infoEntry, newinfo);

			CONTEXT_STACK_RELEASE(global);
			segment = null;

			if (lockSecondary(tx, newkey, newinfoRef, topinfoRef, lock, conductor) == false) {
				ejectSecondary(tx, newkey, newinfo, topinfo, null, RealTimeMap<K>::LOCK_WRITE, true /* ignore debug */);

				if (topinfo == null) {
					segment = getSegment(ctxt, key, true /* create */, false /* fill */);
					CONTEXT_STACK_ASSIGN(segment, global);

					deleteInformation(ctxt, segment, key, false);
				}

				failedConductor(conductor, topinfo);
		
				#ifdef DEEP_COMPRESS_PRIMARY_READ
				if (tx->getFillFlag() == true) {
					goto RETRY;

				} else {
					return false;
				}
				#else
				return false;
				#endif
			}
		}
	}

	if (newinfo->getCreating() == true) {
		conductor->m_createStats++;
		conductor->m_userSpaceSize += newinfo->getSize();

	} else {
		conductor->m_updateStats++;

		if (newinfo->getSize() > topinfo->getSize()) {
			conductor->m_userSpaceSize += (newinfo->getSize() - topinfo->getSize());

		} else {
			conductor->m_userSpaceSize += (topinfo->getSize() - newinfo->getSize());
		}
	}

	// XXX: ensure changes are active until paged out
	if ((segment != null) && (segment->getPaged() == true)) {
		segment->setDirty(true);
	}

	if (rolling == false) {
		m_externalWorkTime = System::currentTimeMillis();
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::removeTransaction(const K key, nbyte* value, DeleteOption option, Transaction* tx, LockOption lock, boolean forCompressedUpdate) {

	boolean result = false;

	RealTimeConductor<K>* conductor = (RealTimeConductor<K>*) tx->getConductor(getIdentifier());
	ThreadContext<K>* ctxt = getConductorContext(tx, conductor);

	tx->setDirty(true);

	RETRY:
	K delkey = key;
	Information* topinfo = null;

	Segment<K>* segment = getSegment(ctxt, key, false, true);
	if (segment != null) {
		CONTEXT_STACK_HANDLER(K,ctxt,segment,global);

		const SegMapEntry* infoEntry = segment->SegTreeMap::getEntry(key);
		if (infoEntry != null) {
			delkey = infoEntry->getKey();
			topinfo = isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);

			if (lock != LOCK_NONE) {
				ErrorCode code = ERR_SUCCESS;
				InfoRef infoRef(m_indexValue, segment, (SegMapEntry*)infoEntry);
				if (checkAccessLock(tx, infoRef, &code, lock) == true) {
					CONTEXT_STACK_UNASSIGN(global);
					goto RETRY;

				} else if (code != ERR_SUCCESS) {
					CONTEXT_STACK_UNASSIGN(global);
					return false;
				}
			}
		}

		if (topinfo != null) {
			if (option == DELETE_POPULATED) {
				if (topinfo->getData() == null) {
					copyValueIntoInformation(topinfo, value);
				}

			} else {
				if (topinfo->getData() == null) {
					copyValueFromInformation(ctxt, topinfo, delkey);
				}

				if (option != DELETE_POPULATED) {
					copyValueFromInformation(topinfo, value);
				}
			}

			Information* trxinfo = isolateInformation(ctxt, topinfo, true);
			if (trxinfo->getDeleting() == false) {
				Information* delinfo = versionInformation(tx, trxinfo, key, value, forCompressedUpdate);
				// XXX: ensure states at identical levels
				delinfo->setDeleting(true);
				delinfo->setCreating(false);

				if (delinfo != trxinfo) {
					delinfo->setUpdating(true);
					delinfo->setNext(infoEntry->getValue());

					#ifdef DEEP_DEBUG
					conductor->add(infoEntry->getStoryLine().getStoryLock(), delkey, delinfo, tx);
					#else
					conductor->add(infoEntry->getStoryLine().getStoryLock(), delkey, delinfo);
					#endif

					if (trxinfo->getLevel() != Information::LEVEL_COMMIT) {
						conductor->m_mergeOptimize = -1;
					}
				}

				if (m_hasSecondaryMaps == true) {

					// XXX: avoid holding primary/secondary segment locks at the same time
					if (segment->getPaged() == true) {
						segment->setDirty(true);
					}

					InfoRef topinfoRef(m_indexValue, segment, (SegMapEntry*) infoEntry, topinfo);

					CONTEXT_STACK_RELEASE(global);
					segment = null;

					if (lockSecondary(tx, Converter<K>::toData(delkey), topinfoRef, lock, true /* primaryLocked */, true) == false) {
						failedConductor(conductor, trxinfo);

						// XXX: restitch storyline if disconnected during versioning
						if (infoEntry->getValue() != trxinfo) {
							trxinfo->setUpdating(true);
							trxinfo->setNext(infoEntry->getValue());
						}

						return false;
					}
				}

			conductor->m_userSpaceSize -= delinfo->getSize();
			conductor->m_deleteStats++;

			/* XXX: typically this is not errored
			} else {
				m_threadContext.setErrorCode(ERR_NOT_FOUND);
				return false;
			*/
			}

			result = true;
		}

		// XXX: ensure changes are active until paged out
		if ((segment != null) && (segment->getPaged() == true)) {
			segment->setDirty(true);
		}
	}
	m_externalWorkTime = System::currentTimeMillis();

	return result;
}

template<typename K>
boolean RealTimeMap<K>::get(const K key, nbyte* value, ReadOption option, K* retkey, Transaction* tx, LockOption lock, MapInformationEntryIterator* iterator, RealTimeCondition<K>* condition) {

	ThreadContext<K>* ctxt;
	if (tx != null) {
		ctxt = getTransactionContext(tx);

	} else {
		ctxt = m_threadContext.getContext();
		#ifdef CXX_LANG_MEMORY_DEBUG
		ctxt->setTransaction(null, true);
		#else
		ctxt->setTransaction(null);
		#endif
	}

	ctxt->setIterator(iterator);
	ctxt->setCondition(condition);

	// XXX: ignoring primary is CURSOR_PART 
	m_keyBuilder->setIgnorePrimary(key, true);

	K getkey = (K) key;
	K setkey = (retkey != null) ? *retkey : ctxt->getKey1();

	RETRY:
	boolean again = false;
	boolean result = false;

	switch (option)
	{
		case EXACT:
		{
			option = NEXT;
			result = get(ctxt, getkey, value, &setkey, &again, lock);
			break;
		}
		case EXACT_OR_NEXT:
		{
			option = NEXT;
			result = get(ctxt, getkey, value, &setkey, &again, lock);
			if (result == true) {
				break;
			}
		}
		case NEXT:
		case NEXT_MATCH:
		{
			result = getNext(ctxt, getkey, value, &setkey, &again, lock, (option == NEXT_MATCH));
			break;
		}
		case EXACT_OR_PREVIOUS:
		{
			option = PREVIOUS;
			result = get(ctxt, getkey, value, &setkey, &again, lock);
			if (result == true) {
				break;
			}
		}
		case PREVIOUS:
		{
			result = getPrevious(ctxt, getkey, value, &setkey, &again, lock);
			break;
		}
		case FIRST:
		{
			option = NEXT;
			result = first(ctxt, value, &setkey, &again, lock);
			break;
		}
		case LAST:
		{
			option = PREVIOUS;
			result = last(ctxt, value, &setkey, &again, lock);
			break;
		}
	}

	m_keyBuilder->setIgnorePrimary(key, false);

	if (result == true) {
		if (again == true) {
			getkey = setkey;
			goto RETRY;
		}

		 if (lock != LOCK_NONE) {

		 	if (tx != null) {
				tx->setDirty(true);

				if (m_primaryIndex != null) {
					bytearray pkey = null;
					if (m_keyBuilder->hasHiddenKey() == true) {
						pkey = m_keyBuilder->getHiddenKey(setkey);
					}

					// XXX: pkey of null causes pkey to be derived in lockInformation
					XInfoRef& ctxtinfoRef = *ctxt->getInformation();
					result = m_primaryIndex->lockInformation(tx, pkey, ctxtinfoRef, lock, false /* primaryLocked */, true /* stitch */);

					if (result == true) {
						result = lockSecondary(tx, pkey, ctxtinfoRef, lock, false /* primaryLocked */, true /* stitch */);
					}

				} else if (m_hasSecondaryMaps == true) {
					XInfoRef& ctxtinfoRef = *ctxt->getInformation();
					result = lockSecondary(tx, Converter<K>::toData(setkey), ctxtinfoRef, lock, false /* primaryLocked */, true /* stitch */);
				}
			}

			ctxt->setInformation(null);
		}

		if (retkey != null) {
			*retkey = setkey;
		}
	}
	m_externalWorkTime = System::currentTimeMillis();

	return result;
}

template<typename K>
boolean RealTimeMap<K>::cursor(const K key, RealTimeIterator<K>* iterator, ReadOption option, Transaction* tx, LockOption lock) {

	RealTimeShare* share = (m_primaryIndex == null) ? &m_share : m_primaryIndex->getShare();

	nbyte value((share->getValueAverage() != -1) ? share->getValueAverage() : 1);

	K retkey = (K) Converter<K>::NULL_VALUE;

	boolean result = get(key, &value, option, &retkey, tx, lock);
	if (result == true) {
		iterator->reset(retkey);

	} else {
		iterator->reset(key);
	}

	return result;
}

template<typename K>
boolean RealTimeMap<K>::contains(const K key, ReadOption option, K* retkey, Transaction* tx, LockOption lock) {
	return get(key, null, option, retkey, tx, lock, null /* iterator */);
}

template<typename K>
boolean RealTimeMap<K>::put(const K key, const nbyte* value, WriteOption option, Transaction* tx, LockOption lock, uinttype position, ushorttype index, uinttype compressedOffset) {

	#ifdef DEEP_DEBUG
	if ((m_state > MAP_RUNNING) && (m_state != MAP_RECOVER)) {
		DEEP_LOG(ERROR, OTHER, "Invalid state: not running, %s\n", getFilePath());

		throw InvalidException("Invalid state: not running");
	}

	if (m_primaryIndex != null) {
		DEEP_LOG(ERROR, OTHER, "Put: secondary not supported, %s\n", getFilePath());

		throw PermissionException("Put: secondary not supported");
	}

	if (tx == null) {
		DEEP_LOG(ERROR, OTHER, "Put: transaction required, %s\n", getFilePath());

		throw PermissionException("Put: transaction required");
	}
	#endif

	return putTransaction(key, value, option, tx, lock, position, index, compressedOffset);
}

template<typename K>
boolean RealTimeMap<K>::remove(const K key, nbyte* value, DeleteOption option, Transaction* tx, LockOption lock, boolean forCompressedUpdate) {

	#ifdef DEEP_DEBUG
	if ((m_state > MAP_RUNNING) && (m_state != MAP_RECOVER)) {
		DEEP_LOG(ERROR, OTHER, "Invalid state: not running, %s\n", getFilePath());

		throw InvalidException("Invalid state: not running");
	}

	if (m_primaryIndex != null) {
		DEEP_LOG(ERROR, OTHER, "Remove: secondary not supported yet, %s\n", getFilePath());

		throw PermissionException("Remove: secondary not supported yet");
	}

	if ((option == DELETE_IGNORE) && (getSecondaryCount() != 0)) {
		DEEP_LOG(ERROR, OTHER, "Remove: ignore with secondary index not supported, %s\n", getFilePath());

		throw PermissionException("Remove: ignore with secondary index not supported");
	}

	if (tx == null) {
		DEEP_LOG(ERROR, OTHER, "Remove: transaction required, %s\n", getFilePath());

		throw PermissionException("Remove: transaction required");
	}
	#endif

	return removeTransaction(key, value, option, tx, lock, forCompressedUpdate);
}

template<typename K>
boolean RealTimeMap<K>::reserve(ulongtype offset, ulongtype block, ulongtype& first, ulongtype& reserved, Transaction* tx) {

	if (m_hasReservation == false) {
		DEEP_LOG(ERROR, OTHER, "reservation: not configured, %s\n", getFilePath());

		throw PermissionException("reservation: not configured");

	} else if (block < 1) {
		block = getSegmentSize();
	}

	boolean result = false;

	ulongtype maxKey = m_keyBuilder->maxReservedKey();

	m_reserveLock.lock();
	{
		if (m_incrementCount < maxKey) {
			first = m_incrementCount + 1;
			if (first < offset) {
				first = offset;
			}

			ulongtype available = maxKey - first + 1;
			reserved = (available < block) ? available : block;

			m_incrementCount = first + (reserved - 1);

			// TODO: fix cast
			const SchemaBuilder<K>* schemaBuilder = (m_primaryIndex == null) ? m_schemaBuilder : ((RealTimeMap<K>*) m_primaryIndex)->m_schemaBuilder;
			((SchemaBuilder<K>*) schemaBuilder)->setAutoIncrementValue(m_incrementCount + 1);

			result = true;

		} else {
			first = maxKey;

			reserved = 0;
		}
	}
	m_reserveLock.unlock();

	// TODO: implement secondary key space reserve (see ::lockInformation)
	if (m_primaryIndex == null) {
		Conductor* conductor = tx->getConductor(getIdentifier());

		conductor->setReservedKey(first);
		conductor->setReservedBlock(reserved);
	}

	return result;
}

template<typename K>
longtype RealTimeMap<K>::size(Transaction* tx, boolean recalculate) {
	// XXX: used during recovery mode only 
	if (recalculate == true) {
		ThreadContext<K>* ctxt = m_threadContext.getContext();

		ArrayList<Segment<K>*> indexSegmentList(Properties::LIST_CAP);

		PurgeReport purgeReport;

		RETRY:
		// XXX: safe context lock: multiple readers / no writer on the branch tree
		m_threadContext.readLock();
		{
			if (m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {
				typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);

				for (int i = 0; i < indexSegmentList.ArrayList<Segment<K>*>::size(); i++) {
					Segment<K>* segment = indexSegmentList.ArrayList<Segment<K>*>::get(i);
					segment->decref();
				}

				indexSegmentList.ArrayList<Segment<K>*>::clear();

				m_branchSegmentTreeMap.entrySet(&stackSegmentSet);
				MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
				while (segIter->MapSegmentEntrySetIterator::hasNext()) {
					MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
					Segment<K>* segment = segEntry->getValue();

					if (segment->getSummary() == true) {
						segment->incref();

						m_threadContext.readUnlock();

						if (forceSetupSegment(ctxt, segment, true /* physical */, false /* values */) == true) {
							segment->unlock();
						}

						goto RETRY;
					}

					segment->incref();

					indexSegmentList.ArrayList<Segment<K>*>::add(segment);
				}
			}
		}
		m_threadContext.readUnlock();

		typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

		longtype entries = 0;
		for (int i = 0; i < indexSegmentList.ArrayList<Segment<K>*>::size(); i++) {
			Segment<K>* segment = indexSegmentList.ArrayList<Segment<K>*>::get(i);

			if (forceSetupSegment(ctxt, segment, true /* physical */, false /* values */) == false) {
				if ((segment->getBeenDeleted() == true) || (segment->SegTreeMap::size() == 0)) {
					continue;
				}

				DEEP_LOG(ERROR, OTHER, "Invalid size recalculate: segment not setup: %s\n", getFilePath());

				throw InvalidException("Invalid size recalculate: segment not setup");
			}

			if ((m_valueCompressMode == true) || (segment->getPaged() == false) || (segment->getDirty() == true)) {
				entries += getActiveSize(ctxt, segment);

			} else {
				entries += segment->SegTreeMap::size();
			}

			// XXX: check whether cache pressure requires segment purging
			purgeSegment(ctxt, segment, false /* not growing */, false /* index */, false /* semi */, null /* purgeList */, null, purgeReport);

			segment->unlock();
		}

		if (Properties::getDebugEnabled(Properties::KEY_SECONDARY) == true) {
			if ((m_primaryIndex != null) && (m_primaryIndex->getEntrySize() != entries)) {
				DEEP_LOG(ERROR, OTHER, "Secondary paging entry size mismatch: primary=%lld != actual=%lld, %s\n", m_primaryIndex->getEntrySize(), entries, getFilePath());

				throw InvalidException("Secondary paging entry size mismatch");
			}
		}

		setEntrySize(entries);
	}

	longtype entries = (m_primaryIndex == null) ? getEntrySize() : m_primaryIndex->getEntrySize();

	if (tx != null) {
		Conductor* conductor = (Conductor*) tx->getConductor(getIdentifier());
		if (conductor != null) {
			entries += (conductor->m_createStats - conductor->m_deleteStats);
		}
	}

	// TODO: incorporate isolation requirements (e.g. repeatable-read)

	return entries;
}

template<typename K>
void RealTimeMap<K>::markSchema(void) {
	if (m_memoryMode == true) {
		return;
	}

	#ifdef DEEP_DEBUG
	if (m_primaryIndex != null) {
		DEEP_LOG(ERROR, OTHER, "Invalid schema checkpoint: not primary: %s\n", getFilePath());

		throw InvalidException("Invalid schema checkpoint: not primary");
	}
	#endif

	if (m_schemaCheckpoint != 0) {
		DEEP_LOG(ERROR, OTHER, "Invalid schema checkpoint: already marked: %llu\n", m_schemaCheckpoint);

		throw InvalidException("Invalid schema checkpoint: alread marked");
	}

	m_schemaCheckpoint = m_share.getSrtWriteFileList()->last()->length();
}

template<typename K>
void RealTimeMap<K>::commitSchema(void) {
	if (m_memoryMode == true) {
		return;
	}

	#ifdef DEEP_DEBUG
	if (m_primaryIndex != null) {
		DEEP_LOG(ERROR, OTHER, "Invalid schema checkpoint: not primary: %s\n", getFilePath());

		throw InvalidException("Invalid schema checkpoint: not primary");
	}
	#endif

	if (m_schemaCheckpoint == 0) {
		DEEP_LOG(ERROR, OTHER, "Invalid schema checkpoint: already committed\n");

		throw InvalidException("Invalid schema checkpoint: already committed");
	}

	m_schemaCheckpoint = 0;
}

template<typename K>
void RealTimeMap<K>::rollbackSchema(void) {
	if (m_memoryMode == true) {
		return;
	}

	#ifdef DEEP_DEBUG
	if (m_primaryIndex != null) {
		DEEP_LOG(ERROR, OTHER, "Invalid schema checkpoint: not primary: %s\n", getFilePath());

		throw InvalidException("Invalid schema checkpoint: not primary");
	}
	#endif

	if (m_schemaCheckpoint == 0) {
		DEEP_LOG(ERROR, OTHER, "Invalid schema checkpoint: already rolled back\n");

		throw InvalidException("Invalid schema checkpoint: already rolled back");
	}

	m_share.getSrtWriteFileList()->last()->setLength(m_schemaCheckpoint, true);
	m_share.getSrtWriteFileList()->last()->seek(m_schemaCheckpoint);

	m_schemaCheckpoint = 0;
}

template<typename K>
longtype RealTimeMap<K>::range(const K* skey, const K* ekey, Transaction* tx) {

	ThreadContext<K>* ctxt = (tx != null) ? getTransactionContext(tx) : m_threadContext.getContext();

	RETRY:
	longtype total = 0;
	MapSegmentIterator iterator;

	// XXX: safe context lock: multiple readers / no writer on the branch tree
	m_threadContext.readLock();
	{
		K startKey = skey ? *skey : m_branchSegmentTreeMap.TreeMap<K, Segment<K>*>::firstKey();

		m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::iterator(startKey, &iterator, false);
		if (iterator.hasNext() == false) {
			m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::iterator(startKey, &iterator, true);
		}

		K endKey = ekey ? *ekey : m_branchSegmentTreeMap.TreeMap<K, Segment<K>*>::lastKey();

		const MapEntry<K,Segment<K>*>* eindex = m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::higherEntry(endKey);

		while (iterator.MapSegmentIterator::hasNext()) {
			MapEntry<K,Segment<K>*>* segEntry = iterator.MapSegmentIterator::next();
			if (segEntry == eindex) {
				break;
			}

			Segment<K>* segment = segEntry->getValue();
			if (segment->getSummary() == true) {
				segment->incref();

				m_threadContext.readUnlock();

				if (forceSetupSegment(ctxt, segment, false /* physical */, false /* values */) == true) {
					segment->unlock();
				}

				goto RETRY;

			} else if (segment->initialized() == false) {
				// XXX: summary segment could be in the process of being opened in fillSegment (flags have been cleared)
				m_threadContext.readUnlock();
				goto RETRY;
			}

			// XXX: more accurately estimate keys in range for beginning segment, helps with small ranges
			if ((total == 0) && (m_keyBuilder->getKeyParts() > 1)) {
				total += m_keyBuilder->range(startKey, endKey, segment->vsize(), segment->getCardinality());

			} else {
				total += segment->vsize();
			}
		}
	}
	m_threadContext.readUnlock();

	return total;
}

template<typename K>
boolean RealTimeMap<K>::optimize(OptimizeOption optType, Transaction* tx) {

	boolean offline = false;

	// XXX: safe context lock: multiple readers / no writer on the branch tree
	m_threadContext.readLock();
	{
		if (m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {

			switch(optType) {
				case OPTIMIZE_OFFLINE_KEY:
					DEEP_LOG(INFO, OPTIZ, "start optimization (OFFLINE KEY) for store: %s\n", getFilePath());
					offline = true;
				case OPTIMIZE_ONLINE_KEY:
					DEEP_LOG(INFO, OPTIZ, "start optimization (ONLINE KEY) for store: %s\n", getFilePath());
					if (m_irtForceReorganization == true) {
						m_threadContext.readUnlock();
						return true;
					}

					m_optimizePaging++;
					m_irtForceReorganization = true;

					m_irtBuildReorganization = true;
					// XXX: invoked on index thread
					#if 0
					pagingFileManagement(true /* lock */, true /* create file */);
					#endif

					// XXX: force re-termination (e.g. finalize and summarize)
					m_endwiseLrtLocality = Locality::LOCALITY_NONE;
					break;
				case OPTIMIZE_OFFLINE_VALUE:
					DEEP_LOG(INFO, OPTIZ, "start optimization (OFFLINE VALUE) for store: %s\n", getFilePath());
					offline = true;
				case OPTIMIZE_ONLINE_VALUE:
					DEEP_LOG(INFO, OPTIZ, "start optimization (ONLINE VALUE) for store: %s\n", getFilePath());
					if (m_vrtForceReorganization == true) {
						m_threadContext.readUnlock();
						return true;
					}

					if (m_primaryIndex != null) {
						m_threadContext.readUnlock();
						return true;
					}

					m_optimizeStream++;
					m_vrtForceReorganization = true;
					streamFileManagement(true /* lock */, true /* create file */);
					break;
				case OPTIMIZE_OFFLINE_PAIRED:
					DEEP_LOG(INFO, OPTIZ, "start optimization (OFFLINE PAIRED) for store: %s\n", getFilePath());
					offline = true;
				case OPTIMIZE_ONLINE_PAIRED:
					DEEP_LOG(INFO, OPTIZ, "start optimization (ONLINE PAIRED) for store: %s\n", getFilePath());
					if (m_vrtForceReorganization == true) {
						m_threadContext.readUnlock();
						return true;
					}

					if (m_irtForceReorganization == true) {
						m_threadContext.readUnlock();
						return true;
					}

					if (m_primaryIndex == null) {
						m_optimizeStream++;
						m_vrtForceReorganization = true;
						streamFileManagement(true /* lock */, true /* create file */);
					}

					m_optimizePaging++;
					m_irtForceReorganization = true;

					m_irtBuildReorganization = true;
					// XXX: invoked on index thread
					#if 0
					pagingFileManagement(true /* lock */, true /* create file */);
					#endif

					break;
				default:
					m_threadContext.readUnlock();
					return false;
			}
		}
	}
	m_threadContext.readUnlock();

	if (offline == true) {
		longtype start = System::currentTimeMillis();
		{
			while ((m_irtForceReorganization == true) || (m_vrtForceReorganization == true)) {
				Thread::sleep(Properties::DEFAULT_CACHE_SLEEP);
			}
		}
		longtype stop = System::currentTimeMillis();

		DEEP_LOG(INFO, OPTIZ, "complete OFFLINE optimization for store: %s, elapsed: %lld\n", getFilePath(), (stop-start));
	}

	return true;
}

template<typename K>
longtype RealTimeMap<K>::length(FileOption fileType, Transaction* tx) {

	longtype total = 0;

	if (fileType == FILE_KEY) {
		// TODO: defragmentation needs to lock (i.e. during archive/erase)
		m_share.getIrtWriteFileList()->lock();
		{
			Iterator<MeasuredRandomAccessFile*>* iter = m_share.getIrtWriteFileList()->iterator();
			while (iter->hasNext() == true) {
				MeasuredRandomAccessFile* iwfile = iter->next();
				if (iwfile == null) {
					continue;
				}

				total += iwfile->length();
			}
			Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
		}
		m_share.getIrtWriteFileList()->unlock();

	} else if (m_primaryIndex == null) {
		// TODO: defragmentation needs to lock (i.e. during archive/erase)
		m_share.getVrtWriteFileList()->lock();
		{
			Iterator<MeasuredRandomAccessFile*>* iter = m_share.getVrtWriteFileList()->iterator();
			while (iter->hasNext() == true) {
				MeasuredRandomAccessFile* vwfile = iter->next();
				if (vwfile == null) {
					continue;
				}

				total += vwfile->length();
			}
			Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
		}
		m_share.getVrtWriteFileList()->unlock();
	}

	return total;
}

template<typename K>
void RealTimeMap<K>::cardinality(longtype* stats, Transaction* tx, boolean recalculate) {

	ThreadContext<K>* ctxt = null;
	if (recalculate == true) {
		ctxt = m_threadContext.getContext();

		RETRY:
		// XXX: safe context lock: multiple readers / no writer on the branch tree
		m_threadContext.readLock();
		{
			if (m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {
				typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);
				m_branchSegmentTreeMap.entrySet(&stackSegmentSet);

				MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
				while (segIter->MapSegmentEntrySetIterator::hasNext()) {
					MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
					Segment<K>* segment = segEntry->getValue();

					if (segment->getSummary() == true) {
						DEEP_LOG(DEBUG, OTHER, "store: %s, unsummarize for cardinality... %p\n", getFilePath(), segment);

						segment->incref();

						m_threadContext.readUnlock();

						if (forceSetupSegment(ctxt, segment, true /* physical */, true /* values */) == true) {
							segment->unlock();
						}

						goto RETRY;
					}
				}
			}
		}
		m_threadContext.readUnlock();
	}

	// XXX: safe context lock: multiple readers / no writer on the branch tree
	m_threadContext.readLock();
	{
		if (m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {

			Segment<K>* recalSegment = null;
			if (recalculate == true) {
				DEEP_LOG(DEBUG, OTHER, "store: %s, cardinality recalculation [%d]... %p\n", getFilePath(), m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size(), this);

				recalSegment = new Segment<K>(m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER, Versions::GET_PROTOCOL_CURRENT(), m_keyBuilder->getKeyParts());
			}

			K lastKey = (K) Converter<K>::NULL_VALUE;

			typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);
			m_branchSegmentTreeMap.entrySet(&stackSegmentSet);

			MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
			while (segIter->MapSegmentEntrySetIterator::hasNext()) {
				MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
				Segment<K>* segment = segEntry->getValue();

				if (recalSegment != null) {
					segment->incref();

					if (forceSetupSegment(ctxt, segment, true /* physical */, false /* values */, false /* force */) == false) {
						DEEP_LOG(WARN, OTHER, "Failure to lock on cardinality recalculation, %p, %s\n", segment, getFilePath());
						continue;
					}

					boolean indexed = false;
					segment->transfer(recalSegment);

					typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

					recalSegment->SegTreeMap::entrySet(&stackSegmentItemSet);
					MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
					while (infoIter->MapInformationEntrySetIterator::hasNext()) {
						SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();
						segment->SegTreeMap::put(infoEntry->getKey(), infoEntry->getValue());

						if (indexed == true) {
							continue;
						}

						Information* info = isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
						if ((info == null) || (info->getDeleting() == true)) {
							continue;
						}

						#if 0
						if (info->hasFields(Information::WRITE) == false) {
							Information* newinfo = Information::newInfo(Information::WRITE, info->getFileIndex(), info->getFilePosition(), info->getSize());

							if (info->getStoryLock() == null) {
								throw InvalidException("Not implemented"); // TODO: info->initStoryLock();
							}

							throw InvalidException("Not implemented"); // TODO: newinfo->setStoryLock(info->getStoryLock());
							newinfo->setLevel(Information::LEVEL_COMMIT);

							if (info->getData() != null) {
								nbyte tmpValue((const bytearray) info->getData(), info->getSize());
								copyValueIntoInformation(newinfo, &tmpValue);

							} else {
								readValue(ctxt, (Information*) info, infoEntry->getKey());

								newinfo->setData(info->getData());
								copyValueFromInformation(ctxt, newinfo, infoEntry->getKey());
							}

							Information* next = info->getNext();
							if (next != null) {
								newinfo->setNext(next);
							}

							info->setDeleting(true);
							info->setNext(newinfo);

						} else {
							info->setIndexed(m_indexValue, false);
						}
						#else
						info->setIndexed(m_indexValue, false);
						#endif

						segment->setDirty(true);
						indexed = true;
					}

					segment->unlock();
				}

				// XXX: key parts of "one" means cardinality is equal to the number of rows
				if (m_keyBuilder->getKeyParts() == 1) {
					stats[0] += segment->vsize();
					continue;
				}

				for (uinttype i = 0; i < m_keyBuilder->getKeyParts(); i++) {
					stats[i] += segment->getCardinality()[i];
				}

				// XXX: adjust for overlap
				if (lastKey != (K) Converter<K>::NULL_VALUE) {
					inttype pos = 0;
					m_comparator->compare(lastKey, segEntry->getKey(), &pos);

					if (pos > 0) {
						stats[0]--;
						stats[pos]++;
					}
				}

				#if 0
				// TODO: for purged segments, lastKey is firstKey
				if (segment->tryLock() == true) {
					lastKey = segment->SegTreeMap::lastKey();

					segment->unlock();

				} else {
					lastKey = segEntry->getKey();
				}
				#else
				lastKey = segEntry->getKey();
				#endif
			}

			delete recalSegment;
		}
	}
	m_threadContext.readUnlock();
}

template<typename K>
void RealTimeMap<K>::unlock(Transaction* tx) {
	// TODO: unlock last row read by this transaction
}

template<typename K>
boolean RealTimeMap<K>::discover(boolean reserve, Transaction* tx) {

	if ((m_state != MAP_INITIAL) && (m_state != MAP_INDXING) && (m_state != MAP_RUNNING)) {
		DEEP_LOG(ERROR, OTHER, "Invalid state: operation in progress, %d, %s\n", m_state, getFilePath());

		throw InvalidException("Invalid state: operation in progress");
	}

	// TODO: allow for dynamic discovery (i.e. add data files to be managed)
	if (m_state == MAP_RUNNING) {
		unmount();
	}

	return RealTimeDiscover<K>::statically(this, reserve);
}

template<typename K>
boolean RealTimeMap<K>::mount(boolean reserve, Transaction* tx) {

	boolean success = true;

	m_mount = MOUNT_OPENING;

	DEEP_LOG(DEBUG, DCVRY, "store: %s, mount start... %p\n", getFilePath(), this);

	synchronized(this) {
		if ((m_state != MAP_INITIAL) && (m_state != MAP_INDXING)) {
			DEEP_LOG(ERROR, OTHER, "Invalid state: %d, %s\n", m_state, getFilePath());

			throw InvalidException("Invalid state");
		}

		if ((m_associated == true) && (m_primaryIndex == null)) {
			DEEP_LOG(ERROR, OTHER, "No associated primary: %s\n", getFilePath());

			throw InvalidException("No associated primary");
		}

		longtype start = System::currentTimeMillis();
		{
			if ((m_share.getOptions() & O_DELETE) == O_DELETE) {
				clobber();
			}

			if ((m_share.getOptions() & O_CREATE) == O_CREATE) {
				success = discover(reserve, tx);

				m_resource.add(this);

				if (success == true) {
					// XXX: assume primary is set due to indexing state 
					if (m_state == MAP_INDXING) {
						success = m_primaryIndex->indexSecondary(tx, this);
					}

					// TODO: create another state for faster error recovery
					if (success == true) {
						m_state = MAP_INDXING;
					}

				} else {
					recoverBegin();
				}
			}
		}
		longtype stop = System::currentTimeMillis();

		longtype elapsed = stop-start;

		if (success == true) {
			DEEP_LOG(DEBUG, DCVRY, "store: %s, elapsed: %lld, segments: %d, size: %lld, mount finished... %p\n", getFilePath(), elapsed, getTotalSegments(), size(), this);
		}

		if ((success == false) && (m_threadContext.getErrorCode() == ERR_SUCCESS)) {
			m_threadContext.setErrorCode(ERR_GENERAL);
		}

		if ((m_dynamic == true) && (m_primaryIndex != null) && (success == true)) {
			recoverComplete(m_primaryIndex->getEntrySize(), m_primaryIndex->getCurrentLrtLocality(), elapsed);
		}
	}

	if ((m_share.getOptions() & O_STATICCONTEXT) == O_STATICCONTEXT) {
		RealTimeAtomic::reset();
	}
	m_mount = MOUNT_OPENED;

	return success;
}

template<typename K>
boolean RealTimeMap<K>::unmount(boolean destroy, Transaction* tx) {

	inttype retries = 0;

	m_mount = MOUNT_CLOSING;

	DEEP_LOG(DEBUG, UNMNT, "store: %s, start... %p\n", getFilePath(), this);

	RETRY:
	synchronized(this) {
		if ((m_state != MAP_RUNNING) && (m_state != MAP_INDXING)) {
			return false;
		}

		if (m_threadIndexing == true) {
			DEEP_LOG(DEBUG, UNMNT, "active indexing %d, waiting to unmount: %s, retry... %p\n", retries++, getFilePath(), this);

			longtype timeout = (longtype) ((m_memoryMode == false) ? Properties::DEFAULT_CACHE_LIMIT : Properties::DEFAULT_CACHE_LIMIT / 10); 
			wait((longtype) timeout);

			goto RETRY;
		}

		setDynamic(false);
		m_state = MAP_EXITING;

		RealTimeShare* share = null;

		// XXX: first finalize schema information state before officially unmounting
		if (m_memoryMode == false) {
			RandomAccessFile* swfile;
			const SchemaBuilder<K>* schemaBuilder;

			if (m_primaryIndex == null) {
				swfile = m_share.getSrtWriteFileList()->last();
				schemaBuilder = m_schemaBuilder;
				share = &m_share;

			} else {
				swfile = m_primaryIndex->getShare()->getSrtWriteFileList()->last();
				// TODO: fix cast
				schemaBuilder = ((RealTimeMap<K>*) m_primaryIndex)->m_schemaBuilder;
				share = m_primaryIndex->getShare();
			}

			if (m_hasReservation == true) {
				if (schemaBuilder->getAutoIncrementValue() > (maximumKey() + 1)) {
					m_share.acquire(swfile);
					{
						RealTimeSchema_v1<K>::updateAutoIncrementValue(schemaBuilder, swfile);
					}
					m_share.release(swfile);
				}
			}
		}

		longtype start = System::currentTimeMillis();
		{
			flushSegmentMemory();

			if (m_memoryMode == false) {
				// XXX: update key statistics in xrt
				{
					m_share.getIrtWriteFileList()->lock();
					share->getXrtWriteFileList()->lock();
					{
						RealTimeAdaptive_v1<K>::writeKeyStatistics(this, true /* force */);
					}
					share->getXrtWriteFileList()->unlock();
					m_share.getIrtWriteFileList()->unlock();
				}

				if (m_primaryIndex == null) {
					// XXX: update value statistics in xrt
					m_share.getVrtWriteFileList()->lock();
					m_share.getXrtWriteFileList()->lock();
					{
						RealTimeAdaptive_v1<K>::writeValueStatistics(this, true /* force */);
					}
					m_share.getXrtWriteFileList()->unlock();
					m_share.getVrtWriteFileList()->unlock();

					// XXX: update storage statistics in xrt
					RandomAccessFile* xwfile = m_share.getXrtWriteFileList()->last();
					if (xwfile != null) {
						m_share.acquire(xwfile);
						{
							longtype total = 0;
							{
								sizeStorage(&total);

								for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
									RealTime* rt = m_share.getSecondaryList()->get(i);
									if (rt == null) {
										// XXX: allow for dynamic add/remove secondary index
										continue;
									}

									rt->sizeStorage(&total);
								}

								m_extraStats.setTotalSize(total);
							}

							ExtraStatistics_v1::updateSize(xwfile, &m_extraStats);

							xwfile->setOnline(false);
						}
						m_share.release(xwfile);

					} else {
						DEEP_LOG(WARN, STATS, "store: %s, failed to find last xrt file\n", getFilePath());
					}
				}
			}

			m_resource.remove(this);

			if (destroy == true) {
				deleteCacheAndFileMemory();
			}

			m_state = MAP_STOPPED;

			// XXX: this will change m_state to MAP_INITIAL
			initialize();
		}
		longtype stop = System::currentTimeMillis();

		DEEP_LOG(DEBUG, UNMNT, "store: %s, elapsed: %lld, segments: %d, finish... %p\n", getFilePath(), (stop-start), getTotalSegments(), this);
	}

	m_mount = MOUNT_CLOSED;

	return true;
}

template<typename K>
boolean RealTimeMap<K>::clobber(boolean final) {
	struct Util {
		void setSecondaryMemoryMode(RealTimeShare* share, boolean flag) {
			for (int i = 0; i < share->getSecondaryList()->size(); i++) {
				RealTime* rt = share->getSecondaryList()->get(i);
				if (rt == null) {
					// XXX: allow for dynamic add/remove secondary index
					continue;
				}

				rt->setMemoryMode(flag);
			}
		}
	} util;

	if ((m_state != MAP_INITIAL) && (m_state != MAP_INDXING) && (m_state != MAP_RUNNING)) {
		DEEP_LOG(ERROR, OTHER, "Invalid state: operation in progress, %d, %s\n", m_state, getFilePath());

		throw InvalidException("Invalid state: operation in progress");
	}

	if (final == true) {
		RealTimeResource::setSkipGarbage(true);
	}

	if (m_state == MAP_RUNNING) {
		boolean memoryMode = m_memoryMode;

		if ((final == true) && (m_primaryIndex == null)) {
			util.setSecondaryMemoryMode(&m_share, true);
		}

		// XXX: ensure cache is not flushed
		m_memoryMode = true;
		{
			unmount();
		}
		m_memoryMode = memoryMode;

		if ((final == true) && (m_primaryIndex == null)) {
			util.setSecondaryMemoryMode(&m_share, memoryMode);
		}
	}

	MapFileUtil::clobber(getFilePath(), true);
	if (MapFileUtil::deepFilesExist(getFilePath()) == false) {
		char* dbname = RealTimeAtomic::getDatabaseName(getFilePath());
		RealTimeAtomic::reset(dbname);
		delete[] dbname;
	}	

	m_checkptRequested = false;
	m_checkpointComplete = true;
	m_checkpointValid = true;

	m_recoverLrtLocality = Locality::LOCALITY_NONE;
	m_summaryLrtLocality = Locality::LOCALITY_NONE;
	m_endwiseLrtLocality = Locality::LOCALITY_NONE;
	m_checkptLrtLocality = Locality::LOCALITY_NONE;
	m_currentLrtLocality = Locality::LOCALITY_NONE;

	if (final == true) {
		RealTimeResource::setSkipGarbage(false);
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::clear(boolean final) {

	clobber(true /* final */);

	if (final == false) {
		if (mount(m_hasReservation) == true) {
			m_state = MAP_RUNNING;
		}
	}

	return true;
}

template<typename K>
boolean RealTimeMap<K>::recover(boolean rebuild) {

	synchronized(this) {
		if (m_primaryIndex != null) {
			return false;
		}

		const boolean nonempty = (m_share.getLrtWriteFileList()->size() > 0) && ((m_share.getLrtWriteFileList()->size() != 1) || (m_share.getLrtWriteFileList()->first()->length() > Properties::DEFAULT_FILE_HEADER));
		RealTimeLocality finalLrtLocality = RealTimeLocality::LOCALITY_NONE;
		if (nonempty == true) {
			MeasuredRandomAccessFile* lwfile = m_share.getLrtWriteFileList()->last();
			finalLrtLocality = RealTimeLocality(lwfile->getFileIndex(), lwfile->length(), RealTimeLocality::VIEWPOINT_NONE);
			DEEP_LOG(DEBUG, DCVRY, "discovered final lrt locality %s (%u), %s\n", (const char*) finalLrtLocality.toString(), getRecoveryEpoch(), getFilePath());

		} else {
			DEEP_LOG(DEBUG, DCVRY, "discovered empty map, %s\n", getFilePath());
		}

		// XXX: order the irt files across _all_ indexes by descending (order(lrtFileIndex),lrtFileLength)
		Comparator<MeasuredRandomAccessFile*> irtCmp(m_share.getLrtWriteFileList());
		PriorityQueue<MeasuredRandomAccessFile*> iwrites(&irtCmp);
		RealTimeRecovery<K>::addAll(iwrites, m_share.getIrtWriteFileList());
		if (m_hasSecondaryMaps == true) {
			for (inttype i = 0; i < m_share.getSecondaryList()->size(); ++i) {
				RealTimeRecovery<K>::addAll(iwrites, m_share.getSecondaryList()->get(i)->getShare()->getIrtWriteFileList());
			}
		}

		m_currentLrtLocality = Locality(m_streamIndex, Properties::DEFAULT_FILE_HEADER, Locality::VIEWPOINT_NONE);

		longtype start = System::currentTimeMillis();
		{
			boolean seenAllIndexes = false;
			RETRY:
			boolean useSummaries = RealTimeRecovery<K>::findRecoveryBoundary(this, iwrites, &irtCmp, nonempty, finalLrtLocality, seenAllIndexes);
			// XXX: no index files found... restart from the beginning (e.g. delete stray irt(s))
			// XXX: when FLUX is not enabled, do not do partial replay
			if ((useSummaries == false) || ((Properties::getCheckpointMode() == Properties::CHECKPOINT_OFF) && (rebuild == true))) {
				if ((rebuild == false) && (getRecoverLrtLocality().isNone() == false)) {
					// XXX: only warn on non-empty map
					if (nonempty == true) {
						DEEP_LOG(WARN, RCVRY, "force error recovery %s\n", getFilePath());
						rebuild = true;
					}
					recoverBegin();
				}

				recoverGenesis();

				if (m_hasSecondaryMaps == true) {
					for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
						RealTime* rt = m_share.getSecondaryList()->get(i);
						if (rt == null) {
							// XXX: allow for dynamic add/remove secondary index
							continue;
						}

						rt->recoverGenesis();
					}
				}

				if ((rebuild == true) && (getRecoverLrtLocality() != finalLrtLocality)) {
					recoverBegin();

					if (m_share.getLrtWriteFileList()->size() > 0) {
						if (Properties::getDebugEnabled(Properties::ONLY_PARTIAL_REPLAY) == true) {
							DEEP_LOG(ERROR, RCVRY, "recovery replay (full) initiated, %s\n", getFilePath());

							throw InvalidException("Recovery replay (full) initiated");
						}
						if (Properties::getDebugEnabled(Properties::NO_RECOVERY_REPLAY) == true) {
							DEEP_LOG(ERROR, RCVRY, "recovery replay initiated, %s\n", getFilePath());

							throw InvalidException("Recovery replay initiated");
						}
						setRecoveryEpoch(getRecoveryEpoch() + 1);
						unsynchronized(this) {
							RealTimeVersion<K>::recoverRealTime(this, m_share.getLrtWriteFileList()->first()->getFileIndex() /* XXX: first lrt */, Properties::DEFAULT_FILE_HEADER);
						}
					}
				}

			} else {
				if (RealTimeRecovery<K>::initializeIndexing(this) == false) {
					DEEP_LOG(ERROR, RCVRY, "Unable to initialize indexing at %s, %s\n", (const char*) getRecoverLrtLocality().toString(), getFilePath());
					rebuild = true;
					goto RETRY;
				}

				if ((rebuild == true) && (getRecoverLrtLocality() != finalLrtLocality)) {
					if (Properties::getDebugEnabled(Properties::NO_PARTIAL_REPLAY) == true) {
						DEEP_LOG(ERROR, RCVRY, "recovery replay (partial) initiated, %s\n", getFilePath());

						throw InvalidException("Recovery replay (partial) initiated");
					}
					if (Properties::getDebugEnabled(Properties::NO_RECOVERY_REPLAY) == true) {
						DEEP_LOG(ERROR, RCVRY, "recovery replay initiated, %s\n", getFilePath());

						throw InvalidException("Recovery replay initiated");
					}

					recoverBegin();

					setRecoveryEpoch(getRecoveryEpoch() + 1);

					unsynchronized(this) {
						RealTimeVersion<K>::recoverRealTime(this, getRecoverLrtLocality().getIndex(), getRecoverLrtLocality().getLength());
					}

				} else {
					m_currentLrtLocality = getRecoverLrtLocality();
				}
			}
		}
		longtype stop = System::currentTimeMillis();

		#if 0
		if (m_primaryIndex == null) {
			RealTimeDiscover<K>::orderLogging(this);
		}
		#endif

		const boolean recalculate = (rebuild == true) || (m_recoverSize < 0);
		if (recalculate == false) {
			if (Properties::getDebugEnabled(Properties::KEY_PRIMARY) == true) {
				DEEP_LOG(DEBUG, DCVRY, "Verifying entry size (read=%lld), %s\n", m_recoverSize, getFilePath());

				if (m_recoverSize != size(null, true /* XXX: recalculate */)) {
					DEEP_LOG(ERROR, OTHER, "Paging entry size mismatch: read=%lld != actual=%lld, %s\n", m_recoverSize, getEntrySize(), getFilePath());

					throw InvalidException("Paging entry size mismatch");
				}
			}

			setEntrySize(m_recoverSize);
		}

		// XXX: ensure current LRT locality can be chosen as checkpoint
		if ((rebuild == false) && (m_currentLrtLocality.isNone() == false)) {
			m_currentLrtLocality = m_currentLrtLocality.updateViewpoint(Transaction::getNextViewpoint());
		}

		recoverComplete(size(null, recalculate), m_currentLrtLocality, stop-start, rebuild);

		if (m_hasSecondaryMaps == true) {
			for (int i = 0; i < m_share.getSecondaryList()->size(); i++) {
				RealTime* rt = m_share.getSecondaryList()->get(i);
				if (rt == null) {
					// XXX: allow for dynamic add/remove secondary index
					continue;
				}

				rt->recoverComplete(size(), m_currentLrtLocality, stop-start, rebuild);
			}
		}
	}

	return true;
}

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEMAP_CXX_*/
