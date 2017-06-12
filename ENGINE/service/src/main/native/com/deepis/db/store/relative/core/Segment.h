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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_SEGMENT_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_SEGMENT_H_ 

#include "cxx/util/TreeMap.h"
#include "cxx/util/Comparator.h"
#include "cxx/util/NumberRangeSet.h"

#if 0 /* DEEP_USERLOCK */
	#include "cxx/util/concurrent/locks/UserSpaceLock.h"
#else
	#include "cxx/util/concurrent/locks/ReentrantLock.h"
#endif

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/Information.h"
#include "com/deepis/db/store/relative/core/RealTimeTypes.h"
#include "com/deepis/db/store/relative/core/RealTimeLocality.h"

using namespace cxx::util;
using namespace cxx::util::concurrent::locks;


namespace cxx { namespace util {

struct StreamReference {
	ushorttype m_fileIndex;
	// XXX: old is true iff all info references to this index are due to non-latest checkpoint isolations
	boolean m_old:1;
	FORCE_INLINE StreamReference(ushorttype index, boolean old) : m_fileIndex(index), m_old(old) { }
};

template<>
class Comparator<StreamReference*>  {
	public:
		FORCE_INLINE Comparator(void)  {
			// nothing to do
		}

		FORCE_INLINE int compare(const StreamReference* const& o1, const StreamReference* const& o2) const {
			if (o1->m_fileIndex > o2->m_fileIndex) {
				return 1; 
			} else if (o1->m_fileIndex == o2->m_fileIndex) {
				return 0;
			} else {
				return -1;	
			}
		}
};

}} // namespace

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename K>
struct InfoReference;

template<typename K>
class Segment : public RealTimeTypes<K>::SegTreeMap /*, public Lockable */ {

	private:
		#if 0 /* DEEP_USERLOCK */
		UserSpaceLock m_lock;
		#else
		ReentrantLock m_lock;
		#endif

		#if 0
		longtype m_version;
		#endif

		uinttype m_pagingPosition;
		union {
			uinttype m_streamPosition;
			uinttype m_recoveryEpoch; // XXX: used for summary segments only
		};

		ushorttype m_backReference;
		TreeSet<ushorttype> m_pagingIndexes;

		static const Comparator<StreamReference*> STREAM_REF_CMP;
		// XXX: note that stream references are 'fuzzy' and segment references may not always match cumulative info refs
		TreeSet<StreamReference*> m_streamIndexes;

		ubytetype m_fragmentCount;

		bytearray m_zipData;
		inttype m_zipSize;

		inttype m_uncompressedSize;

		// XXX: this is the last (most recent) lrt/vrt locality that may have influenced the indexing of this segment
		RealTimeLocality m_indexLocality;
		// XXX: this is the last (most recent) lrt/vrt locality at which this segment was checkpointed
		RealTimeLocality m_summarizedLocality;
		boolean m_checkpointSummarized;

		K m_indexOrderKey;

		volatile ushorttype m_refcnt;

		mutable LwUserSpaceReadWriteLock<uinttype, 0x7FFFFFFF> m_refLock;

		// XXX: locked on segment / local thread
		ubytetype m_stateFlags;
		ubytetype m_extraFlags;
		ubytetype m_moreFlags;

		// XXX: locked on indexed / global thread
		ubytetype m_indexFlags;

		boolean m_rolling;
		pthread_t m_rollOwner;
		TreeMap<inttype, uinttype> m_rowOwners;

		Segment();

	public:
		static const Comparator<ushorttype> USHORT_CMP;
		typedef NumberRangeSet<ushorttype&, ubytetype&, ushorttype> FileRangeSet;

		enum StateFlags {
			SEGMENT_FLAG_RANGEOVER = 0x01,
			SEGMENT_FLAG_VIEWPOINT = 0x02,
			SEGMENT_FLAG_VIRTUAL = 0x04,
			SEGMENT_FLAG_DIRTY = 0x08,
			SEGMENT_FLAG_PURGED = 0x10,
			SEGMENT_FLAG_PAGED = 0x20,
			SEGMENT_FLAG_SUMMARY = 0x40,
			SEGMENT_FLAG_STREAMED = 0x80
		};

		enum ExtraStateFlags {
			SEGMENT_XFLAG_BEENCREATED = 0x01,
			SEGMENT_XFLAG_BEENDELETED = 0x02,
			SEGMENT_XFLAG_BEENSURCEASED = 0x04,
			SEGMENT_XFLAG_BEENRESEEDED = 0x08,
			SEGMENT_XFLAG_BEENFILLED = 0x10,
			SEGMENT_XFLAG_BEENALTERED = 0x20,
			SEGMENT_XFLAG_RELOCATED = 0x40,
			SEGMENT_XFLAG_VIEWED = 0x80
		};

		enum MoreStateFlags {
			SEGMENT_MFLAG_REALIGNED = 0x01
		};

		enum IndexStateFlags {
			SEGMENT_IFLAG_FRAGKEY = 0x01,
			SEGMENT_IFLAG_FRAGVALUE = 0x02
		};

		FORCE_INLINE ushorttype ref(void) {
			return m_refcnt;
		}

		FORCE_INLINE ushorttype incref(void) {
			return __sync_add_and_fetch(&m_refcnt, 1);
		}

		FORCE_INLINE ushorttype decref(void) {
			return __sync_sub_and_fetch(&m_refcnt, 1);
		}

		#if 0
		FORCE_INLINE longtype getVersion(void) const {
			return m_version;
		}
		#endif

		FORCE_INLINE boolean initialized(void) const {
			return (Segment::vsize() != 0);
		}

		FORCE_INLINE boolean isReferenced(void) const {
			if (m_refLock.tryWriteLock() == true) {
				m_refLock.writeUnlock();
				return false;
			}
			return true;
		}

		FORCE_INLINE boolean waitForReferences() const {
			if (m_refLock.writeLock(Properties::getTransactionTimeout()) == false) {
				return false;
			}
			m_refLock.writeUnlock();
			return true;
		}

	private:
		FORCE_INLINE void setFlags(ubytetype& flags, const ubytetype value) {
			#ifdef DEEP_ATOMIC_SEGMENT_FLAGS
			__sync_lock_test_and_set(&flags, value);
			#else
			flags = value;
			#endif
		}

		template<const ubytetype flag> FORCE_INLINE void setFlag(ubytetype& flags, const boolean value) {
			#ifdef DEEP_ATOMIC_SEGMENT_FLAGS
			if (value == true) {
				__sync_or_and_fetch(&flags, flag);
			} else {
				__sync_and_and_fetch(&flags, ~flag);
			}
			#else
			flags = value ? flags | flag : flags & ~flag;
			#endif
		}

		FORCE_INLINE ubytetype getFlags(const ubytetype& flags) const {
			#ifdef DEEP_ATOMIC_SEGMENT_FLAGS
			return __sync_or_and_fetch(&const_cast<ubytetype&>(flags), 0);
			#else
			return flags;
			#endif
		}

		template<const ubytetype flag> FORCE_INLINE boolean getFlag(const ubytetype& flags) const {
			#ifdef DEEP_ATOMIC_SEGMENT_FLAGS
			return (getFlags(flags) & flag) != 0;
			#else
			return (flags & flag) != 0;
			#endif
		}

	public:
		Segment(const Comparator<K>* compare, int order, longtype version, bytetype keyParts = -1):
			RealTimeTypes<K>::SegTreeMap(compare, order, false, false, keyParts),
			#if 0 /* DEEP_USERLOCK */
			// nothing to do
			#else
			m_lock(false),
			#endif
			#if 0
			m_version(version),
			#endif
			m_pagingPosition(0),
			m_streamPosition(0),
			m_pagingIndexes(&USHORT_CMP),
			m_streamIndexes(&STREAM_REF_CMP, TreeSet<StreamReference*>::INITIAL_ORDER, true /* delete values */),
			m_fragmentCount(0),
			m_zipData(null),
			m_zipSize(0),
			m_uncompressedSize(0),
			m_indexLocality(RealTimeLocality::LOCALITY_NONE),
			m_summarizedLocality(RealTimeLocality::LOCALITY_NONE),
			m_checkpointSummarized(false),
			m_indexOrderKey((K)Converter<K>::NULL_VALUE),
			m_refcnt(0),
			m_stateFlags(0),
			m_extraFlags(0),
			m_moreFlags(0),
			m_indexFlags(0),
			m_rolling(false),
			m_rollOwner((pthread_t)null),
			m_rowOwners() {

			RealTimeTypes<K>::SegTreeMap::setStatisticsEnabled(true);
		}

		virtual ~Segment(void) {
			#ifdef DEEP_DEBUG
			if (getRolling() == true) {
				DEEP_LOG(ERROR, OTHER, "Destroy of rolling segment\n");
				throw InvalidException("Destroy of rolling segment");
			}
			#endif
			if (m_indexOrderKey != (K)Converter<K>::NULL_VALUE) {
				Converter<K>::destroy(m_indexOrderKey);
			}
			if (m_zipData != null) {
				free(m_zipData);
			}
		}

		FORCE_INLINE void setStateFlags(ubytetype flags) {
			return setFlags(m_stateFlags, flags);
		}

		FORCE_INLINE ubytetype getStateFlags(void) const {
			return getFlags(m_stateFlags);
		}

		FORCE_INLINE boolean getStreamed(void) const {
			return (m_streamIndexes.size() != 0);
		}

		FORCE_INLINE void setStreamedFlag(boolean flag) {
			return setFlag<SEGMENT_FLAG_STREAMED>(m_stateFlags, flag);
		}

		FORCE_INLINE boolean getStreamedFlag(void) const {
			return getFlag<SEGMENT_FLAG_STREAMED>(m_stateFlags);
		}

		FORCE_INLINE void setSummary(boolean flag) {
			return setFlag<SEGMENT_FLAG_SUMMARY>(m_stateFlags, flag);
		}

		FORCE_INLINE boolean getSummary(void) const {
			return getFlag<SEGMENT_FLAG_SUMMARY>(m_stateFlags);
		}

		FORCE_INLINE void setPaged(boolean flag) {
			return setFlag<SEGMENT_FLAG_PAGED>(m_stateFlags, flag);
		}

		FORCE_INLINE boolean getPaged(void) const {
			return getFlag<SEGMENT_FLAG_PAGED>(m_stateFlags);
		}

		FORCE_INLINE void setPurged(boolean flag) {
			return setFlag<SEGMENT_FLAG_PURGED>(m_stateFlags, flag);
		}

		FORCE_INLINE boolean getPurged(void) const {
			return getFlag<SEGMENT_FLAG_PURGED>(m_stateFlags);
		}

		FORCE_INLINE void setDirty(boolean flag) {
			#ifdef DEEP_DEBUG
			if ((getSummary() == true) && (flag == true)) {
				DEEP_LOG(ERROR, OTHER, "Segment::setDirty on summary\n");
				throw InvalidException("Segment::setDirty on summary");
			}
			#endif

			return setFlag<SEGMENT_FLAG_DIRTY>(m_stateFlags, flag);
		}

		FORCE_INLINE boolean getDirty(void) const {
			return getFlag<SEGMENT_FLAG_DIRTY>(m_stateFlags);
		}

		FORCE_INLINE void setVirtual(boolean flag) {
			#ifdef DEEP_DEBUG
			if ((getSummary() == true) && (flag == true)) {
				DEEP_LOG(ERROR, OTHER, "Segment::setVirtual on summary\n");
				throw InvalidException("Segment::setVirtual on summary");
			}
			#endif

			return setFlag<SEGMENT_FLAG_VIRTUAL>(m_stateFlags, flag);
		}

		FORCE_INLINE boolean getVirtual(void) const {
			return getFlag<SEGMENT_FLAG_VIRTUAL>(m_stateFlags);
		}

		FORCE_INLINE void setStreamRangeOverflowFlag(boolean flag) {
			return setFlag<SEGMENT_FLAG_RANGEOVER>(m_stateFlags, flag);
		}

		FORCE_INLINE boolean getStreamRangeOverflowFlag(void) const {
			return getFlag<SEGMENT_FLAG_RANGEOVER>(m_stateFlags);
		}

		FORCE_INLINE void setViewpoint(boolean flag) {
			#ifdef DEEP_DEBUG
			if ((getSummary() == true) && (flag == true)) {
				DEEP_LOG(ERROR, OTHER, "Segment::setViewpoint on summary\n");
				throw InvalidException("Segment::setViewpoint on summary");
			}
			#endif

			return setFlag<SEGMENT_FLAG_VIEWPOINT>(m_stateFlags, flag);
		}

		FORCE_INLINE boolean getViewpoint(void) const {
			return getFlag<SEGMENT_FLAG_VIEWPOINT>(m_stateFlags);
		}

		FORCE_INLINE boolean getStreamRangeOverflow(boolean includeOld = true) {
			if (getStreamed() == false) {
				return false;
			}
			boolean overflow = false;

			ushorttype currentIndex = getCurrentStreamIndex(includeOld);
			ubytetype range = 0;
			FileRangeSet rangeSet(currentIndex, range);

			Iterator<StreamReference*>* iter = m_streamIndexes.iterator();
			while (iter->hasNext()) {
				StreamReference* sr = iter->next();	
				ushorttype index = sr->m_fileIndex;

				if (index == currentIndex) {
					continue;
				}

				if ((includeOld == true) || (sr->m_old == false)) {
					rangeSet.add(index, overflow);		
					if (overflow == true) {
						break;
					}
				}
			}
			delete iter;
			return overflow;
		}

		FORCE_INLINE void setBeenRelocated(boolean flag) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setBeenRelocated on summary\n");
				throw InvalidException("Segment::setBeenRelocated on summary");
			}
			#endif

			return setFlag<SEGMENT_XFLAG_RELOCATED>(m_extraFlags, flag);
		}

		FORCE_INLINE boolean getBeenRelocated(void) const {
			return getFlag<SEGMENT_XFLAG_RELOCATED>(m_extraFlags);
		}

		FORCE_INLINE void setViewed(boolean flag) {
			#ifdef DEEP_DEBUG
			if ((getSummary() == true) && (flag == true)) {
				DEEP_LOG(ERROR, OTHER, "Segment::setViewed on summary\n");
				throw InvalidException("Segment::setViewed on summary");
			}
			#endif

			return setFlag<SEGMENT_XFLAG_VIEWED>(m_extraFlags, flag);
		}

		/* XXX: a segment was viewed if it was indexed w/ rebuild on and otherwise not dirty */
		FORCE_INLINE boolean getViewed(void) const {
			return getFlag<SEGMENT_XFLAG_VIEWED>(m_extraFlags);
		}

		FORCE_INLINE void setBeenRealigned(boolean flag) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setBeenRealigned on summary\n");
				throw InvalidException("Segment::setBeenRealigned on summary");
			}
			#endif

			return setFlag<SEGMENT_MFLAG_REALIGNED>(m_moreFlags, flag);
		}

		FORCE_INLINE boolean getBeenRealigned(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getBeenRealigned on summary\n");
				throw InvalidException("Segment::getBeenRealigned on summary");
			}
			#endif

			return getFlag<SEGMENT_MFLAG_REALIGNED>(m_moreFlags);
		}

		FORCE_INLINE void setBeenAltered(boolean flag) {
			#ifdef DEEP_DEBUG
			if ((getSummary() == true) && (flag == true)) {
				DEEP_LOG(ERROR, OTHER, "Segment::setBeenAltered on summary\n");
				throw InvalidException("Segment::setBeenAltered on summary");
			}
			#endif

			return setFlag<SEGMENT_XFLAG_BEENALTERED>(m_extraFlags, flag);
		}

		FORCE_INLINE boolean getBeenAltered(void) const {
			return getFlag<SEGMENT_XFLAG_BEENALTERED>(m_extraFlags);
		}

		FORCE_INLINE void setBeenFilled(boolean flag) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setBeenFilled on summary\n");
				throw InvalidException("Segment::setBeenFilled on summary");
			}
			#endif

			return setFlag<SEGMENT_XFLAG_BEENFILLED>(m_extraFlags, flag);
		}

		FORCE_INLINE boolean getBeenFilled(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getBeenFilled on summary\n");
				throw InvalidException("Segment::getBeenFilled on summary");
			}
			#endif

			return getFlag<SEGMENT_XFLAG_BEENFILLED>(m_extraFlags);
		}

		FORCE_INLINE void setBeenReseeded(boolean flag) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setBeenReseeded on summary\n");
				throw InvalidException("Segment::setBeenReseeded on summary");
			}
			#endif

			return setFlag<SEGMENT_XFLAG_BEENRESEEDED>(m_extraFlags, flag);
		}

		FORCE_INLINE boolean getBeenReseeded(void) const {
			return getFlag<SEGMENT_XFLAG_BEENRESEEDED>(m_extraFlags);
		}

		FORCE_INLINE void setBeenSurceased(boolean flag) {
			#ifdef DEEP_DEBUG
			if ((getSummary() == true) && (flag == true)) {
				DEEP_LOG(ERROR, OTHER, "Segment::setBeenSurceased on summary\n");
				throw InvalidException("Segment::setBeenSurceased on summary");
			}
			#endif

			return setFlag<SEGMENT_XFLAG_BEENSURCEASED>(m_extraFlags, flag);
		}

		FORCE_INLINE boolean getBeenSurceased(void) const {
			return getFlag<SEGMENT_XFLAG_BEENSURCEASED>(m_extraFlags);
		}

		FORCE_INLINE void setBeenDeleted(boolean flag) {
			#ifdef DEEP_DEBUG
			if ((getSummary() == true) && (flag == true)) {
				DEEP_LOG(ERROR, OTHER, "Segment::setBeenDeleted on summary\n");
				throw InvalidException("Segment::setBeenDeleted on summary");
			}
			#endif

			return setFlag<SEGMENT_XFLAG_BEENDELETED>(m_extraFlags, flag);
		}

		FORCE_INLINE boolean getBeenDeleted(void) const {
			return getFlag<SEGMENT_XFLAG_BEENDELETED>(m_extraFlags);
		}

		FORCE_INLINE void setBeenCreated(boolean flag) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setBeenCreated on summary\n");
				throw InvalidException("Segment::setBeenCreated on summary");
			}
			#endif

			return setFlag<SEGMENT_XFLAG_BEENCREATED>(m_extraFlags, flag);
		}

		FORCE_INLINE boolean getBeenCreated(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getBeenCreated on summary\n");
				throw InvalidException("Segment::getBeenCreated on summary");
			}
			#endif

			return getFlag<SEGMENT_XFLAG_BEENCREATED>(m_extraFlags);
		}

		FORCE_INLINE void setFragmentedValue(boolean flag) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setFragmentedValue on summary\n");
				throw InvalidException("Segment::setFragmentedValue on summary");
			}
			#endif

			return setFlag<SEGMENT_IFLAG_FRAGVALUE>(m_indexFlags, flag);
		}

		FORCE_INLINE boolean getFragmentedValue(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getFragmentedValue on summary\n");
				throw InvalidException("Segment::getFragmentedValue on summary");
			}
			#endif

			return getFlag<SEGMENT_IFLAG_FRAGVALUE>(m_indexFlags);
		}

		FORCE_INLINE void setFragmentedKey(boolean flag) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setFragmentedKey on summary\n");
				throw InvalidException("Segment::setFragmentedKey on summary");
			}
			#endif

			return setFlag<SEGMENT_IFLAG_FRAGKEY>(m_indexFlags, flag);
		}

		FORCE_INLINE boolean getFragmentedKey(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getFragmentedKey on summary\n");
				throw InvalidException("Segment::getFragmentedKey on summary");
			}
			#endif

			return getFlag<SEGMENT_IFLAG_FRAGKEY>(m_indexFlags);
		}

		FORCE_INLINE void setPagingPosition(uinttype position) {
			m_pagingPosition = position;
		}

		FORCE_INLINE uinttype getPagingPosition(void) const {
			return m_pagingPosition;
		}

		FORCE_INLINE void setStreamPosition(uinttype position) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setStreamPosition on summary\n");
				throw InvalidException("Segment::setStreamPosition on summary");
			}
			#endif

			m_streamPosition = position;
		}

		FORCE_INLINE uinttype getStreamPosition(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getStreamPosition on summary\n");
				throw InvalidException("Segment::getStreamPosition on summary");
			}
			#endif

			return m_streamPosition;
		}

		FORCE_INLINE void setRecoveryEpoch(uinttype recoveryEpoch) {
			m_recoveryEpoch = recoveryEpoch;
		}

		FORCE_INLINE uinttype getRecoveryEpoch(void) const {
			return m_recoveryEpoch;
		}

		FORCE_INLINE boolean getValuesCompressed(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getValuesCompressed on summary\n");
				throw InvalidException("Segment::getValuesCompressed on summary");
			}
			#endif

			return m_streamPosition != 0;
		}

		FORCE_INLINE void setBackReference(ushorttype reference) {
			m_backReference = reference;
		}

		FORCE_INLINE ushorttype getBackReference(void) const {
			return m_backReference;
		}

		FORCE_INLINE void addPagingIndexes(const TreeSet<ushorttype>* iFiles) {
			Iterator<ushorttype>* iter = ((TreeSet<ushorttype>*)iFiles)->iterator();

			while (iter->hasNext()) {
				m_pagingIndexes.add(iter->next());
			}			

			delete iter;
		}

		FORCE_INLINE const TreeSet<ushorttype>* getPagingIndexes(void) const {
			return &m_pagingIndexes;
		}

		FORCE_INLINE boolean wouldOverflowPaging(ushorttype fileIndex, ubytetype fileRange) {
			if (getPaged() == false) {
				return false;
			}

			FileRangeSet incomingFiles(fileIndex, fileRange);

			ushorttype pagingIndex = getCurrentPagingIndex();
			ubytetype pagingRange = getPagingRange();

			FileRangeSet files(pagingIndex, pagingRange);
			boolean overflow = false;
			files.addAll(incomingFiles, overflow);
			return overflow;
		}

		FORCE_INLINE ushorttype getCurrentPagingIndex(void) const {
			return m_pagingIndexes.last();
		}

		FORCE_INLINE boolean hasPagingIndexes(void) const {
			return (m_pagingIndexes.isEmpty() == false);
		}

		FORCE_INLINE void addPagingIndex(ushorttype fileIndex) {
			m_pagingIndexes.add(fileIndex);
		}

		FORCE_INLINE void setPagingRange(ubytetype fileRange) {
			// XXX: note this relies on pagingIndex being set previously
			ushorttype currentIndex = getCurrentPagingIndex();
			FileRangeSet files(currentIndex, fileRange);
			TreeSet<ushorttype>* set = files.toSet();
			
			Iterator<ushorttype>* iter = set->iterator();
			while (iter->hasNext()) {
				m_pagingIndexes.add(iter->next());
			}			

			delete iter;
			delete set;
		}

		#ifdef DEEP_DEBUG
		FORCE_INLINE ubytetype getPagingRange(boolean ignoreOverflow = false) {
		#else
		FORCE_INLINE ubytetype getPagingRange() {
		#endif
			ubytetype range = 0;
			ushorttype currentIndex = getCurrentPagingIndex();
			FileRangeSet files(currentIndex, range);
			
			Iterator<ushorttype>* iter = m_pagingIndexes.iterator();
			while (iter->hasNext()) {
				boolean overflow = false;
				ushorttype index = iter->next();

				if (index == currentIndex) {
					// XXX: already added to range above via FileRangeSet constructor
					continue;
				}

				files.add(index, overflow);
				#ifdef DEEP_DEBUG
				if ((ignoreOverflow == false) && (overflow == true)) {
					DEEP_LOG(ERROR, OTHER, "Paging range overflow\n");
					throw InvalidException("Paging range overflow");
				}
				#endif
			}

			delete iter;
			return files.getRange();	
		}

		FORCE_INLINE void addStreamIndex(ushorttype fileIndex, boolean old = false) {
			StreamReference* sr = new StreamReference(fileIndex, old);
			m_streamIndexes.add(sr);
		}

		FORCE_INLINE void addStreamReferences(const TreeSet<StreamReference*>* iFiles) {
			Iterator<StreamReference*>* iter = ((TreeSet<StreamReference*>*)iFiles)->iterator();

			while (iter->hasNext()) {
				StreamReference* isr = iter->next();
				StreamReference* sr = new StreamReference(isr->m_fileIndex, isr->m_old);
				m_streamIndexes.add(sr);
			}	

			delete iter;
		}

		FORCE_INLINE void removeStreamReference(ushorttype fileIndex) {
			StreamReference sr(fileIndex, false /* not part of lookup */);
			m_streamIndexes.remove(&sr, null /* retElem */);	
		}

		FORCE_INLINE void clearOldStreamReferences(void) {
			Iterator<StreamReference*>* iter = m_streamIndexes.iterator();

			while (iter->hasNext()) {
				StreamReference* isr = iter->next();
				if (isr->m_old == true) {
					iter->remove();
				}
			}	

			delete iter;
		}

		FORCE_INLINE const StreamReference* getStreamReference(ushorttype fileIndex) const {
			StreamReference sr(fileIndex, false /* not part of lookup */);
			return m_streamIndexes.floor(&sr);
		}

		FORCE_INLINE const TreeSet<StreamReference*>* getStreamReferences(void) const {
			return (const TreeSet<StreamReference*>*) &m_streamIndexes;
		}

		// XXX: note that client must destroy the returned set
		FORCE_INLINE void getStreamIndexes(boolean getOld, TreeSet<ushorttype>* retVal) {
			Iterator<StreamReference*>* iter = m_streamIndexes.iterator();

			while (iter->hasNext()) {
				StreamReference* isr = iter->next();
				if ((getOld == true) || (isr->m_old == false)) {
					retVal->add(isr->m_fileIndex);
				}
			}	

			delete iter;
		}

		FORCE_INLINE ushorttype getCurrentStreamIndex(boolean includeOld = true) {
			#ifdef DEEP_DEBUG
			if (getStreamed() == false) {
				DEEP_LOG(ERROR, OTHER, "getCurrentStreamIndex: Invalid stream index, not streamed\n");
				throw InvalidException("getCurrentStreamIndex: Invalid stream index, not streamed");
			}
			#endif

			if (includeOld == true) {
				return m_streamIndexes.last()->m_fileIndex;
			}

			if (m_streamIndexes.last()->m_old == true) {
				inttype retVal = -1;
				Iterator<StreamReference*>* iter = m_streamIndexes.iterator();
				while (iter->hasNext()) {
					StreamReference* sr = iter->next();
					if (sr->m_old == false) {
						retVal = sr->m_fileIndex;
					}
				}
				#ifdef DEEP_DEBUG
				if (retVal == -1) {
					DEEP_LOG(ERROR, OTHER, "getCurrentStreamIndex: Invalid stream index, all old\n");
					throw InvalidException("getCurrentStreamIndex: Invalid stream index, all old");
				}
				#endif
				delete iter;
				return retVal;
			} else {
				return m_streamIndexes.last()->m_fileIndex;
			}
		}

		FORCE_INLINE void setStreamRange(ubytetype fileRange) {
			// XXX: note this relies on streamIndex being set previously
			ushorttype currentIndex = getCurrentStreamIndex();
			FileRangeSet files(currentIndex, fileRange);
			TreeSet<ushorttype>* set = files.toSet();
			
			Iterator<ushorttype>* iter = set->iterator();
			while (iter->hasNext()) {
				StreamReference* sr = new StreamReference(iter->next(), false /* old */);
				m_streamIndexes.add(sr);
			}			

			delete iter;
			delete set;
		}

		#ifdef DEEP_DEBUG
		FORCE_INLINE ubytetype getStreamRange(boolean ignoreOverflow = false) {
		#else
		FORCE_INLINE ubytetype getStreamRange() {
		#endif
			#ifdef DEEP_DEBUG
			if (getStreamed() == false) {
				DEEP_LOG(ERROR, OTHER, "getStreamRange: Invalid stream index\n");
				throw InvalidException("getStreamRange: Invalid stream index");
			}
			#endif

			ushorttype currentIndex = getCurrentStreamIndex(true /* includeOld */);
			ubytetype range = 0;
			FileRangeSet rangeSet(currentIndex, range);
			
			Iterator<StreamReference*>* iter = m_streamIndexes.iterator();
			while (iter->hasNext()) {
				boolean overflow = false;
				StreamReference* sr = iter->next();
				
				ushorttype index = sr->m_fileIndex;

				if (index == currentIndex) {
					// XXX: already added to range above via FileRangeSet constructor
					continue;
				}

				rangeSet.add(index, overflow);
				#ifdef DEEP_DEBUG
				if ((ignoreOverflow == false) && (overflow == true)) {
					DEEP_LOG(ERROR, OTHER, "Stream range overflow\n");
					throw InvalidException("Stream range overflow");
				}
				#endif
			}

			delete iter;
			return rangeSet.getRange();	
		}

		FORCE_INLINE boolean hasPagingIndex(ushorttype fileIndex) {
			if (getPaged() == false) {
				return false;
			}
			return m_pagingIndexes.contains(fileIndex);
		}

		FORCE_INLINE boolean validPagingRange(ushorttype fileIndex) {
			inttype diff = fileIndex - getCurrentPagingIndex();
			if (diff < 0) {
				return false;

			} else if (diff == 0) {
				return true;
			}

			return (diff <= Properties::DEFAULT_SEGMENT_FILE_RANGE) && (( getPagingRange() >> (Properties::DEFAULT_SEGMENT_FILE_RANGE - diff)) == 0);
		}

		FORCE_INLINE boolean hasStreamIndex(ushorttype fileIndex) {
			if (getStreamed() == false) {
				return false;
			}

			StreamReference sr(fileIndex, false /* not part of lookup */);
			return m_streamIndexes.contains(&sr);	
		}

		FORCE_INLINE void incrementFragmentCount(void) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::incrementFragmentCount on summary\n");
				throw InvalidException("Segment::incrementFragmentCount on summary");
			}
			#endif

			m_fragmentCount++;
		}

		FORCE_INLINE ubytetype getFragmentCount(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getFragmentCount on summary\n");
				throw InvalidException("Segment::getFragmentCount on summary");
			}
			#endif

			return m_fragmentCount;
		}

		FORCE_INLINE void resetFragmentCount(ubytetype count = 0) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::resetFragmentCount on summary\n");
				throw InvalidException("Segment::resetFragmentCount on summary");
			}
			#endif

			m_fragmentCount = count;
		}

		FORCE_INLINE void setUncompressedSize(inttype size) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setUncompressedSize on summary\n");
				throw InvalidException("Segment::setUncompressedSize on summary");
			}
			#endif

			m_uncompressedSize = size;
		}

		FORCE_INLINE inttype getUncompressedSize(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getUncompressedSize on summary\n");
				throw InvalidException("Segment::getUncompressedSize on summary");
			}
			#endif

			return m_uncompressedSize;
		}

		FORCE_INLINE void setIndexLocality(const RealTimeLocality& locality) {
			m_indexLocality = locality;
		}

		FORCE_INLINE const RealTimeLocality& getIndexLocality(void) const {
			return m_indexLocality;
		}

		FORCE_INLINE void setSummarizedLocality(const RealTimeLocality& locality, boolean checkpoint) {
			m_summarizedLocality = locality;
			m_checkpointSummarized = checkpoint;
		}

		FORCE_INLINE boolean getCheckpointSummarized(void) const {
			return m_checkpointSummarized;
		}

		FORCE_INLINE const RealTimeLocality& getSummarizedLocality(void) const {
			return m_summarizedLocality;
		}

		FORCE_INLINE void setIndexOrderKey(K key) {
			if (m_indexOrderKey != (K)Converter<K>::NULL_VALUE) {
				Converter<K>::destroy(m_indexOrderKey);
			}
			m_indexOrderKey = key;
		}

		FORCE_INLINE K getIndexOrderKey() const {
			return m_indexOrderKey;
		}

		FORCE_INLINE void setZipSize(inttype zipSize) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setZipSize on summary\n");
				throw InvalidException("Segment::setZipSize on summary");
			}
			#endif

			m_zipSize = zipSize;
		}

		FORCE_INLINE inttype getZipSize(void) const {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::getZipSize on summary\n");
				throw InvalidException("Segment::getZipSize on summary");
			}
			#endif

			return m_zipSize;
		}

		FORCE_INLINE void setZipData(bytearray zipData) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::setZipData on summary\n");
				throw InvalidException("Segment::setZipData on summary");
			}
			#endif

			m_zipData = zipData;
		}

		FORCE_INLINE bytearray getZipData(void) const {
			#ifdef DEEP_DEBUG
			if ((getSummary() == true) && (m_zipData != null)) {
				DEEP_LOG(ERROR, OTHER, "Segment::getZipData on summary\n");
				throw InvalidException("Segment::getZipData on summary");
			}
			#endif

			return m_zipData;
		}

		FORCE_INLINE void freeZipData(void) {
			#ifdef DEEP_DEBUG
			if (getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Segment::freeZipData on summary\n");
				throw InvalidException("Segment::freeZipData on summary");
			}
			#endif

			delete [] m_zipData;
			m_zipData = null;
			m_zipSize = 0;
		}

		FORCE_INLINE void resetStreamIndexes(void) {
			m_streamIndexes.clear();
		}

		FORCE_INLINE void resetPagingIndexes(void) {
			m_backReference = 0;
			m_pagingIndexes.clear();
			setPaged(false);
		}

		FORCE_INLINE void resetFlags(void) {
			setFlags(m_stateFlags, 0);
			setFlags(m_extraFlags, 0);
			setFlags(m_moreFlags, 0);
		}

		FORCE_INLINE void lock(const boolean force = false) {
			if ((force == false) && (m_rolling == true) && (m_rollOwner == pthread_self())) {
				return;
			}
			m_lock.lock();
		}

		FORCE_INLINE boolean tryLock() {
			if ((m_rolling == true) && (m_rollOwner == pthread_self())) {
				return true;
			}
			return m_lock.tryLock();
		}

		FORCE_INLINE void unlock(const boolean force = false) {
			if ((force == false) && (m_rolling == true) && (m_rollOwner == pthread_self())) {
				return;
			}
			m_lock.unlock();
		}

		FORCE_INLINE boolean getRolling() const {
			return m_rolling;
		}

		FORCE_INLINE void setRolling(const boolean rolling) {
			if (rolling == true) {
				m_rolling = true;
				m_rollOwner = pthread_self();
			} else {
				m_rollOwner = (pthread_t)null;
				m_rowOwners.clear();
				m_rolling = false;
			}
		}

		FORCE_INLINE void addLockOwner(const ushorttype txid, const uinttype sequence) {
			const inttype id = ((inttype)txid) & 0xFFFF;
			if (m_rowOwners.containsKey(id) == true) {
				const uinttype ownerSeq = m_rowOwners.get(id);
				if (sequence <= ownerSeq) {
					return;
				}
			}
			m_rowOwners.put(id, sequence);
		}

		FORCE_INLINE boolean hasRowsLockedBy(const ushorttype txid, const uinttype sequence) const {
			const inttype id = ((inttype)txid) & 0xFFFF;
			if (m_rowOwners.containsKey(id) == false) {
				return false;
			}
			const uinttype ownerSeq = m_rowOwners.get(id);
			return sequence <= ownerSeq;
		}

		friend struct InfoReference<K>;
};

template<typename K>
const Comparator<ushorttype> Segment<K>::USHORT_CMP;

template<typename K>
const Comparator<StreamReference*> Segment<K>::STREAM_REF_CMP;

template<typename K>
class InformationIterator {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;

	private:
		typename SegTreeMap::TreeMapEntrySet::EntrySetIterator* m_iterator;

	public:
		InformationIterator(typename SegTreeMap::TreeMapEntrySet::EntrySetIterator* iterator):
			m_iterator(iterator) {
		}

		FORCE_INLINE const SegMapEntry* next(void) {
			return (m_iterator->SegTreeMap::TreeMapEntrySet::EntrySetIterator::hasNext() == true) ? m_iterator->SegTreeMap::TreeMapEntrySet::EntrySetIterator::next() : null;
		}
};

struct XInfoRef {
	private:
	bytetype indexValue;
	Information* info;

	public:
	FORCE_INLINE Information* getInfo() {
		return info;
	}

	FORCE_INLINE void setInfo(Information* newInfo) {
		if (newInfo == info) {
			return;
		}

		CXX_LANG_MEMORY_DEBUG_ASSERT(newInfo);
		if (newInfo != null) {
			newInfo->incref();
		}

		Information* old = info;
		CXX_LANG_MEMORY_DEBUG_ASSERT(old);
		info = newInfo;

		if ((old != null) && (old->decref() == 0)) {
			Information::freeInfo(old);
		}
	}

	FORCE_INLINE XInfoRef(bytetype p_indexValue, Information* p_info) :
		indexValue(p_indexValue),
		info(p_info) {
		CXX_LANG_MEMORY_DEBUG_ASSERT(info);
		if (info != null) {
			info->incref();
		}
	}

	FORCE_INLINE XInfoRef(Information* p_info) :
		indexValue(-1),
		info(p_info) {
		CXX_LANG_MEMORY_DEBUG_ASSERT(info);
		if (info != null) {
			info->incref();
		}
	}

	#if 0
	FORCE_INLINE XInfoRef(const XInfoRef& ref) {
		indexValue(ref.indexValue),
		info(ref.info) {
		CXX_LANG_MEMORY_DEBUG_ASSERT(info);
		if (info != null) {
			info->incref();
		}
	}
	#endif

	FORCE_INLINE virtual ~XInfoRef() {
		CXX_LANG_MEMORY_DEBUG_ASSERT(info);
		if ((info != null) && (info->decref() == 0)) {
			Information::freeInfo(info);
		}
	}

	virtual StoryLine& getStoryLine() const = 0;
};

template<typename K>
struct InfoReference : public XInfoRef {
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;

	Segment<K>* segment;
	SegMapEntry* infoEntry;

	FORCE_INLINE InfoReference(bytetype p_indexValue, Segment<K>* p_segment, SegMapEntry* p_infoEntry, Information* p_info) :
		XInfoRef(p_indexValue, p_info),
		segment(p_segment),
		infoEntry(p_infoEntry) {
		segment->m_refLock.readLock();
	}

	FORCE_INLINE InfoReference(bytetype p_indexValue, Segment<K>* p_segment, SegMapEntry* p_infoEntry) :
		XInfoRef(p_indexValue, p_infoEntry->getValue()),
		segment(p_segment),
		infoEntry(p_infoEntry) {
		segment->m_refLock.readLock();
	}

	FORCE_INLINE InfoReference(Information* p_info) :
		XInfoRef(p_info),
		segment(null),
		infoEntry(null) {
		// XXX: nothing to do
	}

	FORCE_INLINE InfoReference(const InfoReference& ref) :
		XInfoRef(ref.indexValue, ref.info),
		segment(ref.segment),
		infoEntry(ref.infoEntry) {
		if (segment != null) {
			segment->m_refLock.readLock();
		}
	}

	FORCE_INLINE virtual ~InfoReference() {
		if (segment != null) {
			segment->m_refLock.readUnlock();
		}
	}

	FORCE_INLINE virtual StoryLine& getStoryLine() const {
		return infoEntry->getStoryLine();
	}
};

typedef bytetype InfoRefBuf[sizeof(InfoReference<void*>)];

} } } } } } // namespace

// XXX: specialize for Information to track rooted
namespace cxx { namespace util {

template<typename K>
class __attribute__((packed)) MapEntry<K,Information*,bytetype> {
	private:
		K m_key;
		StoryLine m_storyLine;

		FORCE_INLINE void markRooted(const bytetype indexValue) {
			if ((getValue() == null) || (indexValue < 0)) {
				return;
			}

			const shorttype level = getValue()->getLevel();
			if (level != Information::LEVEL_COMMIT) {
				// XXX: debug disabled (move from physical to logical rootedness)
				#if 0
				const boolean wasRooted = getValue()->getRooted(indexValue);
				if ((wasRooted == true) && (getValue()->getLevel() != Information::LEVEL_COMMIT) /* XXX: must check level again */) {
					DEEP_LOG(ERROR, OTHER, "Information already rooted: %d\n", indexValue);
					throw InvalidException("Information already rooted");
				}
				#endif

				getValue()->setRooted(indexValue, true);
			}

			#ifdef DEEP_DEBUG
			getValue()->incRooted();
			#endif
		}

		FORCE_INLINE void clearRooted(const bytetype indexValue) {
			if ((getValue() == null) || (indexValue < 0)) {
				return;
			}

			const shorttype level = getValue()->getLevel();
			if (level != Information::LEVEL_COMMIT) {
				#ifdef DEEP_DEBUG
				const boolean wasRooted = getValue()->getRooted(indexValue);
				if ((wasRooted == false) && (getValue()->getLevel() != Information::LEVEL_COMMIT) /* XXX: must check level again */) {
					DEEP_LOG(ERROR, OTHER, "Information not rooted: %d\n", indexValue);
					throw InvalidException("Information not rooted");
				}
				#endif

				getValue()->setRooted(indexValue, false);
			}

			#ifdef DEEP_DEBUG
			getValue()->decRooted();
			#endif
		}

	public:
		FORCE_INLINE MapEntry(K key, Information* value, bytetype indexValue):
			m_key(key),
			m_storyLine(value) {

			CXX_LANG_MEMORY_DEBUG_VALIDATE(K, m_key);
			CXX_LANG_MEMORY_DEBUG_ASSERT(value);

			markRooted(indexValue);
		}

		FORCE_INLINE ~MapEntry() {
			/* XXX: nothing to do */
		}

		FORCE_INLINE void destroy(bytetype indexValue) {
			if (indexValue < 0) {
				return;
			}

			CXX_LANG_MEMORY_DEBUG_VALIDATE(K, m_key);
			CXX_LANG_MEMORY_DEBUG_ASSERT(getValue());

			clearRooted(indexValue);
		}

		FORCE_INLINE void setKey(K key, bytetype indexValue) {
			CXX_LANG_MEMORY_DEBUG_VALIDATE(K, m_key);

			m_key = key;

			CXX_LANG_MEMORY_DEBUG_VALIDATE(K, m_key);
		}

		FORCE_INLINE K getKey(void) const {
			CXX_LANG_MEMORY_DEBUG_VALIDATE(K, m_key);

			return m_key;
		}

		FORCE_INLINE void setValue(Information* value, bytetype indexValue) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(m_storyLine.getRoot());

			clearRooted(indexValue);

			m_storyLine.setRoot(value);

			markRooted(indexValue);

			CXX_LANG_MEMORY_DEBUG_ASSERT(m_storyLine.getRoot());
		}

		FORCE_INLINE void setValueIgnoreRooted(Information* value, bytetype indexValue, boolean ignorePre = true, boolean ignorePost = true) {

			if (ignorePre == false) {
				clearRooted(indexValue);
			}

			m_storyLine.setRoot(value);

			if (ignorePost == false) {
				markRooted(indexValue);
			}
		}

		FORCE_INLINE Information* getValue(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(m_storyLine.getRoot());

			return m_storyLine.getRoot();
		}

		FORCE_INLINE StoryLine& getStoryLine(void) const {
			return const_cast<StoryLine&>(m_storyLine);
		}

		#ifdef CXX_LANG_MEMORY_DEBUG
		FORCE_INLINE void setKey(K key, bytetype indexValue, boolean ignore) {
			if (ignore == false) {
				setKey(key, indexValue);

			} else {
				m_key = key;
			}
		}

		FORCE_INLINE K getKey(boolean ignore) const {
			if (ignore == false) {
				return getKey();

			} else {
				return m_key;
			}
		}

		FORCE_INLINE void setValue(Information* value, bytetype indexValue, boolean ignore) {
			if (ignore == false) {
				setValue(value, indexValue);

			} else {
				// XXX: if we are ignoring mem debug, it is safe to assume ignore initial value for rooted
				m_storyLine.setRoot(value);

				markRooted(indexValue);
			}
		}

		FORCE_INLINE Information* getValue(boolean ignore) const {
			if (ignore == false) {
				return getValue();

			} else {
				return m_storyLine.getRoot();
			}
		}
		#endif
};

template<typename K>
class MapEntry<K,Information*,void*> {
};

template<typename K>
class Converter<MapEntry<K,Information*,bytetype>*> {
	public:
		static const MapEntry<K,Information*,bytetype>* NULL_VALUE;
		static const MapEntry<K,Information*,bytetype>* RESERVE;

		FORCE_INLINE static int hashCode(const MapEntry<K,Information*,bytetype>* o) {
			// TODO:
			return 0;
		}

		FORCE_INLINE static boolean equals(MapEntry<K,Information*,bytetype>* o1, MapEntry<K,Information*,bytetype>* o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(MapEntry<K,Information*,bytetype>* o, bytetype indexValue = -1) {
			o->destroy(indexValue);
			delete o;
		}

		FORCE_INLINE static void validate(MapEntry<K,Information*,bytetype>* o) {
			// TODO: throw UnsupportedOperationException("Invalid validate request");
		}

		FORCE_INLINE static const bytearray toData(MapEntry<K,Information*,bytetype>* o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

template<typename K>
const MapEntry<K,Information*,bytetype>* Converter<MapEntry<K,Information*,bytetype>*>::NULL_VALUE = null;
template<typename K>
const MapEntry<K,Information*,bytetype>* Converter<MapEntry<K,Information*,bytetype>*>::RESERVE = (MapEntry<K,Information*,bytetype>*)&Converter<MapEntry<K,Information*,bytetype>*>::NULL_VALUE;

template<>
class Converter<XInfoRef*> {
	public:
		static const XInfoRef* NULL_VALUE;
		static const XInfoRef* RESERVE;

		FORCE_INLINE static int hashCode(const XInfoRef* o) {
			// TODO:
			return 0;
		}

		FORCE_INLINE static boolean equals(const XInfoRef* o1, const XInfoRef* o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(XInfoRef* o) {
			o->~XInfoRef();
			// XXX: do not delete (only stack allocated)
		}

		FORCE_INLINE static void validate(const XInfoRef* o) {
			// TODO: throw UnsupportedOperationException("Invalid validate request");
		}

		FORCE_INLINE static const bytearray toData(const XInfoRef* o) {
			throw UnsupportedOperationException("Invalid bytearray conversion");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

const XInfoRef* Converter<XInfoRef*>::NULL_VALUE = null;
const XInfoRef* Converter<XInfoRef*>::RESERVE = Converter<XInfoRef*>::NULL_VALUE;

} } //namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_SEGMENT_H_*/
