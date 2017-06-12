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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESUMMARY_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESUMMARY_H_

#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/util/BasicArray.h"
#include "com/deepis/db/store/relative/util/MapFileUtil.h"

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template <typename K> struct RealTimeAdaptive_v1;

struct PagedSummary : public ReferenceObject<ushorttype, 65535> {
	private:

		// XXX: note the use of int types for index refs (usually shorts) due to null value requirement of TreeSet
		static const Comparator<inttype> INDEX_CMP;

		/* XXX: the LRT locality corresponding to the summary */
		RealTime::Locality m_streamLocality;

		/* XXX: locality of the last indexed segment of this summary (in the IRT) */
		RealTime::Locality m_pagingLocality;

		TreeSet<inttype> m_pagingRefs;
		TreeSet<inttype> m_streamRefs;

	public:
		PagedSummary(const RealTime::Locality& lrtLocality) :
			m_streamLocality(lrtLocality),
			m_pagingLocality(RealTime::Locality::LOCALITY_NONE),
			m_pagingRefs(&INDEX_CMP),
			m_streamRefs(&INDEX_CMP) {
			CXX_LANG_MEMORY_DEBUG_INIT()
			addStreamIndex(lrtLocality.getIndex());
		}

		virtual ~PagedSummary() {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);
			#ifdef DEEP_DEBUG
			const ushorttype ref = this->ref(); 
			if (ref != 0) {
				DEEP_LOG(ERROR, SUMIZ, "incorrect summary ref count, %u\n", ref);
				throw InvalidException("Incorrect summary ref count");
			}
			#endif
			CXX_LANG_MEMORY_DEBUG_CLEAR()
		}

		FORCE_INLINE const RealTime::Locality& getStreamLocality() const {
			return m_streamLocality;
		}

		FORCE_INLINE const RealTime::Locality& getPagingLocality() const {
			return m_pagingLocality;
		}

		FORCE_INLINE TreeSet<inttype>* getStreamRefs() {
			return  &m_streamRefs;
		}

		FORCE_INLINE TreeSet<inttype>* getPagingRefs() {
			return &m_pagingRefs;
		}

		FORCE_INLINE boolean hasRef(const MapFileUtil::FileType& type, ushorttype index) {
			switch(type) {
				case MapFileUtil::VRT:
				case MapFileUtil::LRT:
				case MapFileUtil::SRT:
					return (index == getStreamLocality().getIndex()) || (getStreamRefs()->contains((inttype)index) == true);
				case MapFileUtil::IRT:
					return (index == getPagingLocality().getIndex()) || (getPagingRefs()->contains((inttype)index) == true);
				default:
					break;
			}

			return false;
		}

		FORCE_INLINE void setPagingLocality(const RealTime::Locality& locality) {
			m_pagingLocality = locality;
			addPagingIndex(locality.getIndex());
		}

		FORCE_INLINE void addStreamIndex(ushorttype index) {
			if (m_streamRefs.contains(index) == false) {
				m_streamRefs.add(index);
			}
		}

		FORCE_INLINE void addStreamIndexes(const TreeSet<ushorttype>* indexes) {
			Iterator<ushorttype>* iter = ((TreeSet<ushorttype>*)indexes)->iterator();
			while (iter->hasNext()) {
				addStreamIndex(iter->next());
			}

			delete iter;
		}

		FORCE_INLINE void addStreamIndexes(const TreeSet<inttype>* indexes) {
			Iterator<inttype>* iter = ((TreeSet<inttype>*)indexes)->iterator();
			while (iter->hasNext()) {
				addStreamIndex((ushorttype)iter->next());
			}

			delete iter;
		}

		FORCE_INLINE void addPagingIndex(ushorttype index) {
			if (m_pagingRefs.contains(index) == false) {
				m_pagingRefs.add(index);
			}
		}

		FORCE_INLINE void addPagingIndexes(const TreeSet<ushorttype>* indexes) {
			Iterator<ushorttype>* iter = ((TreeSet<ushorttype>*)indexes)->iterator();
			while (iter->hasNext()) {
				addPagingIndex(iter->next());
			}

			delete iter;
		}

		FORCE_INLINE void addPagingIndexes(const TreeSet<inttype>* indexes) {
			Iterator<inttype>* iter = ((TreeSet<inttype>*)indexes)->iterator();
			while (iter->hasNext()) {
				addPagingIndex((ushorttype)iter->next());
			}

			delete iter;
		}

		FORCE_INLINE String toString() {
			String ret = getStreamLocality().toString();
			ret += String(" / ");
			ret += getPagingLocality().toString();
			ret += String(" / [");
			Iterator<inttype>* iter = getPagingRefs()->iterator();
			while(iter->hasNext()) {
				std::ostringstream stm;
				stm << iter->next();
				ret += String(stm.str());
				if (iter->hasNext()) {
					ret += String(", ");
				}
			}
			delete iter;
			ret += String("] / [");
			iter = getStreamRefs()->iterator();
			while(iter->hasNext()) {
				std::ostringstream stm;
				stm << iter->next();
				ret += String(stm.str());
				if (iter->hasNext()) {
					ret += String(", ");
				}
			}
			delete iter;
			ret += String("]");
			return ret;
		}
};

const Comparator<inttype> PagedSummary::INDEX_CMP;

} } } } } } // namespace

namespace cxx { namespace util {

template<>
class Comparator<PagedSummary*> {
	private:
		Comparator<RealTime::Locality>* m_comparator;
		boolean m_deleteCmp;

	public:
		FORCE_INLINE Comparator() :
			m_comparator(null),
			m_deleteCmp(false) {
		}
		Comparator(Comparator<RealTime::Locality>* comparator, boolean deleteCmp = false) :
			m_comparator(comparator),
			m_deleteCmp(deleteCmp) {
		}

		virtual ~Comparator() {
			if (m_deleteCmp == true) {
				Converter<Comparator<RealTime::Locality>*>::destroy(m_comparator);
			}
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const PagedSummary* o1, const PagedSummary* o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const PagedSummary* o1, const PagedSummary* o2) const {
		#endif
			return m_comparator->compareNoViewpoint(o1->getStreamLocality(), o2->getStreamLocality());
		}
};

template<>
class Converter<PagedSummary*> {
	public:
		static const PagedSummary* NULL_VALUE;
		static const PagedSummary* RESERVE;

		FORCE_INLINE static int hashCode(const PagedSummary* o) {
			long address = (long) o;
 			return address ^ ((long) address >> 32);
		}

		FORCE_INLINE static boolean equals(PagedSummary* o1, PagedSummary* o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static void destroy(PagedSummary* o) {
			validate(o);
			if ((o != null) && (o->decref() == 0)) {
				delete o;
			}
		}

		FORCE_INLINE static void validate(PagedSummary* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}

		FORCE_INLINE static const bytearray toData(const PagedSummary* o) {
			throw UnsupportedOperationException("Invalid operation: toData");
		}

		FORCE_INLINE Converter(void) {
			// nothing to do
		}
};

const PagedSummary* Converter<PagedSummary*>::NULL_VALUE = null;
const PagedSummary* Converter<PagedSummary*>::RESERVE = (PagedSummary*) Converter<PagedSummary*>::NULL_VALUE;

} } // namespace

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

struct PagedSummarySet {
	private:
		Comparator<RealTime::Locality>* m_comparator;
		Comparator<PagedSummary*> m_sumCmp;
		const char* m_filePath;

		TreeMap<RealTime::Locality, PagedSummary*> m_activeSummaries;
		TreeMap<uinttype, PagedSummary*> m_lastSummaryMap;
		TreeMap<uinttype, TreeSet<PagedSummary*>*> m_streamSummaryRefs;
		TreeMap<uinttype, TreeSet<PagedSummary*>*> m_pagingSummaryRefs;

		UserSpaceLock m_containerLock;

		FORCE_INLINE void addRefs(PagedSummary* summary, TreeSet<inttype>* refs, TreeMap<uinttype, TreeSet<PagedSummary*>*>& assoc) {
			Iterator<inttype>* iter = refs->iterator();
			while (iter->hasNext()) {
				const inttype index = iter->next();
				TreeSet<PagedSummary*>* set = assoc.get(index);
				if (set == null) {
					set = new TreeSet<PagedSummary*>(&m_sumCmp);
					set->setDeleteValue(true);
					assoc.put(index, set);
				}

				if (set->contains(summary) == true) {
					PagedSummary* o1 = summary;
					PagedSummary* o2 = set->floor(o1);;

					#ifdef DEEP_DEBUG
					if (o1 != o2) {
						struct Util {
							FORCE_INLINE static boolean compareIters(Iterator<inttype>* o1Iter, Iterator<inttype>* o2Iter) {
								while (o1Iter->hasNext()) {
									inttype ref1 = o1Iter->next();
									inttype ref2 = o2Iter->next();
									
									if (ref1 != ref2) {
										delete o1Iter;	
										delete o2Iter;

										return true;
									}		
								} 
								delete o1Iter;
								delete o2Iter;

								return false;
							}
						};

						boolean fileMismatch = false;
						if (o1->getPagingRefs()->size() == o2->getPagingRefs()->size()) {
							Iterator<inttype>* o1Iter = o1->getPagingRefs()->iterator();
							Iterator<inttype>* o2Iter = o2->getPagingRefs()->iterator();
				
							fileMismatch = Util::compareIters(o1Iter, o2Iter);	
						} else {
							fileMismatch = true;
						}

						if (fileMismatch == false) {
							if (o1->getStreamRefs()->size() == o2->getStreamRefs()->size()) {
								Iterator<inttype>* o1Iter = o1->getStreamRefs()->iterator();
								Iterator<inttype>* o2Iter = o2->getStreamRefs()->iterator();

								fileMismatch = Util::compareIters(o1Iter, o2Iter);	
							} else {
								fileMismatch = true;
							}
						}

						if (fileMismatch == true) {
							DEEP_LOG(WARN, SUMIZ, "PagedSummarySet::addRefs: identical PagedSummary localities with different file sets\n");
						}
					}
					#endif
					if (o1 != o2) {
						PagedSummary* existing = set->floor(summary);	
						existing->addPagingIndexes(summary->getPagingRefs());
						existing->addStreamIndexes(summary->getStreamRefs());
					}
					continue;
				}

				summary->incref();
				set->add(summary);
			}
			delete iter;
		}

		FORCE_INLINE void addStreamRefs(PagedSummary* summary) {
			return addRefs(summary, summary->getStreamRefs(), m_streamSummaryRefs);
		}

		FORCE_INLINE void addPagingRefs(PagedSummary* summary) {
			return addRefs(summary, summary->getPagingRefs(), m_pagingSummaryRefs);
		}

		FORCE_INLINE void removeRefs(PagedSummary* summary, TreeSet<inttype>* refs, TreeMap<uinttype, TreeSet<PagedSummary*>*>& assoc) {
			summary->incref();
			Iterator<inttype>* iter = refs->iterator();
			while (iter->hasNext()) {
				Converter<PagedSummary*>::validate(summary);
				const inttype index = iter->next();
				TreeSet<PagedSummary*>* set = assoc.get(index);
				if (set == null) {
					#ifdef DEEP_DEBUG
					DEEP_LOG(ERROR, SUMIZ, "no active refs for index %u, %s\n", index, (const char*)summary->getStreamLocality().toString());
					throw InvalidException("No active refs for index");
					#else
					continue;
					#endif
				}

				#ifdef DEEP_DEBUG
				if (set->contains(summary) == false) {
					DEEP_LOG(ERROR, SUMIZ, "summary not found in active refs for index %u, %s\n", index, (const char*)summary->getStreamLocality().toString());
					throw InvalidException("Summary not found in active refs for index");
				}
				#endif

				set->remove(summary); // XXX: implied Converter<PagedSummary*>::destroy(summary);
				if (set->isEmpty() == true) {
					assoc.remove(index);
					Converter<TreeSet<PagedSummary*>*>::destroy(set);
				}
			}
			delete iter;
			Converter<PagedSummary*>::destroy(summary);
		}

		FORCE_INLINE void removeStreamRefs(PagedSummary* summary) {
			return removeRefs(summary, summary->getStreamRefs(), m_streamSummaryRefs);
		}

		FORCE_INLINE void removePagingRefs(PagedSummary* summary) {
			return removeRefs(summary, summary->getPagingRefs(), m_pagingSummaryRefs);
		}

	public:
		PagedSummarySet(RealTime* rt) :
			m_comparator(new Comparator<RealTime::Locality>(null /* order by file index, not file time */)),
			m_sumCmp(m_comparator, false /* delete comparator */),
			m_filePath(rt->getFilePath()),
			m_activeSummaries(m_comparator),
			m_containerLock() {
			m_lastSummaryMap.setDeleteValue(true);
			m_activeSummaries.setDeleteValue(true);
			m_streamSummaryRefs.setDeleteValue(true);
			m_pagingSummaryRefs.setDeleteValue(true);
		}

		virtual ~PagedSummarySet() {
			Converter<Comparator<RealTime::Locality>*>::destroy(m_comparator);
		}

		FORCE_INLINE const Comparator<PagedSummary*>& comparator() const {
			return m_sumCmp;
 		}

		FORCE_INLINE void addSummary(PagedSummary* summary, const boolean needsLock = true) {
 			Converter<PagedSummary*>::validate(summary);

 			if (needsLock == true) lock();

 			const uinttype pagingIndex = summary->getPagingLocality().getIndex();

			PagedSummary* existing = m_activeSummaries.get(summary->getStreamLocality());
			if (existing != null) {
				DEEP_LOG(DEBUG, SUMIZ, "update summary: %s, %s\n", (const char*) summary->toString(), m_filePath);

				// XXX: update paging locality
				existing->setPagingLocality(summary->getPagingLocality());

				// XXX: update existing refs
				addStreamRefs(summary);
				addPagingRefs(summary);

				// XXX: mark summary as anchoring
				existing->incref();
				{
					PagedSummary* prevLast = m_lastSummaryMap.put(pagingIndex, existing);
					if (prevLast != null) {
						Converter<PagedSummary*>::destroy(prevLast);
					}
				}

				if (needsLock == true) unlock();

				return;
			}

			DEEP_LOG(DEBUG, SUMIZ, "add summary: %s, %s\n", (const char*) summary->toString(), m_filePath);
 			summary->incref();
 			{
				PagedSummary* prevLast = m_lastSummaryMap.put(pagingIndex, summary);
				if (prevLast != null) {
					Converter<PagedSummary*>::destroy(prevLast);
				}
			}
 			addStreamRefs(summary);
 			addPagingRefs(summary);
 			summary->incref();
			m_activeSummaries.put(summary->getStreamLocality(), summary);

			if (needsLock == true) unlock();
		}

		FORCE_INLINE void removeSummary(PagedSummary* summary, const boolean needsLock = true) {
			Converter<PagedSummary*>::validate(summary);

			if (needsLock == true) lock();

			summary->incref();
			DEEP_LOG(DEBUG, SUMIZ, "remove summary: %s, %s\n", (const char*) summary->toString(), m_filePath);
			const uinttype pagingIndex = summary->getPagingLocality().getIndex();
			PagedSummary* last = m_lastSummaryMap.get(pagingIndex);
			Converter<PagedSummary*>::validate(last);
			if (summary == last) {
				m_lastSummaryMap.remove(pagingIndex);
			}
			Converter<PagedSummary*>::validate(summary);
			removeStreamRefs(summary);
			Converter<PagedSummary*>::validate(summary);
			removePagingRefs(summary);
			Converter<PagedSummary*>::validate(summary);
			m_activeSummaries.remove(summary->getStreamLocality());
			Converter<PagedSummary*>::destroy(summary);

			if (needsLock == true) unlock();
		}

		FORCE_INLINE void clear(const boolean needsLock = true) {
			DEEP_LOG(DEBUG, SUMIZ, "clear summaries: %s\n", m_filePath);
			if (needsLock == true) lock();
			m_lastSummaryMap.clear();
			m_streamSummaryRefs.clear();
			m_pagingSummaryRefs.clear();
			m_activeSummaries.clear();
			if (needsLock == true) unlock();
		}

		FORCE_INLINE uinttype activeSummaryCount(const boolean needsLock = true) {
			if (needsLock == true) lock();
			const uinttype ret = m_activeSummaries.size();
			if (needsLock == true) unlock();
			return ret;
		}

		FORCE_INLINE TreeMap<RealTime::Locality, PagedSummary*>& allSummaries() {
			#ifdef DEEP_DEBUG
			if (tryLock() == true) {
				DEEP_LOG(ERROR, OTHER, "Unsafe access to allSummaries(): unlocked\n");
				throw InvalidException("Unsafe access to allSummaries(): unlocked");
			}
			#endif
			return m_activeSummaries;
		}

		FORCE_INLINE uinttype lastSummaryCount(const boolean needsLock = true) {
			if (needsLock == true) lock();
			const uinttype ret = m_lastSummaryMap.size();
			if (needsLock == true) unlock();
			return ret;
		}

		FORCE_INLINE TreeMap<uinttype, PagedSummary*>& lastSummaries() {
			#ifdef DEEP_DEBUG
			if (tryLock() == true) {
				DEEP_LOG(ERROR, OTHER, "Unsafe access to lastSummaries(): unlocked\n");
				throw InvalidException("Unsafe access to lastSummaries(): unlocked");
			}
			#endif
			return m_lastSummaryMap;
		}

		FORCE_INLINE TreeSet<PagedSummary*>* summariesForStreamFile(uinttype index) {
			#ifdef DEEP_DEBUG
			if (tryLock() == true) {
				DEEP_LOG(ERROR, OTHER, "Unsafe access to summariesForStreamFile(): unlocked\n");
				throw InvalidException("Unsafe access to summariesForStreamFile(): unlocked");
			}
			#endif
			return m_streamSummaryRefs.get(index);
		}

		FORCE_INLINE TreeSet<PagedSummary*>* summariesForPagingFile(uinttype index) {
			#ifdef DEEP_DEBUG
			if (tryLock() == true) {
				DEEP_LOG(ERROR, OTHER, "Unsafe access to summariesForPagingFile(): unlocked\n");
				throw InvalidException("Unsafe access to summariesForPagingFile(): unlocked");
			}
			#endif
			return m_pagingSummaryRefs.get(index);
		}

		FORCE_INLINE boolean contains(RealTime::Locality streamLocality, const boolean needsLock = true) {
			if (needsLock == true) lock();
			const boolean ret = m_activeSummaries.containsKey(streamLocality);
			if (needsLock == true) unlock();
			return ret;
		}

		FORCE_INLINE PagedSummary* getSummaryRef(const RealTime::Locality& streamLocality) {
			#ifdef DEEP_DEBUG
			if (tryLock() == true) {
				DEEP_LOG(ERROR, OTHER, "Unsafe access to summary get(): unlocked\n");
				throw InvalidException("Unsafe access to summary get(): unlocked");
			}
			#endif
			PagedSummary* ret = m_activeSummaries.get(streamLocality);
			if (ret != null) {
				Converter<PagedSummary*>::validate(ret);
				ret->incref();
			}
			// XXX: note that callee has responsibility to destroy (see incref() above)
			return ret;
		}

		FORCE_INLINE boolean isLastSummary(PagedSummary* summary, const boolean needsLock = true) {
			if (needsLock == true) lock();
			Converter<PagedSummary*>::validate(summary);
			const uinttype pagingIndex = summary->getPagingLocality().getIndex();
			PagedSummary* lastSummary = m_lastSummaryMap.get(pagingIndex);
			const boolean ret = (summary == lastSummary);
			if (needsLock == true) unlock();
			return ret;
		}

		FORCE_INLINE void lock() {
			m_containerLock.lock();
		}

		FORCE_INLINE boolean tryLock() {
			return m_containerLock.tryLock();
		}

		FORCE_INLINE void unlock() {
			m_containerLock.unlock();
		}

};

template<typename K>
struct RealTimeSummary {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;

	private:
		BasicArray<Information*> m_reuseList;
		Segment<K> m_sumSegment;
		inttype* m_stats;
		uinttype m_keyParts;
		uinttype m_indexed;
		uinttype m_reindexed;
		ulongtype m_summarized;
		RealTime::Locality m_lrtLocality;
		K m_lastKey;
		RealTime::Locality m_lastSum;
		boolean m_linked;
		boolean m_checkpoint;
		uinttype m_recoveryEpoch;

		PagedSummary* m_summaryRef;

		/* XXX: don't force inline */ inline boolean indexSegment(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* sumSegment, uinttype* indexed) {
			if ((sumSegment->SegTreeMap::size() <= 0) && (m_lastSum != RealTime::Locality::LOCALITY_NONE)) {
				// XXX: nothing to do
				return false;
			}

			if (m_linked == true) {
				sumSegment->setBackReference(m_lastSum.getIndex());
				sumSegment->setPagingPosition(m_lastSum.getLength());
			} else {
				sumSegment->setBackReference(0);
				sumSegment->setPagingPosition(0);
			}
			const boolean ret = map->indexSegment(ctxt, sumSegment, true /* rebuild */);
			if (ret == true) {
				m_lastSum = RealTime::Locality(sumSegment->getCurrentPagingIndex(), sumSegment->getPagingPosition(), RealTime::Locality::VIEWPOINT_NONE);
				m_summaryRef->setPagingLocality(m_lastSum);

				++(*indexed);
			}
			return ret;
		}

	public:
		FORCE_INLINE RealTimeSummary(RealTimeMap<K>* map, const RealTime::Locality& lrtLocality, boolean checkpoint, boolean linked) : 
			m_reuseList(Properties::DEFAULT_SEGMENT_SUMMARY_SIZE, true),
			m_sumSegment(map->m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER, Versions::GET_PROTOCOL_CURRENT(), map->m_keyBuilder->getKeyParts()),
			m_stats((inttype*)m_sumSegment.getCardinality()),
			m_keyParts(map->m_keyBuilder->getKeyParts()),
			m_indexed(0),
			m_reindexed(0),
			m_summarized(0),
			m_lrtLocality(lrtLocality),
			m_lastKey((K) Converter<K>::NULL_VALUE),
			m_lastSum(RealTime::Locality::LOCALITY_NONE),
			m_linked(true),
			m_checkpoint(checkpoint),
			m_recoveryEpoch(map->getRecoveryEpoch()),
			m_summaryRef(new PagedSummary(lrtLocality)) {
			for (inttype i = 0; i < Properties::DEFAULT_SEGMENT_SUMMARY_SIZE; i++) {
				m_reuseList.add(Information::newInfo(Information::WRITE));
			}
			m_sumSegment.setMapContext(-1);
			m_sumSegment.setDeleteKey(true);
			m_sumSegment.setVirtualSizeEnabled(false);
			m_sumSegment.setCardinalityEnabled(false);
			clear();
		}

		FORCE_INLINE static K adjustCardinality(RealTimeMap<K>* map, Segment<K>* segment, K firstKey, K lastKey, inttype* stats, inttype keyParts) {
			if (keyParts != 1) {
				for (inttype i = 0; i < keyParts; ++i) {
					stats[i] += segment->getCardinality()[i];
				}

				// XXX: adjust for overlap
				if (lastKey != (K) Converter<K>::NULL_VALUE) {
					inttype pos = 0;
					inttype result = map->m_comparator->compare(lastKey, firstKey, &pos);

					if ((result != 0) && (pos > 0)) {
						stats[0]--;
	
						#ifdef DEEP_DEBUG
						if (pos >= keyParts) {
							DEEP_LOG(ERROR, SUMIZ, "RealTimeSummary::adjustCardinality: invalid position");
							throw InvalidException("RealTimeSummary::adjustCardinality: invalid position");
						}
						#endif

						stats[pos]++;
					}

					Converter<K>::destroy(lastKey);
				}

				if (segment->getPurged() == true) {
					return map->m_keyBuilder->cloneKey(firstKey);

				} else {
					return map->m_keyBuilder->cloneKey(segment->SegTreeMap::lastKey());
				}
			}

			return lastKey;
		}

		FORCE_INLINE uinttype getIndexed() const {
			return m_indexed;
		}

		FORCE_INLINE uinttype getReindexed() const {
			return m_reindexed;
		}

		FORCE_INLINE ulongtype getSummarized() const {
			return m_summarized;
		}

		FORCE_INLINE boolean summarizeSegment(RealTimeMap<K>* map, ThreadContext<K>* ctxt, K key, Segment<K>* segment) {
			const RealTime::Locality& currentSummary = map->m_endwiseLrtLocality;

			// XXX: debug disabled due to hot index add and idle summarization
			#if 0
			const RealTime::Locality& lastSummarized = segment->getSummarizedLocality();
			const boolean checkpointSummarized = segment->getCheckpointSummarized();
			if ((lastSummarized == currentSummary) && (checkpointSummarized == m_checkpoint) && (currentSummary != RealTime::Locality::LOCALITY_NONE)) {
				DEEP_LOG(ERROR, SUMIZ, "Double-summarized segment: %s, %s\n", (const char*)currentSummary.toString(), map->getFilePath());
				throw InvalidException("Double-summarized segment");
			}
			#endif

			segment->setSummarizedLocality(currentSummary, m_checkpoint);

			// XXX: note that we are adding refs for both summaries and non-summaries here
			if ((segment->getSummary() == true) || ((segment->getPaged() == true) && (segment->getBeenSurceased() == false))) {
				if (segment->getStreamed() == true) {
					TreeSet<ushorttype> streamIndexes(&Segment<K>::USHORT_CMP);
					segment->getStreamIndexes(true /* get old */, &streamIndexes);
					m_summaryRef->addStreamIndexes(&streamIndexes);
				}
				m_summaryRef->addPagingIndexes(segment->getPagingIndexes());
			}

			if (segment->getSummary() == true) {
				if (m_sumSegment.SegTreeMap::size() > 0) {
					indexSummary(map, ctxt);
				}
		
				return reindexSummary(map, ctxt, segment);

			} else if ((segment->getPaged() == false) || (segment->getBeenSurceased() == true)) {
				return false;
			}

			#ifdef DEEP_DEBUG
			if ((segment->getViewpoint() == true) && (m_checkpoint == false)) {
				DEEP_LOG(ERROR, SUMIZ, "Checkpoint-indexed segment included in non-checkpoint summary, %s\n", map->getFilePath());
				throw InvalidException("Checkpoint-indexed segment included in non-checkpoint summary");
			}
			#endif

			/* XXX: note that this first key is fuzzy */
			K firstKey = map->m_keyBuilder->cloneKey(key);

			const boolean streamed = segment->getStreamed();
			#ifdef DEEP_DEBUG
			if ((streamed == false) && (map->m_primaryIndex == null)) {
				DEEP_LOG(ERROR, SUMIZ, "paged not streamed segment on primary, %s\n", map->getFilePath());
				throw InvalidException("Paged not streamed segment on primary");
			}
			#endif

			if (streamed == true) {
				m_sumSegment.addStreamReferences(segment->getStreamReferences());
			}

			m_sumSegment.addPagingIndexes(segment->getPagingIndexes());
			m_sumSegment.setPaged(true);

			// XXX: see fill segment summarization for overload of vrt vs irt locality
			Information* info = m_reuseList.get(m_sumSegment.SegTreeMap::size());
			info->setFileIndex(segment->getCurrentPagingIndex());
			info->setFilePosition(segment->getPagingPosition());

			#ifdef DEEP_DEBUG
			if (m_sumSegment.SegTreeMap::containsKey(firstKey) == true) {
				DEEP_LOG(ERROR, SUMIZ, "keyspace already represented in summary, %s\n", map->getFilePath());
				throw InvalidException("Keyspace already represented in summary");
			}
			#endif

			m_sumSegment.SegTreeMap::put(firstKey, info);
			m_sumSegment.vsize(m_sumSegment.vsize() + segment->vsize());
			++m_summarized;

			m_lastKey = adjustCardinality(map, segment, firstKey, m_lastKey, m_stats, m_keyParts);

			if (m_sumSegment.SegTreeMap::size() >= Properties::DEFAULT_SEGMENT_SUMMARY_SIZE) {
				// XXX: we've reached our max summary size, so flush summary to disk
				return indexSummary(map, ctxt);
			}

			return true;
		}

		FORCE_INLINE boolean indexSummary(RealTimeMap<K>* map, ThreadContext<K>* ctxt) {
			const boolean ret = indexSegment(map, ctxt, &m_sumSegment, &m_indexed);
			if (ret == true) {
				clear();
			}
			return ret;
		}

		FORCE_INLINE boolean reindexSummary(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* sumSegment) {
			m_summarized += sumSegment->SegTreeMap::size();
			return indexSegment(map, ctxt, sumSegment, &m_reindexed);
		}

		FORCE_INLINE void clear() {
			if (m_keyParts != 1) {
				for (uinttype i = 0; i < m_keyParts; ++i) {
					m_stats[i] = 0;
				}
			}

			m_sumSegment.SegTreeMap::clear();
			m_sumSegment.resetPagingIndexes();
			m_sumSegment.resetStreamIndexes();
			m_sumSegment.setPagingPosition(0);
			m_sumSegment.vsize(0);

			m_sumSegment.setStateFlags(0);
			m_sumSegment.setSummary(true);
			m_sumSegment.setRecoveryEpoch(m_recoveryEpoch);
		}

		FORCE_INLINE PagedSummary* getPagedSummary() {
			return m_summaryRef;
		}

		FORCE_INLINE virtual ~RealTimeSummary() {
			clear();
			Converter<PagedSummary*>::destroy(m_summaryRef);
			if (m_lastKey != (K) Converter<K>::NULL_VALUE) {
				Converter<K>::destroy(m_lastKey);
				m_lastKey = (K) Converter<K>::NULL_VALUE;
			}
		}
};

template<typename K>
struct DynamicSummarization {

	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;
	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename TreeMap<K,Segment<K>*>::TreeMapEntrySet::EntrySetIterator MapSegmentEntrySetIterator;

	public: 
		enum Action {
			RELOCK = 0,
			RELOOP,
			IGNORE,
			CONTINUE,
			DONE,
		};

	private:
		RealTimeMap<K>* m_map;
		boolean m_indexThread;
		inttype m_potentialSummaries;
		inttype m_summarized;
		boolean m_requiresWriteLock;
		
		enum Status {
			COLLECT = 0,
			COLLECT_SEQUENTIAL,
			SUMMARIZE,
			SUMMARIZE_SEQUENTIAL,
			SUMMARIZE_COMPLETE,
		};

		Status m_status;
		// XXX: number of sequential segments qualifying for a summary
		inttype m_qualifiedSegments;

		// XXX: an existing segment which we are using as a new summary segment
		Segment<K>* m_newSummary;
		// XXX: m_newSummary's map entry
		MapEntry<K,Segment<K>*>* m_newSummaryEntry;
		MapEntry<K,Segment<K>*>* m_loopStartEntry;

		AtomicLong* m_purgeSize;
		MapSegmentEntrySetIterator* m_segIter;

		// XXX: for cardinality adjustments
		inttype m_keyParts;
		K m_lastKey;

		FORCE_INLINE void setStatus(Status newStatus) {
			#ifdef DEEP_DEBUG
			boolean invalid = false;
			switch (m_status) {
				case COLLECT:
					if ((newStatus != COLLECT) && (newStatus != COLLECT_SEQUENTIAL)) {
						invalid = true;
					}
					break;
				case COLLECT_SEQUENTIAL:
					if ((newStatus != COLLECT) && (newStatus != SUMMARIZE)) {
						invalid = true;
					}
					break;
				case SUMMARIZE:
					if (newStatus != SUMMARIZE_SEQUENTIAL) {
						invalid = true;
					}
					break;
				case SUMMARIZE_SEQUENTIAL:
					if ((newStatus != COLLECT) && (newStatus != SUMMARIZE_COMPLETE)) {
						invalid = true;
					}
					break;
				case SUMMARIZE_COMPLETE:
					invalid = true;
					break;
			}	

			if (invalid == true) {
				throw InvalidException("DynamicSummary::setStatus: invalid state change");
			}
			#endif			

			m_status = newStatus;
		}

		FORCE_INLINE void resetFields() {
			switch (m_status) {
				case COLLECT:
				case COLLECT_SEQUENTIAL:
				case SUMMARIZE_SEQUENTIAL:
					m_newSummary = null;
					m_newSummaryEntry = null;
					m_qualifiedSegments = 0;
					if (m_requiresWriteLock == false) {
						m_potentialSummaries = 0;
					}
					return;

				default:
					throw InvalidException("DynamicSummary::resetFields: invalid status");
			}
		}

		FORCE_INLINE Action completeSummary() {
			switch (m_status) {
				case SUMMARIZE_SEQUENTIAL:
					m_newSummary->unlock();

					m_potentialSummaries--;
					DEEP_LOG(DEBUG, SUMIZ, "dynamic summarized: %d segments, remaining summaries: %d, %s\n", m_summarized, m_potentialSummaries, m_map->getFilePath());

					if (m_potentialSummaries == 0) {
						setStatus(SUMMARIZE_COMPLETE);
						return DONE;
					}

					m_loopStartEntry = m_newSummaryEntry;
					resetFields();
					// XXX: note still require write lock
					setStatus(COLLECT);

					return RELOOP;
				default:
					throw InvalidException("DynamicSummary::completeSummary: invalid status");
			}
		}

		FORCE_INLINE Action handleUnsummarizable() {
			switch (m_status) {
				case COLLECT:
					resetFields();
					return IGNORE;

				case COLLECT_SEQUENTIAL:
					resetFields();
					setStatus(COLLECT);
					return IGNORE;

				case SUMMARIZE:
					return CONTINUE;	

				case SUMMARIZE_SEQUENTIAL:
					completeSummary();
					return DONE;

				default:
					throw InvalidException("DynamicSummary::handleUnsummarizable: invalid status");
			}
		}

	public:
		DynamicSummarization(RealTimeMap<K>* map, boolean indexThread, AtomicLong* purgeSize) :
			m_map(map),
			m_indexThread(indexThread),
			m_potentialSummaries(0),
			m_summarized(0),
			m_requiresWriteLock(false),

			m_status(COLLECT),
			
			m_qualifiedSegments(0),
			m_newSummary(null),
			m_newSummaryEntry(null),
			m_loopStartEntry(null),
			m_purgeSize(purgeSize),
			m_segIter(null),
			
			m_keyParts(map->m_keyBuilder->getKeyParts()),
			m_lastKey((K)Converter<K>::NULL_VALUE) {

			}	

		FORCE_INLINE Action processEntry(MapEntry<K,Segment<K>*>* segEntry, boolean final) {
			Segment<K>* segment = segEntry->getValue();
	
			if (RealTimeAdaptive_v1<K>::allowDynamicSummarization(m_map, segment, m_indexThread) == false) {
				return IGNORE;
			}

			if (segment->tryLock() == false) {
				return handleUnsummarizable();
			}

			if (segment->getBeenDeleted() == true) {
				segment->unlock();
				return handleUnsummarizable();
			}

			if (segment->getSummary() == true) {
				segment->unlock();
				return handleUnsummarizable();
			}

			// XXX: segments with active work cannot be summarized
			if ((segment->getPurged() == false) || (segment->getRolling() == true) || (m_map->m_orderSegmentList.contains(segment, true /* lock */))) {
				segment->unlock();
				return handleUnsummarizable();
			}

			// XXX: we now have a segment we can work with
			switch (m_status) {
				case COLLECT:
					m_qualifiedSegments = 1;
					m_newSummaryEntry = segEntry;
					
					setStatus(COLLECT_SEQUENTIAL);

					segment->unlock();
					return CONTINUE;

				case COLLECT_SEQUENTIAL:
					m_qualifiedSegments++;
		
					if (m_qualifiedSegments == Properties::DEFAULT_SEGMENT_SUMMARY_SIZE) {
						m_qualifiedSegments = 0;
						segment->unlock();

						// XXX: this is our first summary segment
						if (m_requiresWriteLock == false) {
							m_potentialSummaries++;

							if (m_potentialSummaries == Properties::DEFAULT_SEGMENT_SUMMARIZATION_LIMIT) {
								m_requiresWriteLock = true;

								setStatus(SUMMARIZE);
								return RELOCK;

							} else {
								return CONTINUE;	
							}
						// XXX: this is not our first summary segment
						} else {
							m_loopStartEntry = m_newSummaryEntry;

							setStatus(SUMMARIZE);
							return RELOOP;
						}
					}

					segment->unlock();
					return CONTINUE;

				case SUMMARIZE:
					if (segEntry == m_newSummaryEntry) {
						SegMapEntry* infoEntry = (SegMapEntry*) segment->firstEntry();
						Information* summInfo = infoEntry->getValue();
						
						// XXX: don't modify information shared with other indexes
						if (summInfo->ref() > 1) {
							summInfo = Information::newInfo(Information::WRITE);
							Information* oldinfo = segment->SegTreeMap::put(infoEntry->getKey(), summInfo);

							Converter<Information*>::destroy(oldinfo);
						}

						summInfo->setFileIndex(segment->getCurrentPagingIndex(), false /* validate */);
						summInfo->setFilePosition(segment->getPagingPosition(), false /* validate */);

						segment->resetFlags();
						segment->setSummary(true);
						segment->setPaged(true);

						// XXX: note this segment must stay locked until we're done
						m_newSummary = segment;
						m_newSummary->setCardinalityEnabled(true);

						m_summarized++;

						setStatus(SUMMARIZE_SEQUENTIAL);

					} else {
						segment->unlock();
					}

					return CONTINUE;

				case SUMMARIZE_SEQUENTIAL:
					if (m_newSummary == null) {
						throw InvalidException("DynamicSummary::processEntry: summarize sequential will null summary");

					} else {
						Information* summInfo = segment->remove(segment->firstKey());

						// XXX: don't modify information shared with other indexes
						if (summInfo->ref() > 1) {
							Converter<Information*>::destroy(summInfo);
							summInfo = Information::newInfo(Information::WRITE);
						}
					
						summInfo->setFileIndex(segment->getCurrentPagingIndex(), false /* validate */);
						summInfo->setFilePosition(segment->getPagingPosition(), false /* validate */);

						K firstKey = m_map->m_keyBuilder->cloneKey(segEntry->getKey());
						m_newSummary->SegTreeMap::put(firstKey, summInfo);
						m_newSummary->vsize(m_newSummary->vsize() + segment->vsize());

						m_newSummary->addPagingIndexes(segment->getPagingIndexes());
						m_newSummary->addStreamReferences(segment->getStreamReferences());

						inttype* cardinalityStats = (inttype*) m_newSummary->getCardinality();
						m_lastKey = RealTimeSummary<K>::adjustCardinality(m_map, segment, firstKey, m_lastKey, cardinalityStats, m_keyParts);
					
						// XXX: account for less purged segments (actual active vs actual purged)
						m_purgeSize->decrementAndGet();
						m_summarized++;

						m_segIter->MapSegmentEntrySetIterator::remove();
						m_map->deleteSegment(segment, segment->SegTreeMap::firstKey(), true /* destroy */, false /* lock */, false /* remove */); 
					}

					if ((final == true) || (m_newSummary->SegTreeMap::size() == Properties::DEFAULT_SEGMENT_SUMMARY_SIZE)) {
						return completeSummary();
					}
					return CONTINUE;

				default:
					throw InvalidException("DynamicSummary::handleUnsummarizable: invalid status");
			}
		}

		FORCE_INLINE MapEntry<K,Segment<K>*>* getLoopStartEntry() const {
			return m_loopStartEntry;
		}

		FORCE_INLINE void setSegmentIterator(MapSegmentEntrySetIterator* segIter) {
			m_segIter = segIter;
		}

		FORCE_INLINE boolean getRequiresContextWriteLock() const {
			return m_requiresWriteLock;
		}

		#ifdef DEEP_DEBUG
		FORCE_INLINE boolean partialSummarization() const {
			return (m_status == SUMMARIZE_SEQUENTIAL);
		}
		#endif

		FORCE_INLINE virtual ~DynamicSummarization() {

		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESUMMARY_H_*/
