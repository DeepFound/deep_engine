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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMERECOVERY_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMERECOVERY_H_

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

#include "com/deepis/db/store/relative/core/RealTimeMap.h"

template<typename K>
class RealTimeRecovery {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;
	typedef typename TreeMap<K,Segment<K>*>::TreeMapEntrySet::EntrySetIterator MapSegmentEntrySetIterator;
	typedef typename SegTreeMap::TreeMapEntrySet::EntrySetIterator MapInformationEntrySetIterator;

	public:
		static void partitionSegment(RealTimeMap<K>* map, Segment<K>* p) {
			ArrayList<Segment<K>*> partitionSegmentList(Properties::LIST_CAP);
			partitionSegmentList.add(p);

			typename RealTimeIndex::PurgeReport purgeReport;

			while (partitionSegmentList.size() > 0) {
				const int last = partitionSegmentList.size()-1;
				Segment<K>* segment = partitionSegmentList.get(last);
				if (segment->getStreamRangeOverflow() == false) {
					partitionSegmentList.remove(last);

					// XXX: check whether cache pressure requires segment purging
					map->purgeSegment(map->m_threadContext.getContext(), segment, false /* not growing */, false /* index */, false /* semi */, null /* purgeList */, null, purgeReport);

					continue;
				}

				K firstKey = (K) Converter<K>::NULL_VALUE;
				Segment<K>* pSegment = map->splitSegment(null /* ctxt */, segment, &firstKey /* of pSegment */, false /* lock */, true /* TODO: why? */);

				if (segment->getStreamRangeOverflow() == false) {
					partitionSegmentList.remove(last);

					// XXX: check whether cache pressure requires segment purging
					map->purgeSegment(map->m_threadContext.getContext(), segment, false /* not growing */, false /* index */, false /* semi */, null /* purgeList */, null, purgeReport);
				}
				partitionSegmentList.add(pSegment);

				map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(firstKey, pSegment);
			}
		}

		static boolean repairStreamRange(RealTimeMap<K>* map, Segment<K>* segment) {
			if (map->forceSetupSegment(map->m_threadContext.getContext(), segment, true /* physical */, false /* values */) == false) {
				return false;
			}

			typename RealTimeIndex::PurgeReport purgeReport;

			// XXX: clear stream index/range
			segment->resetStreamIndexes();
			typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

			segment->SegTreeMap::entrySet(&stackSegmentItemSet);
			MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
			for (int i = 0; infoIter->MapInformationEntrySetIterator::hasNext(); i++) {
				SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();
				Information* curinfo = map->isolateInformationLocality(map->m_threadContext.getContext(), infoEntry->getValue());

				segment->addStreamIndex(curinfo->getFileIndex());
				if (segment->getStreamRangeOverflow() == true) {
					partitionSegment(map, segment);
					segment->unlock();
					return true;
				}
			}

			// XXX: check whether cache pressure requires segment purging
			map->purgeSegment(map->m_threadContext.getContext(), segment, false /* not growing */, false /* index */, false /* semi */, null /* purgeList */, null, purgeReport);

			segment->unlock();
			return false;
		}

		static void repairStreamRanges(RealTimeMap<K>* map) {
			// XXX: safe context lock: one writer / no readers on the branch tree
			map->m_threadContext.writeLock();
			{
				#ifdef DEEP_DEBUG
				boolean validate = false;
				#endif
				RETRY:
				if (map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {
					typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);

					map->m_branchSegmentTreeMap.entrySet(&stackSegmentSet);
					MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
					while (segIter->MapSegmentEntrySetIterator::hasNext()) {
						MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
						Segment<K>* segment = segEntry->getValue();

						if ((segment->getSummary() == true) || (segment->getStreamRangeOverflow() == false)) {
							continue;
						}

						#ifdef DEEP_DEBUG
						if (validate == true) {
							DEEP_LOG(ERROR, OTHER, "Stream range overflow detected after repair, %s\n", map->getFilePath());
							throw InvalidException("Stream range overflow detected after repair");
						}
						#endif

						if (repairStreamRange(map, segment) == true) {
							// XXX: reset due to split
							goto RETRY;
						}
					}

					#ifdef DEEP_DEBUG
					if (validate == false) {
						validate = true;
						goto RETRY;
					}
					#endif
				}
			}
			map->m_threadContext.writeUnlock();
		}

		static boolean repairSegmentCompressionAlignment(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* prev, RealTime::Locality& lastCompressed, Segment<K>* segment, Segment<K>** lastSegment) {
			struct Util {
				FORCE_INLINE static void markCompressed(Segment<K>* segment, uinttype position) {
					segment->setStreamPosition(position);
				}

				FORCE_INLINE static void markAltered(Segment<K>* segment) {
					segment->setBeenRealigned(true);
					segment->setDirty(true);
					segment->setBeenAltered(true);
				}

				FORCE_INLINE static RealTime::Locality compressedLocality(Segment<K>* segment, Information* info) {
					if (info->getCompressed() == false) {
						return RealTime::Locality::LOCALITY_NONE;
					}
					return RealTime::Locality(info->getFileIndex(), segment->getStreamPosition(), RealTime::Locality::VIEWPOINT_NONE);
				}

				FORCE_INLINE static RealTime::Locality compressedLocality(Information* info) {
					return ((info->getCompressed() == true) ? RealTime::Locality(info->getFileIndex(), info->getFilePosition(), RealTime::Locality::VIEWPOINT_NONE) : RealTime::Locality::LOCALITY_NONE);
				}
			};

			#ifdef DEEP_DEBUG
			if (map->m_primaryIndex != null) {
				DEEP_LOG(ERROR, OTHER, "Segment realignment must be via primary, %s\n", map->getFilePath());
				throw InvalidException("Segment realignment must be via primary");
			}
			#endif

			*lastSegment = segment;

			// XXX: bail out early if nothing to do
			if ((segment->getSummary() == true) || (segment->getValuesCompressed() == true) || (segment->SegTreeMap::size() == 0)) {
				return false;
			}

			boolean branchModified = false;

			typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);
			segment->SegTreeMap::entrySet(&stackSegmentItemSet);
			MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
			SegMapEntry* infoEntry = null;
			boolean modified = false;
			K firstKey = segment->SegTreeMap::firstKey();

			// XXX: add to the previous segment, while compressed block matches
			if ((prev != null) && (lastCompressed != RealTime::Locality::LOCALITY_NONE) && (prev->getValuesCompressed() == true) && (prev->getSummary() == false) && (prev->SegTreeMap::size() != 0)) {
				while (infoIter->MapInformationEntrySetIterator::hasNext() == true) {
					infoEntry = infoIter->MapInformationEntrySetIterator::next();
					K key = infoEntry->getKey();
					Information* info = map->isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
					RealTime::Locality cmpLoc = Util::compressedLocality(info);
					if (info->getDeleting() == true) {
						RealTimeAdaptive_v1<K>::deleteUnviewed(map, ctxt, infoEntry, segment, &modified);
						infoIter->remove();
						if (key != firstKey) {
							Converter<K>::destroy(key);
						}
						modified = true;
						continue;
					}

					// XXX: break when compressed offset diverges from prev
					if ((info->getCompressed() == false) || (lastCompressed != cmpLoc)) {
						break;
					}

					// XXX: move info to prev segment
					prev->SegTreeMap::put(infoEntry->getKey(), infoEntry->getValue());
					infoIter->remove();
					modified = true;
				}
			}

			// XXX: if we moved some infos, reseed or delete the current segment
			if (modified == true) {
				modified = false;
				Util::markAltered(prev);
				Util::markAltered(segment);
				branchModified = true;
				if (segment->SegTreeMap::size() != 0) {
					map->reseedSegment(segment, firstKey, false /* destroy */, false /* lock */);
				} else {
					*lastSegment = prev;
					map->deleteSegment(segment, firstKey, false /* destroy */, false /* lock */);
					return branchModified;
				}
			}

			// XXX: locate the first info past a block of infos in the same compressed block (or all not compressed)
			boolean first = true;
			while (true) {
				if (infoEntry == null) {
					if (infoIter->hasNext() == false) {
						break;
					}
					infoEntry = infoIter->MapInformationEntrySetIterator::next();
				}
				K key = infoEntry->getKey();
				Information* info = map->isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
				RealTime::Locality cmpLoc = Util::compressedLocality(info);
				if (info->getDeleting() == true) {
					RealTimeAdaptive_v1<K>::deleteUnviewed(map, ctxt, infoEntry, segment, &modified);
					infoIter->remove();
					if (key != firstKey) {
						Converter<K>::destroy(key);
					}
					infoEntry = null;
					modified = true;
					continue;
				}

				if (first == true) {
					first = false;
					lastCompressed = cmpLoc;
					if (cmpLoc != RealTime::Locality::LOCALITY_NONE) {
						Util::markCompressed(segment, info->getFilePosition());
					}
				} else if (cmpLoc != lastCompressed) {
					break;
				}

				infoEntry = null;
			}

			// XXX: if we moved some infos, reseed or delete the current segment
			if (modified == true) {
				modified = false;
				Util::markAltered(segment);
				branchModified = true;
				if (segment->SegTreeMap::size() != 0) {
					map->reseedSegment(segment, firstKey, false /* destroy */, false /* lock */);
				} else {
					*lastSegment = prev;
					map->deleteSegment(segment, firstKey, false /* destroy */, false /* lock */);
					return branchModified;
				}
			}

			// XXX: move remaining infos to next segment(s)
			if (infoEntry != null) {
				first = true;
				Segment<K>* next = null;
				while (true) {
					K key = infoEntry->getKey();
					Information* info = map->isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
					RealTime::Locality cmpLoc = Util::compressedLocality(info);
					if (info->getDeleting() == true) {
						RealTimeAdaptive_v1<K>::deleteUnviewed(map, ctxt, infoEntry, segment, &modified);
						infoIter->remove();
						if (key != firstKey) {
							Converter<K>::destroy(key);
						}
						if (infoIter->hasNext() == false) {
							break;
						}
						infoEntry = infoIter->next();
						modified = true;
						continue;
					}

					if (first == true) {
						first = false;
						lastCompressed = cmpLoc;
						branchModified = true;
						next = map->initSegment(ctxt, infoEntry->getKey());
						*lastSegment = next;
						if(cmpLoc != RealTime::Locality::LOCALITY_NONE) {
							Util::markCompressed(next, info->getFilePosition());
						}
					} else if (cmpLoc != lastCompressed) {
						lastCompressed = cmpLoc;
						next->unlock();
						next = map->initSegment(ctxt, infoEntry->getKey());
						*lastSegment = next;
						if (cmpLoc != RealTime::Locality::LOCALITY_NONE) {
							Util::markCompressed(next, info->getFilePosition());
						}
					}

					// XXX: move info to next segment
					next->SegTreeMap::put(infoEntry->getKey(), infoEntry->getValue());

					infoIter->remove();
					modified = true;
					if (infoIter->hasNext() == false) {
						break;
					}
					infoEntry = infoIter->next();
				}
				next->unlock();
			}

			// XXX: if we moved some infos, mark the current segment
			if (modified == true) {
				modified = false;
				Util::markAltered(segment);
			}

			return branchModified;
		}

		static void repairSegmentCompressionAlignment(RealTimeMap<K>* map, ThreadContext<K>* ctxt) {
			// XXX: safe context lock: one writer / no readers on the branch tree
			map->m_threadContext.writeLock();
			{
				Segment<K>* prev = null;
				RealTime::Locality lastCompressed(RealTime::Locality::LOCALITY_NONE);
				if (map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {
					typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);

					map->m_branchSegmentTreeMap.entrySet(&stackSegmentSet);
					MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
					RETRY:
					while (segIter->MapSegmentEntrySetIterator::hasNext()) {
						MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
						Segment<K>* segment = segEntry->getValue();
						RESETUP:
						if (segment->getSummary() == false) {
							map->forceSetupSegment(ctxt, segment, true /* physical */, false /* values */, true /* force */);
						} else {
							segment->lock();
							if (segment->getSummary() == false) {
								segment->unlock();
								goto RESETUP;
							}
						}

						Segment<K>* lastSegment = segment;
						if (repairSegmentCompressionAlignment(map, ctxt, prev, lastCompressed, segment, &lastSegment) == true) {
							// XXX: reset iterator to segment after lastSegment
							if (lastSegment != null) {
								segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset(lastSegment->firstKey(), false /* ceiling */);
							} else {
								segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
							}

							segment->unlock();
							if (prev != null) {
								prev->unlock();
							}

							prev = lastSegment;
							if (prev != null) {
								prev->lock();
							}
							/* XXX: lastCompressed should already be correct */

							goto RETRY;
						}

						if (prev != null) {
							prev->unlock();
						}
						prev = segment;
					}
				}
				if (prev != null) {
					prev->unlock();
				}
			}
			#ifdef DEEP_DEBUG
			verifySegmentCompressionAlignment(map, ctxt);
			#endif
			map->m_threadContext.writeUnlock();
		}

		FORCE_INLINE static void verifySegmentCompressionAlignment(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* segment) {
			typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);
			segment->SegTreeMap::entrySet(&stackSegmentItemSet);
			MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();

			while(infoIter->hasNext() == true) {
				SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();
				Information* info = map->isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);

				if ((info->getCompressed() == true) && (segment->getValuesCompressed() == false)) {
					BUILDER_KEY_TO_STRING(map->m_keyBuilder, firstkeybuf, segment->firstKey())
					BUILDER_KEY_TO_STRING(map->m_keyBuilder, keybuf, infoEntry->getKey())
					DEEP_LOG(ERROR, OTHER, "Detected individually-compressed info: %s in segment: %s, %s\n", keybuf, firstkeybuf, map->getFilePath());
					throw InvalidException("Detected individually-compressed info");
				}
			}
		}

		FORCE_INLINE static void verifySegmentCompressionAlignment(RealTimeMap<K>* map, ThreadContext<K>* ctxt) {
			if (map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() == 0) {
				return;
			}
			typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);

			map->m_branchSegmentTreeMap.entrySet(&stackSegmentSet);
			MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
			while (segIter->MapSegmentEntrySetIterator::hasNext()) {
				MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
				Segment<K>* segment = segEntry->getValue();
				// FLUX: this is not valid w/ FLUX on
				if (segment->getSummary() == true) {
					DEEP_LOG(ERROR, OTHER, "Invalid summary state, %s\n", map->getFilePath());
					throw InvalidException("Invalid summary state");
				}
				map->forceSetupSegment(ctxt, segment, true /* physical */, false /* values */, true /* force */);
				verifySegmentCompressionAlignment(map, ctxt, segment);
				segment->unlock();
			}
		}

		static boolean mayContainSummary(MeasuredRandomAccessFile* iwfile, const RealTimeLocality& lastLrtLocality, const uinttype recoveryEpoch, Comparator<MeasuredRandomAccessFile*>* irtCmp) {
			return (iwfile->getRecoveryEpoch() >= recoveryEpoch) && irtCmp->compare(iwfile, lastLrtLocality.getIndex(), lastLrtLocality.getLength()) <= 0;
		}

		// XXX: find the summary in the irt of a given index; returns true iff the summary is found in the index
		static boolean findSummaryInIndex(RealTime* index, const RealTimeLocality& lastLrtLocality, const uinttype recoveryEpoch, Comparator<MeasuredRandomAccessFile*>* irtCmp) {
			// TODO: make the search for iwfile more efficient (binary search is possible here...)
			PriorityQueue<MeasuredRandomAccessFile*> iwrites(irtCmp);
			{
				Iterator<MeasuredRandomAccessFile*>* iter = index->getShare()->getIrtWriteFileList()->iterator();
				while (iter->hasNext() == true) {
					MeasuredRandomAccessFile* iwfile = iter->next();
					iwrites.add(iwfile);
				}
				Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
			}

			while (iwrites.size() > 0) {
				MeasuredRandomAccessFile* iwfile = iwrites.remove();
				if (mayContainSummary(iwfile, lastLrtLocality, recoveryEpoch, irtCmp) == true) {
					// FLUX
					DEEP_LOG(DEBUG, DCVRY, "may contain summary %s => %s, %s\n", (const char*) lastLrtLocality.toString(), (const char*) iwfile->getLastLrtLocality().toString(), iwfile->getPath());
					longtype lastIrtLength = index->findSummaryPaging(iwfile, lastLrtLocality, recoveryEpoch);
					if (lastIrtLength < Properties::DEFAULT_FILE_HEADER) {
						index->clearRecoverPosition();
						continue;
					}
					index->setRecoverPosition(iwfile->getFileIndex(), lastIrtLength, lastLrtLocality, iwfile->getEntrySize());

					return true;
				}
			}
			return false;
		}

		// XXX: find the summary represented by iwfile across other indexes; returns true iff summary is found globally
		static boolean findSummaryGlobally(RealTimeMap<K>* map, BasicArray<RealTime*>& indexes, MeasuredRandomAccessFile* iwfile, Comparator<MeasuredRandomAccessFile*>* irtCmp) {
			if (iwfile->getInvalidSummary() == true) {
				return false;
			}

			const inttype anchor = iwfile->getSecondaryIndex();
			const RealTimeLocality& lastLrtLocality = iwfile->getLastLrtLocality();
			const uinttype recoveryEpoch = iwfile->getRecoveryEpoch();
			if ((findSummaryInIndex(indexes.get(anchor), lastLrtLocality, recoveryEpoch, irtCmp) == false) || (iwfile->getFileIndex() != indexes.get(anchor)->getRecoverIrtIndex())) {
				indexes.get(anchor)->clearRecoverPosition();
				return false;
			}
			for (inttype i = 0; i < indexes.size(); ++i) {
				if ((i == anchor) || (indexes.get(i) == null)) {
					continue;
				}

				if (findSummaryInIndex(indexes.get(i), lastLrtLocality, recoveryEpoch, irtCmp) == false) {
					indexes.get(anchor)->clearRecoverPosition();
					for (inttype j = i-1; j >= 0; --j) {
						indexes.get(j)->clearRecoverPosition();
					}
					return false;
				}
			}

			return true;
		}

		static boolean hasNoSummary(MeasuredRandomAccessFile* iwfile) {
			return iwfile->getLastLrtLocality().isNone();
		}

		static boolean findRecoveryBoundary(RealTimeMap<K>* map, PriorityQueue<MeasuredRandomAccessFile*>& iwrites, Comparator<MeasuredRandomAccessFile*>* irtCmp, boolean nonempty, RealTimeLocality& finalLrtLocality, boolean& seenAllIndexes) {
			Comparator<inttype> intCmp;
			TreeSet<inttype> indexSet(&intCmp);
			indexSet.add(0);
			BasicArray<RealTime*> indexes(map->m_share.getSecondaryList()->size()+1);
			indexes.add(map);
			#ifdef DEEP_DEBUG
			if (map->getIndexValue() != 0) {
				DEEP_LOG(ERROR, OTHER, "Invalid map index value: %d\n", map->getIndexValue());
				throw InvalidException("Invalid map index value");
			}
			#endif
			if (map->m_hasSecondaryMaps == true) {
				for (inttype i = 0; i < map->m_share.getSecondaryList()->size(); ++i) {
					RealTime* rt = map->m_share.getSecondaryList()->get(i);
					indexes.add(rt);
					if (rt == null) {
						continue;
					}
					#ifdef DEEP_DEBUG
					if (rt->getIndexValue() != (i+1)) {
						DEEP_LOG(ERROR, OTHER, "Invalid map index value: %d\n", rt->getIndexValue());
						throw InvalidException("Invalid map index value");
					}
					#endif
					indexSet.add(i+1);
				}
			}

			// XXX: iterate over candidate recovery boundaries (from higher to lower lrt index/position) until we find one
			while (iwrites.size() > 0) {
				MeasuredRandomAccessFile* iwfile = iwrites.remove();
				if (hasNoSummary(iwfile) == true) {
					// FLUX
					DEEP_LOG(DEBUG, DCVRY, "contains no summary [%d] %s, %s\n", iwfile->getFileIndex(), (const char*) iwfile->getLastLrtLocality().toString(), indexes.get(iwfile->getSecondaryIndex())->getFilePath());
					continue;
				}
				if (indexSet.contains(iwfile->getSecondaryIndex())) {
					DEEP_LOG(DEBUG, DCVRY, "ending lrt/vrt [%d] %s, %s\n", iwfile->getFileIndex(), (const char*) iwfile->getLastLrtLocality().toString(), indexes.get(iwfile->getSecondaryIndex())->getFilePath());
				}
				indexSet.remove(iwfile->getSecondaryIndex());
				if (indexSet.size() != 0) {
					// XXX: We can't choose a candidate recovery boundary until we've seen all the indexes
					continue;
				}

				seenAllIndexes = true;

				const boolean irtBeyondLrt = (finalLrtLocality != RealTimeLocality::LOCALITY_NONE) && (map->getLocalityCmp()->compare(finalLrtLocality, iwfile->getLastLrtLocality()) < 0);
				if (irtBeyondLrt == true) {
					DEEP_LOG(WARN, RCVRY, "irt locality is newer than final lrt locality; %s < %s, %s\n",  (const char*) iwfile->getLastLrtLocality().toString(), (const char*) finalLrtLocality.toString(),indexes.get(iwfile->getSecondaryIndex())->getFilePath());
					continue;
				}

				if (findSummaryGlobally(map, indexes, iwfile, irtCmp) == true) {
					DEEP_LOG(DEBUG, DCVRY, "using global summary for lrt/vrt [%d] %s, %s\n", iwfile->getFileIndex(), (const char*) iwfile->getLastLrtLocality().toString(), indexes.get(iwfile->getSecondaryIndex())->getFilePath());
					return true;
				}

				// XXX: only warn on non-empty map
				if (nonempty == true) {
					DEEP_LOG(WARN, RCVRY, "global summary not found for lrt/vrt [%d] %s, %s\n", iwfile->getFileIndex(), (const char*) iwfile->getLastLrtLocality().toString(), indexes.get(iwfile->getSecondaryIndex())->getFilePath());
				}
			}

			if ((nonempty == true) && (indexSet.size() != 0)) {
				DEEP_LOG(WARN, RCVRY, "global summary not found; missing irt files for %d indexes, %s\n", indexSet.size(), map->getFilePath());
			}

			// XXX: No valid global summary was found
			map->setRecoverPosition(-1, -1L, RealTimeLocality(0, Properties::DEFAULT_FILE_HEADER, RealTimeLocality::VIEWPOINT_NONE), -1L);

			return false;
		}

		static void cleanupUnusedIrtFiles(RealTime* index) {
			MeasuredRandomAccessFile* iwfileRecover = index->getShare()->getIrtWriteFileList()->get(index->getRecoverIrtIndex());
			Iterator<BufferedRandomAccessFile*>* irIter = index->getShare()->getIrtReadFileList()->iterator();
			Iterator<MeasuredRandomAccessFile*>* iwIter = index->getShare()->getIrtWriteFileList()->iterator();
			boolean clean = false;
			while((iwIter->hasNext() == true) && (irIter->hasNext() == true)) {
				MeasuredRandomAccessFile* iwfile = iwIter->next();
				BufferedRandomAccessFile* irfile = irIter->next();
				if (clean == true) {
					irIter->remove();
					iwIter->remove();

					DEEP_LOG(DEBUG, RCVRY, "clobber %s\n", iwfile->getPath());
					index->getShare()->destroy(irfile);
					index->getShare()->destroy(iwfile, true /* clobber */);

					continue;
				}
				if (iwfileRecover == iwfile) {
					clean = true;
				}
			}
			Converter<Iterator<BufferedRandomAccessFile*>*>::destroy(irIter);
			Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iwIter);
		}

		static boolean initializeIndexing(RealTimeMap<K>* map) {
			boolean success = true;
			BasicArray<RealTime*> initialized;
			RealTime* index = map;
			inttype i = -1;
			do {
				cleanupUnusedIrtFiles(index);
				MeasuredRandomAccessFile* iwfile = index->getShare()->getIrtWriteFileList()->get(index->getRecoverIrtIndex());
				iwfile->setPagingState(iwfile->getSecondaryIndex(), iwfile->getSummaryState(), index->getRecoverIrtLength(), index->getRecoverLrtLocality());
				if (index->initSummaryPaging(iwfile) == false) {
					success = false;
					break;
				}

				initialized.add(index, true /* resize */);

				RETRY:
				if (++i < map->m_share.getSecondaryList()->size()) {
					index = map->m_share.getSecondaryList()->get(i);
					if (index == null) {
						goto RETRY;
					}
				} else {
					break;
				}
			} while(true);

			if (success == false) {
				for (i=0; i<initialized.size(); ++i) {
					index = initialized.get(i);
					index->getActiveSummaries()->clear();
					index->deleteSegmentMemory();
				}
			}

			return success;
		}

		static void addAll(PriorityQueue<MeasuredRandomAccessFile*>& iwrites, MapFileSet<MeasuredRandomAccessFile>* l) {
			Iterator<MeasuredRandomAccessFile*>* iter = l->iterator();
			while (iter->hasNext() == true) {
				MeasuredRandomAccessFile* f = iter->next();
				if (f == null) {
					abort();
				}
				iwrites.add(f);
			}
			Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMERECOVERY_H_*/
