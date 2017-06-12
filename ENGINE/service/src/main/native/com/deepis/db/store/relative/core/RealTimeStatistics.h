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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESTATISTICS_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESTATISTICS_H_ 

#include "com/deepis/db/store/relative/core/RealTimeMap.h"

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename K>
class RealTimeStatistics {

	public:
		inline static boolean stream(RealTimeMap<K>* map, ThreadContext<K>* ctxt, HashMap<ushorttype,longtype>* stats) {

			longtype total = 0;
			longtype start = RealTimeResource::currentTimeMillis();

			RETRY:
			total += stats->HashMap<ushorttype,longtype>::size();
			stats->clear();

			// XXX: safe context lock: multiple readers / no writer on the branch tree
			map->m_threadContext.readLock();
			{
				if (map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {
					typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);
					map->m_branchSegmentTreeMap.entrySet(&stackSegmentSet);

					MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
					for (int c = 0; segIter->MapSegmentEntrySetIterator::hasNext(); c++) {
						MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
						Segment<K>* segment = segEntry->getValue();

						longtype stop = RealTimeResource::currentTimeMillis();
						if (((stop - start) > Properties::DEFAULT_CACHE_LIMIT) || (map->m_mount != RealTimeMap<K>::MOUNT_OPENED)) {
							DEEP_LOG(DEBUG, PAUSE, "store: %s, elapsed: %lld, stream statistics: %d / %lld\n", map->getFilePath(), (stop-start), c, total);
							ctxt->readUnlock();
							return false;
						}

						if (segment->getSummary() == true) {
							segment->incref();

							ctxt->readUnlock();

							if (map->forceSetupSegment(ctxt, segment, false /* physical */, false /* values */) == true) {
								segment->unlock();
							}

							goto RETRY;

						} else if (segment->getStreamed() == false) {
							continue;
						}

						inttype files = 1;
						#ifdef DEEP_DEBUG
						if (segment->getStreamRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */) != 0) {
						#else
						if (segment->getStreamRange() != 0) {
						#endif
							#ifdef DEEP_DEBUG
							ubytetype fileRange = segment->getStreamRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */);
							#else
							ubytetype fileRange = segment->getStreamRange();
							#endif
							for (int i = Properties::DEFAULT_SEGMENT_FILE_RANGE - 1; i >= 0; i--) {
								if (fileRange & (1 << i)) {
									files++;
								}
							}
						}

						inttype block = segment->vsize() / files;

						longtype stat = stats->get(segment->getCurrentStreamIndex()) + block;
						// XXX: null get returns -1
						if (stat == 0) {
							stat++;
						}

						stats->put(segment->getCurrentStreamIndex(), stat);
					
						#ifdef DEEP_DEBUG
						if (segment->getStreamRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */) != 0) {
						#else
						if (segment->getStreamRange() != 0) {
						#endif
							#ifdef DEEP_DEBUG
							ubytetype fileRange = segment->getStreamRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */);
							#else
							ubytetype fileRange = segment->getStreamRange();
							#endif
							for (int i = Properties::DEFAULT_SEGMENT_FILE_RANGE - 1; i >= 0; i--) {
								if (fileRange & (1 << i)) {
									ushorttype fileIndex = segment->getCurrentStreamIndex() - i - 1;

									stat = stats->get(fileIndex) + block;
									// XXX: null get returns -1
									if (stat == 0) {
										stat++;
									}

									stats->put(fileIndex, stat);
								}
							}
						}
					}
				}
			}
			map->m_threadContext.readUnlock();

			return true;
		}

		inline static boolean paging(RealTimeMap<K>* map, ThreadContext<K>* ctxt, HashMap<ushorttype,longtype>* stats) {

			longtype total = 0;
			longtype start = RealTimeResource::currentTimeMillis();

			RETRY:
			total += stats->HashMap<ushorttype,longtype>::size();
			stats->clear();

			// XXX: safe context lock: multiple readers / no writer on the branch tree
			map->m_threadContext.readLock();
			{
				if (map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {
					typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);
					map->m_branchSegmentTreeMap.entrySet(&stackSegmentSet);

					MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
					for (int c = 0; segIter->MapSegmentEntrySetIterator::hasNext(); c++) {
						MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
						Segment<K>* segment = segEntry->getValue();

						longtype stop = RealTimeResource::currentTimeMillis();
						if (((stop - start) > Properties::DEFAULT_CACHE_LIMIT) || (map->m_mount != RealTimeMap<K>::MOUNT_OPENED)) {
							DEEP_LOG(DEBUG, PAUSE, "store: %s, elapsed: %lld, paging statistics: %d / %lld\n", map->getFilePath(), (stop-start), c, total);
							ctxt->readUnlock();
							return false;
						}

						if (segment->getSummary() == true) {
							segment->incref();

							ctxt->readUnlock();

							if (map->forceSetupSegment(ctxt, segment, false /* physical */, false /* values */) == true) {
								segment->unlock();
							}

							goto RETRY;

						} else if (segment->getPaged() == false) {
							continue;
						}

						longtype stat = stats->get(segment->getCurrentPagingIndex()) + 1;
						// XXX: null get returns -1
						if (stat == 0) {
							stat++;
						}

						stats->put(segment->getCurrentPagingIndex(), stat);
						#ifdef DEEP_DEBUG
						if (segment->getPagingRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */) != 0) {
						#else
						if (segment->getPagingRange() != 0) {
						#endif
							#ifdef DEEP_DEBUG
							ubytetype fileRange = segment->getPagingRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */);
							#else
							ubytetype fileRange = segment->getPagingRange();
							#endif
							for (int i = Properties::DEFAULT_SEGMENT_FILE_RANGE - 1; i >= 0; i--) {
								if (fileRange & (1 << i)) {
									ushorttype fileIndex = segment->getCurrentPagingIndex() - i - 1;
					
									stat = stats->get(fileIndex) + 1;
									// XXX: null get returns -1
									if (stat == 0) {
										stat++;
									}

									stats->put(fileIndex, stat);
								}
							}
						}
					}
				}
			}
			map->m_threadContext.readUnlock();

			return true;
		}

	typedef typename TreeMap<K,Segment<K>*>::TreeMapEntrySet::EntrySetIterator MapSegmentEntrySetIterator;
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESTATISTICS_H_*/
