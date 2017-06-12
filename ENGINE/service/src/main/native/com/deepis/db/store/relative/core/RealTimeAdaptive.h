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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEADAPTIVE_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEADAPTIVE_H_ 

#include "RealTimeExtra.h"

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename K>
struct RealTimeAdaptive_v1 {

	static const inttype INDEXING_MOD = 333;
	static const inttype FILEREF_MOD = 333;

	// XXX: segment limits need to fit in 2^16
	static const int DEFAULT_FRAGMENT_LIMIT = 25;

	static const inttype NANO_ELEMENT_SIZE = 500000 + 1; /* 500K */
	static const inttype MICRO_ELEMENT_SIZE = 5000000 + 1; /* 5M */
	static const inttype MACRO_ELEMENT_SIZE = 50000000 + 1; /* 50M */

	static const int DEFAULT_FILE_FRAGMENT_CYCLE_COUNT = 2; /* two indexes */

	static const int DEFAULT_FRAGMENT_MINIMUM = DEFAULT_FRAGMENT_LIMIT;
	static const int DEFAULT_FRAGMENT_MAXIMUM = DEFAULT_FRAGMENT_LIMIT * 4;

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;

	typedef typename RealTimeIndex::PurgeReport PurgeReport;

	private:
		FORCE_INLINE static inttype getSegmentVirtualSize(RealTimeMap<K>* map) {
			return map->getSegmentSize() * DEFAULT_FRAGMENT_LIMIT;
		}

	public:
		FORCE_INLINE static boolean getSystemIdle(RealTimeMap<K>* map, boolean external = false) {
			RealTime* primary = (map->m_primaryIndex == null) ? map : map->m_primaryIndex;
			longtype time = external == false ? primary->getOrganizationTime() : primary->getExternalWorkTime();

			boolean idle = (System::currentTimeMillis() - time) > Properties::DEFAULT_CACHE_FINALIZE_IDLE;
			if (idle == true) {
				for (int i = 0; i < primary->getShare()->getSecondaryList()->size(); i++) {
					RealTime* rt = primary->getShare()->getSecondaryList()->get(i);

					if (rt == null) {
						// XXX: allow for dynamic add/remove secondary index
						continue;
					}

					longtype time = external == false ? rt->getOrganizationTime() : rt->getExternalWorkTime();

					idle = (System::currentTimeMillis() - time) > Properties::DEFAULT_CACHE_FINALIZE_IDLE;
					if (idle == false) {
						break;
					}
				}
			}
			return idle;
		}

		FORCE_INLINE static uinttype getFileInitialSize(RealTimeMap<K>* map, boolean adaptive) {
			#if 0
			return Properties::getFileSize();
			#else
			uinttype fileSize = Properties::getFileSize();

			// XXX: override floor for extra-small file size; only possible w/ DEEP_MAGIC
			if (fileSize < Properties::DEFAULT_FILE_MIN_SIZE /* 100MB */) {
				return fileSize;
			}

			if ((adaptive == true) /* && (Properties::getFileSize() == Properties::DEFAULT_FILE_DATA_SIZE)*/) {
				longtype entries = map->size();
				if (entries < (longtype) NANO_ELEMENT_SIZE) {
					fileSize = fileSize / 10;

				} else if (entries < (longtype) MICRO_ELEMENT_SIZE) {
					fileSize = fileSize / 5;

				} else if (entries < (longtype) MACRO_ELEMENT_SIZE) {
					fileSize = fileSize / 2;

				/* XXX: ???
				} else if (entries > (longtype) BIGDATA_ELEMENT_SIZE) {
					fileSize = fileSize * ???;
				*/
				}
			}

			// XXX: hard floor in case default file size was changed
			if (fileSize < 104857600 /* 100MB */) {
				fileSize = 104857600;
			}

			return fileSize;
			#endif
		}

		FORCE_INLINE static inttype getSegmentInitialSize(RealTimeMap<K>* map, boolean adaptive) {
			#if 0
			return Properties::getSegmentSize();
			#else
			inttype segmentSize = Properties::getSegmentSize();

			if ((adaptive == true) && (Properties::getSegmentSize() == Properties::DEFAULT_SEGMENT_SIZE)) {
				longtype entries = map->size();
				if (entries < (longtype) NANO_ELEMENT_SIZE) {
					segmentSize = segmentSize / 10;

				} else if (entries < (longtype) MICRO_ELEMENT_SIZE) {
					segmentSize = segmentSize / 5;

				} else if (entries < (longtype) MACRO_ELEMENT_SIZE) {
					segmentSize = segmentSize / 2;

				/* XXX: not performant due to transactional chucking
				} else if (entries > (longtype) BIGDATA_ELEMENT_SIZE) {
					segmentSize = segmentSize * ???;
				*/
				}
			}

			// XXX: hard floor in case default segment size was changed
			if (segmentSize < 150) {
				segmentSize = 150;
			}

			return segmentSize;
			#endif
		}

		FORCE_INLINE static boolean physicalMinimum(RealTimeMap<K>* map, Segment<K>* segment) {
			return (segment->vsize() < map->getSegmentMinimumSize());
		}

		FORCE_INLINE static boolean virtualMaximum(RealTimeMap<K>* map, Segment<K>* segment, boolean exceeds = false) {
			return (segment->vsize() >= ((exceeds == true) ? getSegmentVirtualSize(map) + 1 : getSegmentVirtualSize(map)));
		}

		FORCE_INLINE static boolean physicalMaximum(RealTimeMap<K>* map, Segment<K>* segment, inttype multiplier = 1) {
			return (segment->SegTreeMap::size() >= (map->getSegmentSize(false /* adaptive */) * multiplier));
		}

		FORCE_INLINE static boolean byteMinimum(RealTimeMap<K>* map, Segment<K>* segment) {
			RealTimeShare* share = (map->m_primaryIndex == null) ? &(map->m_share) : map->m_primaryIndex->getShare();
			inttype rowSize = (share->getValueSize() > -1) ? share->getValueSize() : share->getValueAverage();

			return ((segment->SegTreeMap::size() * rowSize) < map->getSegmentMinimumByteSize());
		}

		FORCE_INLINE static boolean byteMaximum(RealTimeMap<K>* map, Segment<K>* segment) {
			RealTimeShare* share = (map->m_primaryIndex == null) ? &(map->m_share) : map->m_primaryIndex->getShare();
			inttype rowSize = (share->getValueSize() > -1) ? share->getValueSize() : share->getValueAverage();
			inttype segmentSize = (segment->getVirtual() == true) ? segment->vsize() : segment->SegTreeMap::size();

			return ((segmentSize * rowSize) >= Properties::DEFAULT_SEGMENT_BYTE_SIZE);
		}

		static const inttype fileFragmentationCycleCount(RealTimeMap<K>* map) {
			return DEFAULT_FILE_FRAGMENT_CYCLE_COUNT;
		}

		static const doubletype valueCompressionPercentage(RealTimeMap<K>* map) {
			return Properties::getValueCompressPercent();
		}

		static doubletype getFragmentationPercentage(floattype elementCount, doubletype fragmentCount) {
			doubletype percent = 0.0;

			if ((elementCount == 0.0) || (fragmentCount == 0.0)) {
				percent = 1.0;

			} else {
				percent = (fragmentCount / elementCount) * 100.0;
			}

			return percent;
		}

		static const boolean reorganizeFiles(RealTimeMap<K>* map, inttype totalSegments, inttype orderSegments) {
			doubletype workingThreshold = Properties::getReorgWorkPercent();

			if (map->m_resource.getFSUsage((map->m_primaryIndex == null) ? map : map->m_primaryIndex) >= RealTimeResource::IMMENSE) {
				workingThreshold *= 10;
			}

			return (((doubletype) orderSegments) / (totalSegments)) <= workingThreshold;
		}

		// XXX: see writeFragmentation for actual key rebuilding
		FORCE_INLINE static void readKeyFragmentation(RealTimeMap<K>* map, Segment<K>* segment, inttype xfrag) {
			segment->resetFragmentCount(xfrag);

			if (xfrag > DEFAULT_FRAGMENT_MINIMUM) {
				map->m_resetIndex++;

				if ((map->m_resetIndex % INDEXING_MOD) == 0) {
					segment->setFragmentedKey(true);
					segment->setDirty(true);
					xfrag = 0;
				}
			}

			if (xfrag > DEFAULT_FRAGMENT_MAXIMUM) {
				segment->setFragmentedKey(true);
				segment->setDirty(true);
			}
		}

		// XXX: see writeFragmentation for actual key/value rebuilding
		FORCE_INLINE static void readValueFragmentation(RealTimeMap<K>* map, Segment<K>* segment, inttype xfrag) {
			if (xfrag > map->getSegmentMinimumSize()) {
				segment->setFragmentedValue(true);
				segment->setDirty(true);
			}
		}

		FORCE_INLINE static boolean writeFragmentation(RealTimeMap<K>* map, Segment<K>* segment, uinttype viewpoint, boolean growing) {

			if (segment->getSummary() == true) {
				return false;
			}

			#if 0
			if (growing == false) {
				growing = Properties::getMemoryStreaming();
			}
			#endif

			boolean rebuild = segment->getBeenAltered() || segment->getBeenReseeded() || ((viewpoint == 0) && segment->getFragmentedKey());

			// XXX: auto-increment information is naturally sequential
			if (map->m_hasReservation == true) {
				return rebuild;
			}

			if (viewpoint == 0) {
				ubytetype xfrag = segment->getFragmentCount();
				if (xfrag > DEFAULT_FRAGMENT_MINIMUM) {
					map->m_resetIndex++;

					if ((map->m_resetIndex % INDEXING_MOD) == 0) {
						rebuild = true;
						xfrag = 0;
					}
				}

				// XXX: virtual maximum should never be exceeded since indexing will
				//      have done a split, but we're keeping the check here in case
				//      it protects us from some forgotten legacy bug. The ifdef
				//      will alert us if our expectation proves incorrect. The edge
				//      case is that virtualMaximum depends on map->entries, so
				//      technically a segment can exceed the max, but it's unlikely.
				if (((xfrag > DEFAULT_FRAGMENT_MAXIMUM) && (growing == false)) || (virtualMaximum(map, segment, true /* exceeds */) == true)) {
					#ifdef DEEP_DEBUG
					if (virtualMaximum(map, segment, true /* exceeds */) == true) {
						DEEP_LOG(ERROR, OTHER, "Invalid capacity: segment exceeds virtual maximum: %d, %s\n", segment->vsize(), map->getFilePath());
						throw InvalidException("Invalid capacity: segment exceeds virtual maximum");
					}
					#endif

					if (map->m_primaryIndex == null) {
						segment->setFragmentedValue(true);
					}

					rebuild = true;
				}
			}

			return rebuild;
		}

		// XXX: only merge segments in the same files (future will do the math to make this broader)
		static boolean mergeFragmentation(RealTimeMap<K>* map, Segment<K>* segment, Segment<K>* nSegment) {

			if ((segment->getPurged() == true) || (nSegment->getPurged() == true)) {
				return false;
			}

			if ((segment->getViewpoint() == true) || (nSegment->getViewpoint() == true)) {
				return false;
			}

			if ((byteMaximum(map, segment) == true) || (byteMaximum(map, nSegment) == true)) {
				return false;
			}

			if ((byteMinimum(map, segment) == false) || (byteMinimum(map, nSegment) == false)) {
				return false;
			}

			if ((virtualMaximum(map, segment) == true) || (virtualMaximum(map, nSegment) == true)) {
				return false;
			}

			if ((physicalMinimum(map, segment) == false) || (physicalMinimum(map, nSegment) == false)) {
				return false;
			}

			if (/* (nSegment->getDirty() == false) || */ (segment->getStreamed() != nSegment->getStreamed())) {
				return false;
			}

			#if 0
			if (nSegment->getSummarizedLocality() != segment->getSummarizedLocality()) {
				return false;
			}
			#endif

			if (/* (Properties::getMemoryStreaming() == false) && */((segment->getVirtual() == true) || (nSegment->getVirtual() == true))) {
				return false;
			}

			if ((segment->getBeenRealigned() == true) || (nSegment->getBeenRealigned() == true)) {
				return false;
			}

			if ((segment->getStreamed()) != (nSegment->getStreamed())) {
				return false;
			}

			if ((segment->getRolling() == true) || (nSegment->getRolling() == true)) {
				return false;
			}

			
			if (segment->getStreamed() == true) {
				if (segment->getCurrentStreamIndex() != nSegment->getCurrentStreamIndex()) {
					return false;
				}
				#ifdef DEEP_DEBUG
				ubytetype range = segment->getStreamRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */);
				ubytetype nRange = segment->getStreamRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */);
				#else
				ubytetype range = segment->getStreamRange();
				ubytetype nRange = segment->getStreamRange();
				#endif

				if (range != nRange) {
					return false;
				}
			}

			if ((segment->isReferenced() == true) || (nSegment->isReferenced() == true)) {
				return false;
			}

			map->m_orderSegmentList.lock();

			const boolean partial = (map->m_orderSegmentMode != RealTimeMap<K>::MODE_INDEX) && (map->m_orderSegmentList.contains(segment, false /* lock */) == false) && (map->m_orderSegmentList.contains(nSegment, false /* lock */) == true);

			map->m_orderSegmentList.unlock();
			return (partial == false);
		}

		static void fragmentationStatistics(RealTimeMap<K>* map, MapFileSet<MeasuredRandomAccessFile>* wfiles, HashMap<ushorttype,uinttype>* statistics, boolean getDeadCount = false) {

			Iterator<MeasuredRandomAccessFile*>* iter = wfiles->iterator();
			while (iter->hasNext() == true) {
				MeasuredRandomAccessFile* wfile = iter->next();
				if (wfile == null) {
					continue;
				}

				ushorttype index = wfile->getFileIndex();

				uinttype count;
				if (getDeadCount == true) {
					count = wfile->getDeadCount();

				} else {
					count = wfile->getTotalCount();
				}

				statistics->put(index, count);
			}
			Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
		}

		static void compressionStatistics(RealTimeMap<K>* map, MapFileUtil::FileType fileType, HashMap<ushorttype,doubletype>* statistics) {

			MapFileSet<MeasuredRandomAccessFile> wfiles((fileType == MapFileUtil::VRT) ? *(map->m_share.getVrtWriteFileList()) : *(map->m_share.getIrtWriteFileList()));

			Iterator<MeasuredRandomAccessFile*>* iter = wfiles.iterator();
			while (iter->hasNext() == true) {
				MeasuredRandomAccessFile* wfile = iter->next();
				if (wfile == null) {
					continue;
				}

				ushorttype index = wfile->getFileIndex();
				longtype compressTotal = wfile->getCompressTotal();
				doubletype stat = ((doubletype) compressTotal) / ((doubletype) wfile->length());

				statistics->put(index, stat);
			}
			Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
		}

		static boolean keyCompression(RealTimeMap<K>* map, Segment<K>* segment, boolean rebuild, BufferedRandomAccessFile* wfile) {
			if (map->m_keyCompressMode == false) {
				return false;
			}

			// XXX: key compress good ratios
			floattype lastRatio = wfile->getCompressionRatio();
			boolean qualified = (lastRatio == 0.0) || (lastRatio > Properties::DEFAULT_KEY_COMPRESSION_RATIO); 

			// XXX: key compress base deltas
			qualified = (qualified == true) && ((rebuild == true) || (segment->getPaged() == false) || (segment->getFragmentCount() == 0));

			// XXX: compression is most useful on large chunks of data
			qualified = (qualified == true) && (segment->SegTreeMap::size() > (Properties::DEFAULT_SEGMENT_SIZE / 2));

			// XXX: key compress all summaries
			qualified = (qualified == true) || (segment->getSummary() == true);

			return qualified;
		}

		static boolean valueCompression(RealTimeMap<K>* map, ushorttype fileIndex) {

			// XXX: compression is not worth it for this table
			if (map->m_extraStats.getCompressionQualified(fileIndex) == false) {
				return false;
			}

			// XXX: necessary statistics have not yet been calculated
			if (((map->m_share.getValueSize() == -1) && (map->m_share.getValueAverage() == -1)) || (map->m_share.getKeyBlockAverage() == 0)) {
				return true;
			}
	
			inttype valueSize = (map->m_share.getValueSize() != -1) ? map->m_share.getValueSize() : map->m_share.getValueAverage();
			longtype uncompressedSize = map->getSegmentSize() * valueSize;

			longtype compressedSize = 0;
			if (map->m_valueCompressedBlockCount != 0) {
				compressedSize = map->m_valueCompressedBlockSize / map->m_valueCompressedBlockCount;
			}

			longtype spaceSavings = uncompressedSize - (compressedSize + map->m_share.getKeyBlockAverage());
			if (spaceSavings > 0) {
				// XXX: compression must have a higher ratio the more secondaries there are (performance)
				spaceSavings = spaceSavings * (1.0 - (Properties::DEFAULT_SECONDARY_COMPRESSION_FACTOR * map->m_share.getSecondaryList()->size()));
			}

			boolean qualified = (spaceSavings > 0);

			map->m_extraStats.setCompressionQualified(fileIndex, qualified);

			return qualified;			
		}

		static void writeValueStatistics(RealTimeMap<K>* map, boolean force) {

			MapFileSet<RandomAccessFile> xwfiles(*(map->m_share.getXrtWriteFileList()));
			MapFileSet<MeasuredRandomAccessFile> vwfiles(*(map->m_share.getVrtWriteFileList()));

			HashMap<ushorttype,doubletype> compressionStatistics;
			if (map->m_valueCompressMode == true) {
				RealTimeAdaptive_v1<K>::compressionStatistics(map, MapFileUtil::VRT, &compressionStatistics);
			}

			HashMap<ushorttype,uinttype> valueDeadCounts;
			HashMap<ushorttype,uinttype> valueTotalCounts;
			fragmentationStatistics(map, &vwfiles, &valueTotalCounts, false /* use dead counts */);
			fragmentationStatistics(map, &vwfiles, &valueDeadCounts, true /* use dead counts */);
			
			Iterator<RandomAccessFile*>* xwriter = xwfiles.iterator();
			while (xwriter->hasNext() == true) {

				RandomAccessFile* xwfile = xwriter->next();
				if (xwfile == null) {
					continue;
				}

				ushorttype index = xwfile->getFileIndex();
				bytetype delta = (force == false) ? Properties::DEFAULT_VALUE_STATISTIC_PERCENT_REWRITE : 0;

				doubletype compressionStat;
				uinttype deadStat = valueDeadCounts.get(index);
				uinttype totalStat = valueTotalCounts.get(index);

				// XXX: rewrite stats to srt if any changed by +/- 3%
				boolean rewrite = false;

				boolean oldQualified = map->m_extraStats.getCompressionQualified(index);
				boolean qualifiedStat = valueCompression(map, index);

				uinttype oldValueDead = map->m_extraStats.getValueFragmentationDeadCount(index);
				uinttype oldValueTotal = map->m_extraStats.getValueFragmentationTotalCount(index);

				rewrite = (oldQualified != qualifiedStat);
			
				if (rewrite == false) {
					if ((oldValueTotal == 0) && (totalStat != 0)) {
						rewrite = true;
					} else {
						inttype diff = (oldValueTotal == totalStat) ? 0 : ((oldValueTotal - totalStat) / oldValueTotal) * 100;
						rewrite = (diff > delta) || (diff < -delta);
					}
				} 

				if (rewrite == false) {
					if ((oldValueDead == 0) && (deadStat != 0)) {
						rewrite = true;
					} else {
						inttype diff = (oldValueDead == deadStat) ? 0 : ((oldValueDead - deadStat) / oldValueDead) * 100;
						rewrite = (diff > delta) || (diff < -delta);
					}
				}	
				
				compressionStat = compressionStatistics.get(index);
				if ((rewrite == false) && (map->m_valueCompressMode == true)) {
					inttype diff = map->m_extraStats.getCompressionPercentage(index) - (compressionStat * 100);
					rewrite = (diff > delta) || (diff < -delta);
				}

				if (rewrite == true) {
					if (map->m_valueCompressMode == true) {
						map->m_extraStats.setCompressionPercentage(index, compressionStat * 100);
					}

					map->m_extraStats.setValueFragmentationTotalCount(index, totalStat);
					map->m_extraStats.setValueFragmentationDeadCount(index, deadStat);

					map->m_share.acquire(xwfile);
					{
						ExtraStatistics_v1::updateValue(xwfile, &(map->m_extraStats), index, map->m_schemaBuilder->getValueCompression());
					}
					map->m_share.release(xwfile);
				}
			}
			Converter<Iterator<RandomAccessFile*>*>::destroy(xwriter);
		}

		static void writeKeyStatistics(RealTimeMap<K>* map, boolean force) {

			RandomAccessFile* xwfile;
			if (map->m_primaryIndex == null) {
				xwfile = map->getShare()->getXrtWriteFileList()->last();

			} else {
				xwfile = map->m_primaryIndex->getShare()->getXrtWriteFileList()->last();
			}

			if (xwfile == null) {
				return;
			}

			MapFileSet<MeasuredRandomAccessFile> iwfiles(*(map->m_share.getIrtWriteFileList()));

			HashMap<ushorttype,uinttype> keyDeadCounts;
			HashMap<ushorttype,uinttype> keyTotalCounts;
			fragmentationStatistics(map, &iwfiles, &keyDeadCounts, true /* use dead counts */);
			fragmentationStatistics(map, &iwfiles, &keyTotalCounts, false /* use dead counts */);

			Iterator<MeasuredRandomAccessFile*>* iter = iwfiles.iterator();
			while (iter->hasNext() == true) {

				RandomAccessFile* iwfile = iter->next();
				if (iwfile == null) {
					continue;
				}

				ushorttype index = iwfile->getFileIndex();
				uinttype deadStat = keyDeadCounts.get(index);
				uinttype totalStat = keyTotalCounts.get(index);

				map->m_extraStats.setKeyFragmentationDeadCount(index, deadStat);
				map->m_extraStats.setKeyFragmentationTotalCount(index, totalStat);

				map->m_share.acquire(xwfile);
				{
					ExtraStatistics_v1::updateKey(xwfile, &(map->m_extraStats), index, map->m_schemaBuilder->getKeyName());
				}
				map->m_share.release(xwfile);
			}
			Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
		}

		static boolean fileFragmented(RealTimeMap<K>* map, MeasuredRandomAccessFile* wfile, doubletype fPercent) {
			doubletype limit = (Properties::getFragmentationPercent() * 100);

			// XXX: if idle we can reorganize even slightly fragmented files
			if (getSystemIdle(map) == true) {
				return (fPercent > (limit / Properties::DEFAULT_FILE_FRAG_IDLE_DIVISOR)); 
			}

			// XXX: otherwise take into account the file size and amount of fragmentation
			longtype thisFileSize = wfile->length();
			longtype maxFileSize = Properties::getFileSize();

			doubletype sizeRatio = thisFileSize / maxFileSize;

			if (sizeRatio > Properties::DEFAULT_FILE_FRAG_SIZE_RATIO_HIGH) {
				return fPercent > (limit - Properties::DEFAULT_FILE_FRAG_DIFF_HIGH);
			}

			if (sizeRatio > Properties::DEFAULT_FILE_FRAG_SIZE_RATIO_MED_HIGH) {
				return fPercent > (limit - Properties::DEFAULT_FILE_FRAG_DIFF_MED_HIGH);
			}
			
			if (sizeRatio > Properties::DEFAULT_FILE_FRAG_SIZE_RATIO_MED_LOW) {
				return fPercent > (limit - Properties::DEFAULT_FILE_FRAG_DIFF_MED_LOW);
			}

			if (sizeRatio > Properties::DEFAULT_FILE_FRAG_SIZE_RATIO_LOW) {
				return fPercent > (limit - Properties::DEFAULT_FILE_FRAG_DIFF_LOW);
			}

			return (fPercent > limit);
		}

		static boolean driveReorganization(RealTimeMap<K>* map, MeasuredRandomAccessFile* wfile, boolean vCompress, ulongtype optimizeCount, HashMap<ushorttype,uinttype>* totalCountStats, HashMap<ushorttype,uinttype>* deadCountStats, HashMap<ushorttype,doubletype>* compressionStats, boolean* checkFileReferences) {

			uinttype deadCount = deadCountStats->get(wfile->getFileIndex());
			uinttype totalCount = totalCountStats->get(wfile->getFileIndex());

			doubletype fPercent = (deadCount == 0) ? 0 : (((doubletype) deadCount) / totalCount) * 100;
			doubletype cPercent = compressionStats->get(wfile->getFileIndex());

			boolean drive = ((totalCount == 0) && (deadCount != 0)) || ((wfile->getOptimizeCount() != optimizeCount)) || (fileFragmented(map, wfile, fPercent));
			if ((drive == false) && (vCompress == true)) {
				drive = (valueCompression(map, wfile->getFileIndex()) == true) && (cPercent < valueCompressionPercentage(map));
			}

			inttype fileRefCheckMod = Properties::getFileRefCheckMod();
			#ifdef DEEP_DEBUG
			// XXX: force this in debug to test accuracy of statistics (this check should never be necessary)
			if (fileRefCheckMod == 0) {
				fileRefCheckMod = 33;
			}
			#endif
			if ((drive == false) && (fileRefCheckMod > 0)) {
				if (wfile->getReorgIgnoreCount() > Properties::DEFAULT_FILE_MAX_REORG_IGNORE) {
					map->m_resetFileRefCheck++;

					if ((map->m_resetFileRefCheck % fileRefCheckMod) == 0) {
						drive = true;
						(*checkFileReferences) = true;
					}

				} else {
					wfile->incrementReorgIgnoreCount();
				}
			}

			if (drive == true) {
				#if 1
				DEEP_LOG(DEBUG, REORG, "block: %s, executing: ref[%d], f[%f], c[%f], t[%d] d[%d]\n", wfile->getPath(), *checkFileReferences, fPercent, cPercent * 100, totalCount, deadCount);
				#else
				DEEP_LOG(DEBUG, REORG, "block: %s, executing: f[%f], c[%f]\n", wfile->getPath(), fPercent, cPercent * 100);
				#endif
				wfile->resetReorgIgnoreCount();
			}

			if ((wfile->getPercentFragmented() != fPercent) || (wfile->getPercentCompressed() != cPercent)) {
				wfile->setPercentFragmented(fPercent);
				wfile->setPercentCompressed(cPercent);

				if (drive == false) {
					#if 1
					DEEP_LOG(DEBUG, REORG, "block: %s, statistic: f[%f], c[%f], t[%d] d[%d]\n", wfile->getPath(), fPercent, cPercent * 100, totalCount, deadCount);
					#else
					DEEP_LOG(DEBUG, REORG, "block: %s, statistic: f[%f], c[%f]\n", wfile->getPath(), fPercent, cPercent * 100);
					#endif
				}
			}

			return drive;
		}

		static inttype cacheMinimization(RealTimeMap<K>* map, inttype active, boolean deep, boolean growing) {

			#if 0
			if (growing == false) {
				growing = Properties::getMemoryStreaming();
			}
			#endif

			if (map->m_resource.getCacheUsage() < RealTimeResource::IMMENSE) {

				// XXX: allow dynamic indexing to use more memory (see allowPurgeSegment below)
				if ((map->m_reindexing.get() > 0) || (map->m_state == RealTimeMap<K>::MAP_INDXING)) {
					return 0;
				}
			}

			inttype reduce = 1;
			if (active > reduce) {

				if ((deep == false) && (growing == false)) {
					reduce = active * (Properties::DEFAULT_CACHE_SIZE_PERCENT / 2);

				} else {
					reduce = active * Properties::DEFAULT_CACHE_SIZE_PERCENT;
				}

				reduce = active - reduce;
				if (reduce < 0) {
					reduce = 1;
				}
			}

			return (inttype) reduce;
		}

		static boolean reindexCheckpoint(RealTimeMap<K>* map, Segment<K>* segment) {
			if ((segment->getDirty() == false) && (segment->getBeenAltered() == false)) {
				return false;
			}

			if (map->m_resource.getPurgeFlag() == true) {
				return true;
			}

			if (map->m_resource.getCacheUsage() >= RealTimeResource::IMMENSE) {
				return true;
			}

			return false;
		}

		static boolean allowPurgeSegment(RealTimeMap<K>* map, Segment<K>* segment, boolean index, boolean growing, boolean semi, PurgeReport& report) {

			// XXX: no cache pressure
			if (map->m_resource.getPurgeFlag() == false) {
				++report.purgeFlag;
				return false;

			// XXX: summary or active work to be indexed
			} else if (segment->getSummary() == true) {
				++report.summary;
				return false;

			// XXX: already purged or not yet to be indexed
			} else if ((segment->getPurged() == true) || (segment->getPaged() == false)) {
				++report.purged;
				return false;

			// XXX: allow dynamic indexing to use more memory (see cacheMinimization above)
			} else if ((map->m_reindexing.get() > 0) && (map->m_resource.getCacheUsage() < RealTimeResource::IMMENSE)) {
				++report.reindexing;
				return false;

			// XXX: allow dynamic indexing to use more memory (see cacheMinimization above)
			} else if ((map->m_state == RealTimeMap<K>::MAP_INDXING) && (map->m_resource.getCacheUsage() < RealTimeResource::IMMENSE)) {
				++report.indexState;
				return false;

			// XXX: only allow the index thread to purge requested segment reorganizations (i.e. indicate complete on index)
			} else if ((index == false) && ((segment->getFragmentedKey() == true) || (segment->getFragmentedValue() == true))) {
				++report.fragmentedKey;
				return false;

			// XXX: segments are in a critical state where it needs to complete persistence to storage (i.e. don't purge until complete)
			} else if ((segment->getBeenReseeded() == true) || (segment->getBeenAltered() == true) || (segment->getBeenRelocated() == true) || (segment->getViewed() == true) || (segment->getRolling() == true)) {
				++report.reseeded;
				return false;

			// XXX: allow segments to semi-purge when cache pressure is high and memory stream is on (e.g. high insert and read rate, and low update rate)
			} else if ((segment->getDirty() == true) && (semi == false) /* && ((map->m_resource.getCacheUsage() < RealTimeResource::IMMENSE) || (Properties::getMemoryStreaming() == false)) */) {
				++report.dirty;
				return false;

			} else if (segment->isReferenced() == true) {
				return false;
			}

			return true;
		}

		static boolean purgeSegmentCompressedData(RealTimeMap<K>* map, Segment<K>* segment) {
			if (map->m_memoryCompressMode == false) {
				return false;
			}
	
			if (segment->getZipData() == null) {
				return false;
			}

			if (map->getFailedPurgeCycles() == 0) {
				return false;
			}

			if (map->m_resource.getCacheUsage() < RealTimeResource::SHALLOW) {
				return false;
			}

			return true;
		}

		static boolean allowMemoryCompression(RealTimeMap<K>* map, Segment<K>* segment, boolean compressMode, boolean memoryMode, boolean growing) {
			if (compressMode == false) {
				return false;
			}

			if (map->m_state != RealTimeMap<K>::MAP_RUNNING) {
				return false;
			}

			#ifdef DEEP_FORCE_MEMORY_COMPRESSION
			// XXX:for unit test
			return true;
			#endif

			if (memoryMode == true) {
				return true;
			}

			// XXX: CMPRS infos in memory compressed segments will need to be dealt with (getValuesCompressed may not be true)
			if (map->m_valueCompressMode == true) {
				return false;
			}

			if (segment->getValuesCompressed() == true) {
				return false;
			}

			if (segment->SegTreeMap::size() < (Properties::getSegmentSize() / Properties::DEFAULT_MEMORY_COMPRESSION_SEGMENT_SIZE_DIVISOR)) {
				return false;
			}

			if (getSystemIdle(map, true /* use external work */) == true) {
				return false;
			}

			boolean allowKV = allowValueMemoryCompression(map, segment);

			// XXX: don't compress if K ratios are low
			const bytetype keyRatioMin = Properties::DEFAULT_MINIMUM_MEMORY_COMPRESSION_KEY;
			if ((allowKV == false) && (map->getCompressionRatioKey() != CHAR_MIN) && (map->getCompressionRatioKey() < keyRatioMin)) {
				return false;
			}

			// XXX: don't compress if K/V ratios are low
			const bytetype keyValueRatioMin = Properties::DEFAULT_MINIMUM_MEMORY_COMPRESSION_KEY_VALUE;
			if ((allowKV == true) && (map->getCompressionRatioKeyValue() != CHAR_MIN) && (map->getCompressionRatioKeyValue() < keyValueRatioMin)) {
				return false;
			}

			// XXX: purge if we were or are way over cache limit (i.e. past extreme or active extreme cache pressure)
			if (((map->getCycleCacheUsage() == RealTimeResource::EXTREME) || (map->m_resource.getCacheUsage() == RealTimeResource::EXTREME))) {
				return false;
			}

			// XXX: in loading scenarios it is better to purge
			if (growing == false) {
				return true;
			}

			return false;	
		}

		static boolean allowValueMemoryCompression(RealTimeMap<K>* map, Segment<K>* segment) {
			if (segment->getBeenFilled() == false) {
				return false;
			}

			if (map->m_primaryIndex != null) {
				return false;
			}
	
			const bytetype keyValueRatioMin = Properties::DEFAULT_MINIMUM_MEMORY_COMPRESSION_KEY_VALUE;
			if ((map->getCompressionRatioKeyValue() != CHAR_MIN) && (map->getCompressionRatioKeyValue() < keyValueRatioMin)) {
				return false;
			}

			return true;
		}

		static boolean readInfoValueFromPrimary(const RealTimeMap<K>* map, const Information* info) {
			if ((map->m_resource.getPurgeFlag() == true) /* || (map->m_resource.getCacheUsage() > RealTimeResource::IMMENSE) */) {
				return false;
			}
			return true;
		}

		static boolean primaryInformationOpenSegment(RealTimeMap<K>* map) {
			if ((map->m_coldPointReadLimitReached == false) && (map->m_coldPointRead.get() > (map->m_entrySize.get() / Properties::COLD_POINT_READ_DIVISOR))) {
				map->m_coldPointReadLimitReached = true;
			}
			return map->m_coldPointReadLimitReached;
		}

		static boolean deleteUnviewed(RealTimeMap<K>* map, ThreadContext<K>* ctxt, SegMapEntry* infoEntry, Segment<K>* segment, boolean* dirty, boolean inLine = false) {

			StoryLine& storyLine = infoEntry->getStoryLine();
			Information* orginfo = infoEntry->getValue();

			if ((orginfo->getLevel() == Information::LEVEL_COMMIT) && (orginfo->getDeleting() == true)) {
				Transaction* tx = ctxt->getTransaction();

				const longtype viewpoint = (Properties::getCheckpointMode() != Properties::CHECKPOINT_OFF) ? map->getActiveCheckpoint() : RealTime::Locality::VIEWPOINT_NONE;

				// XXX: need to hold onto checkpointed info and keep it alive until finalized/summarized
				Information* chkinfo = (viewpoint > 0) ? map->isolateCheckpoint(ctxt, orginfo, viewpoint, infoEntry->getKey(), null /* divergent */, null /* ignore */, false /* viewlimit */) : null;

				do {
					if (map->m_state == RealTimeMap<K>::MAP_RUNNING) {

						#if 0
						if (Transaction::lockable(orginfo, 0, false) == false) {
							*dirty = true;	
							break;	
						}
						#endif

						if ((orginfo->getViewpoint() != 0) && (Transaction::viewing(orginfo->getViewLimit()) == true)) {
							*dirty = true;
							break;
						}

						if (storyLine.getSharing() == true) {
							*dirty = true;
							break;
						}

						if ((chkinfo != null) && (orginfo == chkinfo)) {
							*dirty = true;
							break;
						}
					}

					Information* next = orginfo->getNext();

					if (map->m_hasSecondaryMaps == true) {
						map->ejectSecondary(tx, infoEntry->getKey(), orginfo, next, chkinfo, RealTimeMap<K>::LOCK_WRITE);
					}

					orginfo->setNext(null);

					// XXX: delete unviewed from indexing will have already counted frag statistics (see processInformation)
					if ((orginfo != infoEntry->getValue()) || (inLine == true)) {
						RealTimeVersion<K>::statisticPagingDead(map, segment, orginfo, false /* checkpoint */);
					}
					
					if (orginfo != infoEntry->getValue()) {
						Converter<Information*>::destroy(orginfo);
					}

					orginfo = next;

				} while ((orginfo != null) && (orginfo->getLevel() == Information::LEVEL_COMMIT) && (orginfo->getDeleting() == true));

				// XXX: storyline has pending work, keep primary root end info around
				if ((orginfo != null) && (orginfo->getNext() == infoEntry->getValue())) {
					infoEntry->getValue()->setNext(orginfo);
					
					return false;
				}

				if (orginfo != null) {
					int depth = 1;
					Information* next = orginfo->getNext();
					while ((next != null) && (next != orginfo) /* bos */) {
						depth++;

						// XXX: re-root active storyline with new origin
						if (next->getNext() == infoEntry->getValue()) {
							next->setNext(orginfo);
						}

						next = next->getNext();
					}

					if (orginfo->getDeleting() == true) {
						if (storyLine.getStoryLock() != null) {
							storyLine.setStoryDepth(depth);
						}
					}
				}

				#ifdef CXX_LANG_MEMORY_DEBUG
				if (orginfo != infoEntry->getValue(true)) {
					Converter<Information*>::destroy(infoEntry->getValue(true));

					infoEntry->setValue(orginfo, map->getIndexValue() /* ctx */, true);
				}
				#else
				if (orginfo != infoEntry->getValue()) {
					// XXX: note the ignorePre (setValueIgnoreRooted)
					Converter<Information*>::destroy(infoEntry->getValue());

					infoEntry->setValueIgnoreRooted(orginfo, map->getIndexValue() /* ctx */, true /* ignorePre */, false /* ignorePost */);
				}
				#endif
			}

			return (orginfo == null);
		}

		struct SummaryImpact {
			Comparator<RealTime::Locality> m_comparator;

			LockableBasicArray<MeasuredRandomAccessFile*>* m_deferred;

			ulongtype m_deferredSize;
			ulongtype m_activeSize;

			RealTimeLocality m_currentLrtLocality;

			TreeSet<RealTimeLocality> m_active;
			TreeSet<RealTimeLocality> m_anchors;

			FORCE_INLINE SummaryImpact(MapFileSet<MeasuredRandomAccessFile>* lwfiles) :
				m_comparator(lwfiles, false /* compareViewpoint */),
				m_deferred(null),
				m_deferredSize(0),
				m_activeSize(0),
				m_currentLrtLocality(RealTimeLocality::LOCALITY_NONE),
				m_active(&m_comparator),
				m_anchors(&m_comparator) {
				// XXX: nothing to do
			}

			FORCE_INLINE ~SummaryImpact() {
				// XXX: nothing to do
			}

			FORCE_INLINE uinttype orderOf(RealTimeMap<K>* map, MeasuredRandomAccessFile* wfile) {
				RealTime* primary = map->m_primaryIndex == null ? map : map->m_primaryIndex;
				const bytetype indexValue = wfile->getIndexValue();
				RealTime* rt = (indexValue == 0) ? primary : primary->getShare()->getSecondaryList()->get(indexValue - 1);
				if (rt == null) {
					return 0;
				}
				PagedSummarySet* activeSummaries = rt->getActiveSummaries();
				activeSummaries->lock();
				TreeSet<PagedSummary*>* summaries = null;
				switch(wfile->getType()) {
					case MapFileUtil::IRT:
						summaries = activeSummaries->summariesForPagingFile(/* iwfile */ wfile->getFileIndex());
						break;
					case MapFileUtil::VRT:
						summaries = activeSummaries->summariesForStreamFile(/* vwfile */ wfile->getFileIndex());
						break;
					default:
						break;
				}
				if ((summaries == null) || (summaries->size() == 0)) {
					activeSummaries->unlock();
					return 0;
				}

				RealTimeLocality minCheckpoint = summaries->first()->getStreamLocality();
				activeSummaries->unlock();
				uinttype order = 0;
				Iterator<RealTimeLocality>* iter = m_active.iterator();
				while (iter->hasNext() == true) {
					++order;
					if (m_comparator.compare(iter->next(), minCheckpoint) == 0) {
						return order;
					}
				}
				Converter<Iterator<RealTimeLocality>*>::destroy(iter);
				return 0;
			}

			FORCE_INLINE void setDeferred(LockableBasicArray<MeasuredRandomAccessFile*>* deferred) {
				m_deferred = deferred;
			}

			FORCE_INLINE void setCurrentLrtLocality(const RealTime::Locality& currentLrtLocality) {
				m_currentLrtLocality = currentLrtLocality;
			}

			FORCE_INLINE void addActive(TreeMap<RealTime::Locality, PagedSummary*>& summaries) {
				typedef MapEntry<RealTime::Locality, PagedSummary*> Entry;
				TreeMap<RealTime::Locality, PagedSummary*>::TreeMapEntrySet set(true);
				summaries.entrySet(&set);
				Iterator<Entry*>* iter = set.iterator();
				while (iter->hasNext() == true) {
					Entry* e = iter->next();
					if (m_active.contains(e->getValue()->getStreamLocality()) == false) {
						m_active.add(e->getValue()->getStreamLocality());
					}
				}
			}

			FORCE_INLINE void addAnchoring(TreeMap<uinttype, PagedSummary*>& anchors) {
				typedef MapEntry<uinttype, PagedSummary*> Entry;
				TreeMap<uinttype, PagedSummary*>::TreeMapEntrySet set(true);
				anchors.entrySet(&set);
				Iterator<Entry*>* iter = set.iterator();
				while (iter->hasNext() == true) {
					Entry* e = iter->next();
					if (m_anchors.contains(e->getValue()->getStreamLocality()) == false) {
						m_anchors.add(e->getValue()->getStreamLocality());
					}
				}
			}

			FORCE_INLINE boolean allowInvalidateSummaries(TreeSet<RealTimeLocality>* summaries) const {
				const uinttype active = m_active.size();
				uinttype impacted = 0;

				const uinttype anchors = m_anchors.size();
				uinttype anchorsImpacted = 0;

				if (summaries != null) {
					Iterator<RealTimeLocality>* iter = summaries->iterator();
					while (iter->hasNext() == true) {
						const RealTime::Locality& locality = iter->next();

						// XXX: don't get rid of a summary that represents the latest locality
						if (m_comparator.compareNoViewpoint(m_currentLrtLocality, locality) == 0) {
							Converter<Iterator<RealTimeLocality>*>::destroy(iter);
							return false;
						}

						if (m_active.contains(locality) == true) {
							++impacted;
						}
						if (m_anchors.contains(locality) == true) {
							++anchorsImpacted;
						}
					}
					Converter<Iterator<RealTimeLocality>*>::destroy(iter);
				}

				// XXX: allow removal if there are enough remaining summaries without given ones
				if ((((active - impacted) >= Properties::getSummaryTarget()) && ((anchors - anchorsImpacted) >= Properties::getSummaryAnchor())) == true) {
					return true;
				}

				if (m_deferredSize > (m_activeSize * Properties::getSummaryStorageThreshold())) {
					DEEP_LOG(DEBUG, SUMIZ, "Forcing summary invalidation due to checkpoint storage requirements: active = %llu, deferred = %llu\n", m_activeSize, m_deferredSize);
					return true;
				}

				return false;
			}
		};

		static void getSummaryImpact(RealTime* rt, SummaryImpact& report) {
			// XXX: count intersection of given summaries and active summaries
			PagedSummarySet* activeSummaries = rt->getActiveSummaries();
			activeSummaries->lock();
			report.addActive(activeSummaries->allSummaries());
			report.addAnchoring(activeSummaries->lastSummaries());
			activeSummaries->unlock();

			report.m_activeSize += rt->getShare()->size(rt->getShare()->getVrtReadFileList());
			report.m_activeSize += rt->getShare()->size(rt->getShare()->getIrtReadFileList());
		}

		static void getFullSummaryImpact(RealTimeMap<K>* map, SummaryImpact& report, const boolean needsLock = true) {
			if (needsLock == true) map->clobberLock();
			RealTime* primary = (map->m_primaryIndex == null) ? map : map->m_primaryIndex;
			report.setCurrentLrtLocality(primary->getCurrentLrtLocality());
			RealTimeAdaptive_v1<K>::getSummaryImpact(primary, report);
			report.setDeferred(primary->getShare()->getAwaitingDeletion());
			report.m_deferredSize = primary->getShare()->getDeferredStorageSize();

			BasicArray<RealTime*>* secondaries = primary->getShare()->getSecondaryList();
			for (inttype i=0; i<secondaries->size(); ++i) {
				RealTime* rt = secondaries->get(i);
				if (rt == null) {
					// XXX: allow for dynamic add/remove secondary index
					continue;
				}

				RealTimeAdaptive_v1<K>::getSummaryImpact(rt, report);
			}
			if (needsLock == true) map->clobberUnlock();
		}

		static boolean allowFileClobber(RealTimeMap<K>* map, BasicArray<MeasuredRandomAccessFile*>& files, SummaryImpact& report, const boolean needsLock = true) {
			if (Properties::getCheckpointMode() == Properties::CHECKPOINT_OFF) {
				return true;
			}

			map->checkpointLock();
			{
				if (map->checkpointFullyComplete(false /* lock */) == false) {
					map->checkpointUnlock();
					return false;
				}
			}
			map->checkpointUnlock();

			if (needsLock == true) map->clobberLock();

			RealTime* primary = map->m_primaryIndex == null ? map : map->m_primaryIndex;
			const uinttype totalSummaries = report.m_active.size();
			Comparator<RealTimeLocality> lCmp(null /* no lrt order */, false /* compare viewpoints */);
			TreeSet<RealTimeLocality> impacted(&lCmp);

			for (inttype i=0; i<files.size(); ++i) {
				MeasuredRandomAccessFile* file = files.get(i);
				const bytetype indexValue = file->getIndexValue();
				RealTime* rt = (indexValue == 0) ? primary : primary->getShare()->getSecondaryList()->get(indexValue - 1);
				if (rt == null) {
					continue;
				}
				PagedSummarySet* activeSummaries = rt->getActiveSummaries();

				activeSummaries->lock();
				TreeSet<PagedSummary*>* summaries = null;
				switch(file->getType()) {
					case MapFileUtil::IRT:
						summaries = activeSummaries->summariesForPagingFile(/* iwfile */ file->getFileIndex());
						break;
					case MapFileUtil::VRT:
						summaries = activeSummaries->summariesForStreamFile(/* vwfile */ file->getFileIndex());
						break;
					default:
						break;
				}
				if (summaries != null) {
					Iterator<PagedSummary*>* iter = summaries->iterator();
					while (iter->hasNext() == true) {
						RealTimeLocality l = iter->next()->getStreamLocality();
						if (impacted.contains(l) == false) {
							impacted.add(l);
						}
					}
					Converter<Iterator<PagedSummary*>*>::destroy(iter);
				}
				activeSummaries->unlock();
 			}

			const uinttype impactedCount = (uinttype)impacted.size();

			// XXX: it is OK to remove a file if we have no active summaries or if its removal impacts no summaries
			if ((totalSummaries == 0) || (impactedCount == 0)) {
				if (needsLock == true) map->clobberUnlock();
				return true;
			}

			if (report.allowInvalidateSummaries(&impacted) == false) {
				if (needsLock == true) map->clobberUnlock();
				return false;
			}

			if (needsLock == true) map->clobberUnlock();

			return true;
		}

		static boolean allowFileClobber(RealTimeMap<K>* map, MeasuredRandomAccessFile* file, const boolean needsLock = true) {
			RealTime* primary = map->m_primaryIndex == null ? map : map->m_primaryIndex;
			file->setIndexValue(map->getIndexValue());

			SummaryImpact report(primary->getShare()->getLrtWriteFileList());
			RealTimeAdaptive_v1<K>::getFullSummaryImpact(map, report, needsLock);

			BasicArray<MeasuredRandomAccessFile*> files;
			files.add(file, true);
			return allowFileClobber(map, files, report, needsLock);
		}

		static boolean allowDynamicSummarization(RealTimeMap<K>* map, Segment<K>* segment, boolean indexThread) {
			return (indexThread == false);
		}

		struct ClobberUtil {
			template<typename E>
			static void combine(BasicArray<BasicArray<E>*>& s, E v) {
				const inttype origSize = s.size();
				// XXX: create all existing sub-lists with element
				for (inttype i=0; i<origSize; ++i) {
					BasicArray<E>* l = new BasicArray<E>(s.get(i));
					l->add(v, true);
					s.add(l, true);
				}
				// XXX: create new list with only element
				BasicArray<E>* l = new BasicArray<E>(1);
				l->add(v, true);
				s.add(l, true);
			}

			template<typename E>
			static void subsets(BasicArray<E>& elements, BasicArray<BasicArray<E>*>& out) {
				for (inttype i=0; i<elements.size(); ++i) {
					combine<E>(out, elements.get(i));
				}
			}

			static ulongtype weightedAggregateSize(RealTimeMap<K>* map, BasicArray<MeasuredRandomAccessFile*>* files, SummaryImpact& report) {
				const uinttype maxOrder = report.m_active.size()+1;

				ulongtype size = 0;
				uinttype order = maxOrder;
				for (inttype i=0; i<files->size(); ++i) {
					MeasuredRandomAccessFile* file = files->get(i);
					longtype len = file->length();
					uinttype o = report.orderOf(map, file);
					if (len > 0) {
						size += (ulongtype)len;
					}
					if (o < order) {
						order = o;
					}
				}
				order = maxOrder - order;
				ulongtype weightedSize = size * (maxOrder + order) / maxOrder;

				return weightedSize;
			}

			// XXX: choose the best set of files to clobber, if any
			static void chooseFiles(RealTimeMap<K>* map, SummaryImpact& report, BasicArray<MeasuredRandomAccessFile*>& out) {
				BasicArray<MeasuredRandomAccessFile*>& deferred = report.m_deferred->getArray();
				
				#ifdef DEEP_DEBUG
				if (map->tryClobberReadLock() == true) {
					DEEP_LOG(ERROR, OTHER, "Invalid lock state in chooseFiles, %s\n", map->getFilePath());
					throw InvalidException("Invalid lock state in chooseFiles");
				}
				#endif

				BasicArray<BasicArray<MeasuredRandomAccessFile*>*> candidates(10, true);
				// TODO: "candidates" can be quite big due to exponential growth -- use more targeted approximation algorithm
				if (deferred.size() <= 10) {
					ClobberUtil::template subsets<MeasuredRandomAccessFile*>(deferred, candidates);
				} else {
					for (inttype i=0; i<deferred.size(); ++i) {
						BasicArray<MeasuredRandomAccessFile*>* l = new BasicArray<MeasuredRandomAccessFile*>(1);
						l->add(deferred.get(i), true);
						candidates.add(l, true);
					}
				}

				ulongtype maxSize = 0;
				BasicArray<MeasuredRandomAccessFile*>* max = null;
				for (inttype i=0; i<candidates.size(); ++i) {
					BasicArray<MeasuredRandomAccessFile*>* candidate = candidates.get(i);
					ulongtype size = weightedAggregateSize(map, candidate, report);
					if ((size > maxSize) && (allowFileClobber(map, *candidate, report, false /* needsLock */) == true)) {
						maxSize = size;
						max = candidate;
					}
				}

				if (max != null) {
					for (inttype i=0; i<max->size(); ++i) {
						out.add(max->get(i), true);
					}
				}
			}
		};

		static boolean clobberUnusedFiles(RealTimeMap<K>* map) {
 			boolean ret = false;
			BasicArray<MeasuredRandomAccessFile*> toClobber;

			RealTime* primary = map->m_primaryIndex == null ? map : map->m_primaryIndex;

			#ifdef DEEP_DEBUG
			if (primary->tryClobberReadLock() == true) {
				DEEP_LOG(ERROR, OTHER, "Invalid lock state in clobberUnusedFiles, %s\n", map->getFilePath());
				throw InvalidException("Invalid lock state in clobberUnusedFiles");
			}
			#endif

			SummaryImpact report(primary->getShare()->getLrtWriteFileList());
			RealTimeAdaptive_v1<K>::getFullSummaryImpact(map, report, false /* needsLock */);

			#if 0
			DEEP_LOG(DEBUG, SUMIZ, "summary impact: %d active; %d anchoring, %s\n", report.m_active.size(), report.m_anchors.size(), map->getFilePath());
			#endif

			primary->getShare()->deferredLock();
			{
				ClobberUtil::chooseFiles(map, report, toClobber);
				for (inttype i=0; i<toClobber.size(); ++i) {
					MeasuredRandomAccessFile* wfile = toClobber.get(i);
					primary->getShare()->removeDeferred(wfile);
					ret = true;
				}
			}
			primary->getShare()->deferredUnlock();

			{
				for (inttype i=0; i<toClobber.size(); ++i) {
					MeasuredRandomAccessFile* wfile = toClobber.get(i);
					const bytetype indexValue = wfile->getIndexValue();
					RealTime* rt = (indexValue == 0) ? primary : primary->getShare()->getSecondaryList()->get(indexValue - 1);
					if (rt == null) {
						continue;
					}
					rt->destroyFile(wfile);
				}
			}

			return ret;
		}

		FORCE_INLINE static boolean needsAddPagingFile(RealTimeMap<K>* map, MeasuredRandomAccessFile* iwfile) {
			if (iwfile->BufferedRandomAccessFile::getFilePointer() > map->getFileSize()) {
				return true;
			}

			if (iwfile->getContainsCheckpoint() == false) {
				return false;
			}

			uinttype factor = Properties::CHECKPOINT_MIN_FILE_SIZE_FACTOR;

			uinttype minSize = ((map->getFileSize() / factor) > Properties::getMinFileSize()) ? (map->getFileSize() / factor) : Properties::getMinFileSize();
			if (iwfile->BufferedRandomAccessFile::getFilePointer() > minSize) {
				return true;
			}

			return false;
		}

		FORCE_INLINE static void pace(RealTimeMap<K>* map, Transaction* tx) {
			if ((map->m_resource.getPaceFlag() == true) && ((tx == null) || ((tx->getRoll() == false) && (tx->getRollWait() == false)))) {
				map->m_resource.pace(map->size() > map->getCycleSize(), true);
			}
		}

		FORCE_INLINE static boolean doPace(RealTimeMap<K>* map, Transaction* tx) {
			return (tx == null) || ((tx->getRoll() == false) && (tx->getRollWait() == false));
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEADAPTIVE_H_*/
