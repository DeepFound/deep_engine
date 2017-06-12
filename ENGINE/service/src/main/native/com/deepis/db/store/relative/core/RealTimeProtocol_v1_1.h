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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEPROTOCOL_V1_1_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEPROTOCOL_V1_1_H_

// XXX: includes required for templating dependencies
#include "cxx/util/TreeMap.cxx"

#include "com/deepis/db/store/relative/core/RealTimeProtocol_v1_0.h"
#include "com/deepis/db/store/relative/core/RealTimeProtocol_v1_0.cxx"
#include "com/deepis/db/store/relative/core/RealTimeUtilities.h"

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template <typename K> struct RecoveryWorkspace_v1_0;

template <typename K>
struct RecoveryWorkspace_v1_1 {

	shorttype txid;
	Transaction* tx;
	RealTimeMap<K>* m_map;

	RecoveryWorkspace_v1_1(shorttype txid, RealTimeMap<K>* map) : 
		txid(txid) {
			tx = Transaction::create();
			tx->setIsolation(Transaction::COMMITTED);
			tx->begin();
			map->associate(tx);
			m_map = map;
	}

	FORCE_INLINE void clear(boolean reset = true) {
		while (tx->getDirty()) {
			tx->rollback(tx->getLevel());
		}
		tx->releaseConductors(RealTimeResource::currentTimeMillis());
		if (reset == false) {
			return;
		}
		if (tx->getLevel() < 0) {
			tx->begin();
		}
		#ifdef DEEP_DEBUG
		if (tx->getLevel() != 0) {
			DEEP_LOG(ERROR, OTHER, "Invalid TX level after clear: %d\n", tx->getLevel());
			throw InvalidException("Invalid TX level after clear");
		}
		#endif
		m_map->associate(tx);
	}

	virtual ~RecoveryWorkspace_v1_1() {
		clear(false);
		Transaction::destroy(tx);
		#ifdef CXX_LANG_MEMORY_DEBUG
		m_map->m_threadContext.getContext()->setTransaction(null, true);
		#else
		m_map->m_threadContext.getContext()->setTransaction(null);
		#endif
	}
};

template<typename K>
struct RecoveryWorkspaceCleanup<TreeMap<shorttype,RecoveryWorkspace_v1_1<K>*>,K> {
	FORCE_INLINE static void clean(TreeMap<shorttype,RecoveryWorkspace_v1_1<K>*> & ws) {
		// XXX: clean up all workspaces
		Set<MapEntry<shorttype,RecoveryWorkspace_v1_1<K>*>*>* entrySet = ws.entrySet();
		Iterator<MapEntry<shorttype,RecoveryWorkspace_v1_1<K>*>*>* iter = entrySet->iterator();

		while (iter->hasNext()) {
			RecoveryWorkspace_v1_1<K>& workspace = *iter->next()->getValue();
			/* for (int i = 0; i < workspace.conductor.size(); i++) {
				SegMapEntry* walkEntry = workspace.conductor.get(i);

				if (walkEntry->getValue()->getLevel() != Information::LEVEL_ROLLED) {
					Converter<K>::destroy(walkEntry->getKey());
				}

				Converter<Information*>::destroy(walkEntry->getValue());
			} */
			workspace.clear();
		}
		delete entrySet;
		delete iter;
	}
};

template<int V, typename K>
struct WriteKeyPaging {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;

	struct StaticState {
		MeasuredRandomAccessFile* iwfile;
		RealTimeMap<K>* map;
		ThreadContext<K>* ctxt;
		Segment<K>* segment;
		boolean rebuild;
		boolean rolled;
		uinttype viewpoint;
		boolean keyed;
		boolean summaryKeyed;
		K firstKey;
		K& summaryFirstKey;
		RealTimeIndex::IndexReport& indexReport;
		const boolean summary;

		FORCE_INLINE StaticState(MeasuredRandomAccessFile* p_iwfile, RealTimeMap<K>* p_map, ThreadContext<K>* p_ctxt, Segment<K>* p_segment, boolean p_rebuild, uinttype p_viewpoint, K& p_summaryFirstKey, RealTimeIndex::IndexReport& p_indexReport) :
			iwfile(p_iwfile),
			map(p_map),
			ctxt(p_ctxt),
			segment(p_segment),
			rebuild(p_rebuild),
			rolled(false),
			viewpoint(p_viewpoint),
			keyed(false),
			summaryKeyed(false),
			firstKey((K) Converter<K>::NULL_VALUE),
			summaryFirstKey(p_summaryFirstKey),
			indexReport(p_indexReport),
			summary(p_segment->getSummary()) {
		}

		FORCE_INLINE virtual ~StaticState() {
			/* XXX: nothing to do */
		}
	};

	struct TransientState {
		StaticState& staticState;
		typename RealTimeMap<K>::MapInformationEntrySetIterator* infoIter;
		inttype actual;
		boolean early;
		inttype ceasing;
		boolean skip;
		boolean dirty;
		boolean viewed;
		boolean keyCompression;

		/* const */ uinttype cursorInit;
		/* const */ longtype positionInit;
		union {
			uinttype streamPosition;
			uinttype recoveryEpoch;
		};
		/* const */ uinttype location;
		/* const */ uinttype pagingPosition;

		FORCE_INLINE TransientState(StaticState& s, typename RealTimeMap<K>::MapInformationEntrySetIterator* i) :
			staticState(s),
			infoIter(i),
			actual(0),
			early(false),
			ceasing(0),
			skip(false),
			dirty(false),
			viewed(false) {

			// XXX: summary segments don't need to be filled (see summary init and segment purging rules)
			if ((s.rebuild == true) && (s.summary == false) && ((s.segment->getPurged() == true) || (s.segment->getVirtual() == true))) {
				s.map->fillSegment(s.ctxt, s.segment, false /* values */, false /* pace */);
				++s.indexReport.filled;
			}

			#ifdef DEEP_DEBUG
			if ((s.segment->getStreamRangeOverflowFlag() == true) || (s.segment->getStreamedFlag() == true)) {
				DEEP_LOG(ERROR, OTHER, "Invalid TransientState: stream overflow / streamed flag already set");

				throw InvalidException("Invalid TransientState: stream overdlow / streamed flag already set");
			}
			#endif

			keyCompression = RealTimeAdaptive_v1<K>::keyCompression(s.map, s.segment, s.rebuild, s.iwfile); 

			// XXX: if key compression is on, we're going to reset this block length automatically via compression init
			if ((s.rebuild == true) && (s.map->m_valueCompressMode == true) && (keyCompression == false)) {
				s.iwfile->getAndResetBlockLength();
			}

			cursorInit = s.iwfile->getCursor();
			positionInit = s.iwfile->getPosition();

			// XXX: to be filled in at the bottom (i.e. see writeMetaData)
			RealTimeProtocol<V,K>::seedMetaData(s.iwfile, s.map, s.segment);

			s.iwfile->writeByte(keyCompression);
			if (s.summary == false) {
				streamPosition = s.segment->getStreamPosition();
				s.iwfile->writeInt(streamPosition);
			} else {
				recoveryEpoch = s.segment->getRecoveryEpoch();
				s.iwfile->writeInt(recoveryEpoch);
			}

			location = s.iwfile->BufferedRandomAccessFile::getFilePointer();
			s.iwfile->writeInt(location /* pre segment location */);

			pagingPosition = s.iwfile->BufferedRandomAccessFile::getFilePointer();
		}

		FORCE_INLINE TransientState(const TransientState& ts) :
			staticState(ts.staticState),
			infoIter(ts.infoIter),
			actual(ts.actual),
			early(ts.early),
			ceasing(ts.ceasing),
			skip(ts.skip),
			dirty(ts.dirty),
			viewed(ts.viewed),
			keyCompression(ts.keyCompression),
			cursorInit(ts.cursorInit),
			positionInit(ts.positionInit),
			streamPosition(ts.streamPosition),
			location(ts.location),
			pagingPosition(ts.pagingPosition) {
		}

		FORCE_INLINE virtual ~TransientState() {
			/* XXX: nothing to do */
		}
	};

	FORCE_INLINE static void deleteUnviewed(TransientState& ts, ulongtype i, SegMapEntry* infoEntry, K key, Information*& info) {
		WriteKeyPaging<V,K>::StaticState& s = ts.staticState;
		RealTimeMap<K>*& map = s.map;
		ThreadContext<K>*& ctxt = s.ctxt;
		Segment<K>*& segment = s.segment;
		uinttype& viewpoint = s.viewpoint;
		K& firstKey = s.firstKey;

		boolean& dirty = ts.dirty;
		boolean& viewed = ts.viewed;

		// XXX: information is deferred for viewing (i.e. isolation), set "dirty" to true to keep segment around
		// XXX: in checkpoint case, the rooted info is not really deletable if it is the same as the checkpoint info
		if (Transaction::deletable(infoEntry->getValue()) == true) {

			// XXX: clean up views due to isolation (e.g. repeatable-read)
			if ((map->m_primaryIndex == null) && ((viewpoint == 0) || (infoEntry->getValue() != info))) {

				const boolean deleted = RealTimeAdaptive_v1<K>::deleteUnviewed(map, ctxt, infoEntry, segment, &dirty);
				if (deleted == false) {
					if (viewpoint == 0) {
						info = map->isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
					}
				} else {
					if ((i == 0) && (key == firstKey) && (map->m_resetIterator == false)) {
						map->m_resetIterator = true;
					}

					#ifdef DEEP_DEBUG
					if (segment->getCardinalityEnabled() == false) {
						DEEP_LOG(ERROR, OTHER, "Delete Unviewed: invalid cardinality, %s\n", map->getFilePath());

						throw InvalidException("Delete Unviewed: invalid cardinality");
					}
					#endif

					ts.infoIter->RealTimeMap<K>::MapInformationEntrySetIterator::remove();

					if (key != firstKey) {
						Converter<K>::destroy(key);
					}

					info = null;
				}

			// XXX: keep segment around until secondary ejection
			} else if (map->m_primaryIndex != null) {
				viewed = true;
			}
		}
	}

	FORCE_INLINE static typename RealTimeUtilities<K>::WalkAction handleMissingInfo(TransientState& ts, ulongtype i, SegMapEntry* infoEntry, boolean ignore) {
		WriteKeyPaging<V,K>::StaticState& s = ts.staticState;
		MeasuredRandomAccessFile*& iwfile = s.iwfile;
		RealTimeMap<K>*& map = s.map;
		Segment<K>*& segment = s.segment;
		boolean& rebuild = s.rebuild;
		uinttype& viewpoint = s.viewpoint;

		inttype& ceasing = ts.ceasing;
		boolean& viewed = ts.viewed;
		const uinttype cursorInit = ts.cursorInit;
		const longtype positionInit = ts.positionInit;

		if ((rebuild == false) && (ignore == false) /* && ((divergent == true) || (viewpoint != 0)) */ ) {
			// XXX: re-position cursor before restarting
			RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);
			rebuild = true;
			++s.indexReport.retry;
			return RealTimeUtilities<K>::RETRY; // XXX: goto RETRY;
		}

		RealTimeProtocol<V,K>::statisticPaging(map, segment, infoEntry->getValue(), null, rebuild, viewpoint != 0);

		// XXX: in regular indexing, this is a diverging case. In checkpoint mode, we may be able to ignore viewed state
		if ((viewpoint == 0) || (ignore == false) || (Transaction::deletable(infoEntry->getValue()) == true)) {
			viewed = true;
		}

		ceasing++;
		return RealTimeUtilities<K>::CONTINUE; // XXX: continue;
	}

	FORCE_INLINE static typename RealTimeUtilities<K>::WalkAction handleDeferredWrite(TransientState& ts, ulongtype i, SegMapEntry* infoEntry, Information* info) {
		WriteKeyPaging<V,K>::StaticState& s = ts.staticState;
		RealTimeMap<K>*& map = s.map;
		Segment<K>*& segment = s.segment;
		boolean& rebuild = s.rebuild;
		#ifdef DEEP_DEBUG
		uinttype& viewpoint = s.viewpoint;
		#endif

		inttype& actual = ts.actual;

		#ifdef DEEP_DEBUG
		if (viewpoint != 0) {
			DEEP_LOG(ERROR, INDEX, "skipping checkpoint info due to write access, %s\n", map->getFilePath());
			throw InvalidException("Skipping checkpoint info due to write access");
		}
		#endif

		if (rebuild == true) {
			actual = 0;
			ts.early = true;
			return RealTimeUtilities<K>::DONE; // XXX: return 0;

		} else {
			// XXX: force accounting of first key next time around (to ensure flush) in case of concurrent rollback
			if ((i == 0) && (info->getLevel() == Information::LEVEL_COMMIT)) {
				if (info->hasFields(Information::WRITE) == true) {
					info->setIndexed(map->m_indexValue, false);

				} else {
					segment->setBeenAltered(true);
				}
			}
			return RealTimeUtilities<K>::CONTINUE; // XXX: continue;
		}
	}

	FORCE_INLINE static typename RealTimeUtilities<K>::WalkAction handleAlteredSegment(TransientState& ts, ulongtype i, SegMapEntry* infoEntry, Information* info) {
		WriteKeyPaging<V,K>::StaticState& s = ts.staticState;
		MeasuredRandomAccessFile*& iwfile = s.iwfile;
		RealTimeMap<K>*& map = s.map;
		Segment<K>*& segment = s.segment;
		boolean& rebuild = s.rebuild;
		K& firstKey = s.firstKey;

		inttype& ceasing = ts.ceasing;
		boolean& viewed = ts.viewed;
		const uinttype cursorInit = ts.cursorInit;
		const longtype positionInit = ts.positionInit;

		if ((rebuild == false) || (segment->SegTreeMap::size() == 0)) {
			// XXX: re-position cursor before restarting
			RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);

			if (segment->SegTreeMap::size() == 0) {
				map->deleteSegment(segment, firstKey, true, true /* lock */);
				map->m_resetIterator = true;
				return RealTimeUtilities<K>::DONE; // XXX: return 0;
			}

			rebuild = true;
			++s.indexReport.retry;
			return RealTimeUtilities<K>::RETRY; // XXX: goto RETRY;
		}

		if (info != null) {
			viewed = true;
			ceasing++;
		}

		return RealTimeUtilities<K>::CONTINUE; // XXX: continue;
	}

	FORCE_INLINE static typename RealTimeUtilities<K>::WalkAction processInfo(TransientState& ts, ulongtype i, SegMapEntry* infoEntry) {
		WriteKeyPaging<V,K>::StaticState& s = ts.staticState;
		MeasuredRandomAccessFile*& iwfile = s.iwfile;
		RealTimeMap<K>*& map = s.map;
		ThreadContext<K>*& ctxt = s.ctxt;
		Segment<K>*& segment = s.segment;
		boolean& rebuild = s.rebuild;
		uinttype& viewpoint = s.viewpoint;

		boolean& keyed = s.keyed;
		K& firstKey = s.firstKey;
		boolean& summaryKeyed = s.summaryKeyed;
		K& summaryFirstKey = s.summaryFirstKey;
		const boolean summary = s.summary;

		inttype& actual = ts.actual;
		boolean& skip = ts.skip;
		boolean& dirty = ts.dirty;

		const uinttype cursorInit = ts.cursorInit;
		const longtype positionInit = ts.positionInit;

		K key = infoEntry->getKey();
		Information* info = infoEntry->getValue();

		// XXX: during checkpointing, this info is what normal indexing would have selected
		Information* indexInfo = null;

		if (summary == false) {

			// XXX: see reseed and deletion
			if ((i == 0) && (keyed == false)) {
				keyed = true;
				firstKey = key;
			}

			boolean ignore = false;
			boolean divergent = (rebuild == true);

			if (viewpoint != 0) {
				// XXX: checkpoint can ignore missing info if current index state is consistent with checkpoint
				#if 0
				ignore = (rebuild == false);
				#else
				ignore = true;
				#endif

				info = map->isolateCheckpoint(ctxt, infoEntry->getValue(), viewpoint, key, &divergent, &ignore);

				// XXX: if we have not marked next indexing cycle as rebuild, may need to come around again
				if (segment->getBeenAltered() == false) {

					// XXX: if writing a deleted info as checkpoint, we must rebuild next index cycle (this is not a valid delta)
					if ((info != null) && (info->getDeleting() == true)) {
						segment->setBeenAltered(true);

					// XXX: if checkpoint info is behind not-yet-indexed committed info, we can write as delta and come back around
					} else {
						indexInfo = map->isolateInterspace(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT, key, null);

						if (info != indexInfo) {
							dirty = true;
						}
					}
				}

			} else {
				info = map->isolateInterspace(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT, key, &divergent);
			}

			// XXX: secondaries may no longer follow the key space
			if (info == null) {
				return handleMissingInfo(ts, i, infoEntry, ignore);

			} else if ((dirty == false) && (viewpoint != 0)) {
				dirty = (info->getNext() != null);
			}

			boolean write = RealTimeProtocol<V,K>::keyWriteAccess(infoEntry->getStoryLine(), info, viewpoint, rebuild, &dirty);

			// XXX: ensure segment is notified of information locality (see reorganize files), perform after write access
			if (RealTimeProtocol<V,K>::keySpacePinning(iwfile, map, ctxt, segment, infoEntry, info, indexInfo, viewpoint, s.rolled) == true) {
				// XXX: re-position cursor before restarting
				RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);
				rebuild = true;
				s.rolled = true;
				++s.indexReport.retry;
				return RealTimeUtilities<K>::RETRY; // XXX: goto RETRY;
			}

			// XXX: wait until information has no active locks, "dirty" could be set to true to keep segment around
			if (write == false) {
				return handleDeferredWrite(ts, i, infoEntry, info);
			}

			// XXX: fragmentation statistics (following are estimates which should cause more defragmentation than not)
			RealTimeProtocol<V,K>::statisticPaging(map, segment, infoEntry->getValue(), info, rebuild, viewpoint != 0);

			// XXX: remove deleted/unviewed information from storyline
			deleteUnviewed(ts, i, infoEntry, key, info);

			// XXX: write and/or rebuild if there is no ending storylines
			if ((info == null) || ((viewpoint == 0) && ((info->getDeleting() == true) || (info->getLevel() != Information::LEVEL_COMMIT)))) {
				return handleAlteredSegment(ts, i, infoEntry, info);
			}

			// XXX: check whether segment information requires to be written to disk
			if (RealTimeProtocol<V,K>::keySpaceIndex(map, segment, iwfile, info, i, &actual, &skip, rebuild, viewpoint) == false) {
				return RealTimeUtilities<K>::CONTINUE; // XXX: continue;
			}
		}

		if ((ts.keyCompression == true) && (iwfile->getCompress() == BufferedRandomAccessFile::COMPRESS_NONE)) {
			iwfile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
		}

		if (((viewpoint != 0) || (summary == true)) && (summaryKeyed == false)) {
			summaryKeyed = true;
			summaryFirstKey = key;
		}

		if ((summary == true) && (i == 0)) {
			// XXX: modifies 'actual'
			WriteKeyPaging<V,K>::writeFileRefs(ts, MapFileUtil::IRT, summaryFirstKey);

			if (segment->getStreamRangeOverflow() == true) { 
				segment->setStreamRangeOverflowFlag(true);
				WriteKeyPaging<V,K>::writeFileRefs(ts, MapFileUtil::VRT, summaryFirstKey);
			}
		}

		bytetype flag = RealTimeProtocol<V,K>::IRT_FLAG_CONTENT;
		RealTimeProtocol<V,K>::writeInfoPaging(iwfile, map, key, flag, info->getFileIndex(), info->getFilePosition(), info->getSize(), info->getCompressedOffset());
		actual++;
		return RealTimeUtilities<K>::CONTINUE;
	}

	FORCE_INLINE static uinttype writeFileRefs(TransientState& ts, MapFileUtil::FileType type, const K key, Iterator<ushorttype>* iter) {
		typedef NumberRangeSet<ushorttype, uinttype> FileRangeSet;

		WriteKeyPaging<V,K>::StaticState& s = ts.staticState;
		MeasuredRandomAccessFile*& iwfile = s.iwfile;
		RealTimeMap<K>*& map = s.map;

		const bytetype flags = (type == MapFileUtil::IRT) ? RealTimeProtocol<V,K>::IRT_FLAG_REF_IRT : RealTimeProtocol<V,K>::IRT_FLAG_REF_VRT;
		uinttype written = 0;
		FileRangeSet fileRefs;

		while (iter->hasNext() == true) {
			FileRangeSet refs(fileRefs);
			const ushorttype fileIndex = iter->next();

			boolean overflow = false;
			fileRefs.add(fileIndex, overflow);

			// XXX: if this index causes overflow, then flush and start again
			if (overflow == true) {
				RealTimeProtocol<V,K>::writeInfoPaging(iwfile, map, key, flags, refs.getIndex(), refs.getRange(), 0 /* size */);
				++written;

				overflow = false;
				fileRefs.clear();
				fileRefs.add(fileIndex, overflow);
			}
		}

		if (fileRefs.isEmpty() == false) {
			RealTimeProtocol<V,K>::writeInfoPaging(iwfile, map, key, flags, fileRefs.getIndex(), fileRefs.getRange(), 0 /* size */);
			++written;
		}

		return written;
	}

	FORCE_INLINE static void writeFileRefs(TransientState& ts, MapFileUtil::FileType type, const K key) {
		WriteKeyPaging<V,K>::StaticState& s = ts.staticState;
		Segment<K>* segment = s.segment;

		inttype& actual = ts.actual;

		if (type == MapFileUtil::IRT) {
			Iterator<ushorttype>* pagingIter = ((TreeSet<ushorttype>*)segment->getPagingIndexes())->iterator();
			actual += WriteKeyPaging<V,K>::writeFileRefs(ts, MapFileUtil::IRT, key, pagingIter);
			delete pagingIter;

		} else {
			TreeSet<ushorttype> streamIndexes(&(Segment<K>::USHORT_CMP));
			segment->getStreamIndexes(true /* get old */, &streamIndexes);	
			Iterator<ushorttype>* streamIter = streamIndexes.iterator();
			actual += WriteKeyPaging<V,K>::writeFileRefs(ts, MapFileUtil::VRT, key, streamIter);
			delete streamIter;
		}
	}
};

template<int V, typename K>
struct SegmentMetadata_v1_1 {
	ubytetype m_segmentFlags;
	ushorttype m_lrtFileIndex;
	uinttype m_lrtFileLength;
	uinttype m_virtualSize;
	uinttype* m_cardinality;
	union {
		struct {
			ubytetype m_vrtFileRange;
			uinttype m_vrtFileIndex;
		};
		struct {
			ushorttype __unused1;
			ubytetype __unused2;
			ushorttype m_ulongtype64_s1;
		};
	};
	ubytetype m_irtFileRange;
	union {
		struct {
			ushorttype m_irtFileIndex;
			uinttype m_irtReference;
		};
		struct {
			ushorttype m_ulongtype64_s2;
			uinttype m_ulongtype64_i;
		};
	};
	ushorttype m_physicalSize;
	ubytetype m_keyCompression;
	uinttype m_streamPosition;
	uinttype m_preSegmentLocation;

	FORCE_INLINE SegmentMetadata_v1_1() :
		m_segmentFlags(0),
		m_lrtFileIndex(0),
		m_lrtFileLength(0),
		m_virtualSize(0),
		m_cardinality(null),
		m_vrtFileRange(0),
		m_vrtFileIndex(0),
		m_irtFileRange(0),
		m_irtFileIndex(0),
		m_irtReference(0),
		m_physicalSize(0),
		m_keyCompression(0),
		m_streamPosition(0),
		m_preSegmentLocation(0) {
	}

	FORCE_INLINE SegmentMetadata_v1_1(
		const ubytetype segmentFlags,
		const ushorttype lrtFileIndex,
		const uinttype lrtFileLength,
		const uinttype virtualSize,
		uinttype* cardinality,
		const ubytetype vrtFileRange,
		const uinttype vrtFileIndex,
		const ubytetype irtFileRange,
		const ushorttype irtFileIndex,
		const uinttype irtReference,
		const ushorttype physicalSize,
		const ubytetype keyCompression,
		const uinttype streamPosition,
		const uinttype preSegmentLocation) :
		m_segmentFlags(segmentFlags),
		m_lrtFileIndex(lrtFileIndex),
		m_lrtFileLength(lrtFileLength),
		m_virtualSize(virtualSize),
		m_cardinality(cardinality),
		m_vrtFileRange(vrtFileRange),
		m_vrtFileIndex(vrtFileIndex),
		m_irtFileRange(irtFileRange),
		m_irtFileIndex(irtFileIndex),
		m_irtReference(irtReference),
		m_physicalSize(physicalSize),
		m_keyCompression(keyCompression),
		m_streamPosition(streamPosition),
		m_preSegmentLocation(preSegmentLocation) {
	}

	FORCE_INLINE SegmentMetadata_v1_1(const SegmentMetadata_v1_1& s) :
		m_segmentFlags(s.m_segmentFlags),
		m_lrtFileIndex(s.m_lrtFileIndex),
		m_lrtFileLength(s.m_lrtFileLength),
		m_virtualSize(s.m_virtualSize),
		m_cardinality(s.m_cardinality),
		m_vrtFileRange(s.m_vrtFileRange),
		m_vrtFileIndex(s.m_vrtFileIndex),
		m_irtFileRange(s.m_irtFileRange),
		m_irtFileIndex(s.m_irtFileIndex),
		m_irtReference(s.m_irtReference),
		m_physicalSize(s.m_physicalSize),
		m_keyCompression(s.m_keyCompression),
		m_streamPosition(s.m_streamPosition),
		m_preSegmentLocation(s.m_preSegmentLocation) {
	}

	FORCE_INLINE ~SegmentMetadata_v1_1() {
		/* XXX: nothing to do */
	}

	FORCE_INLINE static boolean isPreSegmentLocation(BufferedRandomAccessFile& irfile, const longtype pos) {
		if ((pos < ((longtype)Properties::DEFAULT_FILE_HEADER)) || (pos >= (irfile.length() - ((longtype)sizeof(inttype))))) {
			return false;
		}

		boolean eof = false;
		irfile.seek(pos);
		const longtype preSegmentLocation = ((longtype)irfile.readInt(&eof)) & 0xFFFFFFFFL;

		return (eof == false) && (pos == preSegmentLocation);
	}

	FORCE_INLINE static boolean possibleSegmentStart(RealTimeMap<K>* map, BufferedRandomAccessFile& irfile, const longtype pos) {
		return (pos >= Properties::DEFAULT_FILE_HEADER) && (pos <= (irfile.length() - map->m_irtHeader));
	}

	FORCE_INLINE static boolean possiblePostSegmentLocation(BufferedRandomAccessFile& irfile, const longtype pos) {
		return (pos >= Properties::DEFAULT_FILE_HEADER) && ((pos + 1 /* index flags */) < irfile.length());
	}

	FORCE_INLINE static boolean seekSegment(RealTimeMap<K>* map, BufferedRandomAccessFile& irfile, const longtype pos /* preSegmentLocation */, longtype* prev = null /* XXX: preSegmentLocation of previous segment */) {
		static const longtype OFFSET = (4 /* pre segment location */ + 4 /* post segment location */);

		const longtype curpos = irfile.getFilePointer();
		if (isPreSegmentLocation(irfile, pos) == false) {
			irfile.seek(curpos);
			return false;
		}

		const longtype segmentStart = pos - (((longtype)map->m_irtHeader) - OFFSET);

		if (prev == null) {
			irfile.seek(segmentStart);
		} else {
			irfile.seek(segmentStart - sizeof(uinttype));
			*prev = ((longtype)irfile.readInt()) & 0xFFFFFFFFL;
		}

		return true;
	}

	FORCE_INLINE static SegmentMetadata_v1_1 readMetaHeader(RealTimeMap<K>* map, BufferedRandomAccessFile& irfile, boolean* eof) {
		const ubytetype segmentFlags = irfile.readByte(eof);

		const ushorttype lrtFileIndex = irfile.readShort(eof);
		const uinttype lrtFileLength = irfile.readInt(eof);

		const uinttype virtualSize = irfile.readInt(eof);

		RealTimeProtocol<V,K>::readKeyCardinality(&irfile, map, null, eof);

		const ubytetype vrtFileRange = irfile.readByte(eof);
		const uinttype vrtFileIndex = irfile.readInt(eof);

		const ubytetype irtFileRange = irfile.readByte(eof);
		const ushorttype irtFileIndex /* ulongtype64.s2 */ = irfile.readShort(eof);
		const uinttype irtReference /* ulongtype64.i */ = irfile.readInt(eof);

		const ushorttype physicalSize = irfile.readShort(eof);

		const ubytetype keyCompression = irfile.readByte(eof);
		const uinttype streamPosition = irfile.readInt(eof);

		const uinttype preSegmentLocation = irfile.readInt(eof);

		return SegmentMetadata_v1_1(segmentFlags, lrtFileIndex, lrtFileLength, virtualSize, null /* cardinality */, vrtFileRange, vrtFileIndex, irtFileRange, irtFileIndex, irtReference, physicalSize, keyCompression, streamPosition, preSegmentLocation);
	}

	FORCE_INLINE static boolean skipSegmentBody(RealTimeMap<K>* map, BufferedRandomAccessFile& irfile, SegmentMetadata_v1_1<V,K>& segmentHeader, bytetype* indexFlags) {
		boolean eof = false;

		const longtype pos = segmentHeader.m_preSegmentLocation;
		const ushorttype physicalSize = segmentHeader.m_physicalSize;
		const ubytetype keyCompression = segmentHeader.m_keyCompression;

		// XXX: grab position before the flags read to calculate jump position
		longtype jumpPosition = pos + sizeof(uinttype);
		#ifdef DEEP_DEBUG
		if (jumpPosition != irfile.BufferedRandomAccessFile::getFilePointer()) {
			DEEP_LOG(ERROR, OTHER, "Invalid jump position, %lld != %lld, %s\n", jumpPosition, irfile.BufferedRandomAccessFile::getFilePointer(), irfile.getPath());
			throw InvalidException("Invalid jump position");
		}
		#endif

		if (keyCompression == true) {
			irfile.setCompress(BufferedRandomAccessFile::COMPRESS_READ);

			// XXX: BREAKING CONVENTION: in this error case, we can legitimately recover from exception (eg toss IRT)
			try {
				irfile.BufferedRandomAccessFile::seek(jumpPosition, true /* validate */);

			} catch (InvalidException) {
				return false;

			} catch (IOException) {
				return false;
			}

			longtype compressBlockEnd = jumpPosition + irfile.getAndResetBlockLength();

			bytetype flags = irfile.readByte(&eof); /* index flags */
			*indexFlags = flags;

			irfile.BufferedRandomAccessFile::seek(jumpPosition);

			for (int i = 0; (eof == false) && (i < physicalSize); i++) {

				// XXX: BREAKING CONVENTION: in this error case, we can legitimately recover from exception (eg toss IRT)
				try {
					RealTimeProtocol<V,K>::readPagingInfo(&irfile, map, &flags, &eof);

				} catch(InvalidException) {
					return false;

				} catch(IOException) {
					return false;
				}
			}

			irfile.setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
			irfile.BufferedRandomAccessFile::seek(compressBlockEnd);

		} else {
			bytetype flags = irfile.readByte(&eof); /* index flags */
			*indexFlags = flags;

			// TODO: support direct seek
			irfile.BufferedRandomAccessFile::seek(jumpPosition);

			for (int i = 0; (eof == false) && (i < physicalSize) && (jumpPosition < irfile.length()); i++) {
				RealTimeProtocol<V,K>::readPagingInfo(&irfile, map, &flags, &eof);

				jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();
			}

			if (jumpPosition > (irfile.length() - 4 /* post segment location */)) {
				return false;
			}
		}

		if (eof == true) {
			return false;
		}

		const longtype postSegmentLocation = ((longtype)irfile.readInt(&eof)) & 0xFFFFFFFFL;

		return (eof == false) && (pos == postSegmentLocation);
	}

};

template<int V, typename K>
struct RealTimeProtocol_v1_1_0_0 {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;
	typedef typename SegTreeMap::TreeMapEntrySet::EntrySetIterator MapInformationEntrySetIterator;
	typedef typename TreeMap<K,Segment<K>*>::TreeMapEntrySet::EntrySetIterator MapSegmentEntrySetIterator;

	static const inttype RESTORE_DEBUG_MOD = RealTimeProtocol_v1_0_0_0<V,K>::RESTORE_DEBUG_MOD;

	/* IRT HEAD BODY FORMAT
		1 : segment flags
		2 : lrt file index
		4 : lrt file length
		4 : virtual size
		? : cardinality
		1 : vrt file ref count OR vrt file range
		4 : vrt file ref position OR vrt file index
		1 : irt file range
		2 : irt file index
		4 : irt reference
		2 : physical size
		1 : key compressed delta
		4 : vrt position  
		4 : pre segment location
		4 : post segment location
		---------------------
		38 - total offset size
	*/
	static const inttype IRT_HEAD_FORMAT_FIX = 38;

	static const inttype IRT_BODY_FORMAT_FIX = RealTimeProtocol_v1_0_0_0<V,K>::IRT_BODY_FORMAT_FIX;
	static const inttype IRT_BODY_FORMAT_VAR = RealTimeProtocol_v1_0_0_0<V,K>::IRT_BODY_FORMAT_VAR;

	/* FIX - LRT INFO BODY FORMAT
		1 : log flags
		0/8 : transactionId (cross table/db transactions only)
		4 : value position
		? : size of key
		---------------------
		5/13 - total offset size
	*/
	static const inttype LRT_BODY_FORMAT_PART = RealTimeProtocol_v1_0_0_0<V,K>::LRT_BODY_FORMAT_FIX + 4 /* vposition */;

	/* VAR - LRT INFO BODY FORMAT
		1 : log flags
		0/8 : transactionId (cross table/db transactions only)
		4 : value position
		4 : value size
		? : size of key
		---------------------
		9/17 - total offset size
	*/
	static const inttype LRT_BODY_FORMAT_FULL = RealTimeProtocol_v1_0_0_0<V,K>::LRT_BODY_FORMAT_VAR;

	static const bytetype LRT_FLAG_OPENING = RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_OPENING;
	static const bytetype LRT_FLAG_CLOSING = RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_CLOSING;
	static const bytetype LRT_FLAG_ROLLING = RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_ROLLING;

	#if 0
	static const bytetype LRT_FLAG_RESERVE = 0x04;
	#else
	static const bytetype LRT_FLAG_ACP = 0x04;
	#endif

	static const bytetype LRT_FLAG_DESTROY = RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_DESTROY;
	static const bytetype LRT_FLAG_MARKING = RealTimeProtocol_v1_0_0_0<V,K>::LRT_FLAG_MARKING;
	static const bytetype LRT_FLAG_COMPRESS = 0x40;
	static const bytetype LRT_FLAG_CLOSURE;

	static const bytetype IRT_FLAG_CONTENT = RealTimeProtocol_v1_0_0_0<V,K>::IRT_FLAG_CONTENT;
	static const bytetype IRT_FLAG_RESERVE = RealTimeProtocol_v1_0_0_0<V,K>::IRT_FLAG_RESERVE;
	static const bytetype IRT_FLAG_DELETED = RealTimeProtocol_v1_0_0_0<V,K>::IRT_FLAG_DELETED;
	static const bytetype IRT_FLAG_CLOSURE = RealTimeProtocol_v1_0_0_0<V,K>::IRT_FLAG_CLOSURE;
	static const bytetype IRT_FLAG_REF_IRT;
	static const bytetype IRT_FLAG_REF_VRT;
	static const bytetype IRT_MFLAG_VCOMPRESS;

	static const shorttype DEFAULT_TXID = -1;

	static const ubytetype SUMMARY_FILE_REF_MARKER = 0x2A;

	FORCE_INLINE static inttype getIrtHeader(RealTimeMap<K>* map) {
		if (map->m_keyBuilder->getKeyParts() > 1) {
			return (map->m_keyBuilder->getKeyParts() * sizeof(inttype)) + IRT_HEAD_FORMAT_FIX;

		} else {
			return IRT_HEAD_FORMAT_FIX;
		}
	}

	FORCE_INLINE static inttype getIrtBlockSize(RealTimeMap<K>* map) {
		return RealTimeProtocol_v1_0_0_0<V,K>::getIrtBlockSize(map);
	}

	FORCE_INLINE static inttype getLrtBlockSize(RealTimeMap<K>* map) {
		return RealTimeProtocol_v1_0_0_0<V,K>::getLrtBlockSize(map) + 4 /* vposition */;
	}

	FORCE_INLINE static inttype getBufferSize(RealTimeMap<K>* map) {
		return RealTimeProtocol_v1_0_0_0<V,K>::getBufferSize(map);
	}

	FORCE_INLINE static boolean isEmptyMap(RealTimeMap<K>* map, ushorttype& index) {
		return RealTimeProtocol_v1_0_0_0<V,K>::isEmptyMap(map, index);
	}

	FORCE_INLINE static boolean checkpoint(RealTimeMap<K>* map, BufferedRandomAccessFile* lwfile, RealTimeConductor<K>* conductor) {
		return RealTimeProtocol_v1_0_0_0<V,K>::checkpoint(map, lwfile, conductor);
	}

	FORCE_INLINE static bytetype validateKeyPagingSafe(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, boolean lastUsed, RealTimeLocality& lastLrtLocality, longtype* lastIrtLength) {
		try {
			return RealTimeProtocol<V,K>::validateKeyPaging(iwfile, map, lastUsed, lastLrtLocality, lastIrtLength);
		} catch(...) {
			DEEP_LOG(ERROR, FAULT, "e-irt: %s\n", iwfile->getPath());
			return RealTimeMap<K>::SUMMARY_ERROR;
		}
	}

	FORCE_INLINE static bytetype validateKeyPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, boolean lastUsed, RealTimeLocality& lastLrtLocality, longtype* lastIrtLength) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");

		static const longtype OFFSET = (4 /* pre segment location */ + 4 /* post segment location */);

		boolean eof = false;

		enum ValidateMode {
			C_IRT = 0,
			Q_IRT = 1,
			P_IRT = 2
		};

		ushorttype lrtIndex = 0;
		if ((iwfile->length() == Properties::DEFAULT_FILE_HEADER) && (RealTimeProtocol<V,K>::isEmptyMap(map, lrtIndex) == true)) {
			lastLrtLocality = RealTimeLocality(lrtIndex, Properties::DEFAULT_FILE_HEADER, RealTimeLocality::VIEWPOINT_NONE);
			*lastIrtLength = Properties::DEFAULT_FILE_HEADER;
			return RealTimeMap<K>::SUMMARY_EMPTY;
		}

		SegmentMetadata_v1_1<V,K> segmentHeader;

		bytetype flags = IRT_FLAG_REF_VRT + 1;
		ushorttype lrtFileIndex = 0;
		uinttype lrtFileLength = 0;

		BufferedRandomAccessFile irfile(iwfile->getPath(), "r", map->m_irtHeader + map->m_irtOffset + 1 /* +1 ensures no refill */);
		irfile.setProtocol(iwfile->getProtocol());
		irfile.setFileIndex(iwfile->getFileIndex());
		irfile.setFileCreationTime(iwfile->getFileCreationTime());
		irfile.setOnline(true); /* TODO: m_share.acquire(...); */

		irfile.BufferedRandomAccessFile::seek(irfile.length() - 4 /* post segment location */);

		uinttype preSegmentLocation = 0;
		uinttype postSegmentLocation = irfile.readInt(&eof);
		ushorttype physicalSize = 0;

		longtype nextSegmentPSL = 0;

		struct {
			ushorttype s1;
			ushorttype s2;
			uinttype i;
		} ulongtype64;
		memset(&ulongtype64, 0, sizeof(ulongtype64));

		// XXX: 256 trillion entries (i.e. plenty)
		struct {
			uinttype i;
			ushorttype s;
		} uinttype48;

		ValidateMode mode = C_IRT;

		NEXT_SEGMENT:
		boolean keyCompression = false;
		if ((eof == false) && (SegmentMetadata_v1_1<V,K>::possiblePostSegmentLocation(irfile, postSegmentLocation) == true)) {
			const boolean validPos = SegmentMetadata_v1_1<V,K>::seekSegment(map, irfile, postSegmentLocation, &nextSegmentPSL);
			if (validPos == false) {
				eof = true;
				goto RETRY;
			}

			segmentHeader = SegmentMetadata_v1_1<V,K>::readMetaHeader(map, irfile, &eof);

			lrtFileIndex = segmentHeader.m_lrtFileIndex;
			lrtFileLength = segmentHeader.m_lrtFileLength;

			ulongtype64.s1 = segmentHeader.m_ulongtype64_s1;
			ulongtype64.s2 = segmentHeader.m_ulongtype64_s2;
			ulongtype64.i = segmentHeader.m_ulongtype64_i;

			physicalSize = segmentHeader.m_physicalSize;
			keyCompression = segmentHeader.m_keyCompression;

			preSegmentLocation = segmentHeader.m_preSegmentLocation;

			if (keyCompression == true) {
				longtype seekPos = irfile.BufferedRandomAccessFile::getFilePointer();
				irfile.setCompress(BufferedRandomAccessFile::COMPRESS_READ);
				// XXX: BREAKING CONVENTION: in this error case, we can legitimately recover from exception (eg toss IRT)
				try {
					irfile.BufferedRandomAccessFile::seek(seekPos, true /* validate */);
				} catch (InvalidException) {
					eof = true;
					goto RETRY;

				} catch (IOException) {
					eof = true;
					goto RETRY;
				}
			}

			RealTimeProtocol<V,K>::readPagingInfo(&irfile, map, &flags, &eof, null /* retkey */, &uinttype48.s, &uinttype48.i);
		}

		RETRY:
		if ((eof == false) && (flags == IRT_FLAG_CLOSURE) && (postSegmentLocation == preSegmentLocation)) {
			*lastIrtLength = postSegmentLocation - (map->m_irtHeader - OFFSET);

			longtype entrySize = 0;
			uinttype recoveryEpoch = 0;
			{
				memcpy(&entrySize, &uinttype48, 6 /* i.e. 48 bits */);

				if (physicalSize > 1) { // XXX: check is for 1.1 compatibility
					RealTimeProtocol<V,K>::readPagingInfo(&irfile, map, &flags, &eof, null /* retkey */, &uinttype48.s, &uinttype48.i);
					memcpy(&recoveryEpoch, &uinttype48, 4 /* i.e. 32 bits */);
					if (physicalSize > 2) {
						RealTimeProtocol<V,K>::readPagingInfo(&irfile, map, &flags, &eof);
						iwfile->setInvalidSummary(true);
					}
				}
			}

			if ((keyCompression == true) && (irfile.getCompress() != BufferedRandomAccessFile::COMPRESS_NONE)) {
				irfile.setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
			}

			if ((eof == true) || (flags != IRT_FLAG_CLOSURE)) {
				goto RETRY;
			}

			longtype time = 0;
			memcpy(&time, &ulongtype64, sizeof(ulongtype64));
			lastLrtLocality = RealTimeLocality::safeLocality(lrtFileIndex, lrtFileLength, RealTimeLocality::VIEWPOINT_NONE, time);

			iwfile->setEntrySize(entrySize);
			iwfile->setRecoveryEpoch(recoveryEpoch);

			if (((lrtFileIndex == 0) && (lrtFileLength == 0)) || (lastLrtLocality.isNone() == false)) {
				Logger::Level logLevel = Logger::DEBUG;
				const char* modeStr;
				switch(mode) {
				case C_IRT:
					modeStr = "c-irt";
					break;
				case Q_IRT:
					logLevel = Logger::WARN;
					modeStr = "q-irt";
					break;
				default:
					logLevel = Logger::WARN;
					modeStr = "p-irt";
					break;
				}
				DEEP_LOG_LEVEL(logLevel, DCVRY, "%s: %s, valid, %s / %lld\n", modeStr, iwfile->getPath(), (const char*) lastLrtLocality.toString(), iwfile->getInitialLength());

				return RealTimeMap<K>::SUMMARY_INTACT;
			}

		} else if ((eof == true) || ((flags & ~IRT_MFLAG_VCOMPRESS) > IRT_FLAG_REF_VRT) || (postSegmentLocation != preSegmentLocation)) {
			/* XXX: p-irt */
			if (mode != C_IRT) {
				DEEP_LOG(ERROR, FAULT, "e-irt (mode): %s\n", iwfile->getPath());
				return RealTimeMap<K>::SUMMARY_ERROR;
			}
			mode = P_IRT;

			if ((keyCompression == true) && (irfile.getCompress() != BufferedRandomAccessFile::COMPRESS_NONE)) {
				irfile.setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
			}

			irfile.BufferedRandomAccessFile::seek(Properties::DEFAULT_FILE_HEADER);
			eof = false;

			boolean found = false;
			longtype currentPosition = irfile.BufferedRandomAccessFile::getFilePointer();
			while (SegmentMetadata_v1_1<V,K>::possibleSegmentStart(map, irfile, currentPosition) == true) {
				segmentHeader = SegmentMetadata_v1_1<V,K>::readMetaHeader(map, irfile, &eof);
				if (eof == true) {
					break;
				}

				bytetype flags = 0;
				const boolean res = SegmentMetadata_v1_1<V,K>::skipSegmentBody(map, irfile, segmentHeader, &flags);
				if (res == false) {
					break;
				}

				if (flags == IRT_FLAG_CLOSURE) {
					found = true;
					postSegmentLocation = segmentHeader.m_preSegmentLocation;
				}

				currentPosition = irfile.BufferedRandomAccessFile::getFilePointer();
			}

			if (currentPosition == Properties::DEFAULT_FILE_HEADER) {
				DEEP_LOG(ERROR, FAULT, "e-irt: %s\n", iwfile->getPath());
				return RealTimeMap<K>::SUMMARY_ERROR;
			}

			map->m_share.acquire(iwfile);
			{
				iwfile->setLength(currentPosition);
				iwfile->BufferedRandomAccessFile::seek(iwfile->length());
			}
			map->m_share.release(iwfile);

			if (lastUsed == false) {
				iwfile->setOptimizeCount(0);
				iwfile->setReorganizeComplete(true);
			}

			if (found == true) {
				goto NEXT_SEGMENT;
			}

			DEEP_LOG(WARN, DCVRY, "p-irt: %s, valid, %s / %lld\n", iwfile->getPath(), (const char*) RealTime::Locality::LOCALITY_NONE.toString(), *lastIrtLength);

			return RealTimeMap<K>::SUMMARY_IGNORE;

		} else {
			/* XXX: q-irt */
			if ((mode != C_IRT) && (mode != Q_IRT)) {
				DEEP_LOG(ERROR, FAULT, "e-irt (mode): %s\n", iwfile->getPath());
				return RealTimeMap<K>::SUMMARY_ERROR;
			}
			mode = Q_IRT;

			if ((keyCompression == true) && (irfile.getCompress() != BufferedRandomAccessFile::COMPRESS_NONE)) {
				irfile.setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
			}

			if ((nextSegmentPSL < Properties::DEFAULT_FILE_HEADER) || (nextSegmentPSL >= postSegmentLocation)) {
				if ((flags != IRT_FLAG_CLOSURE) && (lastUsed == false)) {
					iwfile->setOptimizeCount(0);
					iwfile->setReorganizeComplete(true);
				}

				DEEP_LOG(WARN, DCVRY, "q-irt: %s, valid, %s / %lld\n", iwfile->getPath(), (const char*) RealTime::Locality::LOCALITY_NONE.toString(), *lastIrtLength);

				return RealTimeMap<K>::SUMMARY_IGNORE;
			}

			postSegmentLocation = nextSegmentPSL;

			goto NEXT_SEGMENT;
		}

		DEEP_LOG(ERROR, FAULT, "e-irt: %s\n", iwfile->getPath());
		return RealTimeMap<K>::SUMMARY_ERROR;
	}

	FORCE_INLINE static void repositionPaging(MeasuredRandomAccessFile* iwfile, const uinttype cursorInit, const longtype positionInit) {
		return RealTimeProtocol_v1_0_0_0<V,K>::repositionPaging(iwfile, cursorInit, positionInit);
	}

	FORCE_INLINE static inttype writeKeyPaging(MeasuredRandomAccessFile* p_iwfile, RealTimeMap<K>* p_map, ThreadContext<K>* p_ctxt, Segment<K>* p_segment, boolean p_rebuild, uinttype p_viewpoint, K& p_summaryFirstKey, boolean& early, RealTimeIndex::IndexReport& p_indexReport) {
		DEEP_VERSION_ASSERT_EQUAL(V,p_iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(p_iwfile->getProtocol(),"Non-current protocol file accessed for write");

		#ifdef DEEP_DEBUG
		if (p_iwfile->getCompress() != BufferedRandomAccessFile::COMPRESS_NONE) {
			DEEP_LOG(ERROR, OTHER, "Invalid index compression: write key paging");

			throw InvalidException("Invalid index compression: write key paging");
		}
		#endif

		// XXX: logical first key written may differ from physical first key when segment viewed state is true (so deltas are unsafe)
		if (p_segment->getViewed() == true) {
			p_rebuild = true;
		}

		typedef typename WriteKeyPaging<V,K>::StaticState SS;
		typedef typename WriteKeyPaging<V,K>::TransientState TS;
		SS s(p_iwfile, p_map, p_ctxt, p_segment, p_rebuild, p_viewpoint, p_summaryFirstKey, p_indexReport);

		MeasuredRandomAccessFile*& iwfile = s.iwfile;
		RealTimeMap<K>*& map = s.map;
		Segment<K>*& segment = s.segment;
		boolean& rebuild = s.rebuild;
		const boolean checkpoint = s.viewpoint != 0;

		map->m_resetIterator = false;

		TS ts = RealTimeUtilities<K>::template walkSegment<SS, TS, WriteKeyPaging<V,K>::processInfo>(map, segment, map->m_indexInformationSet, s);
		early = ts.early;

		if (segment->getBeenDeleted() == true) {
			return 0;
		}

		RealTimeIndex::IndexReport& indexReport = s.indexReport;

		K& firstKey = s.firstKey;
		const boolean summary = s.summary;

		inttype& actual = ts.actual;
		inttype& ceasing = ts.ceasing;
		boolean& skip = ts.skip;
		boolean& dirty = ts.dirty;
		boolean& viewed = ts.viewed;

		const uinttype cursorInit = ts.cursorInit;
		const longtype positionInit = ts.positionInit;
		uinttype& streamPosition = ts.streamPosition;
		const uinttype location = ts.location;
		const uinttype pagingPosition = ts.pagingPosition;

		// XXX: write stream references into paging info block on overflow
		uinttype streamRefLocation = 0;
		ubytetype streamRefCount = 0;
		if ((actual != 0) && (segment->getStreamed() == true) && (summary == false) && (segment->getStreamRangeOverflow() == true)) {
			segment->setStreamRangeOverflowFlag(true);		

			if (ts.keyCompression == true) {
				// XXX: this is a count offset into the compressed block
				streamRefLocation = actual + ((skip == true) ? 1 : 0);	
			} else {
				streamRefLocation = iwfile->BufferedRandomAccessFile::getFilePointer();	
			}

			inttype actualBefore = actual;
			WriteKeyPaging<V,K>::writeFileRefs(ts, MapFileUtil::VRT, firstKey);
			streamRefCount = actual - actualBefore;
		}

		if (iwfile->getCompress() == BufferedRandomAccessFile::COMPRESS_WRITE) {
			iwfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
			if (actual < (segment->SegTreeMap::size() / Properties::DEFAULT_SEGMENT_MINIMUM_KEY_BLOCK_DIVISOR)) {
				iwfile->resetCompressionRatio();	
			}
		}

		if (checkpoint == true) {
			if (actual != 0) {

				segment->setIndexLocality(RealTime::Locality::LOCALITY_NONE);

				if (skip == true) {
					actual += 1;
				}
	
				RealTimeProtocol<V,K>::writeMetaData(iwfile, map, segment, cursorInit, positionInit, rebuild, actual, streamRefCount, streamRefLocation, streamPosition, true /* checkpoint */, ts.keyCompression);
				segment->setPagingPosition(pagingPosition);
				iwfile->writeInt(location /* post segment location */);
				iwfile->flush();

				segment->setBeenSurceased(ceasing == segment->SegTreeMap::size());

			} else {

				RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);

				if (rebuild == true) {
					segment->setBeenSurceased(true);
				} else {
					segment->setIndexLocality(RealTime::Locality::LOCALITY_NONE);
				}
			}

			segment->setViewpoint(true);
			segment->setViewed((rebuild == true) && (viewed == true) && (dirty == false));
			segment->setDirty(dirty || ((rebuild == false) && (viewed == true)));

			if (map->m_resetIterator == true) {
				map->reseedSegment(segment, firstKey, true, true /* lock */);
			}

			return actual;
		}

		#ifdef DEEP_DEBUG
		if (checkpoint == true) {
			DEEP_LOG(ERROR, INDEX, "invalid checkpoint state, %s\n", map->getFilePath());
			throw InvalidException("Invalid checkpoint state");
		}
		#endif

		if ((actual != 0) || (summary == true)) {

			const boolean emptySummary = (actual == 0) && (summary == true);

			if ((emptySummary == true) && (ts.keyCompression == true) && (iwfile->getCompress() == BufferedRandomAccessFile::COMPRESS_NONE)) {
				iwfile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);
				iwfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
			}

			segment->setIndexLocality((map->m_primaryIndex == null) ? map->getCurrentLrtLocality() : map->m_primaryIndex->getCurrentLrtLocality());

			if (skip == true) {
				actual += 1;
			}

			// XXX: track key block sizes for value compression adaptive functionality
			if ((map->m_valueCompressMode == true) && (rebuild == true)) {
				map->m_activeKeyBlockSize += iwfile->getAndResetBlockLength();
				map->m_activeKeyBlockCount++;
			}

			RealTimeProtocol<V,K>::writeMetaData(iwfile, map, segment, cursorInit, positionInit, rebuild, actual, streamRefCount, streamRefLocation, streamPosition, false /* checkpoint */, ts.keyCompression);
			segment->setPagingPosition(pagingPosition);
			iwfile->writeInt(location /* post segment location */);
			iwfile->flush();

			if (emptySummary == true) {
				actual = 1; /* XXX: indicate written == true */
			}

		} else {

			RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);

			if (segment->getPagingPosition() == 0) {
				dirty = true;
			}

			// XXX: maintain any rebuild request set by writeKeyPaging (eg key removal)
			if (rebuild == true) {
				segment->setBeenAltered(true);
			}
		}

		if (map->m_resetIterator == true) {
			map->reseedSegment(segment, firstKey, true, true /* lock */);
		}

		if (segment->getSummary() == false) {
			segment->setBeenSurceased(ceasing == segment->SegTreeMap::size());
		}
		segment->setViewpoint(false);
		segment->setViewed((rebuild == true) && (viewed == true) && (dirty == false));
		segment->setDirty(dirty || ((rebuild == false) && (viewed == true)));

		if (segment->getDirty() == true) {
			++indexReport.dirty;
		}

		if (segment->getViewed() == true) {
			++indexReport.viewed;
		}

		if (rebuild == true) {
			++indexReport.rebuild;
		}

		// XXX: collect post-indexing purge stats
		RealTimeAdaptive_v1<K>::allowPurgeSegment(map, segment, true /* index */, false /* growing */, false /* semi */, indexReport.purgeReport);

		return actual;
	}

	FORCE_INLINE static void statisticPaging(RealTimeMap<K>* map, Segment<K>* segment, const Information* topinfo, const Information* curinfo, boolean rebuild, const boolean checkpoint) {
		RealTimeProtocol_v1_0_0_0<V,K>::statisticPaging(map, segment, topinfo, curinfo, rebuild, checkpoint);
	}

	FORCE_INLINE static void statisticPagingDead(RealTimeMap<K>* map, Segment<K>* segment, const Information* info, const boolean checkpoint) {
		RealTimeProtocol_v1_0_0_0<V,K>::statisticPagingDead(map, segment, info, checkpoint);
	}

	FORCE_INLINE static longtype findSummaryPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, const RealTimeLocality& lastLrtLocality, const uinttype recoveryEpoch) {
		static const inttype OFFSET = (4 /* pre segment location */ + 4 /* post segment location */); // XXX: see validateKeyPaging()
		uinttype preSegmentLocation = iwfile->getLastIrtLength() + (map->m_irtHeader - OFFSET);

		BufferedRandomAccessFile irfile(iwfile->getPath(), "r", map->m_irtHeader + map->m_irtOffset + 1 /* +1 ensures no refill */);
		irfile.setProtocol(iwfile->getProtocol());
		irfile.setOnline(true); /* TODO: m_share.acquire(...); */

		// XXX: cast to unsigned before setting currentPosition
		uinttype position = preSegmentLocation;
		longtype currentPosition = position;

		while (currentPosition > Properties::DEFAULT_FILE_HEADER) {
			irfile.BufferedRandomAccessFile::seek(currentPosition - (map->m_irtHeader - 4 /* post segment location */));

			// XXX: cast to unsigned before setting currentPosition
			uinttype position = irfile.readInt();
			if (position > currentPosition) {
				DEEP_LOG(WARN, FAULT, "x-irt: %u >= %lld, %s\n", position, currentPosition, irfile.getPath());
				break;
			}
			currentPosition = position;

			boolean summarySegment = false;
			RealTimeLocality lrtLocality = RealTimeLocality::LOCALITY_NONE;
			inttype lrtFileIndex = -1;
			longtype lrtFileLength = -1;
			longtype postSegmentLocation = -1;
			uinttype epoch = 0;
			{
				ubytetype segmentFlags = irfile.readByte();
				summarySegment = (segmentFlags & Segment<K>::SEGMENT_FLAG_SUMMARY) != 0;

				lrtFileIndex = irfile.readShort();
				lrtFileLength = irfile.readInt();

				/* uinttype virtualSize = */ irfile.readInt();

				RealTimeProtocol<V,K>::readKeyCardinality(&irfile, map, null);

				/* ubytetype vrt file ref count = */ irfile.readByte();
				/* uinttype vrt file ref location = */ irfile.readInt();

				/* ubytetype irtFileRange = */ irfile.readByte();
				/* ushorttype irtFileIndex = */ irfile.readShort();
				/* uinttype pagingPosition = */ irfile.readInt();

				ushorttype physicalSize = irfile.readShort();

				boolean keyCompression = irfile.readByte();
				epoch /* streamPosition */ = irfile.readInt();

				preSegmentLocation = irfile.readInt();

				// XXX: grab position before the flags read to calculate jump position
				longtype jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();

				if (keyCompression == true) {
					irfile.setCompress(BufferedRandomAccessFile::COMPRESS_READ);
					// XXX: BREAKING CONVENTION: in this error case, we can legitimately recover from exception (eg toss IRT)
					try {
						irfile.BufferedRandomAccessFile::seek(jumpPosition, true /* validate */);
					} catch (InvalidException e) {
						DEEP_LOG(WARN, FAULT, "l-irt: compressed seek failed { %s, %s }\n", e.getMessage()->c_str(), irfile.getPath());
						break;

					} catch (IOException e) {
						DEEP_LOG(WARN, FAULT, "l-irt: compressed seek failed { %s, %s }\n", e.getMessage()->c_str(), irfile.getPath());
						break;
					}
					longtype compressBlockEnd = jumpPosition + irfile.getAndResetBlockLength();

					bytetype flags;

					for (int i = 0; (i < physicalSize); i++) {

						// XXX: BREAKING CONVENTION: in this error case, we can legitimately recover from exception (eg toss IRT)
						try {
							RealTimeProtocol<V,K>::readPagingInfo(&irfile, map, &flags);

						} catch(InvalidException e) {
							DEEP_LOG(WARN, FAULT, "l-irt: compressed read failed { %s, %s }\n", e.getMessage()->c_str(), irfile.getPath());
							break;

						} catch(IOException e) {
							DEEP_LOG(WARN, FAULT, "l-irt: compressed read failed { %s, %s }\n", e.getMessage()->c_str(), irfile.getPath());
							break;
						}
					}

					irfile.setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
					irfile.BufferedRandomAccessFile::seek(compressBlockEnd);
						
				} else {

					bytetype flags = irfile.readByte(); /* index flags */

					// TODO: support direct seek
					#if 0
						if (map->m_share.getKeySize() != -1) {
						jumpPosition += (map->m_irtOffset * physicalSize);
						if (jumpPosition > (irfile.length() - 4 /* post segment location */)) {
							break;
						}

						irfile.BufferedRandomAccessFile::seek(jumpPosition);

					} else {
					#else
						irfile.setCursor(irfile.getCursor() - 1 /* index flags */);
						irfile.setPosition(irfile.getPosition() - 1 /* index flags */);

						jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();
						for (int i = 0; (i < physicalSize) && (jumpPosition < irfile.length()); i++) {
							RealTimeProtocol<V,K>::readPagingInfo(&irfile, map, &flags);

							jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();
						}

						if (jumpPosition > (irfile.length() - 4 /* post segment location */)) {
							DEEP_LOG(WARN, FAULT, "l-irt: { jmp %lld / len %lld, %s }\n", jumpPosition, irfile.length(),  irfile.getPath());
							break;
						}
					#if 0
					}
					#endif
					#endif
				}

				postSegmentLocation = (uinttype)irfile.readInt();

				if (postSegmentLocation != preSegmentLocation) {
					DEEP_LOG(WARN, FAULT, "m-irt: { pre %d / post %lld, %s }\n", preSegmentLocation, postSegmentLocation, irfile.getPath());
					break;
				}
			}

			// XXX: ignore non-summary segments
			if (summarySegment == false) {
				continue;
			}

			if ((recoveryEpoch == epoch) && (lastLrtLocality.getIndex() == lrtFileIndex) && (lastLrtLocality.getLength() == lrtFileLength)) {
				return postSegmentLocation - (map->m_irtHeader - OFFSET);
			}
		}

		return -1;
	}

	FORCE_INLINE static boolean summaryRebuildKeyPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map) {
		return RealTimeProtocol_v1_0_0_0<V,K>::summaryRebuildKeyPaging(iwfile, map);
	}

	FORCE_INLINE static void terminatePaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, boolean summarized, uinttype recoveryEpoch, boolean invalidate) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		RealTimeProtocol<V,K>::writeHeader(iwfile, 0, map->m_summaryLrtLocality.getIndex(), map->m_summaryLrtLocality.getLength(), 0);
		RealTimeProtocol<V,K>::writeKeyCardinality(iwfile, map, null);

		const longtype time = map->m_summaryLrtLocality.getTime();
		struct {
			ushorttype s1;
			ushorttype s2;
			uinttype i;
		} ulongtype64;
		memcpy(&ulongtype64, &time, sizeof(ulongtype64));

		// stream ref count (byte) + stream ref location (int) = 5 bytes
		iwfile->writeShort(0);
		iwfile->writeByte(0);
		iwfile->writeShort(ulongtype64.s1);

		RealTimeProtocol<V,K>::writePagingIndex(iwfile, 0, ulongtype64.s2, ulongtype64.i);

		iwfile->writeShort((invalidate == true) ? 3 : 2 /* physical size */);

		// XXX: there is no need to 'key' compress this small amount of data
		iwfile->writeByte(0 /* key compression */);
		// TODO: use flags to indicate if segment needs compress locality (i.e. smaller disk size)
		iwfile->writeInt(0 /* vrt position */);

		const uinttype location = iwfile->BufferedRandomAccessFile::getFilePointer();
		iwfile->writeInt(location /* pre segment location */);
		{
			K key = map->m_keyBuilder->newKey();
			map->m_keyBuilder->clearKey(key);

			longtype entrySize = map->size();
			{
				// XXX: 256 trillion entries (i.e. plenty)
				struct {
					unsigned int i;
					unsigned short s;
				} uinttype48;
				memcpy(&uinttype48, &entrySize, 6 /* i.e. 48 bits */);

				RealTimeProtocol<V,K>::writeInfoPaging(iwfile, map, key, IRT_FLAG_CLOSURE, uinttype48.s, uinttype48.i, 0);
				memset(&uinttype48, 0, 6 /* i.e. 48 bits */);
				memcpy(&uinttype48, &recoveryEpoch, 4 /* i.e. 32 bits */);
				RealTimeProtocol<V,K>::writeInfoPaging(iwfile, map, key, IRT_FLAG_CLOSURE, uinttype48.s, uinttype48.i, 0);
				if (invalidate == true) {
					RealTimeProtocol<V,K>::writeInfoPaging(iwfile, map, key, IRT_FLAG_CLOSURE, 0, 0, 0);
				}
			}

			Converter<K>::destroy(key);
		}
		iwfile->writeInt(location /* post segment location */);

		iwfile->flush();
	}

	FORCE_INLINE static void readMetaPaging(RealTimeMap<K>* map, Segment<K>* segment) {
		return RealTimeProtocol_v1_0_0_0<V,K>::readMetaPaging(map, segment);
	}
	
	FORCE_INLINE static void readKeyCardinality(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, boolean* eof = null) {
		return RealTimeProtocol_v1_0_0_0<V,K>::readKeyCardinality(irfile, map, segment, eof);
	}

	FORCE_INLINE static void writeKeyCardinality(BufferedRandomAccessFile* iwfile, RealTimeMap<K>* map, Segment<K>* segment) {
		return RealTimeProtocol_v1_0_0_0<V,K>::writeKeyCardinality(iwfile, map, segment);
	}

	FORCE_INLINE static void writeInfoPaging(BufferedRandomAccessFile* iwfile, RealTimeMap<K>* map, const K key, bytetype flags, ushorttype fileIndex, uinttype position, inttype size, uinttype compressedOffset = Information::OFFSET_NONE) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");

		const boolean hasCompressedOffset = (compressedOffset != Information::OFFSET_NONE);
		if (hasCompressedOffset == true) {
			flags |= IRT_MFLAG_VCOMPRESS;
		}
		iwfile->writeByte(flags);

		iwfile->writeShort(fileIndex);
		iwfile->writeInt(position);

		if (map->m_share.getValueSize() == -1) {
			iwfile->writeInt(size);
		}

		if (hasCompressedOffset == true) {
			iwfile->writeInt(compressedOffset);
		}

		KeyProtocol_v1<K>::writeKey(key, iwfile, map->m_share.getKeyProtocol());
	}

	FORCE_INLINE static void readKeyPaging(RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt, boolean modification, boolean compression) {
		return RealTimeProtocol_v1_0_0_0<V,K>::readKeyPaging(map, segment, ctxt, modification, compression);
	}

	FORCE_INLINE static void readKeyPagingIndex(RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt, BufferedRandomAccessFile* irfile /* acquired outside */, inttype* xfrag, boolean modification, boolean compression) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		static const inttype OFFSET = 2 /* irt file index */ + 4 /* irt reference */ + 2 /* physical size */ + 4 /* pre segment location */ + 1 /* keyCompression */ + 4 /* vrt position */;

		bytetype flags = 0;
		boolean sequential = false;

		uinttype pagingPosition = segment->getPagingPosition();
		uinttype lastSeek = pagingPosition;

		if (segment->initialized() == false) {
			#ifdef DEEP_DEBUG
			if (segment->getSummary() == true) {
				DEEP_LOG(ERROR, OTHER, "Invalid key paging: uninitialized summary");

				throw InvalidException("Invalid key paging: uninitialized summary");
			}
			#endif

			// XXX: file position will be aligned to the next protocol read
			RealTimeProtocol<V,K>::readMetaHeader(irfile, map, segment, true);
			
			if (segment->getStreamRangeOverflowFlag() == true) {
				// XXX: stream refs will be read below in the call to readPagingInfo
				/* streamRefCount = */ irfile->readByte();
				/* streamRefLocation = */  irfile->readInt();

			} else {
				ubytetype vrtFileRange = irfile->readByte();
				uinttype vrtFileIndex = irfile->readInt();

				if (segment->getStreamedFlag() == true) {
					segment->addStreamIndex(vrtFileIndex);
					segment->setStreamRange(vrtFileRange);
				}
			}

			ubytetype irtFileRange = irfile->readByte();
			segment->setPagingRange(irtFileRange);

			// XXX: the rest of the footer protocol is read below

		} else {
			lastSeek = pagingPosition - OFFSET;
			irfile->BufferedRandomAccessFile::seek(pagingPosition - OFFSET);
		}

		RETRY:
		ushorttype pagingIndex = irfile->readShort(); /* previous delta: irt file index */
		pagingPosition = irfile->readInt(); /* previous delta: irt reference */

		ushorttype physicalSize = irfile->readShort();

		boolean keyCompression = irfile->readByte();
		// TODO: use flags to indicate if segment needs compress locality (i.e. smaller disk size)
		uinttype streamPosition = irfile->readInt();

		if ((*xfrag == 0) && (segment->getSummary() == false)) {
			segment->setStreamPosition(streamPosition);

		} else if (segment->getSummary() == true) {
			segment->setRecoveryEpoch(streamPosition);
		}

		#ifdef DEEP_DEBUG
		uinttype beforeSegment = irfile->BufferedRandomAccessFile::getFilePointer();
		uinttype preSegmentLocation = irfile->readInt();
		#else
		/* uinttype preSegmentLocation = */ irfile->readInt();
		#endif

		#ifdef DEEP_DEBUG
		if (preSegmentLocation != beforeSegment) {
			DEEP_LOG(ERROR, OTHER, "Invalid metadata: mismatch bracketing = %d / pos = %d, xfrag = %d, %s\n", preSegmentLocation, beforeSegment, *xfrag, map->getFilePath());

			throw InvalidException("Invalid metadata: mismatch bracketing");
		}
		#endif

		if (keyCompression == true) {
			irfile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
			irfile->BufferedRandomAccessFile::seek(lastSeek + OFFSET);
		}

		// XXX: skip first entry (XXX: second time around should be read)
		// XXX: first infos here are file refs for summaries (already read) last infos are file refs for all other segments
		inttype counter = 0;
		if (*xfrag == 0) {
			counter = 1;

			#ifdef DEEP_DEBUG
			K key = (K) Converter<K>::NULL_VALUE;

			RealTimeProtocol<V,K>::readPagingInfo(irfile, map, &flags, null /* eof */, &key);
			K firstKey = segment->SegTreeMap::firstKey();

			if (map->m_comparator->compare(firstKey, key) != 0) {
				DEEP_LOG(ERROR, OTHER, "Invalid key paging: first keys don't match");

				throw InvalidException("Invalid key paging: first keys don't match");
			}
			Converter<K>::destroy(key);
			#else
			RealTimeProtocol<V,K>::readPagingInfo(irfile, map, &flags);	
			#endif
		}

		for (; (counter < physicalSize); counter++) {
			Information* info = RealTimeProtocol<V,K>::readPagingInfo(irfile, map, segment, ctxt, &flags, &sequential, *xfrag, modification, compression);

			if ((info == null) && (flags != IRT_FLAG_REF_IRT) && (flags != IRT_FLAG_REF_VRT)) {
				++counter;
				break;
			}
		}

		if (keyCompression == true) {
			irfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
		}

		#ifdef DEEP_DEBUG
		if (counter != physicalSize) {
			DEEP_LOG(ERROR, OTHER, "Invalid metadata: mismatch elements = %d / actual = %d, xfrag = %d, %s\n", physicalSize, counter, *xfrag, map->getFilePath());

			throw InvalidException("Invalid metadata: mismatch elements");
		}
		#endif

		if ((pagingPosition != 0) && (segment->getSummary() == false)) {
			BufferedRandomAccessFile* file = map->m_share.getIrtReadFileList()->get(pagingIndex);
			if (file != irfile) {
				map->m_share.release(irfile);

				irfile = file;

				map->m_share.acquire(irfile);

				DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");
			}

			lastSeek = pagingPosition - OFFSET;
			irfile->BufferedRandomAccessFile::seek(pagingPosition - OFFSET);

			sequential = false;
			(*xfrag)++;

			goto RETRY;
		}

		// XXX: these are for protocol only, non-flag methods are used in memory
		segment->setStreamRangeOverflowFlag(false);
		segment->setStreamedFlag(false);

		map->m_share.release(irfile);
	}

	FORCE_INLINE static void writeHeader(RandomAccessFile* iwfile, ubytetype flags, ushorttype index, uinttype length, uinttype size) {
		return RealTimeProtocol_v1_0_0_0<V,K>::writeHeader(iwfile, flags, index, length, size);
	}

	FORCE_INLINE static void writeStreamInformation(RandomAccessFile* iwfile, RealTimeMap<K>* map, Segment<K>* segment, boolean checkpoint, ubytetype streamRefCount, uinttype streamRefLocation) {
		#ifdef DEEP_DEBUG
		if ((map->m_state != RealTimeMap<K>::MAP_RECOVER) && (segment->getSummary() == false) && (checkpoint == false) && (segment->getStreamRangeOverflowFlag() == true)) {
			DEEP_LOG(ERROR, OTHER, "Invalid meta data: stream overflow, %s\n", map->getFilePath());

			throw InvalidException("Invalid meta data: stream overflow");
		}
		#endif

		if (segment->getStreamRangeOverflowFlag() == true) {
			iwfile->writeByte(streamRefCount);
			iwfile->writeInt(streamRefLocation);

		} else if (segment->getStreamed() == true) {
			#ifdef DEEP_DEBUG
			iwfile->writeByte(segment->getStreamRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */));
			#else
			iwfile->writeByte(segment->getStreamRange());
			#endif
			iwfile->writeInt(segment->getCurrentStreamIndex());

		} else {
			iwfile->writeByte(0);
			iwfile->writeInt(0);
		}

		// XXX: reset state flag, will be set again in writeKeyPaging if stream refs overflow
		segment->setStreamRangeOverflowFlag(false);
		segment->setStreamedFlag(false);
	}

	FORCE_INLINE static void writePagingIndex(RandomAccessFile* iwfile, ubytetype range, ushorttype index, uinttype reference) {
		return RealTimeProtocol_v1_0_0_0<V,K>::writePagingIndex(iwfile, range, index, reference);
	}

	FORCE_INLINE static void seedMetaData(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, Segment<K>* segment) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		RealTimeProtocol<V,K>::writeHeader(iwfile, segment->getStateFlags(), map->m_endwiseLrtLocality.getIndex(), map->m_endwiseLrtLocality.getLength(), segment->vsize());

		RealTimeProtocol<V,K>::writeKeyCardinality(iwfile, map, segment);

		iwfile->writeByte(0); // stream ref count
		iwfile->writeInt(0); // stream ref location
		RealTimeProtocol<V,K>::writePagingIndex(iwfile, 0, 0, 0);

		iwfile->writeShort(segment->SegTreeMap::size() /* temporary physical size (see written actual) */);
	}

	FORCE_INLINE static void writeMetaData(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, Segment<K>* segment, uinttype cBefore, longtype pBefore, boolean rebuild, uinttype actual, ubytetype streamRefCount, uinttype streamRefLocation, uinttype streamPosition, boolean checkpoint, boolean keyCompression) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		const inttype cAfter = iwfile->getCursor();
		const longtype pAfter = iwfile->getPosition();

		if (iwfile->flushed() == false) {
			iwfile->setCursor(cBefore);
			iwfile->setPosition(pBefore);

		} else {
			iwfile->flush();
			iwfile->BufferedRandomAccessFile::seek(pBefore);
		}

		// XXX: write out this flag as part of the header
		if (segment->getStreamed() == true) {
			segment->setStreamedFlag(true);
		}

		RealTimeProtocol<V,K>::writeHeader(iwfile, segment->getStateFlags(), map->m_endwiseLrtLocality.getIndex(), map->m_endwiseLrtLocality.getLength(), segment->vsize());

		RealTimeProtocol<V,K>::writeKeyCardinality(iwfile, map, segment);

		RealTimeProtocol<V,K>::writeStreamInformation(iwfile, map, segment, checkpoint, streamRefCount, streamRefLocation);
		
		if ((rebuild == true) && (segment->getSummary() == false)) {
			segment->resetFragmentCount();
			segment->resetPagingIndexes();

			RealTimeProtocol<V,K>::writePagingIndex(iwfile, 0, 0, 0);

		} else {
			ushorttype index = 0;
			ubytetype range = 0;

			if (segment->getSummary() == true) {
				index = segment->getBackReference();
			} else {
				index = segment->getCurrentPagingIndex();
				#ifdef DEEP_DEBUG	
				range = segment->getPagingRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */);
				#else
				range = segment->getPagingRange();
				#endif

				// XXX: include incoming file index within index range
				if ((segment->getPagingPosition() != 0) && (index != iwfile->getFileIndex())) {
					// XXX: paging file indexes increase until wrapping
					inttype diff = iwfile->getFileIndex() - index;
					range <<= diff;
					range |= (1 << (diff-1));
				}
			}

			RealTimeProtocol<V,K>::writePagingIndex(iwfile, range, index, segment->getPagingPosition() /* XXX: back reference */);
		}

		iwfile->writeShort(actual /* physical size */);
		iwfile->writeByte(keyCompression);
		iwfile->writeInt(streamPosition);

		if (iwfile->flushed() == false) {
			iwfile->setCursor(cAfter);
			iwfile->setPosition(pAfter);

		} else {
			iwfile->flush();
			iwfile->BufferedRandomAccessFile::seek(iwfile->length());
		}
	}

	FORCE_INLINE static boolean keyWriteAccess(StoryLine& storyLine, Information* info, uinttype viewpoint, boolean rebuild, boolean* dirty) {
		return RealTimeProtocol_v1_0_0_0<V,K>::keyWriteAccess(storyLine, info, viewpoint, rebuild, dirty);
	}

	FORCE_INLINE static boolean keySpaceIndex(RealTimeMap<K>* map, Segment<K>* segment, MeasuredRandomAccessFile* iwfile, Information* info, inttype index, inttype* actual, boolean* skip, boolean rebuild, uinttype viewpoint) {
		boolean writable = true;

		if (info->getIndexed(map->m_indexValue) == false) {
			// XXX: if checkpointing and rebuild is false, this checkpoint is not backwards, any written info is effectively indexed
			if ((viewpoint == 0) || (rebuild == false)) {
				info->setIndexed(map->m_indexValue, true);
			}

		// XXX: write first key to "seed" every segment index block
		} else if (rebuild == false) {

			if (viewpoint == 0) {
				// XXX: though only flush if something else has changed
				if (index == 0) {
					(*skip) = true;
					--(*actual);

				} else {
					writable = false;
				}
			}
		}

		if (viewpoint == 0) {
			// XXX: statistics use for cache and fragmentation calculation
			map->m_activeValueSize += info->getSize();
			map->m_activeValueCount++;
		}

		return writable;
	}

	FORCE_INLINE static boolean keySpacePinning(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* segment, SegMapEntry* infoEntry, Information* info, Information* indexInfo, uinttype viewpoint, const boolean rolled = false) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		boolean rollover = false;

		if (map->m_primaryIndex != null) {
			return rollover;

		} else if ((info->getNext() != null) && (viewpoint == 0)) {
			return rollover;

		} else if ((info->getLevel() != Information::LEVEL_COMMIT) || ((info->getDeleting() == true) && (viewpoint == 0))) {
			return rollover;

		} else {
			// XXX: normal forward indexing, segment marked as normal
			if (viewpoint == 0) {
				if (segment->hasStreamIndex(info->getFileIndex()) == false) {
					rollover = map->markSegment(ctxt, segment, info->getFileIndex());
				}
			// XXX: if checkpointing and info equivalent to indexing info, mark as normal. Otherwise mark as old stream and do not roll.
			} else {
				if (indexInfo == null) {
					indexInfo = map->isolateInterspace(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT, infoEntry->getKey(), null);
				}
				if ((info != indexInfo) || (info->getNext() != null) || (info->getDeleting() == true)) {
					if (segment->hasStreamIndex(info->getFileIndex()) == false) {
						// XXX: note that 'old' here may be replaced with new as we continue indexing
						segment->addStreamIndex(info->getFileIndex(), true /* old */);
					}
					rollover = false;
				} else if (rolled == true) {
					if (segment->hasStreamIndex(info->getFileIndex()) == false) {
						segment->addStreamIndex(info->getFileIndex(), false /* new */);
					}
					rollover = false;
				} else {
					// XXX: if stream exists as 'old', we need to override it here as new
					const StreamReference* sr = segment->getStreamReference(info->getFileIndex());
					if ((sr == null) || (sr->m_old == true)) {
						if (sr != null) {
							segment->removeStreamReference(info->getFileIndex());
						}
						rollover = map->markSegment(ctxt, segment, info->getFileIndex());
					}
				}
			}
		}

		return rollover;
	}

	FORCE_INLINE static boolean summaryKeyPaging(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, RealTime::Locality* pagingPosition) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		struct Util {
			FORCE_INLINE static boolean validateFileRefs(MapFileSet<MeasuredRandomAccessFile>* files, TreeSet<ushorttype>* indexes, ushorttype& missing) {
				Iterator<ushorttype>* iter = indexes->iterator();

				while (iter->hasNext()) {
					ushorttype i = iter->next();
					if (files->containsFileAtIndex(i) == false) {
						missing = (ushorttype)i;
						return false;
					}
				}
				delete iter;
				return true;
			}
		};

		RealTimeProtocol<V,K>::readMetaHeader(irfile, map, segment, false);

		if (segment->getSummary() == false) {
			return false;
		}

		uinttype physicalSize = 0;
		ubytetype streamRefCount = 0;
		uinttype streamRefLocation = 0;
		boolean keyCompression = false;
		RealTimeProtocol<V,K>::readMetaFooter(irfile, map, segment, false, true, &streamRefCount, &streamRefLocation, &physicalSize, &keyCompression);

		*pagingPosition = segment->getPagingPosition() == 0 ? RealTime::Locality::LOCALITY_NONE : RealTime::Locality(segment->getBackReference(), segment->getPagingPosition(), RealTime::Locality::VIEWPOINT_NONE);
		
		segment->addPagingIndex(irfile->getFileIndex());
		segment->setPagingPosition(irfile->BufferedRandomAccessFile::getFilePointer());

		// XXX: empty summary
		if (segment->vsize() == 0) {
			return true;
		}

		if (keyCompression == true) {
			try {
				irfile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
				irfile->seek(segment->getPagingPosition());
			} catch(...) {
				irfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
				return false;
			}
		}

		bytetype flags = 0;
		boolean sequential = false;
		uinttype i=0;
		do {
			RealTimeProtocol<V,K>::readPagingInfo(irfile, map, segment, null /* ctxt */, &flags, &sequential, 0 /* frag */, false /* modification */, false /* compression */);
			++i;
		} while ((i < physicalSize) && ((flags == IRT_FLAG_REF_IRT) || (flags == IRT_FLAG_REF_VRT)));

		if (keyCompression == true) {
			irfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
		}

		// XXX: these are for protocol only, non-flag methods are used in memory
		segment->setStreamRangeOverflowFlag(false);
		segment->setStreamedFlag(false);

		if ((i >= physicalSize) && ((flags == IRT_FLAG_REF_IRT) || (flags == IRT_FLAG_REF_VRT))) {
			// XXX: malformed summary segment: no segment refs, only file refs
			return false;
		}

		// XXX: validate file references
		RealTime* pri = map->m_primaryIndex == null ? map : map->m_primaryIndex;
		ushorttype missing = 0;
		if ((segment->getPaged() == true) && (Util::validateFileRefs(map->getShare()->getIrtWriteFileList(), ((TreeSet<ushorttype>*)segment->getPagingIndexes()), missing) == false)) {
			DEEP_LOG(WARN, RCVRY, "invalid summary paging file reference: %u, %s\n", missing, map->getFilePath());
			if (Properties::getDebugEnabled(Properties::FILE_REF_CHECKS) == true) {
				return false;
			}
		}

		TreeSet<ushorttype> streamIndexes(&(Segment<K>::USHORT_CMP));
		segment->getStreamIndexes(true /* get old */, &streamIndexes);
		if ((segment->getStreamed() == true) && (Util::validateFileRefs(pri->getShare()->getLrtWriteFileList(), &streamIndexes, missing) == false)) {
			DEEP_LOG(WARN, RCVRY, "invalid summary stream file reference: %u, %s\n", missing, map->getFilePath());
			if (Properties::getDebugEnabled(Properties::FILE_REF_CHECKS) == true) {
				return false;
			}
		}

		map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(segment->SegTreeMap::firstKey(), segment);
		map->m_entrySize.addAndGet(segment->vsize() /* i.e. total size of segment including back-references */);
		// XXX: account for summary purged segments (actual active vs actual purged)
		map->m_purgeSize.incrementAndGet();

		return true;
	}

	FORCE_INLINE static void readPagingInfo(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, bytetype* flags, boolean* eof = null, K* retkey = null, ushorttype* index_p = null, uinttype* position_p = null) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		*flags = irfile->readByte(eof);
		const boolean hasCompressedOffset = (*flags & IRT_MFLAG_VCOMPRESS) != 0;
		if (hasCompressedOffset == true) {
			*flags &= ~IRT_MFLAG_VCOMPRESS;
		}

		ushorttype index = irfile->readShort(eof);
		uinttype position = irfile->readInt(eof);

		if (index_p != null) {
			*index_p = index;
		}
		if (position_p != null) {
			*position_p = position;
		}

		if (map->m_share.getValueSize() == -1) {
			/* inttype vsize = */ irfile->readInt(eof);
		}

		if (hasCompressedOffset == true) {
			/* uinttype compressedOffset = */ irfile->readInt(eof);
		}

		if (retkey == null) {
			if (*flags == IRT_FLAG_CONTENT) {
				KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol(), eof);

			} else if (*flags == IRT_FLAG_DELETED) {
				KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol(), eof);

			} else if (*flags == IRT_FLAG_CLOSURE) {
				KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol(), eof);

			} else if ((*flags == IRT_FLAG_REF_IRT) || (*flags == IRT_FLAG_REF_VRT)) { 
				KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol(), eof);

			}

		} else {
			if (*flags == IRT_FLAG_CONTENT) {
				*retkey = KeyProtocol_v1<K>::readKey(irfile, map->m_share.getKeyProtocol(), eof);

			} else if (*flags == IRT_FLAG_DELETED) {
				*retkey = KeyProtocol_v1<K>::readKey(irfile, map->m_share.getKeyProtocol(), eof);

			} else if (*flags == IRT_FLAG_CLOSURE) {
				*retkey = KeyProtocol_v1<K>::readKey(irfile, map->m_share.getKeyProtocol(), eof);

			} else if ((*flags == IRT_FLAG_REF_IRT) || (*flags == IRT_FLAG_REF_VRT)) { 
				*retkey = KeyProtocol_v1<K>::readKey(irfile, map->m_share.getKeyProtocol(), eof);

			}
		}
	}

	FORCE_INLINE static Information* readPagingInfo(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt, bytetype* flags, boolean* sequential, inttype frag, boolean modification = false, boolean compression = false) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		#ifdef DEEP_DEBUG
		if ((modification == true) && (compression == true)) {
			DEEP_LOG(ERROR, OTHER, "Invalid index read: compression with modification");

			throw InvalidException("Invalid index read: compression with modification");
		}
		#endif

		Information* info = null;

		*flags = irfile->readByte();
		const boolean hasCompressedOffset = (*flags & IRT_MFLAG_VCOMPRESS) != 0;
		if (hasCompressedOffset == true) {
			*flags &= ~IRT_MFLAG_VCOMPRESS;
		}

		ushorttype index = irfile->readShort();
		uinttype position = irfile->readInt();

		inttype vsize;
		if (map->m_share.getValueSize() == -1) {
			vsize = irfile->readInt();

		} else {
			vsize = map->m_share.getValueSize();
		}

		uinttype compressedOffset;
		if (hasCompressedOffset == true) {
			compressedOffset = irfile->readInt();

		} else {
			compressedOffset = Information::OFFSET_NONE;
		}

		if (*flags == IRT_FLAG_CONTENT) {
			K key = KeyProtocol_v1<K>::readKey(irfile, map->m_share.getKeyProtocol());

			if (hasCompressedOffset == false) {

				if (compression == true) {
					info = Information::newInfo(Information::CMPRS, index, position, vsize);
					info->resetIndexed(0xffffffffffffffff);

				} else if (modification == true) {
					info = Information::newInfo(Information::WRITE, index, position, vsize);
					info->resetIndexed(0xffffffffffffffff);

				} else {
					#if 1
					info = Information::newInfo(Information::READ, index, position, vsize);
					#else
					info = Information::newInfo(Information::WRITE, index, position, vsize);
					info->resetIndexed(0xffffffffffffffff);
					#endif
				}

			} else {
				info = Information::newInfo(Information::CMPRS, index, position, vsize);
				info->setCompressedOffset(compressedOffset);
				info->resetIndexed(0xffffffffffffffff);
			}

			if (*sequential == false) {
				K lastKey = (K) Converter<K>::NULL_VALUE;
				Information* oldinfo = segment->SegTreeMap::add(key, info, &lastKey, sequential, false);

				// XXX: reset last key and information on duplicate key (i.e. index-referencing logic)
				if (oldinfo != null) {
					#ifdef DEEP_DEBUG
					if ((ctxt != null) && (map->informationIsOld(oldinfo, info, key, ctxt, true) == true)) {
						DEEP_LOG(ERROR, OTHER, "Invalid info paging: latest info is older than previous");

						throw InvalidException("Invalid info paging: latest info is older than previous");
					}
					#endif

					Converter<K>::destroy(key);
					Converter<Information*>::destroy(info);
					info = oldinfo;
				}

			} else {
				segment->SegTreeMap::add(key, info);
			}

		} else if (*flags == IRT_FLAG_DELETED) {
			K key = KeyProtocol_v1<K>::readKey(irfile, map->m_share.getKeyProtocol());

			if (hasCompressedOffset == true) {
				info = Information::newInfo(Information::CMPRS, index, position, vsize);
				info->setCompressedOffset(compressedOffset);
				info->resetIndexed(0xffffffffffffffff);

			} else {
				info = Information::newInfo(Information::WRITE, index, position, vsize);
				info->resetIndexed(0xffffffffffffffff);
			}

			info->setDeleting(true);

			K lastKey = (K) Converter<K>::NULL_VALUE;
			Information* oldinfo = segment->SegTreeMap::add(key, info, &lastKey, sequential, false);

			// XXX: reset last key and information on duplicate key (i.e. index-referencing logic)
			if (oldinfo != null) {
				Converter<K>::destroy(key);
				Converter<Information*>::destroy(info);

				info = oldinfo;
			}
		} else if ((*flags == IRT_FLAG_REF_IRT) || (*flags == IRT_FLAG_REF_VRT)) {
			KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol());

			///XXX: all references are written per delta, ignore all but final delta
			if (frag == 0) {

				// XXX: note that stream references are 'fuzzy' and segment references may not always match cumulative info refs
				typedef NumberRangeSet<ushorttype, uinttype> FileRangeSet;
				FileRangeSet fileRefs(index, position);
				
				TreeSet<ushorttype>* indexSet = fileRefs.toSet();
				Iterator<ushorttype>* iter = indexSet->iterator();

				while (iter->hasNext()) {
					ushorttype index = iter->next();
					if ((*flags == IRT_FLAG_REF_IRT) && (segment->hasPagingIndex(index) == false)) {
						segment->addPagingIndex(index);
						segment->setPaged(true);
					} else if ((*flags == IRT_FLAG_REF_VRT) && (segment->hasStreamIndex(index) == false)) {
						segment->addStreamIndex(index);
					}
				}

				delete iter;
				delete indexSet;
			}

		/* XXX: nothing to do
		} else if (*flags == IRT_FLAG_CLOSURE) {

			KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol());
		*/
		}

		return info;
	}

	FORCE_INLINE static void readFileRefs(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, ubytetype streamRefCount, uinttype streamRefLocation, boolean seek, boolean keyCompression) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		if (streamRefCount > 0) {
			bytetype flags = 0;

			if (seek == true) {
				if (keyCompression == true) {
					// XXX: note the first key has already been read
					for (uinttype counter = 0; (counter < (streamRefLocation - 1)); counter++) {
						RealTimeProtocol<V,K>::readPagingInfo(irfile, map, &flags);
					}
				} else {
					irfile->BufferedRandomAccessFile::seek(streamRefLocation);
				}
			}

			boolean sequential = 0;

			for (ubytetype counter = 0; (counter < streamRefCount); counter++) {
				#ifdef DEEP_DEBUG
				Information* info = RealTimeProtocol<V,K>::readPagingInfo(irfile, map, segment, null /* ctxt */, &flags, &sequential, 0 /* frag */);
				if ((info != null) || ((flags != IRT_FLAG_REF_IRT) && (flags != IRT_FLAG_REF_VRT))) {
					DEEP_LOG(ERROR, OTHER, "Invalid file refs: was not valid reference, %s\n", map->getFilePath());

					throw InvalidException("Invalid file refs: was not valid reference");
				}
				#else
				RealTimeProtocol<V,K>::readPagingInfo(irfile, map, segment, null /* ctxt */, &flags, &sequential, 0 /* frag */);
				#endif
			}
		}
	}

	FORCE_INLINE static void readMetaHeader(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, boolean seek) {
		return RealTimeProtocol_v1_0_0_0<V,K>::readMetaHeader(irfile, map, segment, seek);
	}

	FORCE_INLINE static void readMetaFooter(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, boolean seek, boolean setPosition, ubytetype* streamRefCount, uinttype* streamRefLocation, uinttype* physicalSize, boolean* keyCompression) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		#if 0
		static const inttype OFFSET = 1 + 2 + 1 + 2 + 4 + 2 + 4 + 4;

		if (seek == true) {
			irfile->BufferedRandomAccessFile::seek(segment->getPagingPosition() - OFFSET);
		}
		#endif

		if (segment->getStreamRangeOverflowFlag() == true) {
			(*streamRefCount) = irfile->readByte();
			(*streamRefLocation) = irfile->readInt();

		} else {
			ubytetype vrtFileRange = irfile->readByte();
			uinttype vrtFileIndex = irfile->readInt();
		
			if (segment->getStreamedFlag() == true) {
				segment->addStreamIndex(vrtFileIndex);
				segment->setStreamRange(vrtFileRange);
			}
		}

		ubytetype irtFileRange = irfile->readByte();

		// XXX: see readPagingInfo for summary range 
		if (segment->getSummary() == false) {
			segment->setPagingRange(irtFileRange);
		}

		/* XXX: don't override index, this is a back index */
		ushorttype irtFileIndex = irfile->readShort();
		uinttype pagingPosition = irfile->readInt();
		(*physicalSize) = irfile->readShort();
		if (setPosition == true) {
			segment->setBackReference(irtFileIndex);
			segment->setPagingPosition(pagingPosition);
		}

		(*keyCompression) = irfile->readByte();
		// TODO: use flags to indicate if segment needs compress locality (i.e. smaller disk size)
		uinttype streamPosition = irfile->readInt();
		if (segment->getSummary() == false) {
			segment->setStreamPosition(streamPosition);

		} else {
			segment->setRecoveryEpoch(streamPosition);
		}
		/* uinttype preSegmentLocation = */ irfile->readInt();
	}

	FORCE_INLINE static void readInfoValue(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* segment) {

		inttype xfrag = 0;

		nbyte tmpValue((const bytearray) null, 0);

		RealTimeShare* share = (map->m_primaryIndex == null) ? map->getShare() : map->m_primaryIndex->getShare();

		share->getVrtReadFileList()->lock();
		{
			// XXX: first, read anything in a compressed block
			if (segment->getValuesCompressed() == true) {
				segment->SegTreeMap::entrySet(&map->m_fillInformationSet);
				InformationIterator<K> infoIter((MapInformationEntrySetIterator*) map->m_fillInformationSet.iterator());

				const SegMapEntry* infoEntry = infoIter.next();
				inttype fileIndex = -1;

				// XXX: all compressed values will be in the same file as the first non-deleted compressed value
				while (infoEntry != null) {
					Information* cmpinfo = map->isolateCompressed(ctxt, infoEntry->getValue());
					if ((cmpinfo != null) && (cmpinfo->getDeleting() == false)) {
						fileIndex = cmpinfo->getFileIndex();
						break;
					}
					
					infoEntry = infoIter.next();
				}

				boolean stillCompressed = false;

				if ((fileIndex != -1) && (infoEntry != null)) {
					BufferedRandomAccessFile* vrfile = share->getVrtReadFileList()->get(fileIndex);
					if (vrfile == null) {
						DEEP_LOG(ERROR, OTHER, "Invalid file access: %d, %s\n", fileIndex, map->getFilePath());

						throw InvalidException("Invalid file access");
					}

					DEEP_VERSION_ASSERT_EQUAL(V,vrfile->getProtocol(),"File Protocol Version Mismatch");
					share->acquire(vrfile);
					{
						vrfile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
						vrfile->BufferedRandomAccessFile::seek(segment->getStreamPosition());

						uinttype position = 0;

						while (infoEntry != null) {
							Information* info = map->isolateCompressed(ctxt, infoEntry->getValue());
							infoEntry = infoIter.next();

							// XXX: root is new since roll, or we've moved past it
							if ((info == null) || (info->getDeleting() == true)) {
								continue;
							}

							// XXX: not part of this compressed block
							if (info->getFilePosition() != segment->getStreamPosition()) {
								continue;
							}

							stillCompressed = true;
							position = vrfile->BufferedRandomAccessFile::getCursor();

							if (info->getCompressed() == true) {
								const uinttype offset = info->getCompressedOffset();

								if (offset < position) {
									DEEP_LOG(ERROR, OTHER, "Invalid value compression: value misalignment, %s\n", map->getFilePath());

									throw InvalidException("Invalid value compression: value misalignment");
								}

								if ((offset - position) > 0) {
									vrfile->BufferedRandomAccessFile::skipBytes(offset - position);
								}
							}

							if (info->getData() != null) {
								continue;
							}

							#ifdef DEEP_DEBUG
							if (info->getLevel() < Information::LEVEL_COMMIT) {
								DEEP_LOG(ERROR, OTHER, "Invalid value compression: storyline level not correct, %s\n", map->getFilePath());

								throw InvalidException("Invalid value compression: storyline level not correct");
							}
							#endif

							#ifdef DEEP_VALIDATE_DATA
							uinttype crc1 = vrfile->BufferedRandomAccessFile::readInt();
							#endif

							if (fileIndex != info->getFileIndex()) {
								DEEP_LOG(ERROR, OTHER, "Invalid value compression: crossed file boundary, %s\n", map->getFilePath());

								throw InvalidException("Invalid value compression: crossed file boundary\n");
							}

							info->initData();
							tmpValue.reassign((const bytearray) info->getData(), info->getSize());
							vrfile->BufferedRandomAccessFile::read(&tmpValue, 0, info->getSize());

							#ifdef DEEP_VALIDATE_DATA
							uinttype crc2 = RealTimeValidate::simple(tmpValue, info->getSize());
							if (crc1 != crc2) {
								DEEP_LOG(ERROR, OTHER, "Invalid crc values (compressed): mismatch %u / %u, %s\n", crc1, crc2, map->getFilePath());

								throw InvalidException("Invalid crc values (compressed): mismatch\n");
							}
							#endif
						}

						#if 0
						#ifdef DEEP_DEBUG	
						if (position != vrfile->getLastUncompressedBlockLength()) {
							DEEP_LOG(ERROR, OTHER, "Invalid value compression: position %d not equal to block length %d, %s\n", position, vrfile->getLastUncompressedBlockLength(), map->getFilePath());

							throw InvalidException("Invalid value compression: position not equal to block length");
						}
						#endif
						#endif

						vrfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
						vrfile->BufferedRandomAccessFile::setPosition(0);
						vrfile->BufferedRandomAccessFile::setCursor(0);
					}
					share->release(vrfile);
				}

				// XXX: Everything has been deleted/updated, mark segment as no longer compressed
				if (stillCompressed == false) {
					segment->setStreamPosition(0);
					segment->setBeenAltered(true);
				}
			}

			segment->SegTreeMap::entrySet(&map->m_fillInformationSet);
			InformationIterator<K> infoIter((MapInformationEntrySetIterator*) map->m_fillInformationSet.iterator());

			const SegMapEntry* infoEntry = infoIter.next();
			inttype fileIndex = -1;
			BufferedRandomAccessFile* vrfile = null;

			while (infoEntry != null) {
				Information* info = map->isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);
				infoEntry = infoIter.next();

				if ((info->getData() != null) || (info->getDeleting() == true) /* || (info->getLevel() != Information::LEVEL_COMMIT) */) {
					continue;
				}

				if (fileIndex != info->getFileIndex()) {
					if (vrfile != null) {
						vrfile->BufferedRandomAccessFile::setPosition(0);
						vrfile->BufferedRandomAccessFile::setCursor(0);
						share->release(vrfile);
					}

					fileIndex = info->getFileIndex();
					vrfile = share->getVrtReadFileList()->get(fileIndex);
					if (vrfile == null) {
						DEEP_LOG(ERROR, OTHER, "Invalid file access: %d, %s\n", fileIndex, map->getFilePath());

						throw InvalidException("Invalid file access");
					}

					DEEP_VERSION_ASSERT_EQUAL(V,vrfile->getProtocol(),"File Protocol Version Mismatch");
					share->acquire(vrfile);
				}

				info->initData();
				tmpValue.reassign((const bytearray) info->getData(), info->getSize());

				#ifdef DEEP_VALIDATE_DATA
				uinttype crc1 = 0;
				#endif

				if (info->getCompressed() == false) {
					vrfile->RandomAccessFile::seek(info->getFilePosition());

					#ifdef DEEP_VALIDATE_DATA
					crc1 = vrfile->RandomAccessFile::readIntRaw();
					#endif

					vrfile->RandomAccessFile::readFullyRaw(&tmpValue, 0, info->getSize());

				} else {
					vrfile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);

					vrfile->BufferedRandomAccessFile::seek(info->getFilePosition());
					vrfile->skipBytes(info->getCompressedOffset());

					#ifdef DEEP_VALIDATE_DATA
					crc1 = vrfile->BufferedRandomAccessFile::readInt();
					#endif

					vrfile->BufferedRandomAccessFile::read(&tmpValue);

					vrfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
				}

				#ifdef DEEP_VALIDATE_DATA
				uinttype crc2 = RealTimeValidate::simple(tmpValue, info->getSize());
				if (crc1 != crc2) {
					DEEP_LOG(ERROR, OTHER, "Invalid crc values: mismatch %u / %u, %s\n", crc1, crc2, map->getFilePath());

					throw InvalidException("Invalid crc values: mismatch\n");
				}
				#endif

				if ((infoEntry == null) || (infoEntry->getValue() == null)) {
					continue;
				}

				if ((infoEntry->getValue()->getDeleting()) || ((info->getFilePosition() + info->getSize()) != infoEntry->getValue()->getFilePosition())) {
					xfrag++;
				}
			}

			if (vrfile != null) {
				vrfile->BufferedRandomAccessFile::setPosition(0);
				vrfile->BufferedRandomAccessFile::setCursor(0);
				share->release(vrfile);
			}
		}
		share->getVrtReadFileList()->unlock();

		RealTimeAdaptive_v1<K>::readValueFragmentation(map, segment, xfrag);
	}

	FORCE_INLINE static void readInfoValue(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Information* info, nbyte* value) {
		if (info->getCompressed() == false) {
			return RealTimeProtocol_v1_0_0_0<V,K>::readInfoValue(map, ctxt, info, value);

		} else {
			RealTimeShare* share = (map->m_primaryIndex == null) ? map->getShare() : map->m_primaryIndex->getShare();

			share->getVrtReadFileList()->lock();
			{
				BufferedRandomAccessFile* vrfile = (BufferedRandomAccessFile*) share->getVrtReadFileList()->get(info->getFileIndex());
				if (vrfile == null) {
					DEEP_LOG(ERROR, OTHER, "Invalid file access: %d, %s\n", info->getFileIndex(), map->getFilePath());

					throw InvalidException("Invalid file access");
				}

				DEEP_VERSION_ASSERT_EQUAL(V,vrfile->getProtocol(),"File Protocol Version Mismatch");
				share->acquire(vrfile);
				{
					vrfile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);

					vrfile->BufferedRandomAccessFile::seek(info->getFilePosition());
					vrfile->skipBytes(info->getCompressedOffset());

					#ifdef DEEP_VALIDATE_DATA
					uinttype crc1 = vrfile->BufferedRandomAccessFile::readInt();
					#endif

					vrfile->BufferedRandomAccessFile::read(value, 0, info->getSize());

					#ifdef DEEP_VALIDATE_DATA
					uinttype crc2 = RealTimeValidate::simple(*value, info->getSize());
					if (crc1 != crc2) {
						DEEP_LOG(ERROR, OTHER, "Invalid crc values (recovery/compressed): mismatch %u / %u, %s\n", crc1, crc2, map->getFilePath());

						throw InvalidException("Invalid crc values (recovery/compressed): mismatch\n");
					}
					#endif

					vrfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
				}
				share->release(vrfile);
			}
			share->getVrtReadFileList()->unlock();
		}
	}

	FORCE_INLINE static ushorttype nextRealTime(MeasuredRandomAccessFile*& lwfile, MeasuredRandomAccessFile*& vwfile, RealTimeConductor<K>* conductor, RealTimeMap<K>* map, boolean durable) {
		return RealTimeProtocol_v1_0_0_0<V,K>::nextRealTime(lwfile, vwfile, conductor, map, durable);
	}

	// XXX: Mark that the LRT is "closed" (good up to this point), even if the last entry is not on a transaction boundary
	FORCE_INLINE static void terminateStreaming(MeasuredRandomAccessFile* lwfile, MeasuredRandomAccessFile* vwfile, RealTimeMap<K>* map) {
		Information* info = Information::nullInfo(map->m_share.getValueSize());
		{
			nbyte tmpValue((const bytearray) null, 0);
			tmpValue.reassign((const bytearray) info->getData(), info->getSize());

			info->setFileIndex(vwfile->getFileIndex());
			info->setFilePosition(vwfile->BufferedRandomAccessFile::getFilePointer());

			#ifdef DEEP_VALIDATE_DATA
			uinttype crc = RealTimeValidate::simple(tmpValue, tmpValue.length);
			vwfile->MeasuredRandomAccessFile::writeInt(crc);
			#endif

			vwfile->MeasuredRandomAccessFile::write(&tmpValue, 0, tmpValue.length);
		}

		lwfile->writeByte(LRT_FLAG_CLOSURE);

		// TODO: fix versioning here
		if (lwfile->getProtocol() < CT_DATASTORE_PROTO_VER_1_3) {
			if (map->m_share.getValueSize() == -1) {
				lwfile->writeInt(info->getFilePosition());
				lwfile->writeInt(info->getSize());

			} else if ((map->m_share.getKeyProtocol() == -1) || (map->m_share.getLarge() == true)) {
				lwfile->writeInt(info->getFilePosition());
			}

		} else {
			lwfile->writeInt(info->getFilePosition());
			if (map->m_share.getValueSize() == -1) {
				lwfile->writeInt(info->getSize());
			}
		}

		if (map->m_rowStoreMode == false) {
			K key = (K) Converter<K>::NULL_VALUE;
			KeyProtocol_v1<K>::writeKey(key, lwfile, map->m_share.getKeyProtocol());
		}

		Converter<Information*>::destroy(info);
	}

	FORCE_INLINE static void writeLrtEntry(const RealTimeMap<K>* map, MeasuredRandomAccessFile* lwfile, const inttype i, const K key, Information* info, boolean& first, boolean& next, const boolean marking, const boolean cursor, const boolean purge, const boolean rolling, const boolean compressing, const ulongtype transactionId) {
		ubytetype stateFlags = 0;
		{
			if (first == true) {
				first = false;
				stateFlags |= LRT_FLAG_OPENING;

				if (compressing == true) {
					stateFlags |= LRT_FLAG_COMPRESS;
				}
			}

			if (info->getDeleting() == true) {
				stateFlags |= LRT_FLAG_DESTROY;
				#ifdef DEEBDB_RECOVERY_DEBUG
				/* XXX: add marker to indicate source file reference */
				recoveryIndex = info->getFileIndex();
				next = true;
				#endif
			}

			if ((next == false) && (cursor == false)) {
				stateFlags |= LRT_FLAG_CLOSING;
			}

			// XXX: 1.0.x change (candidate for 1.1 versioning: forward compatible)
			if (marking == true) {
				stateFlags |= LRT_FLAG_MARKING;
			}

			if (cursor == true) {
				if (purge == true) {
					info->freeData();
				}
			}

			if (rolling == true) {
				stateFlags |= LRT_FLAG_ROLLING;
			}

			if (transactionId != 0) {
				stateFlags |= LRT_FLAG_ACP;	
			}

			lwfile->writeByte(stateFlags);
		}

		if (transactionId != 0) {
			lwfile->writeLong(transactionId);
		}

		if (compressing == true) {
			if (stateFlags & LRT_FLAG_COMPRESS) {
				lwfile->writeInt(info->getFilePosition());
			}

			if (map->m_share.getValueSize() == -1) {
				lwfile->writeInt(info->getSize());
			}

		} else {
			//TODO: fix versioning here
			if (lwfile->getProtocol() < CT_DATASTORE_PROTO_VER_1_3) {	
				if (map->m_share.getValueSize() == -1) {
					lwfile->writeInt(info->getFilePosition());
					lwfile->writeInt(info->getSize());

				} else if ((map->m_share.getKeyProtocol() == -1) || (map->m_share.getLarge() == true)) {
					lwfile->writeInt(info->getFilePosition());
				}

			} else {
				lwfile->writeInt(info->getFilePosition());
				if (map->m_share.getValueSize() == -1) {
					lwfile->writeInt(info->getSize());
				}
			}
		}

		if (map->m_rowStoreMode == false) {
			KeyProtocol_v1<K>::writeKey(key, lwfile, map->m_share.getKeyProtocol());
		}
	}

	FORCE_INLINE static void commitRealTime(RealTimeMap<K>* map, RealTimeConductor<K>* conductor, boolean cursor, boolean purge) {
		Transaction* tx = conductor->getTransaction();
		const boolean rolling = tx->getRoll();
		const boolean compressRoll = (rolling == true) && (map->m_valueCompressMode == true);

		nbyte tmpValue((const bytearray) null, 0);
		// XXX: perform dynamic states consistently throughout the following
		const boolean durable = (Properties::getDurable() == true);
		boolean profile = Properties::getProfilingEnabled(Properties::COMMIT);

		#ifndef DEEP_SYNCHRONIZATION_GROUPING
		if (durable == true) {
			MeasuredRandomAccessFile::planSynchronizeGlobally();
		}
		#endif

		RETRY:
		ushorttype fileIndex = map->streamFileManagement(true, false);

		MeasuredRandomAccessFile* lwfile = map->m_share.getLrtWriteFileList()->get(fileIndex);
		MeasuredRandomAccessFile* vwfile = map->m_share.getVrtWriteFileList()->get(fileIndex);

		map->m_share.getVrtWriteFileList()->lock();
		{
			if (fileIndex != map->m_streamIndex) {
				map->m_share.getVrtWriteFileList()->unlock();
				goto RETRY;
			}
			tx->setStreamIndex(fileIndex);

			longtype start = 0;
			if (profile == true) {
				start = System::currentTimeMillis();
			}

			map->m_share.acquire(lwfile);
			map->m_share.acquire(vwfile);

			conductor->referenceFiles(/* lwfile, */ vwfile);

			boolean compressing = false;
			uinttype compressedOffset = 0;
			uinttype streamPosition = 0;
			if (compressRoll == true) {
				vwfile->setCompress(BufferedRandomAccessFile::COMPRESS_WRITE);

				streamPosition = vwfile->BufferedRandomAccessFile::getFilePointer();
				tx->setStreamPosition(streamPosition);
				compressing = true;
			}

			inttype m = 0;
			inttype i = 0;
			boolean next = (i < conductor->size()) || (m < conductor->getMarkerCount());

			#ifdef DEEP_DEBUG
			if ((i == 0) && (m < conductor->getMarkerCount())) {
				DEEP_LOG(ERROR, OTHER, "Invalid state: marker required, but no info has been written for the conductor, %s\n", map->getFilePath());

				throw InvalidException("Invalid state: marker required, but no info has been written for the conductor");
			}
			#endif

			boolean first = true;
			#ifdef DEEP_RECOVERY_DEBUG
			inttype recoveryIndex = -1;
			#endif
			while (next == true) {
				K key;
				Information* info;
				boolean marking = false;
				#ifdef DEEP_RECOVERY_DEBUG
				if ((m < conductor->getMarkerCount()) || (recoveryIndex != -1)) {
				#else
				if (m < conductor->getMarkerCount()) {
				#endif
					info = Information::nullInfo(map->m_share.getValueSize());
					#ifdef DEEP_RECOVERY_DEBUG
					if (recoveryIndex != -1) {
						info->setFilePosition(recoveryIndex);
						recoveryIndex = -1;
					}
					#endif
					marking = true;

					key = (K) Converter<K>::NULL_VALUE;
					++m;
					next = (i < conductor->size()) || (m < conductor->getMarkerCount());

				} else {
					SegMapEntry* infoEntry = conductor->get(i++);
					info = infoEntry->getValue();
					key = infoEntry->getKey();
					next = (i < conductor->size());

					if (info->getLevel() < Information::LEVEL_ACTIVE) {
						if ((next == false) && (cursor == false)) {
							info = Information::nullInfo(map->m_share.getValueSize());
							marking = true;

						} else {
							continue;
						}
					}
				}

				// XXX: fragmentation statistics
				{
					vwfile->incrementTotalCount();

					Information* next = info->getNext();
					while ((next != null) && (next != info)) {
						if ((next->getLevel() == Information::LEVEL_COMMIT) && (next->getExpunged() == false)) {
							MeasuredRandomAccessFile* pfile = map->m_share.getVrtWriteFileList()->get(next->getFileIndex());
							if (pfile != null) {
								pfile->incrementDeadCount();
							}

							next->setExpunged(true);
						}

						next = next->getNext();
					}
				}

				if (info->getFilePosition() != 0) {
					// XXX: purge down already written data
					if (purge == true) {
						info->freeData();
					}

					continue;
				}

				tmpValue.reassign((const bytearray) info->getData(), info->getSize());

				if (compressing == true) {
					info->setCompressedOffset(compressedOffset);
					info->setFileIndex(fileIndex);
					info->setFilePosition(streamPosition);
				} else {
					info->setFileIndex(fileIndex);
					info->setFilePosition(vwfile->BufferedRandomAccessFile::getFilePointer());
				}

				if ((map->m_share.getValueSize() != -1) && (info->getSize() != (uinttype) map->m_share.getValueSize())) {
					info->setSize(map->m_share.getValueSize());
				}

				#ifdef DEEP_VALIDATE_DATA
				uinttype crc = RealTimeValidate::simple(tmpValue, info->getSize());
				vwfile->MeasuredRandomAccessFile::writeInt(crc);
				#endif

				vwfile->MeasuredRandomAccessFile::write(&tmpValue, 0, info->getSize());

				if (compressing == true) {
					compressedOffset += tmpValue.length;
				}

				RealTimeProtocol<V,K>::writeLrtEntry(map, lwfile, i, key, info, first, next, marking, cursor, purge, rolling, compressing, conductor->getTransactionId());

				// XXX: only move to next file in non-roll scenario
				if ((rolling == false) && (vwfile->BufferedRandomAccessFile::getFilePointer() > map->getFileSize()) /* || (lwfile->BufferedRandomAccessFile::getFilePointer() > map->getFileSize()) */) {
					RealTimeProtocol<V,K>::terminateStreaming(lwfile, vwfile, map);

					fileIndex = RealTimeProtocol<V,K>::nextRealTime(lwfile, vwfile, conductor, map, durable);
				}

				if (marking == true) {
					Converter<Information*>::destroy(info);
				}
			}

			#ifdef DEEP_DEBUG
			if (i > conductor->size()) {
				DEEP_LOG(ERROR, OTHER, "Invalid info entry index in conductor: index=%d, size=%d\n", i, conductor->size());

				throw InvalidException("Invalid info entry index in conductor");
			}
			#endif

			conductor->clearMarkers();

			#if 0
			if ((i != 0) && (first == true) && (cursor == false)) {
				// TODO: add closing marker (large transaction case)
			}
			#endif

			if (vwfile->getCompress() == BufferedRandomAccessFile::COMPRESS_WRITE) {
				vwfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);

				map->m_valueCompressedBlockCount++;
				map->m_valueCompressedBlockSize += vwfile->getAndResetBlockLength();
			}

			vwfile->flush();
			lwfile->flush();

			RealTimeProtocol<V,K>::checkpoint(map, lwfile, conductor);

			if (durable == false) {
				map->m_share.release(vwfile);
				map->m_share.release(lwfile);

			} else {
				MeasuredRandomAccessFile::prepareSynchronizeGlobally(lwfile, vwfile);

				vwfile->unlock();
				lwfile->unlock();
			}

			if (profile == true) {
				longtype stop = System::currentTimeMillis();
				if ((stop - start) > Properties::getProfilingTriggerInterval()) {
					DEEP_LOG(DEBUG, COMMT, "store: %s, value commit time: %lld\n", map->getFilePath(), (stop - start));
				}
			}
		}
		map->m_share.getVrtWriteFileList()->unlock();

		#ifndef DEEP_SYNCHRONIZATION_GROUPING
		if ((durable == true) && (Properties::getDurableSyncInterval() == 0)) {
			MeasuredRandomAccessFile::performSynchronizeGlobally();
		}
		#endif
	}

	struct RollUpdate {
		Information* preinfo;
		Information* info;
		ushorttype index;
		uinttype pos;
		boolean compressed;
		uinttype compressedOffset;

		FORCE_INLINE RollUpdate(Information* p_preinfo, Information* p_info, ushorttype p_index, uinttype p_pos, boolean p_compressed, uinttype p_compressedOffset = 0) :
			preinfo(p_preinfo),
			info(p_info),
			index(p_index),
			pos(p_pos),
			compressed(p_compressed),
			compressedOffset(p_compressedOffset) {
		}

		FORCE_INLINE virtual ~RollUpdate() {
			/* XXX: nothing to do */
		}

		FORCE_INLINE void apply() {

			if (info == null) {
				return;
			}
			if (compressed == true) {
				info->setCompressedOffset(compressedOffset);
			}
			info->setFileIndex(index, false /* allow relocation for committed state */);
			info->setFilePosition(pos, false /* allow relocation for committed state */);

			if (preinfo != null) {
				if (preinfo->getLevel() == Information::LEVEL_COMMIT) {
					// XXX: only commit infos we have added to storyline during roll
					uinttype newlimit = Transaction::getCurrentViewpoint();
					uinttype prelimit = preinfo->getViewLimit();

					// XXX: statement order follows update pattern in RealTimeMap::commitCacheMemory
					preinfo->reset(preinfo->getViewpoint(), (prelimit == 0) ? newlimit : prelimit, preinfo->getLevel() == Information::LEVEL_COMMIT);
					preinfo->setDeleting(true);

					info->reset(preinfo->getViewpoint(), prelimit, true);
					info->setLevel(Information::LEVEL_COMMIT);

				#ifdef DEEP_DEBUG
				} else {
					DEEP_LOG(ERROR, OTHER, "Invalid roll update: preinfo with level %d\n", preinfo->getLevel());

					throw InvalidException("Invalid roll update: preinfo level");
				#endif
				}
			}
		}
	};

	FORCE_INLINE static void writeRolloverLrtData(const RealTimeMap<K>* map, const Segment<K>* segment, const K key, const Information* info, const uinttype position, const uinttype streamPosition, MeasuredRandomAccessFile* lwfile, boolean& first, const boolean last, const boolean compressing, const boolean marking) {
		ubytetype stateFlags = 0;
		{
			if (first == true) {
				first = false;
				stateFlags |= LRT_FLAG_OPENING;

				if (compressing == true) {
					stateFlags |= LRT_FLAG_COMPRESS;
				}
			}

			if (last == true) {
				stateFlags |= LRT_FLAG_CLOSING;

				if (marking == true) {
					stateFlags |= LRT_FLAG_MARKING;
				}
			}

			stateFlags |= LRT_FLAG_ROLLING;

			lwfile->writeByte(stateFlags);
		}

		if (compressing == true) {
			if (stateFlags & LRT_FLAG_COMPRESS) {
				lwfile->writeInt(streamPosition);
			}

			if (map->m_share.getValueSize() == -1) {
				lwfile->writeInt(info->getSize());
			}

		} else {
			if (map->m_share.getValueSize() == -1) {
				lwfile->writeInt(position);
				lwfile->writeInt(info->getSize());

			} else if (map->m_share.getKeyProtocol() == -1) {
				lwfile->writeInt(position);
			}
		}

		if (map->m_rowStoreMode == false) {
			KeyProtocol_v1<K>::writeKey(key, lwfile, map->m_share.getKeyProtocol());
		}
	}

	static void recoverRealTime(RealTimeMap<K>* map, const ushorttype lrtStartingFileIndex, const uinttype lrtStartingFileLength) {
		return RealTimeProtocol_v1_0_0_0<RTP_v1_1_0_0,K>::recoverRealTime(map, lrtStartingFileIndex, lrtStartingFileLength);
	}

	FORCE_INLINE static doubletype recoveryPosition(MapFileSet<MeasuredRandomAccessFile>* lwrites, const ushorttype lrtIndexOffset, const uinttype lrtLengthOffset, const ushorttype lrtFileIndex, const uinttype lrtFileLength) {
		return RealTimeProtocol_v1_0_0_0<RTP_v1_1_0_0,K>::recoveryPosition(lwrites, lrtIndexOffset, lrtLengthOffset, lrtFileIndex, lrtFileLength);
	}

	static void recoverRealTimeProcess(RealTimeMap<K>* map, ushorttype lrtFileIndex, uinttype lrtFileLength, MapFileSet<MeasuredRandomAccessFile>& lwrites) {
		TreeMap<shorttype,RecoveryWorkspace_v1_1<K>*> workspaces(TreeMap<shorttype,RecoveryWorkspace_v1_1<K>*>::INITIAL_ORDER, false, true);

		return RealTimeProtocol_v1_0_0_0<V,K>::template recoverRealTimeProcessLoop< TreeMap<shorttype,RecoveryWorkspace_v1_1<K>*> >(map, lrtFileIndex, lrtFileLength, lwrites, workspaces);
	}

	static void recoveryRead(RealTimeMap<K>* map, BufferedRandomAccessFile & lrfile, int & index, longtype & prevKeyPosition, longtype & prevValuePosition, uinttype & lrtFileLength, longtype vend, longtype lend, K key1, RecoveryWorkspace_v1_0<K> & ws, MapFileSet<MeasuredRandomAccessFile>* lwrites, const ushorttype lrtIndexOffset, const uinttype lrtLengthOffset) {

		DEEP_LOG(ERROR, OTHER, "Invalid version for recovery read: 1.1/1.0\n");

		throw InvalidException("Invalid Codepath: recoveryRead 1.1");
	}

	static void recoveryRead(RealTimeMap<K>* map, BufferedRandomAccessFile & lrfile, int & index, longtype & prevKeyPosition, longtype & prevValuePosition, uinttype & lrtFileLength, longtype vend, longtype lend, K key1, TreeMap<shorttype,RecoveryWorkspace_v1_1<K>*> & ws, MapFileSet<MeasuredRandomAccessFile>* lwrites, const ushorttype lrtIndexOffset, const uinttype lrtLengthOffset, uinttype* totalCount, uinttype* deadCount, RealTime** dynamicSecondaries, RealTimeAtomic* atomicCommit, boolean& withinTransaction) {
		struct Recovery {
			FORCE_INLINE static void reset(RealTimeMap<K>* map, BufferedRandomAccessFile*& vrfile, uinttype& vposition) {
				if (vrfile != null) {
					if (vrfile->getCompress() != BufferedRandomAccessFile::COMPRESS_NONE) {
						vrfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
						vposition = vrfile->BufferedRandomAccessFile::getPosition();
					}
					vrfile->BufferedRandomAccessFile::setPosition(0);
					vrfile->BufferedRandomAccessFile::setCursor(0);

					map->m_share.destroy(vrfile, false /* XXX: clobber */);
					vrfile = null;
				}
			}
		};

		lrfile.BufferedRandomAccessFile::seek(lrtFileLength);

		// XXX: if a secondary was added dynamically during this lrt file, check array with each iteration 
		bytetype secondaryCount = map->getShare()->getSecondaryList()->size();
		boolean checkDynamicSecondaries = false;
		for (int x = 0; x < secondaryCount; x++) {
			RealTime* rt = dynamicSecondaries[x];
			if (rt != null) {
				if (rt->getLrtIndex() == lrfile.getFileIndex()) {
					checkDynamicSecondaries = true;
				}
			}
		}
		

		// XXX: for reading compressed vrt blocks (following logic does not use acquire/release bracketing, see vrfile->unlock)
		BufferedRandomAccessFile* vrfile = null;

		longtype currentPosition = lrfile.BufferedRandomAccessFile::getFilePointer();
		shorttype txid = DEFAULT_TXID;
		boolean largeMode = false; // TODO: this really depends on where we start reading in the LRT (maybe put first large pos in the SRT)
		boolean compressed = false;
		uinttype compressedOffset = 0;
		
		uinttype vposition = 0;

		// TODO: fix versioning here
		boolean useOffsetShortcut = (lrfile.getProtocol() < CT_DATASTORE_PROTO_VER_1_3) && (map->m_share.getKeyProtocol() != -1) && (map->m_share.getValueSize() != -1);

		if (useOffsetShortcut == true) {
			vposition = ((((currentPosition - Properties::DEFAULT_FILE_HEADER) / map->m_lrtOffset)) * map->m_share.getValueSize()) + Properties::DEFAULT_FILE_HEADER;
		}

		ThreadContext<K>* ctxt = map->m_threadContext.getContext();

		nbyte retValue(1);
		boolean eof = false;
		#ifdef DEEP_RECOVERY_DEBUG
		boolean verifyRecoveryIndex = false;
		#endif

		boolean virtualEof = false;
	
		boolean noACPFileWarned = false;
		ulongtype minimumTransaction = (atomicCommit == null) ? 0 : atomicCommit->getMinimumTransaction();

		for (int j = 0; (eof == false) && (currentPosition <= lend); j++) {

			// XXX: add any dynamic secondaries
			if (checkDynamicSecondaries == true) {
				for (int x = 0; x < secondaryCount; x++) {
					RealTime* rt = dynamicSecondaries[x];
					if (rt != null) {
						if ((rt->getLrtIndex() == lrfile.getFileIndex()) && (rt->getLrtPosition() == currentPosition)) {
							DEEP_LOG(INFO, RCVRY, "Activating dynamic secondary, %s\n", rt->getFilePath());
							rt->setMount(RealTime::MOUNT_OPENED);
							map->indexSecondary(null /* tx */, rt);
						}
					}
				}
			}

			// XXX: read until end so any final dynamic secondary can be added
			if (currentPosition == lend) {
				break;
			}

			ubytetype stateFlags = lrfile.readByte(&eof);

			if (stateFlags & LRT_FLAG_ACP) {
				if ((atomicCommit == null) && (noACPFileWarned == false)) {
					DEEP_LOG(WARN, RCVRY, "no cross table transaction file (deep.trt) found, proceeding without ACP, %s\n", map->getFilePath());
					noACPFileWarned = true;	
				}	

				ulongtype transactionId = lrfile.readLong(&eof);

				if ((atomicCommit != null) && (transactionId >= minimumTransaction) && (atomicCommit->transactionCompleted(transactionId) == false)) {
					DEEP_LOG(WARN, RCVRY, "cross table transaction %llu did not complete, %s\n", transactionId, map->getFilePath());
					break;
				}
			}

			if ((withinTransaction == false) && (stateFlags == 0)) {
				// XXX: note preallocated space truncated in calling method
				currentPosition = lrfile.BufferedRandomAccessFile::getFilePointer();
				if (virtualEof == false) {
					virtualEof = true;

					// XXX: if this file is effectively empty, don't behave as if transaction was started but not completed
					if (currentPosition == (Properties::DEFAULT_FILE_HEADER + 1)) {
						prevKeyPosition = Properties::DEFAULT_FILE_HEADER;
						prevValuePosition = Properties::DEFAULT_FILE_HEADER;
					}
				}
				continue;
			}
			#ifdef DEEP_DEBUG
			if (virtualEof == true) {
				DEEP_LOG(ERROR, OTHER, "Invalid virtual eof, %s\n", map->getFilePath());

				throw InvalidException("Invalid virtual eof\n");
			}
			#endif

			if (stateFlags & LRT_FLAG_OPENING) {
				txid = DEFAULT_TXID;
			}

			if (ws.containsKey(txid) == false) {
				ws.put(txid, new RecoveryWorkspace_v1_1<K>(txid, map));
			}
			RecoveryWorkspace_v1_1<K>& workspace = *ws.get(txid);

			if (stateFlags & LRT_FLAG_OPENING) {
				withinTransaction = true;
				workspace.clear();

				Recovery::reset(map, vrfile, vposition);

				if (stateFlags & LRT_FLAG_COMPRESS) {
					compressed = true;
					compressedOffset = 0;
					vposition = lrfile.readInt(&eof);

				} else {
					compressed = false;
				}
			}

			const boolean largeTx = txid != DEFAULT_TXID;
			ubytetype txStart = 0;
			ubytetype txAbort = 0;
			ubytetype txMerge = 0;
			if (largeTx == true) {
				txStart = lrfile.readByte(&eof);
				txAbort = lrfile.readByte(&eof);
				txMerge = lrfile.readByte(&eof);
				//workspace.tx->setLevel(workspace.tx->getLevel() + txStart);
				for (inttype i=0; i<txStart; ++i) {
					workspace.tx->begin();
				}
				largeMode = true;

			}

			if (compressed == false) {
				if ((useOffsetShortcut == false) || (largeMode == true)) {
					vposition = lrfile.readInt(&eof);
				}
			}

			inttype vsize;
			if (map->m_share.getValueSize() == -1) {
				vsize = lrfile.readInt(&eof);

			} else {
				vsize = map->m_share.getValueSize();
			}

			if ((vsize == 0) || ((compressed == false) && (vposition > (vend - vsize)))) {
				break;
			}

			if (eof == true) {
				break;
			}

			Information* info = Information::newInfo((compressed == false) ? Information::WRITE : Information::CMPRS, index, vposition, vsize);
			info->setLevel(workspace.tx->getLevel());

			const boolean deleted = stateFlags & LRT_FLAG_DESTROY;
			if (deleted == true) {
				info->setDeleting(true);

			} else {
				info->setUpdating(true);
			}

			nbyte tmpValue((const bytearray) null, 0);

			if (compressed == true) {
				if (vrfile == null) {
					vrfile = new BufferedRandomAccessFile(map->m_share.getVrtReadFileList()->get(index)->getPath(), "r", Properties::DEFAULT_FILE_BUFFER);
					map->m_share.acquire(vrfile);
					vrfile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
					vrfile->BufferedRandomAccessFile::seek(vposition);
				}
				info->setCompressedOffset(compressedOffset);

				#ifdef DEEP_VALIDATE_DATA
				uinttype crc1 = vrfile->BufferedRandomAccessFile::readInt();
				#endif

				info->initData();
				tmpValue.reassign((const bytearray) info->getData(), info->getSize());

				vrfile->BufferedRandomAccessFile::read(&tmpValue);
				compressedOffset += info->getSize();

				#ifdef DEEP_VALIDATE_DATA
				uinttype crc2 = RealTimeValidate::simple(tmpValue, info->getSize());
				if (crc1 != crc2) {
					DEEP_LOG(ERROR, OTHER, "Invalid crc values (recovery/compressed): mismatch %u / %u, %s\n", crc1, crc2, map->getFilePath());

					throw InvalidException("Invalid crc values (recovery/compressed): mismatch\n");
				}
				#endif

			} else {
				map->readValue(ctxt, info, (const K)Converter<K>::NULL_VALUE);
				tmpValue.reassign((const bytearray) info->getData(), info->getSize());
			}

			// XXX: 1.0.x change (candidate for 1.1 versioning: forward compatible)
			const boolean closure = stateFlags & LRT_FLAG_CLOSURE;
			const boolean marking = stateFlags & LRT_FLAG_MARKING;
			if ((marking == true) || (closure == true)) {
				if (map->m_rowStoreMode == false) {
					KeyProtocol_v1<K>::skipKey(&lrfile, map->m_share.getKeyProtocol(), &eof);
				}

				#ifdef DEEP_RECOVERY_DEBUG
				if ((marking == true) && (verifyRecoveryIndex == true)) {
					const ushorttype recoveryIndex = info->getFilePosition();
					if (map->m_share.getVrtReadFileList()->get(recoveryIndex) != null) {
						DEEP_LOG(ERROR, RCVRY, "delete of nonexistent key during replay, %s\n", map->getFilePath());
						throw InvalidException("Delete of nonexistent key during replay");
					}
					verifyRecoveryIndex = false;
				}
				#endif

				Converter<Information*>::destroy(info);

			} else {
				K key = (K) Converter<K>::NULL_VALUE;
				if (map->m_rowStoreMode == false) {
					key = KeyProtocol_v1<K>::readKey(&lrfile, map->m_share.getKeyProtocol(), &eof);
					if (key == (K) Converter<K>::NULL_VALUE) {
						break;
					}

				} else {
					key = map->m_keyBuilder->fillKey(NULL, info->getData(), key1);
					key = map->m_keyBuilder->cloneKey(key);
				}

				if (eof == true) {
					Converter<Information*>::destroy(info);
					break;
				}

				const boolean exists = map->get(key, &retValue, RealTimeMap<K>::EXACT, null, workspace.tx, RealTimeMap<K>::LOCK_WRITE);

				// XXX: rebuild value fragmentation statistics
				(*totalCount)++;
				if ((deleted == true) || ((exists == true) && (workspace.tx->getLastFileIndex() == index))) {
					(*deadCount)++;
				}

				#ifdef DEEP_RECOVERY_DEBUG
				if ((exists == false) && (deleted == true)) {
					// XXX: track info file index for debug check
					verifyRecoveryIndex = true;
				}
				#endif

				if ((exists == true) && (deleted == true)) {
					if (map->remove(key, &tmpValue, RealTimeMap<K>::DELETE_POPULATED, workspace.tx, RealTimeMap<K>::LOCK_NONE, info->getCompressed()) == false) {
						#ifdef DEEP_DEBUG
						DEEP_LOG(ERROR, RCVRY, "remove failed during replay, %s\n", map->getFilePath());
						throw InvalidException("Remove failed during replay");
						#endif
					}

				} else if (exists == true) {
					if (map->put(key, &tmpValue, RealTimeMap<K>::EXISTING, workspace.tx, RealTimeMap<K>::LOCK_NONE, vposition, index, info->getCompressedOffset()) == false) {
						#ifdef DEEP_DEBUG
						DEEP_LOG(ERROR, RCVRY, "update failed during replay, %s\n", map->getFilePath());
						throw InvalidException("Update failed during replay");
						#endif
					}

				} else if (deleted == false) {
					if (map->put(key, &tmpValue, RealTimeMap<K>::UNIQUE, workspace.tx, RealTimeMap<K>::LOCK_WRITE, vposition, index, info->getCompressedOffset()) == false) {
						#ifdef DEEP_DEBUG
						DEEP_LOG(ERROR, RCVRY, "unique put failed during replay, %s\n", map->getFilePath());
						throw InvalidException("Unique put failed during replay");
						#endif
					}
				}

				Converter<Information*>::destroy(info);
			}

			for (inttype t=0; t<txAbort; ++t) {
				workspace.tx->rollback(workspace.tx->getLevel());
			}
			for (inttype t=0; t<txMerge; ++t) {
				workspace.tx->commit(workspace.tx->getLevel());
			}

			if ((useOffsetShortcut == true) && (compressed == false) && (largeMode == false)) {
				vposition += map->m_share.getValueSize();

				#ifdef DEEP_VALIDATE_DATA
				vposition += sizeof(uinttype);
				#endif
			}

			boolean committed = false;
			if (stateFlags & LRT_FLAG_CLOSING) {
				withinTransaction = false;
				committed = true;

				Recovery::reset(map, vrfile, vposition);
				compressed = false;
			}

			// XXX: large tx
			#if 0
			if ((marking == true) && (committed == false)) {
				DEEP_LOG(ERROR, RCVRY, "invalid state flags: marking=true, closing=false, %s\n", map->getFilePath());
				throw InvalidException("Invalid state flags: marking=true, closing=false");
			}
			#endif

			if (committed == true) {
				#ifdef DEEP_DEBUG
				if (workspace.tx->getLevel() != 0) {
					DEEP_LOG(ERROR, OTHER, "Invalid TX level for commit: %d\n", workspace.tx->getLevel());
					throw InvalidException("Invalid TX level for commit");
				}
				#endif
				RealTimeProtocol<V,K>::checkpoint(map, &lrfile, null /* conductor */);
				workspace.tx->commit(workspace.tx->getLevel());
				// TODO: make sure tx level reflects fully committed state
				workspace.clear(); // XXX: tx level reset here
			}

			currentPosition = lrfile.BufferedRandomAccessFile::getFilePointer();

			if ((committed == true) || (largeTx == true) || (closure == true)) {
				prevKeyPosition = currentPosition;
				prevValuePosition = vposition + vsize;
			}

			if ((j % RESTORE_DEBUG_MOD) == 0) {
				DEEP_LOG(INFO, RCVRY, "review lrt/vrt [%d] { %lld / %lld, %s } (%.1f%% done)\n", index, prevKeyPosition, prevValuePosition, map->getFilePath(), RealTimeVersion<K>::recoveryPosition(lwrites, lrtIndexOffset, lrtLengthOffset, index, currentPosition)*100.0);
			}
		}
	}

};

template<int V, typename K>
const bytetype RealTimeProtocol_v1_1_0_0<V,K>::LRT_FLAG_CLOSURE = 0x80;

template<int V, typename K>
const bytetype RealTimeProtocol_v1_1_0_0<V,K>::IRT_FLAG_REF_IRT = 4;

template<int V, typename K>
const bytetype RealTimeProtocol_v1_1_0_0<V,K>::IRT_FLAG_REF_VRT = 5;

template<int V, typename K>
const bytetype RealTimeProtocol_v1_1_0_0<V,K>::IRT_MFLAG_VCOMPRESS = 0x80;

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEPROTOCOL_H_*/
