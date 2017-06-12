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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEVERSION_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEVERSION_H_

#include "com/deepis/db/store/relative/util/Versions.h"
#include "com/deepis/db/store/relative/core/RealTimeAtomic.h"

#define RTP_v1_0_0_0 CT_DATASTORE_PROTO_VER_1_0
#define RTP_v1_1_0_0 CT_DATASTORE_PROTO_VER_1_1
#define RTP_v1_2_0_0 CT_DATASTORE_PROTO_VER_1_2
#define RTP_v1_3_0_0 CT_DATASTORE_PROTO_VER_1_3

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template <typename K> struct RecoveryWorkspace_v1_0;

template <int V, typename K> struct RealTimeProtocol_v1_0_0_0;
template <int V, typename K> struct RealTimeProtocol_v1_1_0_0;
template <int V, typename K> struct RealTimeProtocol_v1_2_0_0;
template <int V, typename K> struct RealTimeProtocol_v1_3_0_0;

template<int V, typename K>
struct RealTimeProtocol {
};

template<typename K>
struct RealTimeProtocol<RTP_v1_3_0_0, K> : RealTimeProtocol_v1_1_0_0<RTP_v1_3_0_0, K> {
};

// XXX: note v1_2 uses v1_1 RealTimeProtocol class with V = v1_2 since new behavior is not needed
template<typename K>
struct RealTimeProtocol<RTP_v1_2_0_0, K> : RealTimeProtocol_v1_1_0_0<RTP_v1_2_0_0, K> {
};

template<typename K>
struct RealTimeProtocol<RTP_v1_1_0_0, K> : RealTimeProtocol_v1_1_0_0<RTP_v1_1_0_0, K> {
};

template<typename K>
struct RealTimeProtocol<RTP_v1_0_0_0, K> : RealTimeProtocol_v1_0_0_0<RTP_v1_0_0_0, K> {
};

} } } } } } // namespace

#include "com/deepis/db/store/relative/core/RealTimeProtocol_v1_1.h"

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

/*
 * XXX: this static functional behavior might someday be instance base (see performance)
 */

template<typename K>
class RealTimeVersion {

	public:
		static inttype getIrtHeader(RealTimeMap<K>* map) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				return RealTimeProtocol<RTP_v1_3_0_0,K>::getIrtHeader(map);
			} else {
				return RealTimeProtocol<RTP_v1_2_0_0,K>::getIrtHeader(map);
			}
		}

		static inttype getIrtBlockSize(RealTimeMap<K>* map) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				return RealTimeProtocol<RTP_v1_3_0_0,K>::getIrtBlockSize(map);
			} else {
				return RealTimeProtocol<RTP_v1_2_0_0,K>::getIrtBlockSize(map);
			}
		}

		static inttype getLrtBlockSize(RealTimeMap<K>* map) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				return RealTimeProtocol<RTP_v1_3_0_0,K>::getLrtBlockSize(map);
			} else {
				return RealTimeProtocol<RTP_v1_2_0_0,K>::getLrtBlockSize(map);
			}
		}

		static inttype getBufferSize(RealTimeMap<K>* map) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				return RealTimeProtocol<RTP_v1_3_0_0,K>::getBufferSize(map);
			} else {
				return RealTimeProtocol<RTP_v1_2_0_0,K>::getBufferSize(map);
			}
		}

		static bytetype validateKeyPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, boolean lastUsed, RealTimeLocality& lastLrtLocality, longtype* lastIrtLength) {
			#if 0
			if (iwfile->getProtocol() == Versions::GET_PROTOCOL_CURRENT()) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					return RealTimeProtocol<RTP_v1_3_0_0,K>::validateKeyPagingSafe(iwfile, map, lastUsed, lastLrtLocality, lastIrtLength);
				} else {
					return RealTimeProtocol<RTP_v1_2_0_0,K>::validateKeyPagingSafe(iwfile, map, lastUsed, lastLrtLocality, lastIrtLength);
				}

			#if 0
			} else if (iwfile->getProtocol() == CT_DATASTORE_PROTO_VER_1_0) {
				return RealTimeProtocol<RTP_v1_0_0_0,K>::validateKeyPaging(iwfile, map, lastUsed, lastLrtLocality, lastIrtLength);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid file protocol version: %lld, %s\n", iwfile->getProtocol(), iwfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

		static inttype writeKeyPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* segment, boolean reset, uinttype viewpoint, K& summaryFirstKey, boolean& early, RealTimeIndex::IndexReport& indexReport) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				return RealTimeProtocol<RTP_v1_3_0_0,K>::writeKeyPaging(iwfile, map, ctxt, segment, reset, viewpoint, summaryFirstKey, early, indexReport);
			} else {
				return RealTimeProtocol<RTP_v1_2_0_0,K>::writeKeyPaging(iwfile, map, ctxt, segment, reset, viewpoint, summaryFirstKey, early, indexReport);
			}
		}

		static longtype findSummaryPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, const RealTimeLocality& lastLrtLocality, const uinttype recoveryEpoch) {
			#if 0
			if (iwfile->getProtocol() == Versions::GET_PROTOCOL_CURRENT()) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					return RealTimeProtocol<RTP_v1_3_0_0,K>::findSummaryPaging(iwfile, map, lastLrtLocality, recoveryEpoch);
				} else {
					return RealTimeProtocol<RTP_v1_2_0_0,K>::findSummaryPaging(iwfile, map, lastLrtLocality, recoveryEpoch);
				}

			#if 0
			} else if (iwfile->getProtocol() == CT_DATASTORE_PROTO_VER_1_0) {
				return RealTimeProtocol<RTP_v1_0_0_0,K>::findSummaryPaging(iwfile, map, lastLrtLocality, recoveryEpoch);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid file protocol version: %lld, %s\n", iwfile->getProtocol(), iwfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

		static boolean summaryRebuildKeyPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map) {
			#if 0
			if (iwfile->getProtocol() == Versions::GET_PROTOCOL_CURRENT()) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					return RealTimeProtocol<RTP_v1_3_0_0,K>::summaryRebuildKeyPaging(iwfile, map);
				} else {
					return RealTimeProtocol<RTP_v1_2_0_0,K>::summaryRebuildKeyPaging(iwfile, map);
				}

			#if 0
			} else if (iwfile->getProtocol() == CT_DATASTORE_PROTO_VER_1_0) {
				return RealTimeProtocol<RTP_v1_0_0_0,K>::summaryRebuildKeyPaging(iwfile, map);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid file protocol version: %lld, %s\n", iwfile->getProtocol(), iwfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

		static void terminatePaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, boolean summarized, uinttype recoveryEpoch, boolean invalidate = false) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				RealTimeProtocol<RTP_v1_3_0_0,K>::terminatePaging(iwfile, map, summarized, recoveryEpoch, invalidate);
			} else {
				RealTimeProtocol<RTP_v1_2_0_0,K>::terminatePaging(iwfile, map, summarized, recoveryEpoch, invalidate);
			}
		}

		static void readKeyPaging(RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt, boolean modification, boolean compression) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				RealTimeProtocol<RTP_v1_3_0_0,K>::readKeyPaging(map, segment, ctxt, modification, compression);
			} else {
				RealTimeProtocol<RTP_v1_2_0_0,K>::readKeyPaging(map, segment, ctxt, modification, compression);
			}
		}

		static void readKeyPagingIndex(RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt, BufferedRandomAccessFile* irfile, inttype* xfrag, boolean modification, boolean compression) {
			#if 0
			if (irfile->getProtocol() == RTP_v1_1_0_0) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					RealTimeProtocol<RTP_v1_3_0_0,K>::readKeyPagingIndex(map, segment, ctxt, irfile, xfrag, modification, compression);
				} else {
					RealTimeProtocol<RTP_v1_2_0_0,K>::readKeyPagingIndex(map, segment, ctxt, irfile, xfrag, modification, compression);
				}

			#if 0
			} else if (irfile->getProtocol() == RTP_v1_0_0_0) {
				RealTimeProtocol<RTP_v1_0_0_0,K>::readKeyPagingIndex(map, segment, ctxt, irfile, xfrag, modification, compression);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid protocol version: %lld, %s\n", irfile->getProtocol(), irfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

		static void readPagingInfo(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, bytetype* flags) {
			#if 0
			if (irfile->getProtocol() == RTP_v1_1_0_0) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					return RealTimeProtocol<RTP_v1_3_0_0,K>::readPagingInfo(irfile, map, flags);
				} else {
					return RealTimeProtocol<RTP_v1_2_0_0,K>::readPagingInfo(irfile, map, flags);
				}

			#if 0
			} else if (irfile->getProtocol() == RTP_v1_0_0_0) {
				return RealTimeProtocol<RTP_v1_0_0_0,K>::readPagingInfo(irfile, map, flags);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid protocol version: %lld, %s\n", irfile->getProtocol(), irfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

		static Information* readPagingInfo(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt, bytetype* flags) {
			boolean sequential = false;

			#if 0
			if (irfile->getProtocol() == RTP_v1_1_0_0) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					return RealTimeProtocol<RTP_v1_3_0_0,K>::readPagingInfo(irfile, map, segment, ctxt, flags, &sequential, 0 /* frag */, false /* modification */, false /* compression */);
				} else {
					return RealTimeProtocol<RTP_v1_2_0_0,K>::readPagingInfo(irfile, map, segment, ctxt, flags, &sequential, 0 /* frag */, false /* modification */, false /* compression */);
				}

			#if 0
			} else if (irfile->getProtocol() == RTP_v1_0_0_0) {
				return RealTimeProtocol<RTP_v1_0_0_0,K>::readPagingInfo(irfile, map, segment, ctxt, flags, &sequential, false /* modification */);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid protocol version: %lld, %s\n", irfile->getProtocol(), irfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

		static void readMetaHeader(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, boolean seek) {
			#if 0
			if (irfile->getProtocol() == RTP_v1_1_0_0) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					RealTimeProtocol<RTP_v1_3_0_0,K>::readMetaHeader(irfile, map, segment, true);
				} else {
					RealTimeProtocol<RTP_v1_2_0_0,K>::readMetaHeader(irfile, map, segment, true);
				}

			#if 0
			} else if (irfile->getProtocol() == RTP_v1_0_0_0) {
				RealTimeProtocol<RTP_v1_0_0_0,K>::readMetaHeader(irfile, map, segment, true);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid protocol version: %lld, %s\n", irfile->getProtocol(), irfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

		static void readPagingHeader(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt) {
			#if 0
			if (irfile->getProtocol() == RTP_v1_1_0_0) {
			#endif
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				RealTimeProtocol<RTP_v1_3_0_0,K>::readMetaHeader(irfile, map, segment, true);
			} else {
				RealTimeProtocol<RTP_v1_2_0_0,K>::readMetaHeader(irfile, map, segment, true);
			}
			ubytetype streamRefCount = 0;
			uinttype streamRefLocation = 0;
			uinttype physicalSize = 0;
			boolean keyCompression = false;
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				RealTimeProtocol<RTP_v1_3_0_0,K>::readMetaFooter(irfile, map, segment, false, false, &streamRefCount, &streamRefLocation, &physicalSize, &keyCompression);
			} else {
				RealTimeProtocol<RTP_v1_2_0_0,K>::readMetaFooter(irfile, map, segment, false, false, &streamRefCount, &streamRefLocation, &physicalSize, &keyCompression);
			}

			if (keyCompression == true) {
				irfile->setCompress(BufferedRandomAccessFile::COMPRESS_READ);
			}

			irfile->seek(segment->getPagingPosition());

			// XXX: first key
			bytetype flags = 0;
			readPagingInfo(irfile, map, segment, ctxt, &flags);

			if (segment->getStreamRangeOverflowFlag() == true) {
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					RealTimeProtocol<RTP_v1_3_0_0,K>::readFileRefs(irfile, map, segment, streamRefCount, streamRefLocation, true /* seek */, keyCompression);
				} else {
					RealTimeProtocol<RTP_v1_2_0_0,K>::readFileRefs(irfile, map, segment, streamRefCount, streamRefLocation, true /* seek */, keyCompression);
				}
			}

			if (keyCompression == true) {
				irfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
			}	

			#if 0
			} else if (irfile->getProtocol() == RTP_v1_0_0_0) {
				RealTimeProtocol<RTP_v1_0_0_0,K>::readMetaHeader(irfile, map, segment, true);
				RealTimeProtocol<RTP_v1_0_0_0,K>::readMetaFooter(irfile, map, segment, false, false);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid protocol version: %lld, %s\n", irfile->getProtocol(), irfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

		static void readInfoValue(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* segment) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				RealTimeProtocol<RTP_v1_3_0_0,K>::readInfoValue(map, ctxt, segment);
			} else {
				RealTimeProtocol<RTP_v1_2_0_0,K>::readInfoValue(map, ctxt, segment);
			}
		}

		static void readInfoValue(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Information* info, nbyte* value) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				RealTimeProtocol<RTP_v1_3_0_0,K>::readInfoValue(map, ctxt, info, value);
			} else {
				RealTimeProtocol<RTP_v1_2_0_0,K>::readInfoValue(map, ctxt, info, value);
			}
		}

		static void terminateStreaming(MeasuredRandomAccessFile* lwfile, MeasuredRandomAccessFile* vwfile, RealTimeMap<K>* map) {
			#if 0
			if (lwfile->getProtocol() == CT_DATASTORE_PROTO_VER_1_1) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					RealTimeProtocol<RTP_v1_3_0_0,K>::terminateStreaming(lwfile, vwfile, map);
				} else {
					RealTimeProtocol<RTP_v1_2_0_0,K>::terminateStreaming(lwfile, vwfile, map);
				}

			#if 0
			} else if (lwfile->getProtocol() == CT_DATASTORE_PROTO_VER_1_0) {
				return RealTimeProtocol<RTP_v1_0_0_0,K>::terminateStreaming(lwfile, vwfile, map);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid file protocol version: %lld, %s\n", lwfile->getProtocol(), lwfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

		static void commitRealTime(RealTimeMap<K>* map, RealTimeConductor<K>* conductor, boolean cursor, boolean purge) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				RealTimeProtocol<RTP_v1_3_0_0,K>::commitRealTime(map, conductor, cursor, purge);
			} else {
				RealTimeProtocol<RTP_v1_2_0_0,K>::commitRealTime(map, conductor, cursor, purge);
			}
		}

		static void statisticPaging(RealTimeMap<K>* map, Segment<K>* segment, const Information* topinfo, const Information* curinfo, boolean rebuild, const boolean checkpoint) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				RealTimeProtocol<RTP_v1_3_0_0,K>::statisticPaging(map, segment, topinfo, curinfo, rebuild, checkpoint);
			} else {
				RealTimeProtocol<RTP_v1_2_0_0,K>::statisticPaging(map, segment, topinfo, curinfo, rebuild, checkpoint);
			}
		} 

		static void statisticPagingDead(RealTimeMap<K>* map, Segment<K>* segment, const Information* info, const boolean checkpoint) {
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				RealTimeProtocol<RTP_v1_3_0_0,K>::statisticPagingDead(map, segment, info, checkpoint);
			} else {
				RealTimeProtocol<RTP_v1_2_0_0,K>::statisticPagingDead(map, segment, info, checkpoint);
			}
		} 

		static void recoverRealTime(RealTimeMap<K>* map, const ushorttype lrtStartingFileIndex, const uinttype lrtStartingFileLength) {
			if (Properties::getRecoveryReplay() == true) {
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					RealTimeProtocol_v1_1_0_0<RTP_v1_3_0_0,K>::recoverRealTime(map, lrtStartingFileIndex, lrtStartingFileLength);
				} else {
					RealTimeProtocol_v1_1_0_0<RTP_v1_2_0_0,K>::recoverRealTime(map, lrtStartingFileIndex, lrtStartingFileLength);
				}
			}
		}

		FORCE_INLINE static doubletype recoveryPosition(MapFileSet<MeasuredRandomAccessFile>* lwrites, const ushorttype lrtIndexOffset, const uinttype lrtLengthOffset, const ushorttype lrtFileIndex, const uinttype lrtFileLength) {
			// XXX: Just use the 1.1 version for right now. Even w/ mixed version files, the calculation will be the same
			if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
				return RealTimeProtocol_v1_1_0_0<RTP_v1_3_0_0,K>::recoveryPosition(lwrites, lrtIndexOffset, lrtLengthOffset, lrtFileIndex, lrtFileLength);
			} else {
				return RealTimeProtocol_v1_1_0_0<RTP_v1_2_0_0,K>::recoveryPosition(lwrites, lrtIndexOffset, lrtLengthOffset, lrtFileIndex, lrtFileLength);
			}
		}

		static void recoverRealTimeProcess(RealTimeMap<K>* map, const ushorttype lrtFileIndex, uinttype lrtFileLength, MapFileSet<MeasuredRandomAccessFile>& lwrites) {
			#if 0
			MeasuredRandomAccessFile* lwfile = lwrites.get(lrtFileIndex);

			if (lwfile->getProtocol() == CT_DATASTORE_PROTO_VER_1_1) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					RealTimeProtocol<RTP_v1_3_0_0,K>::recoverRealTimeProcess(map, lrtFileIndex, lrtFileLength, lwrites);
				} else {
					RealTimeProtocol<RTP_v1_2_0_0,K>::recoverRealTimeProcess(map, lrtFileIndex, lrtFileLength, lwrites);
				}

			#if 0
			} else if (lwfile->getProtocol() == CT_DATASTORE_PROTO_VER_1_0) {
				RealTimeProtocol<RTP_v1_0_0_0,K>::recoverRealTimeProcess(map, lrtFileIndex, lrtFileLength, lwrites);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid file protocol version: %lld, %s\n", lwfile->getProtocol(), lwfile->getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}

 		template <typename RecoveryWorkspace>
 		static void recoveryRead(RealTimeMap<K>* map, BufferedRandomAccessFile& lrfile, inttype& index, longtype& prevKeyPosition, longtype& prevValuePosition, uinttype& lrtFileLength, longtype vend, longtype lend, K key1, RecoveryWorkspace& ws, MapFileSet<MeasuredRandomAccessFile>* lwrites, const ushorttype lrtIndexOffset, const uinttype lrtLengthOffset, uinttype* totalCount, uinttype* deadCount, RealTime** dynamicSecondaries, RealTimeAtomic* atomicCommit, boolean& withinTransaction) {

			#if 0
			if (lrfile.getProtocol() == CT_DATASTORE_PROTO_VER_1_1) {
			#endif
				if (Versions::GET_PROTOCOL_CURRENT() == RTP_v1_3_0_0) {
					RealTimeProtocol_v1_1_0_0<RTP_v1_3_0_0,K>::recoveryRead(map,lrfile,index, prevKeyPosition,prevValuePosition,lrtFileLength,vend,lend,key1,ws,lwrites,lrtIndexOffset,lrtLengthOffset,totalCount,deadCount,dynamicSecondaries,atomicCommit,withinTransaction);
				} else {
					RealTimeProtocol_v1_1_0_0<RTP_v1_2_0_0,K>::recoveryRead(map,lrfile,index, prevKeyPosition,prevValuePosition,lrtFileLength,vend,lend,key1,ws,lwrites,lrtIndexOffset,lrtLengthOffset,totalCount,deadCount,dynamicSecondaries,atomicCommit,withinTransaction);
				}

			#if 0
			} else if (lrfile.getProtocol() == CT_DATASTORE_PROTO_VER_1_0) {
				RealTimeProtocol_v1_0_0_0<CT_DATASTORE_PROTO_VER_1_0, K>::recoveryRead(map,lrfile,index, prevKeyPosition,prevValuePosition,lrtFileLength,vend,lend,key1,ws,lwrites,lrtIndexOffset,lrtLengthOffset);

			} else {
				DEEP_LOG(ERROR, OTHER, "Invalid file protocol version: %lld, %s\n", lrfile.getProtocol(), lrfile.getPath());

				throw InvalidException("Invalid File Protocol Version");
			}
			#endif
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEVERSION_H_*/
