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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEPROTOCOL_V1_0_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEPROTOCOL_V1_0_H_

// XXX: includes required for templating dependencies
#include "cxx/util/TreeMap.cxx"

#include "com/deepis/db/store/relative/core/RealTimeMap.h"
#include "com/deepis/db/store/relative/core/RealTimeAdaptive.h"
#include "com/deepis/db/store/relative/util/Versions.h"

#ifdef DEEP_VALIDATE_DATA
#include "com/deepis/db/store/relative/core/RealTimeValidate.h"
#endif

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename K>
struct KeyProtocol_v1 {

	FORCE_INLINE static K readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		if (keySize == -1) {
			keySize = keyFile->readShort(eof);
		}

		nbyte* key = new nbyte(keySize);
		keyFile->BufferedRandomAccessFile::readFully(key, 0, keySize, eof);

		return (K) key;
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		if (keySize == -1) {
			keySize = keyFile->readShort(eof);
		}

		keyFile->BufferedRandomAccessFile::skipBytes(keySize, eof);
	}

	FORCE_INLINE static void writeKey(K key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		if (keySize == -1) {
			keySize = key->length;
			keyFile->writeShort(keySize);
		}

		keyFile->BufferedRandomAccessFile::write(key, 0, keySize);
	}
};

template<>
struct KeyProtocol_v1<longtype> {

	FORCE_INLINE static longtype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readLong(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		keyFile->BufferedRandomAccessFile::skipBytes(8 /* longtype */, eof);
	}

	FORCE_INLINE static void writeKey(longtype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeLong(key);
	}
};

template<>
struct KeyProtocol_v1<ulongtype> {

	FORCE_INLINE static ulongtype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readLong(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		keyFile->BufferedRandomAccessFile::skipBytes(8 /* longtype */, eof);
	}

	FORCE_INLINE static void writeKey(ulongtype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeLong(key);
	}
};

template<>
struct KeyProtocol_v1<doubletype> {

	FORCE_INLINE static doubletype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readDouble(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		keyFile->BufferedRandomAccessFile::skipBytes(8 /* doubletype */, eof);
	}

	FORCE_INLINE static void writeKey(doubletype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeDouble(key);
	}
};

template<>
struct KeyProtocol_v1<inttype> {

	FORCE_INLINE static inttype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readInt(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		keyFile->BufferedRandomAccessFile::skipBytes(4 /* inttype */, eof);
	}

	FORCE_INLINE static void writeKey(inttype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeInt(key);
	}
};

template<>
struct KeyProtocol_v1<uinttype> {

	FORCE_INLINE static uinttype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readInt(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		keyFile->BufferedRandomAccessFile::skipBytes(4 /* uinttype */, eof);
	}

	FORCE_INLINE static void writeKey(uinttype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeInt(key);
	}
};

template<>
struct KeyProtocol_v1<floattype> {

	FORCE_INLINE static floattype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readFloat(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		keyFile->BufferedRandomAccessFile::skipBytes(4 /* floattype */, eof);
	}

	FORCE_INLINE static void writeKey(floattype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeFloat(key);
	}
};

template<>
struct KeyProtocol_v1<shorttype> {

	FORCE_INLINE static shorttype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readShort(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		keyFile->BufferedRandomAccessFile::skipBytes(2 /* shorttype */, eof);
	}

	FORCE_INLINE static void writeKey(shorttype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeShort(key);
	}
};

template<>
struct KeyProtocol_v1<ushorttype> {

	FORCE_INLINE static ushorttype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readShort(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		keyFile->BufferedRandomAccessFile::skipBytes(2 /* ushorttype */, eof);
	}

	FORCE_INLINE static void writeKey(ushorttype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeShort(key);
	}
};

template<>
struct KeyProtocol_v1<chartype> {

	FORCE_INLINE static chartype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readChar(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		// XXX: chartype is stored as two bytes
		keyFile->BufferedRandomAccessFile::skipBytes(2 /* chartype */, eof);
	}

	FORCE_INLINE static void writeKey(chartype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeChar(key);
	}
};

template<>
struct KeyProtocol_v1<uchartype> {

	FORCE_INLINE static uchartype readKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		return keyFile->readChar(eof);
	}

	FORCE_INLINE static void skipKey(BufferedRandomAccessFile* keyFile, shorttype keySize, boolean* eof = null) {
		// XXX: chartype is stored as two bytes
		keyFile->BufferedRandomAccessFile::skipBytes(2 /* uchartype */, eof);
	}

	FORCE_INLINE static void writeKey(uchartype key, BufferedRandomAccessFile* keyFile, shorttype keySize) {
		keyFile->writeChar(key);
	}
};

template <typename K>
struct RecoveryWorkspace_v1_0 {
	Segment<K> workspace;
	RealTimeConductor<K> conductor;

	RecoveryWorkspace_v1_0(RealTimeMap<K>* map) : workspace(map->m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER, CT_DATASTORE_PROTO_VER_1_0, map->m_keyBuilder->getKeyParts()), conductor(null, map) { }
};

template<typename RecoveryWorkspace, typename K>
struct RecoveryWorkspaceCleanup {
	FORCE_INLINE static void clean(RecoveryWorkspace & ws) {
		DEEP_LOG(ERROR, OTHER, "Unsupported operation\n");
		throw InvalidException("Unsupported operation");
	}
};

template<typename K>
struct RecoveryWorkspaceCleanup<RecoveryWorkspace_v1_0<K>,K> {
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;

	FORCE_INLINE static void clean(RecoveryWorkspace_v1_0<K> & ws) {
		for (int i = 0; i < ws.conductor.size(); i++) {
			SegMapEntry* walkEntry = ws.conductor.get(i);

			if (walkEntry->getValue()->getLevel() != Information::LEVEL_ROLLED) {
				Converter<K>::destroy(walkEntry->getKey());
			}

			Converter<Information*>::destroy(walkEntry->getValue());
		}
	}
};

template<int V, typename K>
struct RealTimeProtocol_v1_0_0_0 {

	static const inttype RESTORE_DEBUG_MOD = 100000;

	static const inttype IRT_HEAD_FORMAT_FIX;

	static const inttype IRT_BODY_FORMAT_FIX;
	static const inttype IRT_BODY_FORMAT_VAR;
	static const inttype LRT_BODY_FORMAT_FIX;
	static const inttype LRT_BODY_FORMAT_VAR;

	static const bytetype LRT_FLAG_OPENING;
	static const bytetype LRT_FLAG_CLOSING;
	static const bytetype LRT_FLAG_PENDING;
	static const bytetype LRT_FLAG_ROLLING;
	static const bytetype LRT_FLAG_DESTROY;
	// XXX: 1.0.x change (candidate for 1.1 versioning: forward compatible)
	static const bytetype LRT_FLAG_MARKING;

	static const bytetype IRT_FLAG_CONTENT;
	static const bytetype IRT_FLAG_RESERVE;
	static const bytetype IRT_FLAG_DELETED;
	static const bytetype IRT_FLAG_CLOSURE;

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;

	FORCE_INLINE static inttype getIrtHeader(RealTimeMap<K>* map) {
		if (map->m_keyBuilder->getKeyParts() > 1) {
			return (map->m_keyBuilder->getKeyParts() * sizeof(inttype)) + IRT_HEAD_FORMAT_FIX;

		} else {
			return IRT_HEAD_FORMAT_FIX;
		}
	}

	FORCE_INLINE static inttype getIrtBlockSize(RealTimeMap<K>* map) {
		return (map->m_share.getValueSize() == -1 ? IRT_BODY_FORMAT_VAR : IRT_BODY_FORMAT_FIX) + map->m_share.getKeyBuffer();
	}

	FORCE_INLINE static inttype getLrtBlockSize(RealTimeMap<K>* map) {
		inttype size = 0;

		if ((map->m_share.getKeyProtocol() == -1) || (map->m_share.getValueSize() == -1)) {
			// XXX: lrt block size is not used (i.e. see map->m_lrtOffset)

		} else {
			size += LRT_BODY_FORMAT_FIX;
		}

		if (map->m_rowStoreMode == false) {
			size += map->m_share.getKeyBuffer();
		}

		return size;
	}

	FORCE_INLINE static inttype getBufferSize(RealTimeMap<K>* map) {
		return (map->getBufferSize(RealTimeProtocol<V,K>::getIrtBlockSize(map), map->m_share.getKeyBuffer(), map->getSegmentBufferedSize(false /* adaptive */)) + map->m_irtHeader);
	}

	FORCE_INLINE static boolean isEmptyMap(RealTimeMap<K>* map, ushorttype& index) {
		RealTimeShare* share = map->m_primaryIndex == null ? map->getShare() : map->m_primaryIndex->getShare();

		MeasuredRandomAccessFile* lwfile = share->getLrtWriteFileList()->size() > 0 ? share->getLrtWriteFileList()->first() : null;
		MeasuredRandomAccessFile* vwfile = share->getVrtWriteFileList()->size() > 0 ? share->getVrtWriteFileList()->first() : null;

		if (lwfile != null) {
			index = lwfile->getFileIndex();
		}

		return (vwfile == null) || (lwfile == null) || ((vwfile->length() == Properties::DEFAULT_FILE_HEADER) && (lwfile->length() == Properties::DEFAULT_FILE_HEADER));
	}

	FORCE_INLINE static boolean checkpoint(RealTimeMap<K>* map, BufferedRandomAccessFile* lwfile, RealTimeConductor<K>* conductor) {
		#ifdef DEEP_DEBUG
		if ((conductor == null) && (map->m_state != RealTimeMap<K>::MAP_RECOVER)) {
			DEEP_LOG(ERROR, OTHER, "Invalid conductor in checkpoint(), %s\n", map->getFilePath());
		}
		#endif
		map->m_currentLrtLocality = map->getLocality(lwfile, conductor);

		return true;
	}

	FORCE_INLINE static void readMetaHeader(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, boolean seek) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		if (seek == true) {
			irfile->BufferedRandomAccessFile::seek(segment->getPagingPosition() - (map->m_irtHeader - (4 /* post segment location */)));
		}

		ubytetype stateFlags = irfile->readByte();
		segment->setStateFlags(stateFlags);

		// XXX: reset transient states
		segment->setDirty(false);
		segment->setVirtual(false);
		segment->setViewpoint(false);

		/* ushorttype lrtFileIndex = */ irfile->readShort();
		/* uinttype lrtFileLength = */ irfile->readInt();

		uinttype virtualSize = irfile->readInt();
		segment->vsize(virtualSize);

		RealTimeProtocol<V,K>::readKeyCardinality(irfile, map, segment);
	}

	FORCE_INLINE static void readMetaFooter(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, boolean seek, boolean setPosition) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		#if 0
		static const inttype OFFSET = 1 + 2 + 1 + 2 + 4 + 2 + 4;

		if (seek == true) {
			irfile->BufferedRandomAccessFile::seek(segment->getPagingPosition() - OFFSET);
		}
		#endif

		ubytetype vrtFileRange = irfile->readByte();
		segment->setStreamRange(vrtFileRange);
		ushorttype vrtFileIndex = irfile->readShort();
		segment->addStreamIndex(vrtFileIndex);

		ubytetype irtFileRange = irfile->readByte();

		/* XXX: don't override index, this is a back index */
		ushorttype pagingIndex = irfile->readShort();
		uinttype pagingPosition = irfile->readInt();
		/* ushorttype physicalSize = */ irfile->readShort();
		/* uinttype preSegmentLocation = */ irfile->readInt();
		if (setPosition == true) {
			segment->setBackReference(pagingIndex);	
			segment->setPagingPosition(pagingPosition);
		}
		// XXX: range must be set after index to correctly derive file indexes
		segment->setPagingRange(irtFileRange);
	}

	FORCE_INLINE static void readPagingInfo(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, bytetype* flags, boolean* eof = null) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		*flags = irfile->readByte(eof);

		/* ushorttype index = */ irfile->readShort(eof);
		/* uinttype position = */ irfile->readInt(eof);

		if (map->m_share.getValueSize() == -1) {
			/* inttype vsize = */ irfile->readInt(eof);
		}

		if (*flags == IRT_FLAG_CONTENT) {
			KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol(), eof);

		} else if (*flags == IRT_FLAG_DELETED) {
			KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol(), eof);

		} else if (*flags == IRT_FLAG_CLOSURE) {
			KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol(), eof);
		}
	}

	FORCE_INLINE static Information* readPagingInfo(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt, bytetype* flags, boolean* sequential, boolean modification) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		Information* info = null;

		*flags = irfile->readByte();

		ushorttype index = irfile->readShort();
		uinttype position = irfile->readInt();

		inttype vsize;
		if (map->m_share.getValueSize() == -1) {
			vsize = irfile->readInt();

		} else {
			vsize = map->m_share.getValueSize();
		}

		if (*flags == IRT_FLAG_CONTENT) {
			K key = KeyProtocol_v1<K>::readKey(irfile, map->m_share.getKeyProtocol());

			if (modification == true) {
				info = Information::newInfo(Information::WRITE, index, position, vsize);
				info->resetIndexed(0xffffffffffffffff);

			} else {
				info = Information::newInfo(Information::READ, index, position, vsize);
			}

			if (*sequential == false) {
				K lastKey = (K) Converter<K>::NULL_VALUE;
				Information* oldinfo = segment->SegTreeMap::add(key, info, &lastKey, sequential, false);

				// XXX: reset last key and information on duplicate key (i.e. index-referencing logic)
				if (oldinfo != null) {
					Converter<K>::destroy(key);
					Converter<Information*>::destroy(info);

					info = oldinfo;
				}

			} else {
				segment->SegTreeMap::add(key, info);
			}

		} else if (*flags == IRT_FLAG_DELETED) {
			KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol());

		/* XXX: nothing to do
		} else if (*flags == IRT_FLAG_CLOSURE) {

			KeyProtocol_v1<K>::skipKey(irfile, map->m_share.getKeyProtocol());
		*/
		}

		return info;
	}

	FORCE_INLINE static void readKeyCardinality(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, boolean* eof = null) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		const bytetype keyParts = map->m_keyBuilder->getKeyParts();
		if (keyParts > 1) {
			inttype* cardinality = (segment != null) && ((segment->getVirtual() == false)) ? (inttype*) segment->getCardinality() : null;
			if (cardinality != null) {
				for (int i = 0; i < keyParts; i++) {
					cardinality[i] = irfile->readInt(eof);
				}

			} else {
				for (int i = 0; i < keyParts; i++) {
					irfile->readInt(eof);
				}
			}
		}
	}

	FORCE_INLINE static void writeKeyCardinality(BufferedRandomAccessFile* iwfile, RealTimeMap<K>* map, Segment<K>* segment) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");

		const bytetype keyParts = map->m_keyBuilder->getKeyParts();
		if (keyParts > 1) {
			const inttype* cardinality = (segment != null) && ((segment->getVirtual() == false)) ? segment->getCardinality() : null;
			if (cardinality != null) {
				for (int i = 0; i < keyParts; i++) {
					iwfile->writeInt(cardinality[i]);
				}

			} else {
				for (int i = 0; i < keyParts; i++) {
					iwfile->writeInt(-1);
				}
			}
		}
	}

	FORCE_INLINE static void writeInfoPaging(BufferedRandomAccessFile* iwfile, RealTimeMap<K>* map, const K key, bytetype flags, ushorttype fileIndex, uinttype position, inttype size) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");

		iwfile->writeByte(flags);

		iwfile->writeShort(fileIndex);
		iwfile->writeInt(position);

		if (map->m_share.getValueSize() == -1) {
			iwfile->writeInt(size);
		}

		KeyProtocol_v1<K>::writeKey(key, iwfile, map->m_share.getKeyProtocol());
	}

	FORCE_INLINE static void readKeyPagingIndex(RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt, BufferedRandomAccessFile* irfile /* acquired outside */, inttype* xfrag, boolean modification, boolean compression) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		static const inttype OFFSET = 2 /* irt file index */ + 4 /* irt reference */ + 2 /* physical size */ + 4 /* pre segment location */;

		bytetype flags = 0;
		boolean sequential = false;

		uinttype pagingPosition = segment->getPagingPosition();

		if (segment->initialized() == false) {
			// XXX: file position will be aligned to the next protocol read
			RealTimeVersion<K>::readMetaHeader(irfile, map, segment, true);

			ubytetype vrtFileRange = irfile->readByte();
			segment->setStreamRange(vrtFileRange);
			ushorttype vrtFileIndex = irfile->readShort();
			segment->addStreamIndex(vrtFileIndex);

			ubytetype irtFileRange = irfile->readByte();
			segment->setPagingRange(irtFileRange);

			// XXX: the rest of the footer protocol is read below

		} else {
			irfile->BufferedRandomAccessFile::seek(pagingPosition - OFFSET);
		}

		RETRY:
		ushorttype pagingIndex = irfile->readShort(); /* previous delta: irt file index */
		pagingPosition = irfile->readInt(); /* previous delta: irt reference */

		ushorttype physicalSize = irfile->readShort();

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

		// XXX: skip first entry (XXX: second time around should be read)
		inttype counter = 0;
		if (*xfrag == 0) {
			counter = 1;
			RealTimeVersion<K>::readPagingInfo(irfile, map, &flags);
		}

		for (; (counter < physicalSize); counter++) {
			Information* info = RealTimeVersion<K>::readPagingInfo(irfile, map, segment, ctxt, &flags, &sequential, modification);
			if (info == null) {
				++counter;
				break;
			}
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
			}

			irfile->BufferedRandomAccessFile::seek(pagingPosition - OFFSET);

			sequential = false;
			(*xfrag)++;

			goto RETRY;
		}

		map->m_share.release(irfile);
	}

	FORCE_INLINE static void readKeyPaging(RealTimeMap<K>* map, Segment<K>* segment, ThreadContext<K>* ctxt, boolean modification, boolean compression) {
		inttype xfrag = 0;

		ushorttype fileIndex = segment->getCurrentPagingIndex();

		map->m_share.getIrtReadFileList()->lock();
		{
			BufferedRandomAccessFile* irfile = map->m_share.getIrtReadFileList()->get(fileIndex);
			map->m_share.acquire(irfile);

			RealTimeVersion<K>::readKeyPagingIndex(map, segment, ctxt, irfile /* released inside */, &xfrag, modification, compression);
		}
		map->m_share.getIrtReadFileList()->unlock();

		// XXX: virtual size is an estimate due to inclusion of pending work (make it exact now)
		// XXX: this might change with dynamic summarization
		if (segment->getSummary() == false) {
			segment->vsize(segment->SegTreeMap::size());
			RealTimeAdaptive_v1<K>::readKeyFragmentation(map, segment, xfrag);
		}
	}

	FORCE_INLINE static void readInfoValue(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* segment) {
		inttype xfrag = 0;

		nbyte tmpValue((const bytearray) null, 0);

		RealTimeShare* share = (map->m_primaryIndex == null) ? map->getShare() : map->m_primaryIndex->getShare();

		// XXX: BufferedRandomAccessFile will be invoked as a RandomAccessFile (i.e. no buffering)
		share->getVrtReadFileList()->lock();
		{
			segment->SegTreeMap::entrySet(&map->m_fillInformationSet);

			InformationIterator<K> infoIter((MapInformationEntrySetIterator*) map->m_fillInformationSet.iterator());
			const SegMapEntry* infoEntry = infoIter.next();

			inttype fileIndex = -1;
			BufferedRandomAccessFile* vrfile = null;

			while (infoEntry != null) {
				Information* info = infoEntry->getValue();
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
				vrfile->BufferedRandomAccessFile::seek(info->getFilePosition());

				uinttype crc1 = vrfile->BufferedRandomAccessFile::readInt();
				vrfile->BufferedRandomAccessFile::read(&tmpValue, 0, info->getSize());

				uinttype crc2 = RealTimeValidate::simple(tmpValue, info->getSize());
				if (crc1 != crc2) {
					DEEP_LOG(ERROR, OTHER, "Invalid crc values: mismatch %u / %u, %s\n", crc1, crc2, map->getFilePath());

					throw InvalidException("Invalid crc values: mismatch");
				}
				#else
				vrfile->RandomAccessFile::seek(info->getFilePosition());

				vrfile->RandomAccessFile::readFullyRaw(&tmpValue, 0, info->getSize());
				#endif

				if ((infoEntry == null) || (infoEntry->getValue() == null)) {
					continue;
				}

				if ((infoEntry->getValue()->getDeleting()) || ((info->getFilePosition() + info->getSize()) != infoEntry->getValue()->getPosition())) {
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
		RealTimeShare* share = (map->m_primaryIndex == null) ? map->getShare() : map->m_primaryIndex->getShare();

		// XXX: BufferedRandomAccessFile will be invoked as a RandomAccessFile (i.e. no buffering)
		share->getVrtReadFileList()->lock();
		{
			BufferedRandomAccessFile* vrfile = share->getVrtReadFileList()->get(info->getFileIndex());
			if (vrfile == null) {
				DEEP_LOG(ERROR, OTHER, "Invalid file access: %d, %s\n", info->getFileIndex(), map->getFilePath());

				throw InvalidException("Invalid file access");
			}

			share->acquire(vrfile);
			{
				vrfile->RandomAccessFile::seek(info->getFilePosition());

				#ifdef DEEP_VALIDATE_DATA
				uinttype crc1 = vrfile->RandomAccessFile::readIntRaw();
				#endif

				vrfile->RandomAccessFile::readFullyRaw(value, 0, info->getSize());

				#ifdef DEEP_VALIDATE_DATA
				uinttype crc2 = RealTimeValidate::simple(*value, info->getSize());

				if (crc1 != crc2) {
					DEEP_LOG(ERROR, OTHER, "Invalid crc values: mismatch %u / %u, %s\n", crc1, crc2, map->getFilePath());

					throw InvalidException("Invalid crc values: mismatch");
				}
				#endif

				vrfile->BufferedRandomAccessFile::setPosition(0);
				vrfile->BufferedRandomAccessFile::setCursor(0);
			}
			share->release(vrfile);
		}
		share->getVrtReadFileList()->unlock();
	}

	FORCE_INLINE static void writeHeader(RandomAccessFile* iwfile, ubytetype flags, ushorttype index, uinttype length, uinttype size) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		iwfile->writeByte(flags /* segment flags */);
		iwfile->writeShort(index /* lrt file index */);
		iwfile->writeInt(length /* lrt file length */);
		iwfile->writeInt(size /* virtual size */);
	}

	FORCE_INLINE static void writeStreamIndex(RandomAccessFile* iwfile, ubytetype range, ushorttype index, uinttype reference) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		iwfile->writeByte(range /* vrt file range */);
		iwfile->writeShort(index /* vrt file index */);
		// TODO: iwfile->writeInt(reference /* vrt reference */);
	}

	FORCE_INLINE static void writePagingIndex(RandomAccessFile* iwfile, ubytetype range, ushorttype index, uinttype reference) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		iwfile->writeByte(range /* irt file range */);
		iwfile->writeShort(index /* irt file index */);
		iwfile->writeInt(reference /* irt reference */);
	}

	FORCE_INLINE static void seedMetaData(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, Segment<K>* segment) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		RealTimeProtocol<V,K>::writeHeader(iwfile, segment->getStateFlags(), map->m_endwiseLrtLocality.getIndex(), map->m_endwiseLrtLocality.getLength(), segment->vsize());

		RealTimeProtocol<V,K>::writeKeyCardinality(iwfile, map, segment);

		RealTimeProtocol<V,K>::writeStreamIndex(iwfile, 0, 0, 0);
		RealTimeProtocol<V,K>::writePagingIndex(iwfile, 0, 0, 0);

		iwfile->writeShort(segment->SegTreeMap::size() /* temporary physical size (see written actual) */);
	}

	FORCE_INLINE static void writeMetaData(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, Segment<K>* segment, uinttype cBefore, longtype pBefore, boolean rebuild, uinttype actual) {
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

		RealTimeProtocol<V,K>::writeHeader(iwfile, segment->getStateFlags(), map->m_endwiseLrtLocality.getIndex(), map->m_endwiseLrtLocality.getLength(), segment->vsize());

		RealTimeProtocol<V,K>::writeKeyCardinality(iwfile, map, segment);

		#ifdef DEEP_DEBUG
		RealTimeProtocol<V,K>::writeStreamIndex(iwfile, segment->getStreamRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */), segment->getCurrentStreamIndex(), 0 /* TODO */);
		#else
		RealTimeProtocol<V,K>::writeStreamIndex(iwfile, segment->getStreamRange(), segment->getCurrentStreamIndex(), 0 /* TODO */);
		#endif

		if ((rebuild == true) && (segment->getSummary() == false)) {
			segment->resetFragmentCount();
			segment->resetPagingIndexes();

			RealTimeProtocol<V,K>::writePagingIndex(iwfile, 0, 0, 0);

		} else {
			#ifdef DEEP_DEBUG
			ubytetype range = segment->getPagingRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */);
			#else
			ubytetype range = segment->getPagingRange();
			#endif
			ushorttype index = segment->getCurrentPagingIndex();

			// XXX: include incoming file index within index range
			if ((segment->getPagingPosition() != 0) && (index != iwfile->getFileIndex())) {
				// XXX: paging file indexes increase until wrapping
				inttype diff = iwfile->getFileIndex() - index;
				range <<= diff;
				range |= (1 << (diff-1));
			}

			RealTimeProtocol<V,K>::writePagingIndex(iwfile, range, index, segment->getPagingPosition() /* XXX: back reference */);
		}

		iwfile->writeShort(actual /* actual physical size (see seeded temporary) */);

		if (iwfile->flushed() == false) {
			iwfile->setCursor(cAfter);
			iwfile->setPosition(pAfter);

		} else {
			iwfile->flush();
			iwfile->BufferedRandomAccessFile::seek(iwfile->length());
		}
	}

	FORCE_INLINE static boolean keyWriteAccess(StoryLine& storyLine, Information* info, uinttype viewpoint, boolean rebuild, boolean* dirty) {
		boolean writable = (info->getLevel() == Information::LEVEL_COMMIT) && ((info->getNext() == null) || (viewpoint != 0));
		if (writable == false) {
			*dirty = true;

			if ((Properties::getCheckpointMode() != Properties::CHECKPOINT_OFF) && (info->getLevel() == Information::LEVEL_COMMIT) && (rebuild == true)) {
				writable = true;
			}

		} else if (Transaction::lockable(storyLine, 0, false) == false) {
			*dirty = true;
		}

		return writable;
	}

	FORCE_INLINE static boolean keySpaceIndex(RealTimeMap<K>* map, Information* info, inttype index, inttype* actual, boolean* skip, boolean rebuild, uinttype viewpoint) {
		boolean writable = true;
		if (info->getIndexed(map->m_indexValue) == false) {
			info->setIndexed(map->m_indexValue, true);

		// XXX: write first key to "seed" every segment index block
		} else if (rebuild == false) {

			// XXX: though only flush if something else has changed
			if (index == 0) {
				(*skip) = true;
				--(*actual);

			} else if (info->getDeleting() == false) {
				writable = false;
			}
		}

		// XXX: statistics use for cache and fragmentation calculation
		map->m_activeValueSize += info->getSize();
		map->m_activeValueCount++;

		return writable;
	}

	FORCE_INLINE static boolean keySpacePinning(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* segment, Information* info, const boolean rolled = false) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		boolean rollover = false;

		if ((map->m_primaryIndex != null) || (info->getLevel() != Information::LEVEL_COMMIT) || (info->getDeleting() == true)) {
			return rollover;

		} else if ((segment->hasStreamIndex(info->getFileIndex()) == false) && (map->markSegment(ctxt, segment, info->getFileIndex()) == true)) {
			rollover = true;
		}

		return rollover;
	}

	FORCE_INLINE static void repositionPaging(MeasuredRandomAccessFile* iwfile, const uinttype cursorInit, const longtype positionInit) {

		if (iwfile->getCompress() == BufferedRandomAccessFile::COMPRESS_WRITE) {
			iwfile->setCompress(BufferedRandomAccessFile::COMPRESS_NONE);
		}

		// XXX: re-position cursor before restarting
		if (iwfile->flushed() == false) {
			iwfile->setCursor(cursorInit);
			iwfile->setPosition(positionInit);

		} else {
			iwfile->flush();
			// XXX: cut in case future disk writes do not cover
			iwfile->BufferedRandomAccessFile::setLength(positionInit);
			iwfile->BufferedRandomAccessFile::seek(iwfile->length());
		}
	}

	FORCE_INLINE static inttype writeKeyPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* segment, boolean rebuild, uinttype viewpoint, K& summaryFirstKey) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		boolean keyed = false;
		map->m_resetIterator = false;
		K firstKey = (K) Converter<K>::NULL_VALUE;
		const boolean summary = segment->getSummary();

		RETRY:
		inttype actual = 0;
		inttype ceasing = 0;
		boolean skip = false;
		boolean dirty = false;

		// XXX: summary segments don't need to be filled (see summary init and segment purging rules)
		if ((rebuild == true) && (summary == false) && ((segment->getPurged() == true) || (segment->getVirtual() == true))) {
			map->fillSegment(ctxt, segment, false /* values */, false /* pace */);
		}

		const uinttype cursorInit = iwfile->getCursor();
		const longtype positionInit = iwfile->getPosition();

		// XXX: to be filled in at the bottom (i.e. see writeMetaData)
		RealTimeProtocol<V,K>::seedMetaData(iwfile, map, segment);

		const uinttype location = iwfile->BufferedRandomAccessFile::getFilePointer();
		iwfile->writeInt(location /* pre segment location */);

		const uinttype pagingPosition = iwfile->BufferedRandomAccessFile::getFilePointer();

		segment->SegTreeMap::entrySet(&map->m_indexInformationSet);
		MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) map->m_indexInformationSet.iterator();
		for (int i = 0; infoIter->MapInformationEntrySetIterator::hasNext(); i++) {
			SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();

			K key = infoEntry->getKey();
			Information* info = infoEntry->getValue();

			if (summary == false) {

				// XXX: see reseed and deletion
				if ((i == 0) && (keyed == false)) {
					keyed = true;
					firstKey = key;
				}

				boolean divergent = (rebuild == true);
				if (viewpoint != 0) {
					info = map->isolateCheckpoint(ctxt, infoEntry->getValue(), viewpoint, key);

				} else {
					info = map->isolateInterspace(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT, key, &divergent);
				}

				// XXX: secondaries may no longer follow the key space
				if (info == null) {
					if ((rebuild == false) && (divergent == true)) {
						// XXX: re-position cursor before restarting
						RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);
						rebuild = true;
						goto RETRY;
					}

					RealTimeProtocol<V,K>::statisticPaging(map, segment, infoEntry->getValue(), null, rebuild, false /* checkpoint */);

					// XXX: wait for primary to eject first entry for reseed logic
					if ((i == 0) && (viewpoint == 0)) {
						RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);
						return 0;
					}

					// TODO: keep alive for viewing clients / can simplify when moving this block inside keyWriteAccess
					if (Transaction::lockable(infoEntry->getValue(), 0, false) == false) {
						dirty = true;
					}

					ceasing++;
					continue;

				} else if ((dirty == false) && (viewpoint != 0)) {
					dirty = (info->getNext() != null);
				}

				boolean write = RealTimeProtocol<V,K>::keyWriteAccess(infoEntry->getStoryLine(), info, viewpoint, rebuild, &dirty);

				// XXX: ensure segment is notified of information locality (see reorganize files), perform after write access
				if (RealTimeProtocol<V,K>::keySpacePinning(iwfile, map, ctxt, segment, info) == true) {
					// XXX: re-position cursor before restarting
					RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);
					rebuild = true;
					goto RETRY;
				}

				// XXX: wait until information has no active locks, "dirty" could be set to true to keep segment around
				if (write == false) {
					if (rebuild == true) {
						RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);

						if (map->m_resetIterator == true) {
							map->reseedSegment(segment, firstKey, true, true /* lock */);
						}

						// XXX: rebuild has been requested, ensure this state is not dropped before exiting
						segment->setBeenAltered(true);
						return 0;

					} else {
						// XXX: force reseed of first key next time around in case of concurrent rollback
						if ((i == 0) && (info->getLevel() == Information::LEVEL_COMMIT)) {

							if (info->hasFields(Information::WRITE) == true) {
								info->setIndexed(map->m_indexValue, false);

							} else {
								segment->setBeenAltered(true);
							}
						}

						continue;
					}
				}

				// XXX: information is deferred for viewing (i.e. isolation), set "dirty" to true to keep segment around
				if (Transaction::deletable(infoEntry->getValue()) == true) {

					// XXX: while not checkpointing, clean up views due to isolation (e.g. repeatable-read)
					if ((viewpoint == 0) && (map->m_primaryIndex == null)) {

						if (RealTimeAdaptive_v1<K>::deleteUnviewed(map, ctxt, infoEntry, segment, &dirty) == false) {
							info = map->isolateInformation(ctxt, infoEntry->getValue(), Information::LEVEL_COMMIT);

						} else {
							if ((i == 0) && (key == firstKey) && (map->m_resetIterator == false)) {
								map->m_resetIterator = true;
							}

							infoIter->MapInformationEntrySetIterator::remove();

							if (key != firstKey) {
								Converter<K>::destroy(key);
							}

							info = null;
						}

					// XXX: specify dirty (i.e. index again) after checkpoint or secondary ejection
					} else {
						dirty = true;
					}
				}

				// XXX: write and/or rebuild if there is no ending storylines
				if ((info == null) || ((viewpoint == 0) && (info->getDeleting() == true))) {

					if (info != null) {
						ceasing++;
					}

					if ((rebuild == false) || (segment->SegTreeMap::size() == 0)) {
						// XXX: re-position cursor before restarting
						RealTimeProtocol<V,K>::repositionPaging(iwfile, cursorInit, positionInit);

						if (segment->SegTreeMap::size() == 0) {
							map->deleteSegment(segment, firstKey, true, true /* lock */);
							map->m_resetIterator = true;
							return 0;
						}

						rebuild = true;
						goto RETRY;
					}

					continue;
				}

				// XXX: check whether segment information requires to be written to disk
				if (RealTimeProtocol<V,K>::keySpaceIndex(map, info, i, &actual, &skip, rebuild, viewpoint) == false) {
					continue;
				}
			}
			
			RealTimeProtocol<V,K>::writeInfoPaging(iwfile, map, key, IRT_FLAG_CONTENT, info->getFileIndex(), info->getFilePosition(), info->getSize());
			actual++;
		}

		if (actual != 0) {

			const boolean emptySummary = (actual == 0) && (summary == true);

			if (viewpoint == 0) {
				segment->setIndexLocality((map->m_primaryIndex == null) ? map->getCurrentLrtLocality() : map->m_primaryIndex->getCurrentLrtLocality());

			} else {
				segment->setIndexLocality(RealTime::Locality::LOCALITY_NONE);
			}

			if (skip == true) {
				actual += 1;
			}

			RealTimeProtocol<V,K>::writeMetaData(iwfile, map, segment, cursorInit, positionInit, rebuild, actual);

			segment->setPagingPosition(pagingPosition);

			iwfile->writeInt(location /* post segment location */);

			iwfile->flush();

			if (emptySummary == true) {
				actual = 1; /* XXX: indicate written == true */
			}

		} else {
			iwfile->setCursor(cursorInit);
			iwfile->setPosition(positionInit);

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

		segment->setBeenSurceased(ceasing == segment->SegTreeMap::size());
		segment->setViewpoint(viewpoint != 0);
		segment->setDirty(dirty);

		return actual;
	}

	FORCE_INLINE static void statisticPaging(RealTimeMap<K>* map, Segment<K>* segment, const Information* topinfo, const Information* curinfo, boolean rebuild, const boolean checkpoint) {

		if ((rebuild == true) || (curinfo != topinfo)) {
			statisticPagingDead(map, segment, topinfo, checkpoint);
		}

                if ((curinfo != null) && (curinfo->getLevel() == Information::LEVEL_COMMIT)) {
                        if ((rebuild == true) || (curinfo->getIndexed(map->m_indexValue) == false)) {
                                MeasuredRandomAccessFile* pfile = map->m_share.getIrtWriteFileList()->get(map->m_pagingIndex);
                                if (pfile != null) {
                                        pfile->incrementTotalCount();
                                }
                        }
                }
	}

	FORCE_INLINE static void statisticPagingDead(RealTimeMap<K>* map, Segment<K>* segment, const Information* info, const boolean checkpoint) {

		if ((segment->hasPagingIndexes() == true) && (info->getLevel() == Information::LEVEL_COMMIT) && (info->getIndexed(map->m_indexValue) == true)) {
			#ifdef DEEP_DEBUG
			const ubytetype fileRange = segment->getPagingRange(map->m_state == RealTimeMap<K>::MAP_RECOVER /* ignore overflow */);
			#else
			const ubytetype fileRange = segment->getPagingRange();
			#endif
			if (fileRange != 0) {
				for (int i = Properties::DEFAULT_SEGMENT_FILE_RANGE - 1; i >= 0; i--) {
					if (fileRange & (1 << i)) {
						ushorttype fileIndex = segment->getCurrentPagingIndex() - i - 1;
						MeasuredRandomAccessFile* pfile = map->m_share.getIrtWriteFileList()->get(fileIndex);
						if (pfile != null) {
							pfile->incrementDeadCount();
						}
					}
				}
			}

			ushorttype fileIndex = segment->getCurrentPagingIndex();
			MeasuredRandomAccessFile* pfile = map->m_share.getIrtWriteFileList()->get(fileIndex);
			if (pfile != null) {
				pfile->incrementDeadCount();
			}
		}
	}

	FORCE_INLINE static void terminatePaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, boolean summarized) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_CURRENT(iwfile->getProtocol(),"Non-current protocol file accessed for write");

		RealTimeProtocol<V,K>::writeHeader(iwfile, 0, map->m_summaryLrtLocality.getIndex(), map->m_summaryLrtLocality.getLength(), 0);
		RealTimeProtocol<V,K>::writeKeyCardinality(iwfile, map, null);

		RealTimeProtocol<V,K>::writeStreamIndex(iwfile, 0, 0, 0);
		RealTimeProtocol<V,K>::writePagingIndex(iwfile, 0, 0, 0);

		iwfile->writeShort(1 /* physical size */);

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
			}

			Converter<K>::destroy(key);
		}
		iwfile->writeInt(location /* post segment location */);

		iwfile->flush();
	}

	FORCE_INLINE static boolean summaryKeyPaging(BufferedRandomAccessFile* irfile, RealTimeMap<K>* map, Segment<K>* segment, RealTime::Locality* pagingPosition) {
		DEEP_VERSION_ASSERT_EQUAL(V,irfile->getProtocol(),"File Protocol Version Mismatch");

		RealTimeProtocol<V,K>::readMetaHeader(irfile, map, segment, false);

		if (segment->getSummary() == false) {
			return false;
		}

		RealTimeProtocol<V,K>::readMetaFooter(irfile, map, segment, false, true);

		*pagingPosition = segment->getPagingPosition() == 0 ? RealTime::Locality::LOCALITY_NONE : RealTime::Locality(segment->getCurrentPagingIndex(), segment->getPagingPosition(), RealTime::Locality::VIEWPOINT_NONE);

		segment->addPagingIndex(irfile->getFileIndex());
		segment->setPagingPosition(irfile->BufferedRandomAccessFile::getFilePointer());

		bytetype flags = 0;
		boolean sequential = false;
		RealTimeProtocol<V,K>::readPagingInfo(irfile, map, segment, null /* ctxt */, &flags, &sequential, false /* modification */);

		map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::put(segment->SegTreeMap::firstKey(), segment);
		map->m_entrySize.addAndGet(segment->vsize() /* i.e. total size of segment including back-references */);
		// XXX: account for summary purged segments (actual active vs actual purged)
		map->m_purgeSize.incrementAndGet();

		return true;
	}

	FORCE_INLINE static bytetype validateKeyPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, boolean lastUsed, RealTimeLocality& lastLrtLocality, longtype* lastIrtLength) {
		DEEP_VERSION_ASSERT_EQUAL(V,iwfile->getProtocol(),"File Protocol Version Mismatch");

		static const inttype OFFSET = (4 /* pre segment location */ + 4 /* post segment location */);

		boolean eof = false;

		if ((iwfile->length() == Properties::DEFAULT_FILE_HEADER) && (RealTimeProtocol<V,K>::isEmptyMap(map, iwfile->getFileIndex()) == true)) {
			lastLrtLocality = RealTimeLocality(0, Properties::DEFAULT_FILE_HEADER, RealTimeLocality::VIEWPOINT_NONE);
			*lastIrtLength = Properties::DEFAULT_FILE_HEADER;
			return RealTimeMap<K>::SUMMARY_EMPTY;
		}
		
		bytetype flags = IRT_FLAG_CLOSURE + 1;
		ushorttype lrtFileIndex = 0;
		uinttype lrtFileLength = 0;

		BufferedRandomAccessFile irfile(iwfile->getPath(), "r", map->m_irtHeader + map->m_irtOffset + 1 /* +1 ensures no refill */);
		irfile.setProtocol(iwfile->getProtocol());
		irfile.setOnline(true); /* TODO: m_share.acquire(...); */

		irfile.BufferedRandomAccessFile::seek(irfile.length() - 4 /* post segment location */);

		uinttype preSegmentLocation = 0;
		uinttype postSegmentLocation = irfile.readInt(&eof);

		if ((postSegmentLocation != 0) && (irfile.length() > (postSegmentLocation + 1 /* index flags */))) {
			irfile.BufferedRandomAccessFile::seek(postSegmentLocation - (map->m_irtHeader - OFFSET));

			/* ubytetype segmentFlags = */ irfile.readByte(&eof);

			lrtFileIndex = irfile.readShort(&eof);
			// XXX: cast to unsigned before setting lastLrtLength
			lrtFileLength = irfile.readInt(&eof);

			/* uinttype virtualSize = */ irfile.readInt(&eof);

			RealTimeProtocol<V,K>::readKeyCardinality(&irfile, map, null, &eof);

			/* ubytetype vrtFileRange = */ irfile.readByte(&eof);
			/* ushorttype vrtFileIndex = */ irfile.readShort(&eof);

			/* ubytetype irtFileRange = */ irfile.readByte(&eof);
			/* ushorttype irtFileIndex = */ irfile.readShort(&eof);
			/* uinttype pagingPosition = */ irfile.readInt(&eof);

			/* ushorttype physicalSize = */ irfile.readShort(&eof);

			preSegmentLocation = irfile.readInt(&eof);
			flags = irfile.readByte(&eof);
		}

		RETRY:
		if ((eof == false) && (flags == IRT_FLAG_CLOSURE) && (postSegmentLocation == preSegmentLocation)) {
			*lastIrtLength = postSegmentLocation - (map->m_irtHeader - OFFSET);

			lastLrtLocality = RealTimeLocality(lrtFileIndex, lrtFileLength, RealTimeLocality::VIEWPOINT_NONE);

			longtype entrySize = 0;
			{
				// XXX: 256 trillion entries (i.e. plenty)
				struct {
					unsigned int i;
					unsigned short s;
				} uinttype48;

				uinttype48.s = irfile.readShort(&eof);
				uinttype48.i = irfile.readInt(&eof);

				memcpy(&entrySize, &uinttype48, 6 /* i.e. 48 bits */);
			}

			if (eof == true) {
				goto RETRY;
			}

			iwfile->setEntrySize(entrySize);

			DEEP_LOG(DEBUG, DCVRY, "c-irt: %s, valid, %s / %lld\n", iwfile->getPath(), (const char*) lastLrtLocality.toString(), iwfile->getInitialLength());

			return RealTimeMap<K>::SUMMARY_INTACT;

		} else if ((eof == true) || (flags > IRT_FLAG_CLOSURE) || (postSegmentLocation != preSegmentLocation)) {
			irfile.BufferedRandomAccessFile::seek(Properties::DEFAULT_FILE_HEADER);
			eof = false;

			longtype currentPosition = irfile.BufferedRandomAccessFile::getFilePointer();
			while ((eof == false) && (currentPosition <= (irfile.length() - map->m_irtHeader))) {
				/* ubytetype segmentFlags = */ irfile.readByte(&eof);

				lrtFileIndex = irfile.readShort(&eof);
				lrtFileLength = irfile.readInt(&eof);

				/* uinttype virtualSize = */ irfile.readInt(&eof);

				RealTimeProtocol<V,K>::readKeyCardinality(&irfile, map, null, &eof);

				/* ubytetype vrtFileRange = */ irfile.readByte(&eof);
				/* ushorttype vrtFileIndex = */ irfile.readShort(&eof);

				/* ubytetype irtFileRange = */ irfile.readByte(&eof);
				/* ushorttype irtFileIndex = */ irfile.readShort(&eof);
				/* uinttype pagingPosition = */ irfile.readInt(&eof);

				ushorttype physicalSize = irfile.readShort(&eof);

				preSegmentLocation = irfile.readInt(&eof);

				// XXX: grap position before the flags read to calculate jump position
				longtype jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();

				flags = irfile.readByte(&eof); /* index flags */
				if ((flags != IRT_FLAG_CONTENT) && (flags != IRT_FLAG_DELETED)) {
					break;
				}

				if (map->m_share.getKeySize() != -1) {
					jumpPosition += (map->m_irtOffset * physicalSize);
					if (jumpPosition > (irfile.length() - 4 /* post segment location */)) {
						break;
					}

					irfile.BufferedRandomAccessFile::seek(jumpPosition);

				} else {
					irfile.setCursor(irfile.getCursor() - 1 /* index flags */);
					irfile.setPosition(irfile.getPosition() - 1 /* index flags */);

					jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();
					for (int i = 0; (eof == false) && (i < physicalSize) && (jumpPosition < irfile.length()); i++) {
						RealTimeProtocol<V,K>::readPagingInfo(&irfile, map, &flags, &eof);

						jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();
					}

					if (jumpPosition > (irfile.length() - 4 /* post segment location */)) {
						break;
					}
				}

				postSegmentLocation = irfile.readInt(&eof);
				if (postSegmentLocation != preSegmentLocation) {
					break;
				}

				lastLrtLocality = RealTimeLocality(lrtFileIndex, lrtFileLength, RealTimeLocality::VIEWPOINT_NONE);
				*lastIrtLength = postSegmentLocation - (map->m_irtHeader - OFFSET);

				currentPosition = irfile.BufferedRandomAccessFile::getFilePointer();
			}

			if (lastLrtLocality.isNone()) {
				DEEP_LOG(ERROR, FAULT, "e-irt: %s\n", iwfile->getPath());
				return RealTimeMap<K>::SUMMARY_ERROR;
			}

			map->m_share.acquire(iwfile);
			{
				iwfile->setLength(currentPosition);
				iwfile->BufferedRandomAccessFile::seek(iwfile->length());
			}
			map->m_share.release(iwfile);

			DEEP_LOG(WARN, DCVRY, "p-irt: %s, valid, %s / %lld\n", iwfile->getPath(), (const char*) lastLrtLocality.toString(), *lastIrtLength);

			if (lastUsed == false) {
				iwfile->setOptimizeCount(0);
				iwfile->setReorganizeComplete(true);
			}

			return RealTimeMap<K>::SUMMARY_IGNORE;

		} else {
			lastLrtLocality = RealTimeLocality(lrtFileIndex, lrtFileLength, RealTimeLocality::VIEWPOINT_NONE);
			*lastIrtLength = postSegmentLocation - (map->m_irtHeader - OFFSET);

			DEEP_LOG(ERROR, DCVRY, "q-irt: %s, valid, %s / %lld\n", iwfile->getPath(), (const char*) lastLrtLocality.toString(), irfile.length());

			if ((flags != IRT_FLAG_CLOSURE) && (lastUsed == false)) {
				iwfile->setOptimizeCount(0);
				iwfile->setReorganizeComplete(true);
			}

			return RealTimeMap<K>::SUMMARY_IGNORE;
		}
	}

	FORCE_INLINE static longtype findSummaryPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map, const RealTimeLocality& lastLrtLocality) {
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
			currentPosition = position;

			boolean summarySegment = false;
			inttype lrtFileIndex = -1;
			longtype lrtFileLength = -1;
			longtype postSegmentLocation = -1;
			{
				ubytetype segmentFlags = irfile.readByte();
				summarySegment = (segmentFlags & Segment<K>::SEGMENT_FLAG_SUMMARY) != 0;

				lrtFileIndex = irfile.readShort();
				lrtFileLength = irfile.readInt();

				/* uinttype virtualSize = */ irfile.readInt();

				RealTimeProtocol<V,K>::readKeyCardinality(&irfile, map, null);

				/* ubytetype vrtFileRange = */ irfile.readByte();
				/* ushorttype vrtFileIndex = */ irfile.readShort();

				/* ubytetype irtFileRange = */ irfile.readByte();
				/* ushorttype irtFileIndex = */ irfile.readShort();
				/* uinttype pagingPosition = */ irfile.readInt();

				ushorttype physicalSize = irfile.readShort();

				preSegmentLocation = irfile.readInt();

				// XXX: grap position before the flags read to calculate jump position
				longtype jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();

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
				{
				#endif
					irfile.setCursor(irfile.getCursor() - 1 /* index flags */);
					irfile.setPosition(irfile.getPosition() - 1 /* index flags */);

					jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();
					for (int i = 0; (i < physicalSize) && (jumpPosition < irfile.length()); i++) {
						RealTimeProtocol<V,K>::readPagingInfo(&irfile, map, &flags);

						jumpPosition = irfile.BufferedRandomAccessFile::getFilePointer();
					}

					if (jumpPosition > (irfile.length() - 4 /* post segment location */)) {
						DEEP_LOG(WARN, FAULT, "l-irt: %s\n", irfile.getPath());
						break;
					}
				}

				postSegmentLocation = irfile.readInt();
				if (postSegmentLocation != preSegmentLocation) {
					DEEP_LOG(WARN, FAULT, "m-irt: %s\n", irfile.getPath());
					break;
				}
			}

			// XXX: ignore non-summary segments
			if (summarySegment == false) {
				continue;
			}

			if ((lastLrtLocality.getIndex() == lrtFileIndex) && (lastLrtLocality.getLength() == lrtFileLength)) {
				return postSegmentLocation - (map->m_irtHeader - OFFSET);
			}
		}

		return -1;
	}

	FORCE_INLINE static boolean summaryRebuildKeyPaging(MeasuredRandomAccessFile* iwfile, RealTimeMap<K>* map) {
		static const inttype OFFSET = (4 /* pre segment location */ + 4 /* post segment location */);
		enum SummaryMode {
			INITIAL = 0,
			CONTIGUOUS = 1,
			LINKED = 2
		};

		PagedSummary* summaryRef = new PagedSummary(iwfile->getLastLrtLocality());

		uinttype preSegmentLocation = iwfile->getLastIrtLength() + (map->m_irtHeader - OFFSET);

		// XXX: note that irfile may diverge from iwfile when mode == LINKED
		BufferedRandomAccessFile* irfile = map->m_share.getIrtReadFileList()->get(iwfile->getFileIndex()); // TODO: adjust buffer...

		// XXX: cast to unsigned before setting currentPosition
		uinttype position = preSegmentLocation;
		longtype currentPosition = position;

		// XXX: the following has an implicit association with index/purge threads with respects to setting paged/purge state below
		map->m_share.acquire(irfile);
		{
			RealTime::Locality pagingPosition = RealTime::Locality::LOCALITY_NONE;
			SummaryMode mode = LINKED;
			longtype initIrtPosition = preSegmentLocation;
			longtype prevIrtPosition = initIrtPosition;

			while (currentPosition > Properties::DEFAULT_FILE_HEADER) {
				irfile->BufferedRandomAccessFile::seek(currentPosition - (map->m_irtHeader - 4 /* post sgement location */));

				// XXX: cast to unsigned before setting currentPosition
				uinttype position = irfile->readInt();
				currentPosition = position;

				initIrtPosition = prevIrtPosition;
				prevIrtPosition = irfile->BufferedRandomAccessFile::getFilePointer();

				Segment<K>* segment = new Segment<K>(map->m_comparator, Properties::DEFAULT_SEGMENT_LEAF_ORDER,irfile->getProtocol(), map->m_keyBuilder->getKeyParts());
				segment->setMapContext(map->getIndexValue());

				if (RealTimeProtocol<V,K>::summaryKeyPaging(irfile, map, segment, &pagingPosition) == false) {
					if (mode == LINKED) {
						// XXX: this is an error case for LINKED mode since all references should be valid
						DEEP_LOG(WARN, RCVRY, "unable to rebuild keyspace from summary segment: %s, %s\n", RealTime::Locality(irfile->getFileIndex(), prevIrtPosition, RealTime::Locality::VIEWPOINT_NONE).toString().data(), map->getFilePath());
						delete segment;
						Converter<PagedSummary*>::destroy(summaryRef);
						return false;
					}

					delete segment;
					break;
				}

				if (mode == INITIAL) {
					mode = pagingPosition.isNone() == true ? CONTIGUOUS : LINKED;
				}

				if (mode == CONTIGUOUS) {
					continue;
				}

				// XXX: mode == LINKED
				if (pagingPosition.isNone() == true) {
					// XXX: there are no more summary segments in the chain, so break out
					break;
				}

				// XXX: grab the irfile corresponding to the reference
				if (pagingPosition.getIndex() != irfile->getFileIndex()) {
					map->m_share.release(irfile);
					irfile = map->m_share.getIrtReadFileList()->safeGet(pagingPosition.getIndex()); // TODO: adjust buffer...
					if (irfile == null) {
						DEEP_LOG(WARN, RCVRY, "invalid summary link %d: %s, %s\n", pagingPosition.getIndex(), RealTime::Locality(irfile->getFileIndex(), prevIrtPosition, RealTime::Locality::VIEWPOINT_NONE).toString().data(), map->getFilePath());
						Converter<PagedSummary*>::destroy(summaryRef);
						return false;
					}
					map->m_share.acquire(irfile);
				}

				currentPosition = pagingPosition.getLength() - 4;
			}
			map->m_share.release(irfile);

			#if 0
			if (iwfile->getClosureLength() != -1) {
				DEEP_LOG(ERROR, DCVRY, "indexing during keyspace initialization, %s\n", iwfile->getPath());

				#ifdef DEEP_DEBUG
				throw InvalidException("Indexing during keyspace initialization");
				#endif
			}
			#endif

			MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) map->m_orderSegmentSet.reset();
			while (segIter->MapSegmentEntrySetIterator::hasNext()) {
				MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
				Segment<K>* sumSegment = segEntry->getValue();

				RealTimeProtocol<V,K>::readKeyPaging(map, sumSegment, null /* ctxt */, false /* modification */, false /* compression */);

				// XXX: see fillSegment to complete expansion (i.e. fill one more level)
				sumSegment->setPaged(true);
				sumSegment->setPurged(true);

				TreeSet<ushorttype> streamIndexes(&(Segment<K>::USHORT_CMP));
				sumSegment->getStreamIndexes(true /* include old */, &streamIndexes);
				summaryRef->addPagingIndexes(sumSegment->getPagingIndexes());
				summaryRef->addStreamIndexes(&streamIndexes);
			}

			DEEP_LOG(DEBUG, DCVRY, "rebuild keyspace using %d summary segments, %s\n", map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size(), map->getFilePath());

		}

		iwfile->setInitialLrtLocality(iwfile->getLastLrtLocality());

		map->getActiveSummaries()->addSummary(summaryRef);

		// XXX: the above summary segment logic generates an invalid entry size value, but ultimately this entry size will be reset after init/recovery

		return true;
	}

	FORCE_INLINE static ushorttype nextRealTime(MeasuredRandomAccessFile*& lwfile, MeasuredRandomAccessFile*& vwfile, RealTimeConductor<K>* conductor, RealTimeMap<K>* map, boolean durable) {
		DEEP_VERSION_ASSERT_EQUAL(V,lwfile->getProtocol(),"File Protocol Version Mismatch");
		DEEP_VERSION_ASSERT_EQUAL(V,vwfile->getProtocol(),"File Protocol Version Mismatch");

		vwfile->flush();
		lwfile->flush();

		if (durable == false) {
			map->m_share.release(vwfile);
			map->m_share.release(lwfile);

		} else {
			#if 0
			MeasuredRandomAccessFile::performSynchronizeGlobally(false /* decrement */);
			#else
			MeasuredRandomAccessFile::syncAndCloseFiles(vwfile, lwfile);
			#endif

			vwfile->unlock();
			lwfile->unlock();
		}

		ushorttype fileIndex = map->streamFileManagement(false /* lock */, true /* create */);

		MeasuredRandomAccessFile* lnfile = map->m_share.getLrtWriteFileList()->get(fileIndex);
		MeasuredRandomAccessFile* vnfile = map->m_share.getVrtWriteFileList()->get(fileIndex);

		DEEP_VERSION_ASSERT_CURRENT(lnfile->getProtocol(),"Non-current protocol file accessed for write");
		DEEP_VERSION_ASSERT_CURRENT(vnfile->getProtocol(),"Non-current protocol file accessed for write");

		lwfile = lnfile;
		vwfile = vnfile;

		map->m_share.acquire(lwfile);
		map->m_share.acquire(vwfile);

		conductor->referenceFiles(/* lwfile, */ vwfile);

		return fileIndex;
	}

	FORCE_INLINE static void terminateStreaming(MeasuredRandomAccessFile* lwfile, MeasuredRandomAccessFile* vwfile, RealTimeMap<K>* map) {
		// XXX: do nothing -- LRT termination was introduced in 1.1
	}

	FORCE_INLINE static void commitRealTime(RealTimeMap<K>* map, RealTimeConductor<K>* conductor, boolean cursor, boolean purge) {
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

		DEEP_VERSION_ASSERT_CURRENT(lwfile->getProtocol(),"Non-current protocol file accessed for write");
		DEEP_VERSION_ASSERT_CURRENT(vwfile->getProtocol(),"Non-current protocol file accessed for write");

		map->m_share.getVrtWriteFileList()->lock();
		{
			if (fileIndex != map->m_streamIndex) {
				map->m_share.getVrtWriteFileList()->unlock();
				goto RETRY;
			}

			longtype start = 0;
			if (profile == true) {
				start = System::currentTimeMillis();
			}

			map->m_share.acquire(lwfile);
			map->m_share.acquire(vwfile);

			conductor->referenceFiles(/* lwfile, */ vwfile);

			inttype i = 0;
			boolean next = (i < conductor->size());

			boolean first = true;
			while (next == true) {
				SegMapEntry* infoEntry = conductor->get(i++);
				Information* info = infoEntry->getValue();
				next = (i < conductor->size());

				if (info->getLevel() < Information::LEVEL_ACTIVE) {
					continue;
				}

				tmpValue.reassign((const bytearray) info->getData(), info->getSize());

				info->setFileIndex(fileIndex);
				info->setFilePosition(vwfile->BufferedRandomAccessFile::getFilePointer());

				#ifdef DEEP_VALIDATE_DATA
				uinttype crc = RealTimeValidate::simple(tmpValue, tmpValue.length);
				vwfile->MeasuredRandomAccessFile::writeInt(crc);
				#endif

				vwfile->MeasuredRandomAccessFile::write(&tmpValue, 0, tmpValue.length);

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

				ubytetype stateFlags = 0;
				{
					if (first == true) {
						first = false;
						if (i == 0) {
							stateFlags |= LRT_FLAG_OPENING;
						}
					}

					if (next == false) {
						if (cursor == false) {
							stateFlags |= LRT_FLAG_CLOSING;

							// XXX: 1.0.x change (candidate for 1.1 versioning: forward compatible)
							#if 0
							if (marking == true) {
								stateFlags |= LRT_FLAG_MARKING;
							}
							#endif
						}
					}

					if (info->getDeleting() == true) {
						stateFlags |= LRT_FLAG_DESTROY;
					}

					if (cursor == true) {
						stateFlags |= LRT_FLAG_PENDING;

						if (purge == true) {
							info->freeData();
						}
					}

					lwfile->writeByte(stateFlags);
				}

				if (map->m_share.getValueSize() == -1) {
					lwfile->writeInt(info->getFilePosition());
					lwfile->writeInt(info->getSize());

				} else if (map->m_share.getKeyProtocol() == -1) {
					lwfile->writeInt(info->getFilePosition());
				}

				if (map->m_rowStoreMode == false) {
					KeyProtocol_v1<K>::writeKey(infoEntry->getKey(), lwfile, map->m_share.getKeyProtocol());
				}

				if ((vwfile->BufferedRandomAccessFile::getFilePointer() > map->getFileSize()) /* || (lwfile->BufferedRandomAccessFile::getFilePointer() > map->getFileSize()) */) {
					/* RealTimeProtocol<V,K>::terminateStreaming(lwfile, vwfile, map); */

					fileIndex = RealTimeProtocol<V,K>::nextRealTime(lwfile, vwfile, conductor, map, durable);
				}

				// XXX: 1.0.x change (candidate for 1.1 versioning: forward compatible)
				#if 0
				if (marking == true) {
					Converter<Information*>::destroy(info);
				}
				#endif
			}

			#ifdef DEEP_DEBUG
			if (i > conductor->size()) {
				DEEP_LOG(ERROR, OTHER, "Invalid info entry index in conductor: index=%d, size=%d\n", i, conductor->size());

				throw InvalidException("Invalid info entry index in conductor");
			}
			#endif
			conductor->setLastIndex(i);

			#if 0
			if ((i != 0) && (first == true) && (cursor == false)) {
				// TODO: add closing marker (large transaction case)
			}
			#endif

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

	static void recoveryRead(RealTimeMap<K>* map, BufferedRandomAccessFile & lrfile, int & index, longtype & prevKeyPosition, longtype & prevValuePosition, uinttype & lrtFileLength, longtype vend, longtype lend, K key1, TreeMap<shorttype,RecoveryWorkspace_v1_1<K>*> & ws, MapFileSet<MeasuredRandomAccessFile>* lwrites, const ushorttype lrtIndexOffset, const uinttype lrtLengthOffset) {

		DEEP_LOG(ERROR, OTHER, "Invalid version for recovery read: 1.0/1.1\n");

		throw InvalidException("Invalid Codepath: recoveryRead 1.0");
	}

	static void recoveryRead(RealTimeMap<K>* map, BufferedRandomAccessFile & lrfile, int & index, longtype & prevKeyPosition, longtype & prevValuePosition, uinttype & lrtFileLength, longtype vend, longtype lend, K key1, RecoveryWorkspace_v1_0<K> & ws, MapFileSet<MeasuredRandomAccessFile>* lwrites, const ushorttype lrtIndexOffset, const uinttype lrtLengthOffset) {

		DEEP_LOG(ERROR, OTHER, "Invalid version for recovery read: 1.0 read no longer supported\n");

		throw UnsupportedOperationException("Invalid version for recovery read: 1.0 read no longer supported");

		lrfile.BufferedRandomAccessFile::seek(lrtFileLength);

		ThreadContext<K>* ctxt = map->m_threadContext.getContext();

		longtype currentPosition = lrfile.BufferedRandomAccessFile::getFilePointer();
		for (int j = 0; currentPosition <= lend; j++) {
			ubytetype stateFlags = lrfile.readByte();

			uinttype position;
			if ((map->m_share.getKeyProtocol() == -1) || (map->m_share.getValueSize() == -1)) {
				position = lrfile.readInt();

			} else {
				position = ((((currentPosition - Properties::DEFAULT_FILE_HEADER) / map->m_lrtOffset)) * map->m_share.getValueSize()) + Properties::DEFAULT_FILE_HEADER;
			}

			inttype vsize;
			if (map->m_share.getValueSize() == -1) {
				vsize = lrfile.readInt();

			} else {
				vsize = map->m_share.getValueSize();
			}

			if ((vsize == 0) || (position > (vend - vsize))) {
				break;
			}

			Information* info = Information::newInfo(Information::WRITE, index, position, vsize);
			info->setLevel(Information::LEVEL_ACTIVE);

			const boolean deleted = stateFlags & LRT_FLAG_DESTROY;
			if (deleted == true) {
				info->setDeleting(true);

			} else {
				info->setUpdating(true);
			}

			map->readValue(ctxt, info, (const K)Converter<K>::NULL_VALUE);
			nbyte tmpValue((const bytearray) info->getData(), info->getSize());

			// XXX: 1.0.x change (candidate for 1.1 versioning: forward compatible)
			const boolean marking = stateFlags & LRT_FLAG_MARKING;
			if (marking == true) {
				Converter<Information*>::destroy(info);

			} else {
				K key = (K) Converter<K>::NULL_VALUE;
				if (map->m_rowStoreMode == false) {
					key = KeyProtocol_v1<K>::readKey(&lrfile, map->m_share.getKeyProtocol());
					if (key == (K) Converter<K>::NULL_VALUE) {
						break;
					}

				} else {
					key = map->m_keyBuilder->fillKey(NULL, info->getData(), key1);
					key = map->m_keyBuilder->cloneKey(key);
					//map->readValue(ctxt, info, &tmpValue, (const K)Converter<K>::NULL_VALUE);
				}

				K retkey = (K) Converter<K>::NULL_VALUE;
				Information* oldinfo = ws.workspace.SegTreeMap::put(key, info, &retkey);
				if (oldinfo != null) {
					Converter<K>::destroy(retkey);
					oldinfo->setLevel(Information::LEVEL_ROLLED);
				}

				ws.conductor.add(key, info);
			}

			const boolean committed = stateFlags & LRT_FLAG_CLOSING;
			#if DEEP_DEBUG
			if ((marking == true) && (committed == false)) {
				DEEP_LOG(ERROR, RCVRY, "invalid state flags: marking=true, closing=false, %s\n", map->getFilePath());
				throw InvalidException("Invalid state flags: marking=true, closing=false");
			}
			#endif
			if (committed == true) {
				map->recoverCacheMemory(&ws.conductor);
				ws.conductor.clear();
				ws.workspace.clear();
			}

			currentPosition = lrfile.BufferedRandomAccessFile::getFilePointer();

			map->m_currentLrtLocality = RealTime::Locality(index, currentPosition, RealTime::Locality::VIEWPOINT_NONE);

			if (committed == true) {
				prevKeyPosition = currentPosition;
				prevValuePosition = position + vsize;
			}

			if ((j % RESTORE_DEBUG_MOD) == 0) {
				DEEP_LOG(INFO, RCVRY, "review lrt/vrt [%d] { %lld / %lld, %s } (%.1f%% done)\n", index, prevKeyPosition, prevValuePosition, map->getFilePath(), RealTimeVersion<K>::recoveryPosition(lwrites, lrtIndexOffset, lrtLengthOffset, index, currentPosition));
			}
		}
	}

	static void recoverRealTime(RealTimeMap<K>* map, const ushorttype lrtStartingFileIndex, const uinttype lrtStartingFileLength) {
		/* TODO: use iterator */
		// XXX: snap-shot files (i.e. don't error recovery new file information)
		MapFileSet<MeasuredRandomAccessFile> lwrites(*(map->m_share.getLrtWriteFileList()));

		return RealTimeVersion<K>::recoverRealTimeProcess(map, lrtStartingFileIndex, lrtStartingFileLength, lwrites);
	}

	static void recoverRealTimeProcess(RealTimeMap<K>* map, ushorttype lrtFileIndex, uinttype lrtFileLength, MapFileSet<MeasuredRandomAccessFile>& lwrites) {
		RecoveryWorkspace_v1_0<K> ws(map);

		return RealTimeProtocol_v1_0_0_0<V,K>::template recoverRealTimeProcessLoop< RecoveryWorkspace_v1_0<K> > (map, lrtFileIndex, lrtFileLength, lwrites, ws);
	}

	FORCE_INLINE static doubletype recoveryPosition(MapFileSet<MeasuredRandomAccessFile>* lwrites, const ushorttype lrtIndexOffset, const uinttype lrtLengthOffset, const ushorttype lrtFileIndex, const uinttype lrtFileLength) {
		// XXX: lwrites must be in chronological (discovery) order
		struct Position {
			FORCE_INLINE static inttype total(MapFileSet<MeasuredRandomAccessFile>* lwrites, const inttype lrtStartIndex, const ushorttype lrtFileIndex, const uinttype lrtFileLength, ulongtype& total) {
				boolean start = false;
				Iterator<MeasuredRandomAccessFile*>* iter = lwrites->iterator();
				while (iter->hasNext() == true) {
					MeasuredRandomAccessFile* lwfile = iter->next();
					if (start == false) {
						if (lwfile->getFileIndex() != lrtStartIndex) {
							continue;
						}
						start = true;
					}

					total += lwfile->length();
					if (lwfile->getFileIndex() == lrtFileIndex) {
						Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);
						return (lrtFileLength <= lwfile->length()) ? lrtFileIndex : -1;
					}
				}
				Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);

				return -1;
			}
		};

		ulongtype startSize = 0;
		ulongtype totalSize = 0;
		ulongtype remSize = 0;

		if (lwrites->size() == 0) {
			return 0.0;
		}

		inttype startidx = Position::total(lwrites, lwrites->first()->getFileIndex(), lrtIndexOffset, lrtLengthOffset, startSize);
		if (startidx < 0) {
			return 0.0;
		}

		inttype posidx = Position::total(lwrites, startidx, lrtFileIndex, lrtFileLength, totalSize);
		if (posidx < 0) {
			return 0.0;
		}
		MeasuredRandomAccessFile* lwpos = lwrites->get(posidx);
		totalSize -= lwpos->length();

		Position::total(lwrites, posidx, lwrites->last()->getFileIndex(), 0, remSize);
		totalSize += remSize;

		totalSize -= lrtLengthOffset;
		remSize -= lrtFileLength;

		if ((totalSize == 0) || (remSize > totalSize)) {
			return 0.0;
		}

		DEEP_LOG(DEBUG, RCPOS, "lrts=%d, offset={%d: %u}, pos={%d: %u}, totalSize=%llu, remSize=%llu\n", lwrites->size(), lrtIndexOffset, lrtLengthOffset, lrtFileIndex, lrtFileLength, totalSize, remSize);

		return ((doubletype)(totalSize - remSize)) / ((doubletype)totalSize);

	}

	template<typename RecoveryWorkspace>
	static void recoverRealTimeProcessLoop(RealTimeMap<K>* map, const ushorttype lrtFileIndex, uinttype lrtFileLength, MapFileSet<MeasuredRandomAccessFile>& lwrites, RecoveryWorkspace & ws) {
		ThreadContext<K>* ctxt = map->m_threadContext.getContext();
		const uinttype lrtIndexOffset = lwrites.get(lrtFileIndex)->getFileIndex();
		const uinttype lrtLengthOffset = lrtFileLength;
		ulongtype curprotocol = 0;

		MeasuredRandomAccessFile* lwfile = null;
		Iterator<MeasuredRandomAccessFile*>* iter = lwrites.iterator();

		while (iter->hasNext() == true) {
			lwfile = iter->next();
			if (lwfile->getFileIndex() == lrtFileIndex) {
				break;
			}
		}

		if (lwfile->getFileIndex() != lrtFileIndex) {
			lwfile = null;
			#ifdef DEEP_DEBUG
			// TODO:
			#endif
		}

		// XXX: collect secondaries that were added after our starting point
		RealTimeShare* share = map->getShare();
		RealTime** dynamicSecondaries = new RealTime*[share->getSecondaryList()->size()]; 

		RealTimeLocality fileEndLocality(lrtFileIndex, lrtFileLength, 0 /* viewpoint */);
		Comparator<RealTimeLocality> lcmp(&lwrites, false /* compareViewpoints */);

		for (int i = 0; i < share->getSecondaryList()->size(); i++) {
			RealTime* rt = share->getSecondaryList()->get(i);

			ushorttype index = rt->getLrtIndex();
			uinttype pos = (rt->getLrtPosition() == 0) ? Properties::DEFAULT_FILE_HEADER : rt->getLrtPosition();

			RealTimeLocality indexLocality(index, pos, 0 /* viewpoint */);

			if (lcmp.compareNoViewpoint(indexLocality, fileEndLocality) > 0) {
				rt->setMount(RealTime::MOUNT_IGNORED);
				dynamicSecondaries[i] = rt;

			} else {
				dynamicSecondaries[i] = null;
			}
		}

		// XXX: get cross table transaction ids for verifying consistency (ACP)
		#if 1
		char* dbname = RealTimeAtomic::getDatabaseName(map->getFilePath());
		RealTimeAtomic* atomicCommit = RealTimeAtomic::getInstance(dbname, RealTimeAtomic::READ);
		delete[] dbname;
		#else
		RealTimeAtomic* atomicCommit = null;
		#endif
		if (atomicCommit != null) {
			atomicCommit->initializeEntries();
		}

		boolean withinTransaction = false;

		while (lwfile != null) {
			inttype index = lwfile->getFileIndex();

			BufferedRandomAccessFile* vrfile = map->m_share.getVrtReadFileList()->get(index);
			MeasuredRandomAccessFile* vwfile = map->m_share.getVrtWriteFileList()->get(index);

			#ifdef DEEP_DEBUG
			if ((vrfile == null) || (vwfile == null)) {
				DEEP_LOG(ERROR, RCVRY, "Missing vrt for file index %d, %s\n", index, map->getFilePath());
				throw InvalidException("Missing vrt for file index");
			}
			#endif

			// XXX: lrt read files are only created when map->m_streamMode is set to true
			BufferedRandomAccessFile lrfile(lwfile->getPath(), "r", Properties::DEFAULT_FILE_BUFFER);
			lrfile.setFileIndex(lwfile->getFileIndex());
			lrfile.setProtocol(lwfile->getProtocol());
			lrfile.setWriter(lwfile);
			lrfile.setOnline(true); /* TODO: map->m_share.acquire(...); */

			if ((lrfile.length() > Properties::DEFAULT_FILE_HEADER) && (lrfile.length() != lrtFileLength)) {
				longtype lend = lrfile.length();
				longtype vend = vrfile->length();

				#if 0
				if ((map->m_share.getKeyProtocol() == -1) || (map->m_share.getValueSize() == -1)) {
					lend -= 4 /* value position */;
				}

				if (map->m_share.getValueSize() == -1) {
					lend -= 4 /* value size */;
				}
				#endif

				DEEP_LOG(INFO, RCVRY, "source lrt/vrt [%d] { %u / %d, %s }\n", index, lrtFileLength, -1, map->getFilePath());
				longtype prevKeyPosition = -1;
				longtype prevValuePosition = -1;
				ulongtype lrprotocol = lrfile.getProtocol();

				if ((curprotocol != 0) && (curprotocol != lrprotocol)) {
					cleanRecoveryWorkspace(ws);

					boolean cont = false;
					boolean reorg = false;
					map->indexCacheManagement(&cont, &reorg);
					map->addPagingFile(true);

					delete iter;
					return RealTimeVersion<K>::recoverRealTimeProcess(map,lwfile->getFileIndex(),lrtFileLength,lwrites);

				} else {
					uinttype totalCount = 0;
					uinttype deadCount = 0;

					RealTimeVersion<K>::template recoveryRead<RecoveryWorkspace>(map,lrfile,index,prevKeyPosition,prevValuePosition,lrtFileLength,vend,lend,ctxt->getKey1(),ws,&lwrites,lrtIndexOffset,lrtLengthOffset,&totalCount,&deadCount,dynamicSecondaries,atomicCommit,withinTransaction);

					// XXX: if manual recovery is required, either restore backup or delete irt files to get back to last good state
					if ((prevKeyPosition == -1) || (prevValuePosition == -1)) {
						if (Properties::getRecoverySafe() == true) {
							DEEP_LOG(ERROR, OTHER, "Invalid lrt/vrt (manual recovery required): %d, %s\n", index, map->getFilePath());

							String dname(".");
							String sname = map->getFilePath();
							inttype index = sname.lastIndexOf(File::separator);

							if (index != -1) {
								dname = sname.substring(0, index);
								sname = sname.substring(index + 1, sname.length() - 1);
							}

							// XXX: the following is how to programmatically remove irt files (however there might be a bad disk)
							#if 0
							MapFileUtil::clobber(map->getFilePath(), false);
							#endif

							DEEP_LOG(ERROR, OTHER, "\tRestore or manually remove irt files (e.g. rm %s/*.%s.irt)\n", dname.data(), sname.data());

							throw InvalidException("Invalid lrt/vrt (manual recovery required)");

						} else {
							prevKeyPosition = Properties::DEFAULT_FILE_HEADER;
							prevValuePosition = Properties::DEFAULT_FILE_HEADER;
						}
					}

					map->m_share.acquire(lwfile);
					map->m_share.acquire(vwfile);
					{
						vwfile->setTotalCount(totalCount);
						vwfile->setDeadCount(deadCount);

						#ifdef DEEP_DEBUG
						if (prevValuePosition < Properties::DEFAULT_FILE_HEADER) {
							DEEP_LOG(ERROR, RCVRY, "invalid vrt size [%d] { %lld / %lld, %s }\n", index, prevValuePosition, vwfile->length(), map->getFilePath());
							throw InvalidException("Invalid vrt size");
						}
						#endif

						if (prevKeyPosition > lwfile->length()) {
							#ifdef DEEP_DEBUG
							DEEP_LOG(ERROR, RCVRY, "invalid lrt size [%d] { %lld / %lld, %s }\n", index, prevKeyPosition, lwfile->length(), map->getFilePath());
							throw InvalidException("Invalid lrt size");
							#endif
							prevKeyPosition = lwfile->length();
						}

						// XXX: 1.0 DBs do not have terminated LRTs -- so, don't truncate
						if (V >= RTP_v1_1_0_0) {
							if (prevKeyPosition != lwfile->length()) {
								DEEP_LOG(WARN, RCVRY, "resize unterminated lrt [%d] { %lld => %lld, %s }\n", index, lwfile->length(), prevKeyPosition, map->getFilePath());
								withinTransaction = false;
							}
							lwfile->setLength(prevKeyPosition);
						}
						lwfile->BufferedRandomAccessFile::seek(lwfile->length());
					}
					map->m_share.release(vwfile);
					map->m_share.release(lwfile);

					DEEP_LOG(INFO, RCVRY, "adjust lrt/vrt [%d] { %lld / %lld, %s }\n", index, lwfile->length(), vwfile->length(), map->getFilePath());
				}

				curprotocol = lrprotocol;

			} else {
				map->m_currentLrtLocality = lrtFileLength <= 0 ? RealTime::Locality::LOCALITY_NONE : RealTime::Locality(lrtFileIndex, lrtFileLength, RealTime::Locality::VIEWPOINT_NONE);

				DEEP_LOG(DEBUG, RCVRY, "intact lrt/vrt [%d] { %lld / %lld, %s }\n", index, lwfile->length(), vwfile->length(), map->getFilePath());
			}

			DEEP_LOG(DEBUG, RCVRY, "commit lrt/vrt [%d] { %lld / %lld, %s } (%.1f%% done)\n", index, lwfile->length(), vwfile->length(), map->getFilePath(), RealTimeVersion<K>::recoveryPosition(&lwrites, lrtIndexOffset, lrtLengthOffset, index, lwfile->length())*100.0);

			lrtFileLength = Properties::DEFAULT_FILE_HEADER;
			lwfile = (iter->hasNext() == true) ? iter->next() : null;
		}

		delete[] dynamicSecondaries;
		delete iter;
		cleanRecoveryWorkspace(ws);
	}

	template<typename RecoveryWorkspace>
	FORCE_INLINE static void cleanRecoveryWorkspace(RecoveryWorkspace & ws) {
		RecoveryWorkspaceCleanup<RecoveryWorkspace, K>::clean(ws);
	}

	typedef typename TreeMap<K,Segment<K>*>::TreeMapEntrySet::EntrySetIterator MapSegmentEntrySetIterator;
	typedef typename SegTreeMap::TreeMapEntrySet::EntrySetIterator MapInformationEntrySetIterator;
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEPROTOCOL_V1_0_H_*/
