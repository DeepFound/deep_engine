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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECOMPRESS_CXX_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECOMPRESS_CXX_

#include <zlib.h>
#include "cxx/lang/Math.h"
#include "cxx/lang/System.h"

#include "com/deepis/db/store/relative/core/RealTimeBuilder.h"
#include "com/deepis/db/store/relative/core/RealTimeConverter.h"
#include "com/deepis/db/store/relative/core/RealTimeShare.h"

using namespace cxx::io;
using namespace cxx::lang;
using namespace cxx::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template <typename K> class RealTimeMap;
template <typename K> class RealTimeVersion;
template <typename K> class RealTimeAdaptive_v1;

template<typename K>
class RealTimeCompress {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;
	typedef typename SegTreeMap::TreeMapEntrySet::EntrySetIterator MapInformationEntrySetIterator;

	public:

		#define SIZE_SIZE 2

		inline static void compress(RealTimeMap<K>* map, ThreadContext<K>* ctxt, Segment<K>* segment, const RealTimeShare* realTimeShare,  const KeyBuilder<K>* keyBuilder) {
			#ifdef DEEP_DEBUG
			if (segment->getDirty() == true /* || segment->tryLock() == true */ || segment->getPurged() == true) {
				DEEP_LOG(ERROR, OTHER, "Invalid compress: segment state %d%d\n", segment->getDirty()?1:0, segment->getPurged()?1:0);

				throw InvalidException("Invalid compress: segment state");
			}
			#endif

			inttype keySize = realTimeShare->getKeySize();
			inttype valueSize = realTimeShare->getValueSize();
			inttype valueAverage = realTimeShare->getValueAverage() > -1 ? realTimeShare->getValueAverage() : 1;

			boolean useValue = RealTimeAdaptive_v1<K>::allowValueMemoryCompression(map, segment);

			boolean fixedKey = (keySize != -1);
			boolean fixedValue = (valueSize != -1);

			ulongtype dataSize = ((fixedKey ? keySize : 100) * (segment->vsize() - 1)) + (Information::getInMemoryCompressedSize() * segment->vsize());
			if (useValue == true) {
				dataSize += ((fixedValue ? valueSize : valueAverage) * segment->vsize());
			}

			// XXX: estimate high to avoid resizing
			dataSize += 100;

			Bytef* indata = new Bytef[dataSize];
			memcpy(indata, &useValue, 1);

			ulongtype sizein = 1;
			typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);

			segment->SegTreeMap::entrySet(&stackSegmentItemSet);
			MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();

			// XXX: skip first entry
			K firstKey = segment->SegTreeMap::firstKey();
			infoIter->MapInformationEntrySetIterator::next();

			while (infoIter->MapInformationEntrySetIterator::hasNext()) {
				SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();
				Information* info = infoEntry->getValue();

				#ifdef DEEP_DEBUG
				StoryLine& storyLine = infoEntry->getStoryLine();
				if (((info->getData() == null) && (useValue == true)) || (info->getLevel() != Information::LEVEL_COMMIT)) {
					DEEP_LOG(ERROR, OTHER, "Invalid compress: info state %d, %d, %d\n", storyLine.getStoryLock()?1:0, info->getNext()?1:0, info->getLevel());

					throw InvalidException("Invalid compress: info state");
				}
				#endif

				K key = infoEntry->getKey();

				if (fixedKey == false) {
					keySize = keyBuilder->getPackLength(Converter<K>::toData(key));
					while (dataSize < (sizein + SIZE_SIZE)) {
						 resize(&indata, dataSize);
					}
					memcpy(indata + sizein, &keySize, SIZE_SIZE);
					sizein += SIZE_SIZE;
				}

				bytearray keydata = Converter<K>::toData(key);
				while (dataSize < (sizein + keySize)) {
					 resize(&indata, dataSize);
				}
				memcpy(indata + sizein, keydata, keySize);
				sizein += keySize;	

				// XXX: copy info state (state includes info type) 
				ubytetype compressedStateSize = Information::getInMemoryCompressedSize(info);
				bytearray infodata = info->getBytes();

				while (dataSize < (sizein + compressedStateSize)) {
					 resize(&indata, dataSize);
				}
				memcpy(indata + sizein, infodata, compressedStateSize);
				sizein += compressedStateSize;
				delete [] infodata;

				// XXX: copy value size
				if (fixedValue == false) {
					valueSize = info->getSize();
					while (dataSize < (sizein + SIZE_SIZE)) {
						 resize(&indata, dataSize);
					}
					memcpy(indata + sizein, &valueSize, SIZE_SIZE);
					sizein += SIZE_SIZE;	
				}

				if (useValue == true) {
					// XXX: copy info data
					bytearray valuedata = info->getData();
					while (dataSize < (sizein + valueSize)) {
						 resize(&indata, dataSize);
					}
					memcpy(indata + sizein, valuedata, valueSize);
					sizein += valueSize;
				}
			}

			Information* firstValue = segment->SegTreeMap::remove(firstKey);
			segment->SegTreeMap::clear(true, true);
			segment->SegTreeMap::put(firstKey, firstValue);

			#ifdef DEEP_DEBUG
			if (sizein > LONG_MAX) {
				DEEP_LOG(ERROR, OTHER, "Invalid compress: compress size %lld\n", sizein);

				throw InvalidException("Invalid compress: compress size");
			}
			#endif

			// XXX: zlib uses ulongtype, but size will never be larger than max inttype
			segment->setUncompressedSize(sizein);

			z_stream inZstream;

			inZstream.zfree = Z_NULL;
			inZstream.zalloc = Z_NULL;
			inZstream.opaque = Z_NULL;

			inttype code = deflateInit(&inZstream, Z_DEFAULT_COMPRESSION);
			if (code != Z_OK) {
				compressionError(code, 0);
			}

			Bytef* outdata = new Bytef[dataSize];

			deflateData(&inZstream, /*in data*/ (Bytef*)indata, /*in data size */ sizein, (Bytef**)&outdata, /*total avail*/ dataSize);

			delete [] indata;

			code = deflateEnd(&inZstream);
			if (code != Z_OK) {
				compressionError(code, 1);
			}

			segment->setZipSize(inZstream.total_out);
			segment->setZipData((bytearray) new char[inZstream.total_out]);

			if (useValue == true) {
				map->setCompressionRatioKeyValue(sizein / inZstream.total_out);
			} else {
				map->setCompressionRatioKey(sizein / inZstream.total_out);
			}

			memcpy(segment->getZipData(), outdata, inZstream.total_out);
			delete [] outdata;
		} 

		inline static void decompress(RealTimeMap<K>* map, Segment<K>* segment, const RealTimeShare* realTimeShare,  KeyBuilder<K>* keyBuilder) {

			#ifdef DEEP_DEBUG
			if (segment->getDirty() == true /* || segment->tryLock() == true */ || segment->getPurged() == false) {
				DEEP_LOG(ERROR, OTHER, "Invalid decompress: segment state %d%d\n", segment->getDirty()?1:0, segment->getPurged()?1:0);

				throw InvalidException("Invalid decompress: segment state");
			}
			#endif

			inttype keySize = realTimeShare->getKeySize();
			inttype valueSize = realTimeShare->getValueSize();
			boolean fixedKey = (keySize != -1);
			boolean fixedValue = (valueSize != -1);

			// XXX: zlib uses ulongtype, but size will never be larger than max inttype
			ulongtype dataSize = segment->getUncompressedSize();

			Bytef* data = new Bytef[dataSize];
			z_stream outZstream = {0};

			outZstream.zfree = Z_NULL;
			outZstream.zalloc = Z_NULL;
			outZstream.opaque = Z_NULL;
			outZstream.next_in = Z_NULL;

			inttype code = inflateInit(&outZstream);
			if (code != Z_OK) {
				compressionError(code, 2);
			}
			
			outZstream.next_in = (Bytef*) segment->getZipData();
			do {
				#if 0 //should not happen
				if (outZstream.total_out != 0) {
					resize(&data, dataSize);
				}
				#endif

				outZstream.avail_out = dataSize - outZstream.total_out;
				outZstream.next_out = (Bytef*) data + outZstream.total_out;
				outZstream.next_in = (Bytef*) segment->getZipData() + outZstream.total_in;
				outZstream.avail_in = segment->getZipSize() - outZstream.total_in;
				
				code =  inflate(&outZstream, Z_FINISH);
				if (code != Z_STREAM_END /* should not happen && code != Z_OK */)
				{
					compressionError(code, 3);
				}

			} while (code == Z_OK && outZstream.avail_out == 0);

			#ifdef DEEP_DEBUG
			if (outZstream.total_out != dataSize) {
				DEEP_LOG(ERROR, OTHER, "Invalid decompress: inflate sizing %ld %lld \n", outZstream.total_out, dataSize);

				throw InvalidException("Invalid decompress: inflate sizing");
			}
			#endif

			code = inflateEnd(&outZstream);
			if (code != Z_OK) {
				compressionError(code, 4);
			}

			ulongtype offset = 0;

			#ifdef DEEP_DEBUG
			if (segment->size() != 1) {
				DEEP_LOG(ERROR, OTHER, "Invalid decompress: segment size is not 1 (%d)\n", segment->size());	
				
				throw InvalidException("Invalid decompress: segment size is not 1");
			}
			#endif

			boolean useValue = false;
			memcpy(&useValue, data, 1);
			offset += 1;

			boolean sequential = true;
			K retKey = (K) Converter<K>::NULL_VALUE;

			while (offset < outZstream.total_out) {

				Information* info = null;
				K key = (K) Converter<K>::NULL_VALUE;
				bytearray keydata = null; 
				
				if (fixedKey == false) {
					keySize = 0;
					memcpy(&keySize, data + offset, SIZE_SIZE);
					offset += SIZE_SIZE;
				}	
				keydata = (bytearray) data + offset;
				offset += keySize;

				info = Information::newInfo(Information::WRITE);
				info->resetIndexed(0xffffffffffffffff);

				info->setBytes((bytearray) data + offset);
				offset += Information::getInMemoryCompressedSize(info);

				if (fixedValue == false) {
					valueSize = 0;
					memcpy(&valueSize, data + offset, SIZE_SIZE);
					offset += SIZE_SIZE;
				}
				info->setSize(valueSize);

				if (useValue == true) {
					info->initData();

					memcpy(info->getData(), data + offset, valueSize);
					offset += info->getSize();
				}
				
				key = keyBuilder->newKey();
				key = keyBuilder->fillKey(keydata, key);
				Information* oldinfo = segment->add(key, info, &retKey, &sequential, false /* replace */);

				if (oldinfo != null) {
					Converter<K>::destroy(key);
					Converter<Information*>::destroy(info);
				}
			} 

			segment->freeZipData();
			delete [] data;
		}

		inline static void buildLoggingString(RealTimeMap<K>* map, String* str, inttype count) {
			char num[11] = {0};
			
			sprintf(num, "%d", count);
			(*str).append("memory compressed: ").append(num).append(",");

			if (map->getCompressionRatioKeyValue() != CHAR_MIN) {
				sprintf(num, "%d", map->getCompressionRatioKeyValue());
                       		(*str).append(" ratio K/V: ").append(num); 
                        }

			if (map->getCompressionRatioKey() != CHAR_MIN) {
				sprintf(num, "%d", map->getCompressionRatioKey());
                       		(*str).append(" ratio K: ").append(num); 
                        }
		}	

	private:

		inline static void deflateData(z_stream* inZstream, Bytef* indata, ulongtype indatasize, Bytef** outdata, ulongtype &totalAvail) {
			int code = 0;

			inZstream->next_in = indata;
			do {
				while ((totalAvail - inZstream->total_out) <= indatasize) {
					resize(outdata, totalAvail);
				}

				inZstream->avail_out = totalAvail - inZstream->total_out; 
				inZstream->next_out  = (Bytef*) (*outdata + inZstream->total_out);
				inZstream->next_in   = (Bytef*) (indata + inZstream->total_in);
				inZstream->avail_in  = indatasize - inZstream->total_in;;

				code = deflate(inZstream, Z_FINISH);
				if (code != Z_STREAM_END  && code != Z_OK) {
					compressionError(code, 5);
				}

			} while (code == Z_OK && inZstream->avail_out == 0);
		}

		inline static void resize(Bytef** arry, ulongtype &size) {
			// XXX: zlib uses ulongtype, but size will never be larger than max inttype
			Bytef* tmp = new Bytef[size * 2];
			memcpy(tmp, *arry, size);
			
			delete [] *arry;
			*arry = tmp;
			size *= 2;
		}

		FORCE_INLINE static void compressionError(inttype code, bytetype location) {
			DEEP_LOG(ERROR, OTHER, "invalid compression code: %d location %d \n", code, location);

			throw InvalidException("invalid compression code");
		}
	};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECOMPRESS_CXX_*/
