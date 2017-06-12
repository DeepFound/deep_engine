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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEDISCOVER_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEDISCOVER_H_ 

#include "com/deepis/db/store/relative/util/MapFileUtil.h"

#include "com/deepis/db/store/relative/core/RealTimeMap.h"

using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename K>
class RealTimeDiscover {

	typedef typename TreeMap<K,Segment<K>*>::TreeMapEntrySet::EntrySetIterator MapSegmentEntrySetIterator;

	private:
		static inttype getIndex(const char* name) {
			String path = name;

			inttype index = path.lastIndexOf(File::separator);
			if (index != -1) {
				path = path.substring(index + 1, path.length() - 1);
			}

			// XXX: 0000_00_00_00_00_00.sindex.datastore.vrt
			String sindex = path.substring(20, 5);
			inttype iindex = atoi(sindex.data());

			return iindex;
		}

		static void modelValues(RealTimeMap<K>* map, const String& dname, const String& sname, const ArrayList<String*>* list) {
			String vrtSuffix = sname + MapFileUtil::FILE_SUFFIX_VRT;
			for (int i = 0; i < list->size(); i++) {
				String* item = list->get(i);
				if (item->length() <= Properties::DEFAULT_FILE_NAME_SIZE + 1) {
					continue;
				}

				String fname = item->substring(Properties::DEFAULT_FILE_NAME_SIZE + 1, item->length() - 1);
				if (fname.equals(&vrtSuffix) == true) {
					String vrname(dname + File::separator + *item);
					if (Files::isSymbolicLink(vrname) == true) {
						String target = Files::readSymbolicLink(vrname);
						if (target.length() == 0) {
							DEEP_LOG(WARN, DCVRY, "Unable to resolve linked file, %s\n", (const char*)vrname);
						} else {
							vrname = target;
						}
					}
					File file(vrname);

					longtype protocol = 0;
					if (MapFileUtil::validate(file, MapFileUtil::VRT, map->m_share.getValueSize() /* TODO: should be schema hash */, Properties::DEFAULT_FILE_HEADER, &protocol) == false) {
						FileUtil::clobber(file);
						
						String srname = vrname.replaceLast(MapFileUtil::FILE_SUFFIX_VRT, MapFileUtil::FILE_SUFFIX_SRT);
						File srfile(srname);
						FileUtil::clobber(srfile);
						
						String xrname = vrname.replaceLast(MapFileUtil::FILE_SUFFIX_VRT, MapFileUtil::FILE_SUFFIX_XRT);
						File xrfile(xrname);
						FileUtil::clobber(xrfile);
						continue;
					}

					const ushorttype fileIndex = getIndex(file);
					const time_t creationTime = MapFileUtil::fileCreationTime(*item);

					MeasuredRandomAccessFile* vwfile = new MeasuredRandomAccessFile(file, "rw", MapFileUtil::VRT, Properties::DEFAULT_FILE_BUFFER);
					vwfile->setOptimizeCount(map->m_optimizeStream /* disguise between pre/post optimizing files */);
					vwfile->setFileIndex(fileIndex);
					#if 0
					vwfile->setProtocol(protocol);
					#else
					// TODO: since MapFileUtil validate passed, bump to current protocol
					if (protocol <= Versions::PROTOCOL_MINIMUM) {
						vwfile->setProtocol(Versions::PROTOCOL_MINIMUM);

					} else {
						vwfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
					}
					#endif
					vwfile->setFileCreationTime(creationTime);

					BufferedRandomAccessFile* vrfile = new BufferedRandomAccessFile(file, "r", Properties::DEFAULT_FILE_BUFFER);
					#if 0
					vrfile->setProtocol(protocol);
					#else
					// TODO: since MapFileUtil validate passed, bump to current protocol
					if (protocol <= Versions::PROTOCOL_MINIMUM) {
						vrfile->setProtocol(Versions::PROTOCOL_MINIMUM);

					} else {
						vrfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
					}
					#endif
					vrfile->setWriter(vwfile);
					vrfile->setFileIndex(fileIndex);
					vrfile->setFileCreationTime(creationTime);

					map->m_share.getVrtReadFileList()->add(vrfile);
					map->m_share.getVrtWriteFileList()->add(vwfile);
				}
			}
		}

		static void modelLogging(RealTimeMap<K>* map, const String& dname, const String& sname, const ArrayList<String*>* list, RealTimeLocality& initLrtLocality) {
			String lrtSuffix = sname + MapFileUtil::FILE_SUFFIX_LRT;
			for (int i = 0; i < list->size(); i++) {
				String* item = list->get(i);
				if (item->length() <= Properties::DEFAULT_FILE_NAME_SIZE + 1) {
					continue;
				}

				String fname = item->substring(Properties::DEFAULT_FILE_NAME_SIZE + 1, item->length() - 1);
				if (fname.equals(&lrtSuffix) == true) {
					String lrname(dname + File::separator + *item);
					if (Files::isSymbolicLink(lrname) == true) {
						String target = Files::readSymbolicLink(lrname);
						if (target.length() == 0) {
							DEEP_LOG(WARN, DCVRY, "Unable to resolve linked file, %s\n", (const char*)lrname);
						} else {
							lrname = target;
						}
					}
					File file(lrname);

					longtype protocol = 0;
					if (MapFileUtil::validate(file, MapFileUtil::LRT, map->m_share.getKeySize() /* TODO: should be schema hash */, Properties::DEFAULT_FILE_HEADER, &protocol) == false) {
						FileUtil::clobber(file);
						continue;
					}

					const ushorttype fileIndex = getIndex(file);
					const time_t creationTime = MapFileUtil::fileCreationTime(*item);

					MeasuredRandomAccessFile* lwfile = new MeasuredRandomAccessFile(file, "rw", MapFileUtil::LRT, Properties::DEFAULT_FILE_BUFFER);
					lwfile->setOptimizeCount(map->m_optimizeStream /* disguise between pre/post optimizing files */);
					lwfile->setFileIndex(fileIndex);
					#if 0
					lwfile->setProtocol(protocol);
					#else
					// TODO: since MapFileUtil validate passed, bump to current protocol
					if (protocol <= Versions::PROTOCOL_MINIMUM) {
						lwfile->setProtocol(Versions::PROTOCOL_MINIMUM);

					} else {
						lwfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
					}
					#endif
					lwfile->setFileCreationTime(creationTime);

					map->m_share.getLrtWriteFileList()->add(lwfile);

					map->m_share.acquire(lwfile);
					{
						initLrtLocality = RealTimeLocality(lwfile->getFileIndex(), lwfile->length(), RealTimeLocality::VIEWPOINT_NONE);
					}
					map->m_share.release(lwfile);
				}
			}
		}

		static void modelIndexing(RealTimeMap<K>* map, const String& dname, const String& sname, const ArrayList<String*>* list, inttype lrtSize, boolean irtInvalid) {
			String irtSuffix = sname + MapFileUtil::FILE_SUFFIX_IRT;
			for (int i = 0; i < list->size(); i++) {
				String* item = list->get(i);
				if (item->length() <= Properties::DEFAULT_FILE_NAME_SIZE + 1) {
					continue;
				}

				String fname = item->substring(Properties::DEFAULT_FILE_NAME_SIZE + 1, item->length() - 1);
				if (fname.equals(&irtSuffix) == true) {
					String irname(dname + File::separator + *item);
					if (Files::isSymbolicLink(irname) == true) {
						String target = Files::readSymbolicLink(irname);
						if (target.length() == 0) {
							DEEP_LOG(WARN, DCVRY, "Unable to resolve linked file, %s\n", (const char*)irname);
						} else {
							irname = target;
						}
					}
					File file(irname);

					longtype protocol = 0;
					if (MapFileUtil::validate(file, MapFileUtil::IRT, map->m_share.getKeySize() /* TODO: should be schema hash */, Properties::DEFAULT_FILE_HEADER, &protocol) == false) {
						//DEEP_LOG(WARN, OTHER, "Invalid file, irt size not valid: %s\n", file.data());
						FileUtil::clobber(file);
						continue;
					}

					if (lrtSize == 0) {
						//DEEP_LOG(WARN, OTHER, "Invalid file, lrt/vrt not found: %s\n", file.data());
						FileUtil::clobber(file);
						continue;
					}

					if (irtInvalid == true) {
						FileUtil::clobber(file);
						continue;
					}

					const ushorttype fileIndex = getIndex(file);
					const time_t creationTime = MapFileUtil::fileCreationTime(*item);

					MeasuredRandomAccessFile* iwfile = new MeasuredRandomAccessFile(file, "rw", MapFileUtil::IRT, map->m_irtBuffer);
					iwfile->setOptimizeCount(map->m_optimizePaging /* disguise between pre/post optimizing files */);
					iwfile->setFileIndex(fileIndex);
					#if 0 
					iwfile->setProtocol(protocol);
					#else
					// TODO: since MapFileUtil validate passed, bump to current protocol
					if (protocol <= Versions::PROTOCOL_MINIMUM) {
						iwfile->setProtocol(Versions::PROTOCOL_MINIMUM);

					} else {
						iwfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
					}
					#endif
					iwfile->setFileCreationTime(creationTime);
					iwfile->setInitialLength(iwfile->length());

					BufferedRandomAccessFile* irfile = new BufferedRandomAccessFile(file, "r", map->m_irtBuffer);
					#if 0 
					irfile->setProtocol(protocol);
					#else
					// TODO: since MapFileUtil validate passed, bump to current protocol
					if (protocol <= Versions::PROTOCOL_MINIMUM) {
						irfile->setProtocol(Versions::PROTOCOL_MINIMUM);

					} else {
						irfile->setProtocol(Versions::GET_PROTOCOL_CURRENT());
					}
					#endif
					irfile->setWriter(iwfile);
					irfile->setFileIndex(fileIndex);
					irfile->setFileCreationTime(creationTime);

					map->m_share.getIrtReadFileList()->add(irfile);
					map->m_share.getIrtWriteFileList()->add(iwfile);
				}
			}
		}
		
		static void modelSchema(RealTimeMap<K>* map, const String& dname, const String& sname, const ArrayList<String*>* list, inttype lrtSize) {
			String srtSuffix = sname + MapFileUtil::FILE_SUFFIX_SRT;
			
			if (map->m_primaryIndex != null) {
				inttype index = srtSuffix.indexOf(".");
				if (index == -1) {
					DEEP_LOG(ERROR, OTHER, "Invalid index file name: %s\n", sname.data());

					throw InvalidException("Invalid index file name");
				}

				srtSuffix = srtSuffix.substring(index + 1, srtSuffix.length() - 1);
			}
			
			for (int i = 0; i < list->size(); i++) {
				String* item = list->get(i);
				if (item->length() <= Properties::DEFAULT_FILE_NAME_SIZE + 1) {
					continue;
				}

				const String fname = item->substring(Properties::DEFAULT_FILE_NAME_SIZE + 1, item->length() - 1);
				if (fname.equals(&srtSuffix) == true) {
					String srname(dname + File::separator + *item);
					if (Files::isSymbolicLink(srname) == true) {
						String target = Files::readSymbolicLink(srname);
						if (target.length() == 0) {
							DEEP_LOG(WARN, DCVRY, "Unable to resolve linked file, %s\n", (const char*)srname);
						} else {
							srname = target;
						}
					}
					File file(srname);

					// XXX: srt may have been removed by modelValues()
					if (file.exists() == false) {
						continue;
					}

					if ((lrtSize == 0) || (file.length() == 0)) {
						//DEEP_LOG(WARN, OTHER, "Invalid file, lrt/vrt not found: %s\n", file.data());
						FileUtil::clobber(file);
						continue;
					}

					const ushorttype fileIndex = getIndex(file);
					const time_t creationTime = MapFileUtil::fileCreationTime(*item);

					RealTimeShare* share = null;
					RandomAccessFile* srfile = null;
					RandomAccessFile* swfile = null;
					if (map->m_primaryIndex != null) {
						share = ((RealTimeMap<K>*) map->m_primaryIndex)->getShare();
						srfile = share->getSrtReadFileList()->last();
						swfile = share->getSrtWriteFileList()->last();

						share->acquire(srfile);

					} else {
						share = map->getShare();
						srfile = new RandomAccessFile(file, "r", false);
						srfile->setProtocol(Versions::GET_PROTOCOL_CURRENT()); // TODO:
						srfile->setFileIndex(fileIndex);
						srfile->setFileCreationTime(creationTime);
						
						swfile = new RandomAccessFile(file, "rw", false);
						swfile->setProtocol(Versions::GET_PROTOCOL_CURRENT()); // TODO:
						swfile->setFileIndex(fileIndex);
						swfile->setFileCreationTime(creationTime);
						
						map->m_share.acquire(srfile);
						
						map->m_share.getSrtReadFileList()->add(srfile);
						map->m_share.getSrtWriteFileList()->add(swfile);
					}

					srfile->syncFilePointer();
					longtype term = RealTimeSchema_v1<K>::validate(srfile)+1;
					// XXX: check if there is a marker, but it is not at the end of the file
					if ((term < srfile->length()) && (term > 0)) {
						DEEP_LOG(WARN, RCVRY, "unterminated srt: %lld / %lld, %s\n", term, srfile->length(), map->getFilePath());

						share->acquire(swfile);
						{
							swfile->setLength(term, true);
						}
						share->release(swfile);
					}
					
					SchemaBuilder<K>* schemaBuilder = RealTimeSchema_v1<K>::read(srfile, map->m_schemaBuilder->getKeyName(), (map->m_primaryIndex == null));
					if ((schemaBuilder == null) && ((map->m_primaryIndex == null) || ((map->m_primaryIndex->getCurrentLrtLocality().getLength() > Properties::DEFAULT_FILE_HEADER) && (map->m_share.getIrtWriteFileList()->size() > 0)))) {
						DEEP_LOG(ERROR, OTHER, "Key not found when reading schema: %s\n", map->m_schemaBuilder->getKeyName());

						// XXX: check if there was no marker at all
						if (term <= 0) {
							DEEP_LOG(ERROR, RCVRY, "invalid srt: %s\n", map->getFilePath());

							throw InvalidException("Invalid srt");
						}

						throw InvalidException("Key not found when reading schema");
					}

					share->release(srfile);

					inttype cmp = 0;
					if (schemaBuilder != null) {
						cmp = map->m_schemaBuilder->compare(schemaBuilder);
						((SchemaBuilder<K>*) map->m_schemaBuilder)->setAutoIncrementValue(schemaBuilder->getAutoIncrementValue());
						// XXX: preserve the following even if server is restarted using different options
						((SchemaBuilder<K>*) map->m_schemaBuilder)->setIndexOrientation(schemaBuilder->getIndexOrientation());
						((SchemaBuilder<K>*) map->m_schemaBuilder)->setKeyCompression(schemaBuilder->getKeyCompression());
						((SchemaBuilder<K>*) map->m_schemaBuilder)->setValueCompression(schemaBuilder->getValueCompression());
						((SchemaBuilder<K>*) map->m_schemaBuilder)->setLrtIndex(schemaBuilder->getLrtIndex());
						((SchemaBuilder<K>*) map->m_schemaBuilder)->setLrtPosition(schemaBuilder->getLrtPosition());
						((SchemaBuilder<K>*) map->m_schemaBuilder)->setDirectoryPaths(schemaBuilder->getDataDirectory(), schemaBuilder->getIndexDirectory());
						map->m_keyCompressMode = schemaBuilder->getKeyCompression();
						map->m_valueCompressMode = schemaBuilder->getValueCompression();
					}

					delete schemaBuilder;

					if (cmp != 0) {
						#if 0
						DEEP_LOG(ERROR, OTHER, "Schema definition mismatch: %s\n", file.data());

						throw InvalidException("Schema definition mismatch");
						#endif
					}
				}
			}
		}

		static void modelStatistics(RealTimeMap<K>* map, const String& dname, const String& sname, const ArrayList<String*>* list, inttype lrtSize) {
			String xrtSuffix = sname + MapFileUtil::FILE_SUFFIX_XRT;
			
			if (map->m_primaryIndex != null) {
				inttype index = xrtSuffix.indexOf(".");
				if (index == -1) {
					DEEP_LOG(ERROR, OTHER, "Invalid index file name: %s\n", sname.data());

					throw InvalidException("Invalid index file name");
				}

				xrtSuffix = xrtSuffix.substring(index + 1, xrtSuffix.length() - 1);
			}
			
			for (int i = 0; i < list->size(); i++) {
				String* item = list->get(i);
				if (item->length() <= Properties::DEFAULT_FILE_NAME_SIZE + 1) {
					continue;
				}

				const String fname = item->substring(Properties::DEFAULT_FILE_NAME_SIZE + 1, item->length() - 1);
				if (fname.equals(&xrtSuffix) == true) {
					String xrname(dname + File::separator + *item);
					if (Files::isSymbolicLink(xrname) == true) {
						String target = Files::readSymbolicLink(xrname);
						if (target.length() == 0) {
							DEEP_LOG(WARN, DCVRY, "Unable to resolve linked file, %s\n", (const char*)xrname);
						} else {
							xrname = target;
						}
					}
					File file(xrname);

					// XXX: xrt may have been removed by modelValues()
					if (file.exists() == false) {
						continue;
					}

					if ((lrtSize == 0) || (file.length() == 0)) {
						//DEEP_LOG(WARN, OTHER, "Invalid file, lrt/vrt not found: %s\n", file.data());
						FileUtil::clobber(file);
						continue;
					}

					const ushorttype fileIndex = getIndex(file);
					const time_t creationTime = MapFileUtil::fileCreationTime(*item);

					RealTimeShare* share = null;
					RandomAccessFile* xrfile = null;
					RandomAccessFile* xwfile = null;
					if (map->m_primaryIndex != null) {
						share = ((RealTimeMap<K>*) map->m_primaryIndex)->getShare();
						xrfile = share->getXrtReadFileList()->last();
						xwfile = share->getXrtWriteFileList()->last();

						share->acquire(xrfile);

					} else {
						share = map->getShare();
						xrfile = new RandomAccessFile(file, "r", false);
						xrfile->setProtocol(Versions::GET_PROTOCOL_CURRENT()); // TODO:
						xrfile->setFileIndex(fileIndex);
						xrfile->setFileCreationTime(creationTime);
						
						xwfile = new RandomAccessFile(file, "rw", false);
						xwfile->setProtocol(Versions::GET_PROTOCOL_CURRENT()); // TODO:
						xwfile->setFileIndex(fileIndex);
						xwfile->setFileCreationTime(creationTime);
						
						map->m_share.acquire(xrfile);
						
						map->m_share.getXrtReadFileList()->add(xrfile);
						map->m_share.getXrtWriteFileList()->add(xwfile);
					}

					xrfile->syncFilePointer();

					longtype term = ExtraStatistics_v1::validate(xrfile)+1;
					// XXX: check if there is a marker, but it is not at the end of the file
					if ((term < xrfile->length()) && (term > 0)) {
						DEEP_LOG(WARN, RCVRY, "unterminated xrt: %lld / %lld, %s\n", term, xrfile->length(), map->getFilePath());

						share->acquire(xwfile);
						{
							xwfile->setLength(term, true);
						}
						share->release(xwfile);
					}
					
					if (ExtraStatistics_v1::read(xrfile, &(map->m_extraStats), map->m_schemaBuilder->getKeyName(), (map->m_primaryIndex == null), map->m_schemaBuilder->getValueCompression()) == false) {
						DEEP_LOG(ERROR, OTHER, "Invalid xrt file, %s\n", map->getFilePath());

						throw InvalidException("Invalid xrt file");
					}

					share->release(xrfile);

					// XXX: set key statistic values from last shutdown
					MapFileSet<MeasuredRandomAccessFile> iwfiles(*(map->m_share.getIrtWriteFileList()));
					Iterator<MeasuredRandomAccessFile*>* iter = iwfiles.iterator();
					while (iter->hasNext() == true) {
						MeasuredRandomAccessFile* iwfile = iter->next();

						ushorttype fileIndex = iwfile->getFileIndex();

						uinttype deadKeyCount = map->m_extraStats.getKeyFragmentationDeadCount(fileIndex);
						uinttype totalKeyCount = map->m_extraStats.getKeyFragmentationTotalCount(fileIndex);

						iwfile->setDeadCount(deadKeyCount);
						iwfile->setTotalCount(totalKeyCount);
					}
					Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iter);

					// XXX: set value statistic values from last shutdown
					if (map->m_primaryIndex == null) {
						ushorttype fileIndex = xwfile->getFileIndex();

						MeasuredRandomAccessFile* vwfile = map->m_share.getVrtWriteFileList()->get(fileIndex);
						if (vwfile == null) {
							DEEP_LOG(WARN, OTHER, "invalid vrt/xrt pairing; no such vrt [%u], %s\n", fileIndex, map->getFilePath());
							continue;
						}

						if (map->m_valueCompressMode == true) {
							vwfile->setCompressTotal(((((doubletype) map->m_extraStats.getCompressionPercentage(fileIndex)) / 100.0) * vwfile->length()) + 1 /* round up */);
						}

						uinttype deadValueCount = map->m_extraStats.getValueFragmentationDeadCount(fileIndex);
						uinttype totalValueCount = map->m_extraStats.getValueFragmentationTotalCount(fileIndex);

						vwfile->setDeadCount(deadValueCount);
						vwfile->setTotalCount(totalValueCount);
					}
				}
			}
		}

		static void modelClosure(RealTimeMap<K>* map) {
			if (map->m_share.getLrtWriteFileList()->size() != 0) {
				MeasuredRandomAccessFile* lwfile = map->m_share.getLrtWriteFileList()->last();
				lwfile->setClosureFile(true);
			}

			if (map->m_share.getVrtWriteFileList()->size() != 0) {
				MeasuredRandomAccessFile* vwfile = map->m_share.getVrtWriteFileList()->last();
				vwfile->setClosureFile(true);
			}

			if (map->m_share.getIrtWriteFileList()->size() != 0) {
				MeasuredRandomAccessFile* iwfile = map->m_share.getIrtWriteFileList()->last();
				iwfile->setClosureFile(true);
			}
		}

	public:
		FORCE_INLINE static boolean validateIndexing(RealTimeMap<K>* map, MapFileSet<BufferedRandomAccessFile>& ireads, MapFileSet<MeasuredRandomAccessFile>& iwrites, const RealTimeLocality& initLrtLocality, boolean& foundIrt, boolean &irtInvalid) {
			inttype lastSummarized = -1;
			inttype lastValid = -1;
			const inttype ifileCount = ireads.size();
			boolean isEmpty = (ifileCount == 0);

			Iterator<BufferedRandomAccessFile*>* irIter = ireads.iterator();
			Iterator<MeasuredRandomAccessFile*>* iwIter = iwrites.iterator();

			BufferedRandomAccessFile* irfile = null;
			MeasuredRandomAccessFile* iwfile = null;

			uinttype recoveryEpoch = map->getRecoveryEpoch();

			// XXX: determine the state of the irts (from first to last); stop at the first invalid file
			for (inttype i = 0; (irtInvalid == false) && (i < ifileCount); ++i) {
				irfile = irIter->next();
				iwfile = iwIter->next();

				bytetype state;
				{
					RealTimeLocality lastLrtLocality = RealTimeLocality::LOCALITY_NONE;
					longtype lastIrtLength = -1;

					// XXX: map->m_recoverLrtIndex or map->m_recoverLrtLength might be set in the following method
					state = RealTimeVersion<K>::validateKeyPaging(iwfile, map, (i == (ifileCount - 1)), lastLrtLocality, &lastIrtLength);
					iwfile->setPagingState(map->m_indexValue, state, lastIrtLength, lastLrtLocality);
					if (iwfile->getRecoveryEpoch() > recoveryEpoch) {
						recoveryEpoch = iwfile->getRecoveryEpoch();
					}
				}

				switch(state) {
					case RealTimeMap<K>::SUMMARY_INTACT:
						lastSummarized = iwfile->getFileIndex();
						lastValid = iwfile->getFileIndex();
						break;
					case RealTimeMap<K>::SUMMARY_IGNORE:
						lastValid = iwfile->getFileIndex();
						break;
					case RealTimeMap<K>::SUMMARY_EMPTY:
						if ((i == 0) && (ifileCount == 1)) {
							isEmpty = true;
						}
						break;
					default:
						DEEP_LOG(ERROR, OTHER, "Invalid irt file: %s\n", irfile->getPath());
						irtInvalid = true;
						break;
				}
			}

			// XXX: Rename invalid IRTs to ERTs
			if (irtInvalid == true) {
				while (true) {
					if (irfile != null && iwfile != null) {
						irIter->remove();
						iwIter->remove();

						FileUtil::move(iwfile->getPath(), iwfile->getPath() + MapFileUtil::FILE_SUFFIX_ERT);

						map->m_share.destroy(irfile);
						map->m_share.destroy(iwfile);
					}

					if ((irIter->hasNext() == true) && (iwIter->hasNext() == true)) {
						irfile = irIter->next();
						iwfile = iwIter->next();

						if (iwfile->getRecoveryEpoch() > recoveryEpoch) {
							recoveryEpoch = iwfile->getRecoveryEpoch();
						}
					} else {
						break;
					}
				}
			}

			Converter<Iterator<BufferedRandomAccessFile*>*>::destroy(irIter);
			Converter<Iterator<MeasuredRandomAccessFile*>*>::destroy(iwIter);

			foundIrt = (lastValid >= 0);
			irtInvalid |= (initLrtLocality.isNone() == false) && (isEmpty == false) && ((lastSummarized < 0) || (iwrites.get(lastSummarized)->getLastLrtLocality() != initLrtLocality));

			if (map->m_primaryIndex == null) {
				map->setRecoveryEpoch(recoveryEpoch);
			} else {
				map->m_primaryIndex->setRecoveryEpoch(recoveryEpoch);
			}

			return true;
		}

		static boolean statically(RealTimeMap<K>* map, boolean reserve) {

			map->m_hasReservation = reserve;

			/* XXX: allow for memory only configuration see things in 'read-only'
			if (map->m_memoryMode == true) {
				return true;
			}
			*/

			String dname(".");
			String sname = map->getFilePath();

			inttype index = sname.lastIndexOf(File::separator);
			if (index != -1) {
				dname = sname.substring(0, index);
				sname = sname.substring(index + 1, sname.length() - 1);
			}

			File directory(dname);
			ArrayList<String*>* list = File(dname).list();
			boolean irtInvalid = false;
			if (list != null) {
				Collections::sort(list);

				const boolean hotAdd = map->m_dynamic;

				RETRY:
				boolean foundIrt = false;
				inttype lrtSize = 0;
				RealTimeLocality initLrtLocality = RealTimeLocality::LOCALITY_NONE;

				if (hotAdd == true) {
					map->deleteCacheAndFileMemory();
				}

				if (map->m_primaryIndex == null) {
					modelValues(map, dname, sname, list);

					modelLogging(map, dname, sname, list, initLrtLocality /* modified */);

					if (Properties::getAllowLrtVrtMismatch() == false) {
						if (map->m_share.getLrtWriteFileList()->size() != map->m_share.getVrtWriteFileList()->size()) {
							DEEP_LOG(ERROR, OTHER, "Invalid lrt/vrt pairing, %s\n", map->getFilePath());

							throw InvalidException("Invalid lrt/vrt pairing");
						}
					}

					map->m_currentLrtLocality = initLrtLocality;
					lrtSize = map->m_share.getLrtWriteFileList()->size();

				} else {
					initLrtLocality = map->m_primaryIndex->getCurrentLrtLocality();
					lrtSize = map->m_primaryIndex->getShare()->getLrtWriteFileList()->size();
				}

				modelIndexing(map, dname, sname, list, lrtSize, irtInvalid);

				#ifdef DEEP_DEBUG
				RealTimeShare* share = (map->m_primaryIndex == null) ? map->getShare() : map->m_primaryIndex->getShare();
				if (share->getSrtWriteFileList()->tryLock() == false) {
					DEEP_LOG(ERROR, OTHER, "Invalid schema locking, %s\n", map->getFilePath());

					throw InvalidException("Invalid schema locking");
				}

				share->getSrtWriteFileList()->unlock();
				#endif

				modelSchema(map, dname, sname, list, lrtSize);

				modelStatistics(map, dname, sname, list, lrtSize);

				// XXX: indicate which files where used last, to be used next
				modelClosure(map);

				// XXX: validate (i.e. error check and seed "first key" segment paging)
				{
					/* TODO: check vrt/lrt/srt/xrt file list pairing */

					// XXX: These are in chronological "discovery" order (populated in orderIndexing() before reorder)
					MapFileSet<BufferedRandomAccessFile>& ireads = *map->m_share.getIrtReadFileList();
					MapFileSet<MeasuredRandomAccessFile>& iwrites = *map->m_share.getIrtWriteFileList();

					if ((hotAdd == false) && (validateIndexing(map, ireads, iwrites, initLrtLocality, foundIrt, irtInvalid) == false)) {
						// XXX: right now, this never happens...
						goto RETRY;
					}
				}

				delete list;
			}

			map->seedFinalFile();

			// XXX: unconditionally defer log order and auto-inc because, even if this map doesn't need recovery,
			//  we don't know if we need recovery overall until the last secondary is discovered

			return (irtInvalid == false);
		}

		static boolean dynamically(RealTimeMap<K>* map, boolean reserve) {
			// TODO: implement dynamic discover
			return false;
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEDISCOVER_H_*/
