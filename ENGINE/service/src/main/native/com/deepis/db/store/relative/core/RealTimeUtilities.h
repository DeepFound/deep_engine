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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEUTILITIES_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEUTILITIES_H_

#include "cxx/io/FileUtil.h"

#include "com/deepis/db/store/relative/util/BufferedRandomAccessFile.h"

#include "com/deepis/db/store/relative/core/Information.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.h"

using namespace com::deepis::core::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

// TODO: maybe find a home for this in PLATFORM

#define OBJECT_STACK_FINALIZER(construct, destruct)	\
	struct Finally {				\
		Finally() { construct; }		\
		~Finally() { destruct; }		\
	} finalizer;

#define KEY_TO_STRING(buffer,key) 			\
	char buffer[4096]; 				\
	memset(buffer, 0, 4096); 			\
	m_keyBuilder->toString(key, buffer);

#define BUILDER_KEY_TO_STRING(builder,buffer,key)	\
	char buffer[4096]; 				\
	memset(buffer, 0, 4096); 			\
	builder->toString(key, buffer);


template<typename K>
class RealTimeUtilities {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;
	typedef typename SegTreeMap::TreeMapEntrySet::EntrySetIterator MapInformationEntrySetIterator;
	typedef typename TreeMap<K,Segment<K>*>::TreeMapEntrySet::EntrySetIterator MapSegmentEntrySetIterator;

	public:

		static void printConductor(RealTimeMap<K>* map, RealTimeConductor<K>* conductor) {
			printf("\n<---------- Conductor %p contents ---------->\n\n", conductor);

			if (conductor != null) {
				const SegMapEntry* infoEntry = null;
				K key =  (K) Converter<K>::NULL_VALUE;

				for (int i = conductor->size() - 1; i >= 0; i--) {
					infoEntry = conductor->get(i);
					key = infoEntry->getKey();
					
					printStory(map, infoEntry->getValue(), Converter<K>::toData(key));
				}
			}
		}

		static void printSegment(RealTimeMap<K>* map, Segment<K>* segment) {
			printf("\n<---------- Segment %p contents ---------->\n\n", segment);

			if (segment != null) {
				const SegMapEntry* infoEntry = segment->firstEntry();
				K key = (K) Converter<K>::NULL_VALUE;

				while (infoEntry != null) {	
					key = infoEntry->getKey();
					printStory(map, infoEntry->getValue(), Converter<K>::toData(key));

					infoEntry = segment->higherEntry(key);
				}
			}
		}

		static void printStory(RealTimeMap<K>* map, const Information* info, const bytearray pkey) {
			inttype depth = 0;
			char buffer[4096];

			K fillkey = map->m_keyBuilder->newKey();

			for (const Information* next = info; next != null; next = next->getNext()) {

				if (next->getData() == null) {
					continue;
				}

				K key = map->m_keyBuilder->fillKey(pkey, next->getData(), fillkey);

				memset(buffer, 0, 4096);
				map->m_keyBuilder->toString(key, buffer);

				if ((depth % 2) == 0) {
					printf("\e[1;34m");

				} else {
					printf("\e[1;10m");
				}

				for (inttype i=0; i<depth; i++) {
					printf("--");
				}

				printf("-- %p %s", next, buffer);

				if (next->getLevel() != Information::LEVEL_COMMIT) {
					printf(" rtd: %d", next->getRooted(map->getIndexValue()));
					printf(" vpt: n/a");
					printf(" vlt: n/a");

				} else {
					printf(" sur: n/a");
					printf(" res: n/a");
					printf(" vpt: %u", next->getViewpoint());
					printf(" vlt: %u", next->getViewLimit());
				}

				printf(" cmp: %d", next->getCompressed());
				printf(" div: %d", next->getDiverging());
				printf(" del: %d", next->getDeleting());
				printf(" upd: %d", next->getUpdating());
				printf(" cre: %d", next->getCreating());
				printf(" lvl: %d", next->getLevel());
				printf(" ref: %u", next->ref());

				printf("\e[0m");
				printf("\n");

				if (next->getUpdating() == true) {
					break;
				}

				depth++;
			}

			printf("\n");
		}

		void printKeySpaces(RealTimeMap<K>* map, const bytearray pkey, boolean lock = false) {

			ThreadContext<K>* ctxt = map->m_threadContext.getContext();

			RETRY:
			// XXX: safe context lock: multiple readers / no writer on the branch tree
			if (lock == true) map->m_threadContext.readLock();
			{
				if (map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::size() != 0) {
					printf("\nSHOW: %s\n", map->m_filePath.getPath());

					typename TreeMap<K,Segment<K>*>::TreeMapEntrySet stackSegmentSet(true);
					map->m_branchSegmentTreeMap.entrySet(&stackSegmentSet);
					MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) stackSegmentSet.reset();
					while (segIter->MapSegmentEntrySetIterator::hasNext()) {
						MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();
						Segment<K>* segment = segEntry->getValue();

						if (segment->getSummary() == true) {
							segment->incref();

							if (lock == true) ctxt->readUnlock();

							if (forceSetupSegment(ctxt, segment, true /* physical */, true /* values */) == true) {
								segment->unlock();
							}

							goto RETRY;
						}

						typename SegTreeMap::TreeMapEntrySet stackSegmentItemSet(true);
						segment->SegTreeMap::entrySet(&stackSegmentItemSet);

						printf("\tSEGMENT: %p\n", segment);

						MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) stackSegmentItemSet.iterator();
						while (infoIter->MapInformationEntrySetIterator::hasNext()) {
							SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();

							K key = infoEntry->getKey();
							BUILDER_KEY_TO_STRING(map->m_keyBuilder,buffer1,key);

							Information* topinfo = infoEntry->getValue();
							K ckey = map->m_keyBuilder->fillKey(pkey, topinfo->getData(), ctxt->getKey3());
							BUILDER_KEY_TO_STRING(map->m_keyBuilder,buffer2,ckey);
							printf("\t\tKEY: %s / %s, %d, %d\n", buffer1, buffer2, topinfo->getLevel(), topinfo->getDeleting());

							Information* next = topinfo->getNext();
							while (next != null) {
								K nkey = map->m_keyBuilder->fillKey(pkey, next->getData(), ctxt->getKey3());
								BUILDER_KEY_TO_STRING(map->m_keyBuilder,buffer3,nkey);
								printf("\t\t\tKEY: %s / %s, %d, %d\n", buffer1, buffer3, next->getLevel(), next->getDeleting());

								if (next->getUpdating() == true) {
									break;
								}

								next = next->getNext();
							}
						}
					}
				}
			}
			if (lock == true) map->m_threadContext.readUnlock();
		}

		enum WalkAction {
			CONTINUE,
			RETRY,
			DONE,
			ERROR
		};

		template<typename SS, typename TS, WalkAction (*f)(TS&, ulongtype, MapEntry<K,Segment<K>*>*)>
		FORCE_INLINE static boolean walkMapSegments(RealTimeMap<K>* map, typename TreeMap<K,Segment<K>*>::TreeMapEntrySet& entries, SS& staticState) {
			RETRY:
			map->m_branchSegmentTreeMap.TreeMap<K,Segment<K>*>::entrySet(&entries);
			MapSegmentEntrySetIterator* segIter = (MapSegmentEntrySetIterator*) entries.iterator();
			TS transientState(staticState, segIter);
			for (ulongtype i = 0; segIter->MapSegmentEntrySetIterator::hasNext(); ++i) {
				MapEntry<K,Segment<K>*>* segEntry = segIter->MapSegmentEntrySetIterator::next();

				const WalkAction action = f(transientState, i, segEntry);
				switch(action) {
				case DONE:
					return true;
				case ERROR:
					return false;
				case RETRY:
					goto RETRY;
				case CONTINUE:
					break;
				}
			}

			return false;
		}

		template<typename SS, typename TS, WalkAction (*f)(TS&, ulongtype, SegMapEntry*)>
		FORCE_INLINE static TS walkSegment(RealTimeMap<K>* map, Segment<K>* segment, typename SegTreeMap::TreeMapEntrySet& entries, SS& staticState) {
			RETRY:
			segment->SegTreeMap::entrySet(&entries);
			MapInformationEntrySetIterator* infoIter = (MapInformationEntrySetIterator*) entries.iterator();
			TS transientState(staticState, infoIter);
			for (ulongtype i = 0; infoIter->MapInformationEntrySetIterator::hasNext(); ++i) {
				SegMapEntry* infoEntry = infoIter->MapInformationEntrySetIterator::next();

				const WalkAction action = f(transientState, i, infoEntry);
				switch(action) {
				case DONE:
					// TODO: set success state
					return transientState;
				case ERROR:
					// TODO: set error state 
					return transientState;
				case RETRY:
					goto RETRY;
				case CONTINUE:
					break;
				}
			}

			return transientState;
		}

	private:
		RealTimeUtilities(void) {
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEUTILITIES_H_*/
