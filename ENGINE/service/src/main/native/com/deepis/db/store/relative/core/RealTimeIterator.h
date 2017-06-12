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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEITERATOR_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEITERATOR_H_ 

#include "cxx/util/TreeMap.h"

#include "com/deepis/db/store/relative/core/Segment.h"
#include "com/deepis/db/store/relative/core/Information.h"
#include "com/deepis/db/store/relative/core/RealTimeMap.h"

using namespace cxx::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename K>
class RealTimeIterator {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;

	private:
		RealTimeMap<K>* m_map;
		ThreadContext<K>* m_ctxt;
		TreeIterator<MapEntry<K,Segment<K>*>*>* m_bIterator;
		TreeIterator<SegMapEntry*>* m_lIterator;

	public:
		inline RealTimeIterator(RealTimeMap<K>* map):
			m_map(map),
			m_ctxt(null),
			m_bIterator(null),
			m_lIterator(null) {

			m_ctxt = map->m_threadContext.getContext();
		}

		inline ~RealTimeIterator(void) {
			delete m_bIterator;
			delete m_lIterator;
		}

		inline const SegMapEntry* next() {
			// XXX: currently m_lIterator->next does not throw NoSuchElementException
			const SegMapEntry* entry = m_lIterator->next();
			if (entry == null) {
				RETRY:
				// XXX: currently m_bIterator->next does not throw NoSuchElementException
				MapEntry<K,Segment<K>*>* bEntry = m_bIterator->next();
				if (bEntry != null) {
					Segment<K>* segment = bEntry->getValue();
					if (segment->getPurged() == true) {
						boolean summary = segment->getSummary();

						m_map->fillSegment(m_ctxt, segment, false /* values */, true /* pace */);

						if (summary == true) {
							reset(segment->SegTreeMap::firstKey());
							goto RETRY;
						}
					}

					segment->iterator(segment->SegTreeMap::firstKey(), m_lIterator, true);
					entry = m_lIterator->next();
				}
			}

			return entry;
		}

		inline const SegMapEntry* previous() {
			// XXX: currently m_lIterator->previous does not throw NoSuchElementException
			const SegMapEntry* entry = m_lIterator->previous();
			if (entry == null) {
				RETRY:
				// XXX: currently m_bIterator->previous does not throw NoSuchElementException
				MapEntry<K,Segment<K>*>* bEntry = m_bIterator->previous();
				if (bEntry != null) {
					Segment<K>* segment = bEntry->getValue();
					if (segment->getPurged() == true) {
						boolean summary = segment->getSummary();

						m_map->fillSegment(m_ctxt, segment, false /* values */, true /* pace */);

						if (summary == true) {
							reset(segment->SegTreeMap::firstKey());
							goto RETRY;
						}
					}

					segment->iterator(segment->SegTreeMap::lastKey(), m_lIterator, true);
					m_lIterator->next(); // XXX: need to move to the last element

					entry = m_lIterator->previous();
				}
			}

			return entry;
		}

		inline void reset(const K startKey) {
			if (m_bIterator == null) {
				m_bIterator = m_map->m_branchSegmentTreeMap.iterator(startKey, null, false);

			} else {
				m_map->m_branchSegmentTreeMap.iterator(startKey, m_bIterator, false);
			}

			MapEntry<K,Segment<K>*>* bEntry = m_bIterator->next();
			if (bEntry != null) {
				Segment<K>* segment = bEntry->getValue();
				if (segment->getPurged() == true) {
					m_map->fillSegment(m_ctxt, segment, false /* values */, true /* pace */);
				}

				if (m_lIterator == null) {
					m_lIterator = segment->iterator(startKey, null, true);

				} else {
					segment->iterator(startKey, m_lIterator, true);
				}
			}
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEITERATOR_H_*/
