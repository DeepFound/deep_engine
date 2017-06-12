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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMETRANSACTION_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMETRANSACTION_H_ 

// XXX: this header is specifically split out from RealTimeMaph.h

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename K>
struct SegMapEntryCmp {
	const Comparator<K>* m_cmp;
	FORCE_INLINE SegMapEntryCmp(const Comparator<K>* cmp) :
		m_cmp(cmp) {
	}

	FORCE_INLINE ~SegMapEntryCmp() {
	}

	FORCE_INLINE inttype compare(typename RealTimeTypes<K>::SegMapEntry* v1, typename RealTimeTypes<K>::SegMapEntry* v2) const {
		if (v1 == v2) {
			return 0;
		}
		if (v1 == null) {
			return 1;
		}
		if (v2 == null) {
			return -1;
		}

		return m_cmp->compare(v1->getKey(), v2->getKey());
	}
};

template<typename K>
class RealTimeConductor : public Conductor {

	typedef typename RealTimeTypes<K>::SegTreeMap SegTreeMap;
	typedef typename RealTimeTypes<K>::SegMapEntry SegMapEntry;

	private:
		struct Marker {
			Information* info;
			boolean persisted;
		};

		BasicArray<SegMapEntry*> m_operations;
		RealTimeMap<K>* m_primaryMap;
		BasicArray<Marker*> m_markers;
		inttype m_size;

	public:
		inline void referenceFiles(MeasuredRandomAccessFile* file) {
			Transaction* tx = getTransaction();
			if (/* (tx != null) && */ tx->getLarge() == true) {
				file->incref();

				tx->getStreamFiles()->ArrayList<MeasuredRandomAccessFile*>::add(file);
			}
		}

		inline void dereferenceFiles(void) {
			Transaction* tx = getTransaction();
			if ((tx != null) && (tx->getLarge() == true)) {
				ArrayList<MeasuredRandomAccessFile*>* files = tx->getStreamFiles();
				for (int i = 0; i < files->ArrayList<MeasuredRandomAccessFile*>::size(); i++) {
					files->ArrayList<MeasuredRandomAccessFile*>::get(i)->decref();
				}

				files->ArrayList<MeasuredRandomAccessFile*>::clear();
			}
		}

	public:
		inline RealTimeConductor(Transaction* tx, RealTimeMap<K>* map):
			Conductor(tx, Properties::FIXED_INDEX_CAP),
			m_operations(Properties::LIST_CAP, true),
			m_primaryMap(map),
			m_markers(0, true),
			m_size(0) {

			m_contexts.add(m_primaryMap->getThreadContext());

			for (int i = 0; i < m_primaryMap->m_share.getSecondaryList()->size(); i++) {
				RealTime* rt = m_primaryMap->m_share.getSecondaryList()->get(i);
				if (rt == null) {
					// XXX: create empty slot
					m_contexts.add((ConcurrentObject*) null);

				} else {
					m_contexts.add(rt->getThreadContext());
				}
			}
		}

		virtual ~RealTimeConductor(void) {
			// XXX: ensuring deletion (i.e. see attributes)
		}

		FORCE_INLINE void addMarker(Information* info, boolean persisted = true) {
			for (int i = 0; i < m_markers.size(); ++i) {
				if (m_markers.get(i)->info == info) {
					return;
				}
			}
			Marker* m = new Marker();
			m->info = info;
			m->persisted = persisted;
			m_markers.add(m, true);
		}

		FORCE_INLINE inttype getMarkerCount() const {
			return m_markers.size();
		}

		FORCE_INLINE Information* getMarker(inttype i) const {
			return m_markers.get(i)->info;
		}

		FORCE_INLINE boolean getMarkerPersisted(inttype i) const {
			return m_markers.get(i)->persisted;
		}

		FORCE_INLINE void clearMarkers() {
			m_markers.clear();
		}

		FORCE_INLINE SegMapEntry* get(inttype index) const {
			return m_operations.get(index);
		}

		#ifdef DEEP_DEBUG
		FORCE_INLINE void add(StoryLock* slock, K key, Information* info, Transaction* tx = null) {
			if ((tx != null) && (slock->getIdentifier() != tx->getIdentifier())) {
				DEEP_LOG(ERROR, OTHER, "Invalid conductor add: %d, %d\n", slock->getIdentifier(), tx->getIdentifier());

				throw InvalidException("Invalid conductor add");
			}
		#else
		FORCE_INLINE void add(StoryLock* slock, K key, Information* info) {
		#endif
			SegMapEntry* infoEntry = null;
			if (m_size < m_operations.size()) {
				infoEntry = m_operations.get(m_size++);

			} else {
				m_size++;
			}

			if (infoEntry == null) {
				infoEntry = new SegMapEntry(key, info, -1 /* ctx */);
				m_operations.add(infoEntry, true);
			}

			StoryLine& storyLine = infoEntry->getStoryLine();
			StoryLock* pslock = storyLine.getStoryLock();
			storyLine.setStoryLock(slock);
			if ((pslock != null) && (pslock->decref() == 0)) {
				delete pslock;
			}

			#ifdef CXX_LANG_MEMORY_DEBUG
			infoEntry->setKey(key, -1 /* ctx */, true);
			infoEntry->setValue(info, -1 /* ctx */, true);
			#else
			infoEntry->setKey(key, -1 /* ctx */);
			
			// XXX: note this is safe since ctxt -1
			#ifdef DEEP_DEBUG
			infoEntry->setValueIgnoreRooted(info, -1 /* ctx */);
			#else
			infoEntry->setValue(info, -1 /* ctx */);
			#endif

			#endif

			if (m_mergeOptimize != -1) {
				m_mergeOptimize++;
			}
		}

		FORCE_INLINE SegMapEntry* remove(inttype index) {
			m_size--;
			return m_operations.remove(index);
		}

		FORCE_INLINE virtual inttype size(void) const {
			return m_size;
		}

		FORCE_INLINE void clear(Information* info, inttype index) {
			for (int i = index - 1; i >= 0; i--) {
				SegMapEntry* walkEntry = m_operations.get(i);
				if (walkEntry->getValue() == info) {
					clear(walkEntry, i);
					break;
				}
			}
		}

		FORCE_INLINE void clear(SegMapEntry* walkEntry, inttype index) {
			#ifdef DEEP_DEBUG
			if (walkEntry != m_operations.get(index)) {
				DEEP_LOG(ERROR, OTHER, "Invalid conductor clear\n");
				throw InvalidException("Invalid conductor clear");
			}
			#endif

			walkEntry->setValue(null, -1 /* ctx */);
			StoryLine& storyLine = walkEntry->getStoryLine();
			StoryLock* slock = storyLine.getStoryLock();
			storyLine.setStoryLock(null /* no lock */);
			if ((slock != null) && (slock->decref() == 0)) {
				delete slock;
			}
		}

		FORCE_INLINE const char* getFilePath(void) const {
			const char* path = m_primaryMap->getFilePath();
			return path;
		}

		FORCE_INLINE void clear(void) {
			m_mergeOptimize = 0;
			m_createStats = 0;
			m_deleteStats = 0;
			m_updateStats = 0;
			#if 0
			m_readStats = 0;
			#endif
			m_totalSize = 0;
			m_userSpaceSize = 0;

			m_loadData = false;
			m_override = false;
			m_reservedKey = 0;
			m_reservedBlock = 0;

			clearMarkers();

			m_size = 0;
		}

		virtual void index(inttype index, ConcurrentObject* context) {
			// XXX: thread contexts occurr dynamically per request
			#ifdef DEEP_DEBUG
			if (index == -1) {
				DEEP_LOG(ERROR, OTHER, "Invalid index: -1\n");
				throw InvalidException("Invalid index");
			}
			#endif

			// XXX: adjust space for dynamic add secondary index
			if (index >= m_contexts.size()) {
				for (inttype i = m_contexts.size(); i<=index; i++) {
					m_contexts.add((ConcurrentObject*) null, true);
				}
			}

			m_contexts.set(index, context);
		}

		virtual void clearIndex(void) {
			#ifdef DEEP_DEBUG
			if (size() != 0) {
				DEEP_LOG(ERROR, OTHER, "Invalid clear index size: %d\n", size());

				throw InvalidException("Invalid clear index size");
			}
			#endif

			if (m_primaryMap->getDestructor() == true) {
				return;
			}

			// XXX: remove space for dynamic remove secondary index
			m_primaryMap->removeThreadContexts(&m_contexts);

			// XXX: size() is the determination of an active map
			m_primaryMap = null;
		}

		virtual void prepare(shorttype level) {
			/* TODO
			m_primaryMap->prepare(...);
			*/
		}

		#ifdef DEEP_SYNCHRONIZATION_GROUPING
		virtual void commit(shorttype level, bytetype mode) {
		#else
		virtual void commit(shorttype level) {
		#endif

			if (size() != 0) {
				if (level == 0) {
					#ifdef DEEP_SYNCHRONIZATION_GROUPING
					m_primaryMap->commit(this, mode);
 
					if ((mode == 0 /* non-durable */) || (mode == 1 /* durable-phase2 */)) {
						clear();
					}
					#else
					m_primaryMap->commit(this);

					clear();
					#endif

				} else {
					m_primaryMap->mergeCacheMemory(this);
				}
			}
		}

		virtual void rollback(shorttype level) {

			if (size() != 0) {
				if (level == 0) {
					m_primaryMap->rollback(this);

					clear();

				} else {
					m_primaryMap->abortCacheMemory(this);
				}
			}
		}

		FORCE_INLINE void sort(void) {

			#ifdef DEEP_DEBUG
			if (getTransaction()->getRoll() == false) {
				DEEP_LOG(ERROR, OTHER, "Conductor sort of non-roll transaction, %s\n", m_primaryMap->getFilePath());
				throw InvalidException("Conductor sort of non-roll transaction");
			}
			#endif

			SegMapEntryCmp<K> cmp(m_primaryMap->m_comparator);
			Collections::template mergeSort2<SegMapEntry*, BasicArray<SegMapEntry*>, SegMapEntryCmp<K> >(&m_operations, &cmp);
		}

};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMETRANSACTION_H_*/
