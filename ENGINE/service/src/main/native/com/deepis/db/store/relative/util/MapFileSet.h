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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MAPFILESET_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MAPFILESET_H_ 

#include "cxx/lang/System.h"
#include "cxx/io/RandomAccessFile.h"
#include "cxx/util/ArrayList.h"
#include "cxx/util/Collections.h"
#include "cxx/util/Converter.h"
#include "cxx/util/QueueSet.h"
#include "cxx/util/TreeSet.h"
#include "cxx/util/Logger.h"
#include "cxx/util/concurrent/locks/ReentrantLock.h"
#include "cxx/util/concurrent/locks/UserSpaceLock.h"

#include "com/deepis/db/store/relative/util/BasicArray.h"
#include "com/deepis/db/store/relative/util/InvalidException.h"

using namespace cxx::lang;
using namespace cxx::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

template<typename F>
class MapFileSet {
	private:
		struct FileCmp {
			FORCE_INLINE FileCmp() {
				// XXX: nothing to do
			}

			FORCE_INLINE virtual ~FileCmp() {
				// XXX: nothing to do
			}

			#ifdef COM_DEEPIS_DB_CARDINALITY
			FORCE_INLINE int compare(const F* o1, const F* o2, inttype* pos = null) const {
			#else
			FORCE_INLINE int compare(const F* o1, const F* o2) const {
			#endif
				longtype delta = (o1->getFileCreationTime() - o2->getFileCreationTime());
				if (delta != 0) {
					return (inttype) delta;
				}

				delta = o1->getFileIndex() - o2->getFileIndex();
				if (delta != 0) {
					return (inttype) delta;
				}

				return (inttype) (o1 - o2);
			}
		};
		static FileCmp s_fileCmp;

		FORCE_INLINE static void removeFile(BasicArray<F*, true /* realloc */>& files, F* const& file) {
			files.set(file->getFileIndex(), null);
		}

	private:
		ReentrantLock m_lock;
		BasicArray<F*, true /* realloc */> m_fileList;
		TreeSet<F*, FileCmp> m_fileSet;
		QueueSet<F*, BasicArray<F*, true /* realloc */>&, TreeSet<F*,FileCmp>&, UserSpaceLock, MapFileSet<F>::removeFile> m_files;
		boolean m_deleteValue:1;

	public:
		FORCE_INLINE MapFileSet(boolean deleteValue = false) :
			m_lock(),
			m_fileList(0, false),
			m_fileSet(&s_fileCmp),
			m_files(m_fileList, m_fileSet, UserSpaceLock()),
			m_deleteValue(deleteValue) {
		}

		FORCE_INLINE MapFileSet(const MapFileSet& files) :
			m_lock(),
			m_fileList((files.containerLock(),files.m_fileList)),
			m_fileSet(&s_fileCmp),
			m_files(m_fileList, m_fileSet, UserSpaceLock()),
			m_deleteValue(false) {
			Iterator<F*>* iter = const_cast<MapFileSet&>(files).m_fileSet.iterator();
			while (iter->hasNext() == true) {
				m_fileSet.add(iter->next());
			}
			Converter<Iterator<F*>*>::destroy(iter);
			files.containerUnlock();
		}

		FORCE_INLINE virtual ~MapFileSet() {
			clear();
		}

		FORCE_INLINE void lock() {
			m_lock.lock();
		}

		FORCE_INLINE boolean tryLock() {
			return m_lock.tryLock();
		}

		FORCE_INLINE void unlock() {
			m_lock.unlock();
		}

		FORCE_INLINE void containerLock() const {
			const_cast<MapFileSet<F>*>(this)->m_files.lock();
		}

		FORCE_INLINE void containerUnlock() const {
			const_cast<MapFileSet<F>*>(this)->m_files.unlock();
		}

		FORCE_INLINE void add(F* f) {
			m_files.set(f->getFileIndex(), f);
			#ifdef DEEP_DEBUG
			if (get(f->getFileIndex()) != f) {
				DEEP_LOG(ERROR, OTHER, "\n");
				throw InvalidException("");
			}
			#endif
		}

		FORCE_INLINE F* remove(inttype i) {
			F* f = get(i);
			if (f != null) {
				#ifdef DEEP_DEBUG
				if (f->getFileIndex() != i) {
					DEEP_LOG(ERROR, OTHER, "MapFileSet file index mismatch: %u != %d\n", f->getFileIndex(), i);
					throw InvalidException("MapFileSet file index mismatch");
				}
				if (remove(f) == false) {
					DEEP_LOG(ERROR, OTHER, "MapFileSet failed remove: %d\n", i);
					throw InvalidException("MapFileSet failed remove");
				}
				#else
				remove(f);
				#endif
			}
			return f;
		}

		FORCE_INLINE boolean remove(F* f) {
			return m_files.remove(f);
		}

		FORCE_INLINE F* get(inttype i) const {
			return m_files.get(i);
		}

		FORCE_INLINE F* safeGet(inttype i) const {
			F* ret = null;
			containerLock();
			if ((i >= 0) && (i < m_fileList.size())) {
				ret = m_fileList.get(i);
			}
			containerUnlock();
			return ret;
		}

		FORCE_INLINE boolean containsFileAtIndex(inttype i) const {
			return (safeGet(i) != null);
		}

		FORCE_INLINE F* first() const {
			F* ret = null;
			containerLock();
			{
				ret = m_fileSet.first();
			}
			containerUnlock();
			return ret;
		}

		FORCE_INLINE F* last() const {
			F* ret = null;
			containerLock();
			{
				ret = m_fileSet.last();
			}
			containerUnlock();
			return ret;
		}

		FORCE_INLINE inttype getMaxIndex() {
			inttype maxIndex = -1;
			containerLock();
			{
				Iterator<F*>* iter = iterator(false /* needsLock */);
				while (iter->hasNext() == true) {
					F* file = iter->next();
					if (file->getFileIndex() > maxIndex) {
						maxIndex = file->getFileIndex();
					}
				}
				Converter<Iterator<F*>*>::destroy(iter);
			}
			containerUnlock();
			return maxIndex;
		}

		FORCE_INLINE void clear(const boolean needsLock = true) {
			if (needsLock == true) containerLock();
			{
				BasicArray<F*> toDelete(0, true);
				if (m_deleteValue == true) {
					Iterator<F*>* iter = iterator(false /* needsLock */);
					while (iter->hasNext() == true) {
						F* file = iter->next();
						toDelete.add(file, true /* resize */);
					}
					Converter<Iterator<F*>*>::destroy(iter);
				}
				m_files.clear(false);
			}
			if (needsLock == true) containerUnlock();
		}

		FORCE_INLINE inttype size() const {
			return m_files.size();
		}

		FORCE_INLINE Iterator<F*>* iterator(const boolean needsLock = true) {
			return m_files.iterator(false /* use queue */, needsLock);
		}
};

template<typename F>
typename MapFileSet<F>::FileCmp MapFileSet<F>::s_fileCmp;

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_MAPFILESET_H_*/
