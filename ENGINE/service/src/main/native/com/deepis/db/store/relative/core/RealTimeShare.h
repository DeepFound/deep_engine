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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESHARE_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESHARE_H_ 

#include "cxx/lang/nbyte.h"

#include "cxx/io/FileUtil.h"

#include "cxx/util/ArrayList.h"

#ifdef DEEP_USERLOCK
	#include "cxx/util/concurrent/locks/UserSpaceLock.h"
#else
	#include "cxx/util/concurrent/locks/ReentrantLock.h"
#endif

#include "com/deepis/db/store/relative/util/LockableArrayList.h"
#include "com/deepis/db/store/relative/util/LockableBasicArray.h"
#include "com/deepis/db/store/relative/util/PermissionException.h"
#include "com/deepis/db/store/relative/util/BufferedRandomAccessFile.h"
#include "com/deepis/db/store/relative/util/MapFileSet.h"
#include "com/deepis/db/store/relative/util/MeasuredRandomAccessFile.h"

#include "com/deepis/db/store/relative/core/Properties.h"

using namespace cxx::io;
using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent::locks;

using namespace com::deepis::core::util;
using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

class RealTime;

class RealTimeShare {

	private:
		const File m_filePath;
		const longtype m_options;

		boolean m_large;
		boolean m_hasPrimary;

		ulongtype m_keyBlockAverage;
		uinttype  m_lastAverageKeyBlock;

		shorttype m_keySize;
		shorttype m_keyBuffer;
		shorttype m_keyProtocol;

		inttype m_valueSize;
		inttype m_valueAverage;

		BasicArray<RealTime*> m_secondaryList;
		MapFileSet<RandomAccessFile> m_srtReadFileList;
		MapFileSet<RandomAccessFile> m_srtWriteFileList;
		MapFileSet<RandomAccessFile> m_xrtReadFileList;
		MapFileSet<RandomAccessFile> m_xrtWriteFileList;
		MapFileSet<BufferedRandomAccessFile> m_vrtReadFileList;
		MapFileSet<MeasuredRandomAccessFile> m_vrtWriteFileList;
		MapFileSet<MeasuredRandomAccessFile> m_lrtWriteFileList;
		MapFileSet<BufferedRandomAccessFile> m_irtReadFileList;
		MapFileSet<MeasuredRandomAccessFile> m_irtWriteFileList;

		LockableBasicArray<MeasuredRandomAccessFile*>* m_awaitingDeletion;
		ulongtype* m_deferredStorageSize;

		static inttype s_fileLimitIndex;
		#ifdef DEEP_USERLOCK
		static UserSpaceLock s_fileLimitLock;
		#else
		static ReentrantLock s_fileLimitLock;
		#endif
		static ArrayList<RandomAccessFile*> s_fileLimitQueue;

	private:
		static void queue(RandomAccessFile* file) {
			boolean limit = true;

			s_fileLimitLock.lock();
			{
				if (file->getOnline() == false) {
					s_fileLimitQueue.ArrayList::remove(file);

					if (s_fileLimitQueue.ArrayList::size() < Properties::getFileLimit()) {
						s_fileLimitQueue.ArrayList::add(file);
						limit = false;

					} else {
						/* TODO: make this a "fair" priority replacement (i.e. deactive lowest first)

							* zero being the most important priority

							0 - lrt/vrt write files
							1 - lrt/vrt read files
							2 - irt write files
							3 - irt read files

						*/
						inttype exit = s_fileLimitQueue.ArrayList::size() * 2;
						for (int i = 0; i < exit; i++, s_fileLimitIndex++) {
							if (s_fileLimitIndex >= s_fileLimitQueue.ArrayList::size()) {
								s_fileLimitIndex = 0;
							}

							RandomAccessFile* item = s_fileLimitQueue.ArrayList::get(s_fileLimitIndex);
							if (item->tryLock() == false) {
								continue;
							}

							if (item->getActive() == false) {
								s_fileLimitQueue.ArrayList::set(s_fileLimitIndex, file);

								item->setOnline(false);
								item->unlock();
								limit = false;
								break;
							}

							item->unlock();
						}
					}

					file->setOnline(true);

				} else {
					limit = false;
				}
			}
			s_fileLimitLock.unlock();

			if (limit == true) {
				DEEP_LOG(ERROR, OTHER, "Failure to limit file descriptors: %d\n", s_fileLimitQueue.ArrayList::size());

				throw PermissionException("Failure to limit file descriptors");
			}
		}

		static void dequeue(RandomAccessFile* file) {
			file->setActive(false);

			s_fileLimitLock.lock();
			{
				s_fileLimitQueue.ArrayList::remove(file);

				file->setOnline(false);
			}
			s_fileLimitLock.unlock();
		}

	public:
		FORCE_INLINE void acquire(RandomAccessFile* file) {
			#ifdef DEEP_DEBUG
			if (file == null) {
				DEEP_LOG(ERROR, OTHER, "Invalid file access: null reference\n");

				throw InvalidException("Invalid file access: null reference");
			}
			#endif

			file->lock();
			file->setActive(true);

			if (file->getOnline() == false) {
				queue(file);
			}
		}

		FORCE_INLINE void release(RandomAccessFile* file) {
			file->setActive(false);
			file->unlock();
		}

		FORCE_INLINE void deferredLock() {
			getAwaitingDeletion()->lock();
		}

		FORCE_INLINE boolean tryDeferredLock() {
			return getAwaitingDeletion()->tryLock();
		}

		FORCE_INLINE void deferredUnlock() {
			getAwaitingDeletion()->unlock();
		}

		void deferredDestroy(const bytetype indexValue, MeasuredRandomAccessFile* file) {
			dequeue(file);

			file->close();

			getAwaitingDeletion()->lock();
			{
				#ifdef DEEP_DEBUG
				if ((file->getAwaitingDeletion() == true) || (getAwaitingDeletion()->contains(file) == true)) {
					DEEP_LOG(ERROR, OTHER, "File already scheduled for deletion, %s\n", file->getPath());
					throw InvalidException("File already scheduled for deletion");
				}
				#endif

				file->setAwaitingDeletion(indexValue, true);
				getAwaitingDeletion()->add(file, true);
				const longtype length = file->getAwaitingDeletionLength();
				__sync_add_and_fetch(m_deferredStorageSize, length);
			}
			getAwaitingDeletion()->unlock();
		}

		void removeDeferred(MeasuredRandomAccessFile* wfile) {
			const inttype i = getAwaitingDeletion()->indexOf(wfile);
			if (i < 0) {
				return;
			}
			const longtype length = wfile->getAwaitingDeletionLength();
			__sync_sub_and_fetch(m_deferredStorageSize, length);
			getAwaitingDeletion()->remove(i);

			if (getAwaitingDeletion()->size() == 0) {
				*m_deferredStorageSize = 0;
			}
		}

		FORCE_INLINE void clearDeferred() {
			if (m_awaitingDeletion == null) {
				return;
			}

			getAwaitingDeletion()->lock();
			{
				getAwaitingDeletion()->clear();
				*m_deferredStorageSize = 0;
			}
			getAwaitingDeletion()->unlock();
		}

		FORCE_INLINE ulongtype getDeferredStorageSize() {
			ulongtype ret = __sync_or_and_fetch(m_deferredStorageSize, 0);

			#ifdef DEEP_DEBUG
			if (getAwaitingDeletion()->tryLock() == true)
			{
				ulongtype res = 0;
				for (inttype i=0; i < getAwaitingDeletion()->size(); ++i) {
					MeasuredRandomAccessFile* wfile = getAwaitingDeletion()->get(i);
					const longtype length = wfile->getAwaitingDeletionLength();
					if (length < 0) {
						DEEP_LOG(ERROR, OTHER, "Invalid file size for deferred deletion: %lld, %s\n", length, wfile->getPath());
						throw InvalidException("Invalid file size for deferred deletion");
					}
					res += length;
				}

				if (res != ret) {
					DEEP_LOG(ERROR, OTHER, "Invalid aggregate file size for deferred deletion: %llu != %llu\n", res, ret);
					throw InvalidException("Invalid aggregate file size for deferred deletion");
				}

				getAwaitingDeletion()->unlock();
			}
			#endif

			return ret;
		}

		void destroy(RandomAccessFile* file, boolean clobber = false) {
			#ifdef DEEP_DEBUG
			if (file->getActive() == true) {
				DEEP_LOG(ERROR, OTHER, "Invalid file usage (destroy on active file): %s\n", file->getPath());
				throw InvalidException("Invalid file usage (destroy on active file)");
			}
			#endif

			dequeue(file);

			file->close();

			if (clobber == true) {
				MapFileUtil::clobberFile(m_filePath, file->getPath());
			}

			delete file;
		}

		template<typename F>
		longtype size(MapFileSet<F>* array) {
			longtype total = 0;
			array->containerLock();
			{
				Iterator<F*>* iter = array->iterator(false /* needsLock */);
				while(iter->hasNext() == true) {
					RandomAccessFile* file = iter->next();
					if (file != null) {
						total += file->length();
					}
				}
				Converter<Iterator<F*>*>::destroy(iter);
			}
			array->containerUnlock();

			return total;
		}

		template<typename F>
		void destroy(MapFileSet<F>* array) {
			array->containerLock();
			{
				Iterator<F*>* iter = array->iterator(false /* needsLock */);
				while(iter->hasNext() == true) {
					RandomAccessFile* file = iter->next();
					if (file != null) {
						dequeue(file);
					}
				}
				Converter<Iterator<F*>*>::destroy(iter);

				array->clear(false /* needsLock */);
			}
			array->containerUnlock();
		}

	public:
		RealTimeShare(const char* filePath, const longtype options, const shorttype keySize, const inttype valueSize):
			m_filePath(filePath),
			m_options(options),

			m_large(false),
			m_hasPrimary(false),

			m_keyBlockAverage(0),
			m_lastAverageKeyBlock(0),

			m_keySize(keySize),
			m_keyBuffer(-1),
			m_keyProtocol(-1),

			m_valueSize(valueSize),
			m_valueAverage(valueSize),

			m_secondaryList(Properties::FIXED_INDEX_CAP, false),
			m_srtReadFileList(true /* delete */),
			m_srtWriteFileList(true /* delete */),
			m_xrtReadFileList(true /* delete */),
			m_xrtWriteFileList(true /* delete */),
			m_vrtReadFileList(true /* delete */),
			m_vrtWriteFileList(true /* delete */),
			m_lrtWriteFileList(true /* delete */),
			m_irtReadFileList(true /* delete */),
			m_irtWriteFileList(true /* delete */),
			m_awaitingDeletion(null),
			m_deferredStorageSize(null) {
		}

		FORCE_INLINE ~RealTimeShare() {
			if (m_awaitingDeletion != null) {
				Converter<LockableBasicArray<MeasuredRandomAccessFile*>*>::destroy(m_awaitingDeletion);
			}

			if (m_deferredStorageSize != null) {
				Converter<ulongtype*>::destroy(m_deferredStorageSize);
			}
		}

		FORCE_INLINE const File& getFilePath() {
			return m_filePath;
		}

		FORCE_INLINE void setLarge(boolean large) {
			m_large = large;
		}

		FORCE_INLINE inttype getLarge(void) const {
			return m_large;
		}

		FORCE_INLINE void setHasPrimary(boolean hasPrimary) {
			m_hasPrimary = hasPrimary;
		}

		FORCE_INLINE boolean getHasPrimary(void) {
			return m_hasPrimary;
		}

		FORCE_INLINE longtype getOptions(void) const {
			return m_options;
		}

		FORCE_INLINE void setKeyBlockAverage(inttype keyBlockAverage) {
			m_keyBlockAverage = keyBlockAverage;
		}

		FORCE_INLINE inttype getKeyBlockAverage(void) const {
			return m_keyBlockAverage;
		}

		FORCE_INLINE void setLastAverageKeyBlock(uinttype lastAverageKeyBlock) {
			m_lastAverageKeyBlock = lastAverageKeyBlock;
		}

		FORCE_INLINE uinttype getLastAverageKeyBlock() const {
			return m_lastAverageKeyBlock;
		}

		FORCE_INLINE void setKeySize(shorttype keySize) {
			m_keySize = keySize;
		}

		FORCE_INLINE shorttype getKeySize(void) const {
			return m_keySize;
		}

		FORCE_INLINE void setKeyBuffer(shorttype keyBuffer) {
			m_keyBuffer = keyBuffer;
		}

		FORCE_INLINE shorttype getKeyBuffer(boolean primitive = true) const {
			return primitive ? m_keyBuffer : m_keyBuffer + sizeof(nbyte);
		}

		FORCE_INLINE void setKeyProtocol(shorttype keyProtocol) {
			m_keyProtocol = keyProtocol;
		}

		FORCE_INLINE shorttype getKeyProtocol(void) const {
			return m_keyProtocol;
		}

		FORCE_INLINE void setValueSize(inttype valueSize) {
			m_valueSize = valueSize;
		}

		FORCE_INLINE inttype getValueSize(void) const {
			return m_valueSize;
		}

		FORCE_INLINE void setValueAverage(inttype valueAverage) {
			m_valueAverage = valueAverage;
		}

		FORCE_INLINE inttype getValueAverage(void) const {
			return m_valueAverage;
		}

		FORCE_INLINE BasicArray<RealTime*>* getSecondaryList(void) {
			return &m_secondaryList;
		}

		FORCE_INLINE MapFileSet<RandomAccessFile>* getSrtReadFileList(void) {
			return &m_srtReadFileList;
		}
		
		FORCE_INLINE MapFileSet<RandomAccessFile>* getSrtWriteFileList(void) {
			return &m_srtWriteFileList;
		}

		FORCE_INLINE MapFileSet<RandomAccessFile>* getXrtReadFileList(void) {
			return &m_xrtReadFileList;
		}
		
		FORCE_INLINE MapFileSet<RandomAccessFile>* getXrtWriteFileList(void) {
			return &m_xrtWriteFileList;
		}

		FORCE_INLINE MapFileSet<BufferedRandomAccessFile>* getVrtReadFileList(void) {
			return &m_vrtReadFileList;
		}

		FORCE_INLINE MapFileSet<MeasuredRandomAccessFile>* getVrtWriteFileList(void) {
			return &m_vrtWriteFileList;
		}

		FORCE_INLINE MapFileSet<MeasuredRandomAccessFile>* getLrtWriteFileList(void) {
			return &m_lrtWriteFileList;
		}

		FORCE_INLINE MapFileSet<BufferedRandomAccessFile>* getIrtReadFileList(void) {
			return &m_irtReadFileList;
		}

		FORCE_INLINE MapFileSet<MeasuredRandomAccessFile>* getIrtWriteFileList(void) {
			return &m_irtWriteFileList;
		}

		FORCE_INLINE void setAwaitingDeletion(LockableBasicArray<MeasuredRandomAccessFile*>* awaitingDeletion, ulongtype* deferredStorageSize) {
			m_awaitingDeletion = awaitingDeletion;
			m_deferredStorageSize = deferredStorageSize;
		}

		FORCE_INLINE LockableBasicArray<MeasuredRandomAccessFile*>* getAwaitingDeletion(void) {
			if (m_awaitingDeletion == null) {
				LockableBasicArray<MeasuredRandomAccessFile*>* awaitingDeletion = new LockableBasicArray<MeasuredRandomAccessFile*>();
				if (__sync_bool_compare_and_swap(&m_awaitingDeletion, null, awaitingDeletion) == false) {
					delete awaitingDeletion;
				}
				ulongtype* deferredStorageSize = new ulongtype;
				*deferredStorageSize = 0;
				m_deferredStorageSize = deferredStorageSize;
			}
			return m_awaitingDeletion;
		}

		FORCE_INLINE ulongtype* getDeferredStorageSizeAddr() {
			getAwaitingDeletion();
			return m_deferredStorageSize;
		}
};

inttype RealTimeShare::s_fileLimitIndex = 0;
#ifdef DEEP_USERLOCK
UserSpaceLock RealTimeShare::s_fileLimitLock;
#else
ReentrantLock RealTimeShare::s_fileLimitLock(false);
#endif
ArrayList<RandomAccessFile*> RealTimeShare::s_fileLimitQueue(Properties::LIST_CAP, false);

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMESHARE_H_*/
