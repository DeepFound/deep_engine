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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEATOMIC_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEATOMIC_H_
        
#include "cxx/io/FileUtil.h"
#include "cxx/util/TreeMap.h"
#include "cxx/util/concurrent/locks/UserSpaceLock.h"
#include "cxx/util/concurrent/atomic/AtomicLong.h"
#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/util/MeasuredRandomAccessFile.h"

using namespace cxx::util::concurrent::locks;
using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

class RealTimeAtomic {

	public:
		enum Mode {
			READ,
			WRITE
		};

	private:
		RealTimeAtomic(const char* databaseName, const char* fileName, Mode mode, ulongtype startingId):
			m_databaseName(databaseName),
			m_trtFile(fileName, (mode == WRITE) ? "rw" : "r", MapFileUtil::TRT, Properties::DEFAULT_FILE_BUFFER),
			m_entries(null),
			m_sequence(startingId),
			m_mode(mode),
			m_fileLock()
		{
			// XXX: in READ mode, file may not exist (there were no cross table transactions)
			if ((mode == WRITE) || (FileUtil::exists(fileName) == true)) {
				m_trtFile.setOnline(true);
			}
		}

		static UserSpaceLock s_instanceLock;	

		static const Comparator<String*> STRING_CMP;
		static const Comparator<ulongtype> ULONG_CMP;

		// database name -> instance
		static boolean s_theInstancesCreated;
		static TreeMap<String*,RealTimeAtomic*>* getTheInstances(boolean create) {
			if ((create == false) && (s_theInstancesCreated == false)) {
				return null;
			}

			static TreeMap<String*,RealTimeAtomic*> theInstances(&STRING_CMP);
			theInstances.setDeleteKey(true);
			theInstances.setDeleteValue(true);

			s_theInstancesCreated = true;
			return &theInstances;
		}

		const char* m_databaseName;

		// XXX: file protocol - contains a series of transactionIds (unsigned long long) in ascending order
		MeasuredRandomAccessFile m_trtFile;
		TreeSet<ulongtype>* m_entries;
		AtomicLong m_sequence;
		Mode m_mode;
		UserSpaceLock m_fileLock;	

	public:
		MeasuredRandomAccessFile* getFile() {
			if ((m_mode == READ) && (m_trtFile.getOnline() == false)) {
				return null;
			}	
			return &m_trtFile;
		}	

		void writeEndTransaction(ulongtype transactionId) {
			m_fileLock.lock();
			{
				m_trtFile.RandomAccessFile::writeLong(transactionId);
				if (m_trtFile.RandomAccessFile::getFilePointer() > Properties::getTrtFileSize()) {
					m_trtFile.seek(0);
				}
				m_trtFile.flush();
			}
			m_fileLock.unlock();
		}

		void initializeEntries() {
			if (m_entries != null) {
				delete m_entries;
			}
			m_entries = new TreeSet<ulongtype>(&ULONG_CMP);

			initializeEntries(&m_trtFile, m_entries);

			DEEP_LOG(DEBUG, RCVRY, "found %d recent cross table transactions in database %s\n", m_entries->size(), m_databaseName);	
		}
		
		ulongtype getMinimumTransaction() const {
			return m_entries->first();
		}

		boolean transactionCompleted(ulongtype transactionId) {
			return m_entries->contains(transactionId);
		}

		ulongtype getTransactionId() {
			ulongtype tid = 0;

			#if 0
			do {
				ulongtype upper = (rand() << sizeof (uinttype));
				tid = (upper | rand());

			} while (tid == 0); // 0 reserved for non-acp transactions
			#else
			tid = m_sequence.incrementAndGet();
			#endif

			return tid;
		}

		static boolean useAtomicCommit() {
			return (Properties::getTrtFileSize() > 0);
		}

		static void reset(const char* databaseName = null, boolean lock = true) {
			if (lock == true) s_instanceLock.lock();
			{
				if ((databaseName != null) && (getTheInstances(false) != null)) {
					String* databaseNameStr = new String(databaseName);

					RealTimeAtomic* atomicCommit = getTheInstances(false)->remove(databaseNameStr);
					delete databaseNameStr;

					if (atomicCommit != null) {
						MeasuredRandomAccessFile* trtFile = atomicCommit->getFile();
						if (trtFile != null) {
							trtFile->setOnline(false);
							if (FileUtil::exists((const char*)trtFile->getPath()) == true) {
								FileUtil::clobber((const char*)trtFile->getPath(), true /* followSymlinks */);
							}
						}
						delete atomicCommit;
					}

				} else {
					if (getTheInstances(false) != null) {
						getTheInstances(false)->clear();
					}
				}
			}
			if (lock == true) s_instanceLock.unlock();
		}

		static void initializeEntries(MeasuredRandomAccessFile* trtFile, TreeSet<ulongtype>* entries) {
			boolean eof = false;

			trtFile->seek(0, true /* validate */);
			ulongtype id = trtFile->readLong(&eof);

			// XXX: 0 is preallocated range sync space
			while ((eof == false) && (id != 0)) {
				entries->add(id);
				id = trtFile->readLong(&eof);
			}
		}

		static ulongtype getMaximumTransaction(String* fileName) {
			if (FileUtil::exists(*fileName) == false) {
				return 0;
			}

			MeasuredRandomAccessFile trtFile(fileName->data(), "r", MapFileUtil::TRT, Properties::DEFAULT_FILE_BUFFER);
			trtFile.setOnline(true);
	
			TreeSet<ulongtype>* entries = new TreeSet<ulongtype>(&ULONG_CMP);
			initializeEntries(&trtFile, entries);

			ulongtype max = entries->last();

			delete entries;
			trtFile.setOnline(false);

			return max;
		}

		static char* getDatabaseName(const char* path) {
			inttype len = 0;
			inttype x;
			for (x = 2; path[x] != '/' && path[x] != '\0'; x++,len++); 

			// XXX: if running in a flat directory, just use current dir as db name
			if (path[x] == '\0') {
				char* name = new char[2];
				name[0] = '.';
				name[0] = '\0';
				return name;
			}
			
			char* name = new char[len + 1];
			for (x = 0; x < len; x++) {
				name[x] = path[x + 2];
			}
			name[x] = '\0';

			return name;
		}

		static RealTimeAtomic* getInstance(const char* databaseName, Mode mode, boolean create = true) {
			RealTimeAtomic* theInstance = null;
			s_instanceLock.lock();
			{
				String* databaseNameStr = new String(databaseName);
				theInstance = (getTheInstances(false) == null) ? null : getTheInstances(false)->get(databaseNameStr);

				if ((create == true) && ((theInstance == null) || (theInstance->m_mode != mode))) {
					theInstance = getTheInstances(true)->get(databaseNameStr);

					// XXX: mode changes during restart, use a new instance
					if ((theInstance != null) && (theInstance->m_mode != mode)) {
						reset(null, false /* lock */);
						theInstance = null;
					}

					if (theInstance == null) {
						String fileName(String("./") + (String(databaseName) + String("/deep.trt")));
						ulongtype startingId = getMaximumTransaction(&fileName) + 1;
						if (mode == WRITE) {
							if (FileUtil::exists(fileName.data()) == true) {	
								FileUtil::clobber(fileName.data(), false);
							}
						}
						theInstance = new RealTimeAtomic(databaseName, fileName.data(), mode, startingId);
						getTheInstances(false)->put(databaseNameStr, theInstance);
					} else {
						delete databaseNameStr;
					}

				} else {
					delete databaseNameStr;
				}

				if ((mode == READ) && (theInstance->getFile() == null)) {
					theInstance = null;
				}
			}
			s_instanceLock.unlock();
			return theInstance;
		}

		virtual ~RealTimeAtomic() {  
			delete m_entries;
		}

};

const Comparator<ulongtype> RealTimeAtomic::ULONG_CMP;
const Comparator<String*> RealTimeAtomic::STRING_CMP;

} } } } } } // namespace

UserSpaceLock RealTimeAtomic::s_instanceLock = UserSpaceLock();
boolean RealTimeAtomic::s_theInstancesCreated = false;

#endif /* COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEATOMIC_H_ */

