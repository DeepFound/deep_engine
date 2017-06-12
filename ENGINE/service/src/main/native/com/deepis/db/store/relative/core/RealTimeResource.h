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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECACHE_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECACHE_H_ 

#include <math.h>

#include "cxx/lang/Memory.h"
#include "cxx/lang/Runtime.h"

#include "cxx/lang/Thread.h"
#include "cxx/lang/Runnable.h"

#include "cxx/util/Logger.h"

#include "cxx/nio/file/FileStore.h"

#include "cxx/util/TreeSet.h"
#include "cxx/util/ArrayList.h"
#include "cxx/util/Collections.h"
#include "cxx/util/concurrent/Synchronize.h"
#include "cxx/util/concurrent/atomic/AtomicLong.h"
#include "cxx/util/concurrent/atomic/AtomicInteger.h"
#include "cxx/util/concurrent/atomic/AtomicBoolean.h"
#include "cxx/util/concurrent/locks/ReentrantReadWriteLock.h"

#include "com/deepis/db/store/relative/core/RealTime.h"
#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/core/Transaction.h"

#include "com/deepis/db/store/relative/util/DynamicUtils.h"
#include "com/deepis/db/store/relative/util/LockableHashMap.h"
#include "com/deepis/db/store/relative/util/LockableArrayList.h"
#include "com/deepis/db/store/relative/util/MeasuredRandomAccessFile.h"

using namespace cxx::lang;
using namespace cxx::util;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::atomic;
using namespace cxx::util::concurrent::locks;
using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

class RealTimeResource : public Synchronizable, private Runnable {

	public:
		enum Limit {
			EXTREME = 4,
			IMMENSE = 3,
			SHALLOW = 2,
			NEUTRAL = 1,
			IGNORE = 0
		};

	private:
		class Cleanup {
			public:
				virtual ~Cleanup() {
					immediateShutdown();

					s_inCleanup.set(true);

					while (s_exitNotify.get() != 0) {
						Thread::yield();
					}
				}
		};

	private:
		boolean m_modified;
		boolean m_continue;
		boolean m_reorganize;
		boolean m_exitThread;

		ArrayList<RealTime*> m_rtObjects;

		// XXX: This must be the first static variable
		static AtomicBoolean s_inCleanup;

		static boolean s_gc;
		static boolean s_exit;
		static boolean s_pace;
		static boolean s_shutdown;
		static boolean s_fragmented;
		static boolean s_reorganize;
		static inttype s_infinitelimit;
		static inttype s_cacheFsUsageMode;

		static Limit s_cacheLimit;

		static longtype s_paceTimeout;
		static longtype s_maxCacheSize;

		static longtype s_lastTimeStamp;
		static longtype s_lastTimePurge;
		static longtype s_lastTimeLimit;
		static longtype s_lastTimeTasks[];

		static AtomicLong s_exitNotify;
		static AtomicInteger s_skipGarbage;

		static ArrayList<Object*> s_gcObjects;
		static LockableArrayList<RealTime*> s_rtObjects;
		static LockableHashMap<RealTime*,Limit> s_fsLimits;

		static ReentrantReadWriteLock s_rtReadWriteLock;

		// XXX: order of static initializer requires garbage instance first
		static RealTimeResource s_theGarbage;

		static RealTimeResource s_theSync;
		static RealTimeResource s_theLimit;
		static RealTimeResource s_thePurge;
		static RealTimeResource s_theFsusage;
		static RealTimeResource s_theTasks[];
		static RealTimeResource s_theStats;

		// XXX: This must be the last static variable
		static Cleanup s_cleanup;

		static const double GB = 1024 * 1024 * 1024;

	private:
		FORCE_INLINE char color(Limit limit) {
			switch (limit) {
				case EXTREME:
					return 31;

				case IMMENSE:
					return 35;

				case SHALLOW:
					return 36;

				default:
					return 33;
			}
		}

		void logging(longtype cache, longtype consumed, longtype freelist, Limit limit) {
			if (Properties::getDebugEnabled(Properties::MEMORY_STATISTICS) == true) {
				Memory::dump(true);
			}

			if (limit < EXTREME) {
				DEEP_LOG(DEBUG, CACHE, "allow: %.2fG, %c[%d;%dmconsumed:%c[%d;%dm %.2fG, maximum: %.2fG, freelist: %.2fG\n", cache / GB, 27,1,color(limit),27,0, 25, consumed / GB, s_maxCacheSize / GB, freelist / GB);

			} else {
				DEEP_LOG(WARN, CACHE, "allow: %.2fG, %c[%d;%dmconsumed:%c[%d;%dm %.2fG, maximum: %.2fG, freelist: %.2fG\n", cache / GB, 27,1,color(limit),27,0, 25, consumed / GB, s_maxCacheSize / GB, freelist / GB);
			}
		}

		void normalize(doubletype magnitude, doubletype& extreme, doubletype& immense, doubletype& shallow) {
			while (magnitude > Properties::DEFAULT_CACHE_DIVISER) {
				magnitude /= 10.0;

				extreme /= 1.5;
				immense /= 1.5;
				shallow /= 1.5;
			}
		}

		void copylist(void) {
			if (m_modified == true) {
				m_rtObjects.ArrayList<RealTime*>::clear();

				s_rtObjects.lock();
				{
					m_modified = false;

					for (int i = 0; i < s_rtObjects.ArrayList<RealTime*>::size(); i++) {
						m_rtObjects.ArrayList<RealTime*>::add(s_rtObjects.ArrayList<RealTime*>::get(i));
					}
				}
				s_rtObjects.unlock();
			}
		}

		Limit usage(longtype cache, boolean log) {
			longtype consumed = Memory::getProcessAllocatedBytes();
			if (consumed > s_maxCacheSize) {
				s_maxCacheSize = consumed;
			}

			doubletype size = (doubletype) cache;
			if (cache > Properties::DEFAULT_CACHE_SIZE) {
				size = (doubletype) Properties::DEFAULT_CACHE_SIZE;
				size *= Properties::DEFAULT_CACHE_SIZE_PERCENT;

			} else {
				size *= Properties::DEFAULT_CACHE_SIZE_PERCENT;
			}

			longtype freelist = Memory::getFreeListBytes();

			if (freelist > (longtype) size) {
				consumed -= freelist;
			}

			if (Properties::getMemoryFragment() == true) {
				// TOOD: mathify this calculation (i.e. this could take a bit of study)
				s_fragmented = (freelist > (consumed / 2)) || (freelist > Properties::DEFAULT_CACHE_DIVISER);
			}

			Limit limit = (consumed > cache) ? NEUTRAL : IGNORE;
			if (limit > 0) {
				doubletype extreme = Properties::DEFAULT_CACHE_EXTREME_PERCENT;
				doubletype immense = Properties::DEFAULT_CACHE_IMMENSE_PERCENT;
				doubletype shallow = Properties::DEFAULT_CACHE_SHALLOW_PERCENT;

				doubletype percent = 0.0;
				if (consumed > cache) {
					percent = (doubletype) cache / (doubletype) consumed;

					// XXX: normalize percentage down to 1G (i.e. reduce %)
					normalize((doubletype) consumed, extreme, immense, shallow);
				}

				if (s_lastTimePurge == 0) {
					if ((1.0 - percent) < extreme) {
						s_lastTimePurge = s_lastTimeStamp; /* XXX: delay */

					} else {
						s_lastTimePurge = 1; /* XXX: pace now */
					}
				}

				if ((1.0 - percent) > extreme) {
					if (s_paceTimeout != Properties::DEFAULT_PACE_EXTREME) {
						s_paceTimeout = Properties::DEFAULT_PACE_EXTREME;
						log = true;
					}

					limit = EXTREME;

				} else if ((1.0 - percent) > immense) {
					if (s_paceTimeout != Properties::DEFAULT_PACE_IMMENSE) {
						s_paceTimeout = Properties::DEFAULT_PACE_IMMENSE;
						log = true;
					}

					limit = IMMENSE;

				} else if ((1.0 - percent) > shallow) {
					if (s_paceTimeout != Properties::DEFAULT_PACE_SHALLOW) {
						s_paceTimeout = Properties::DEFAULT_PACE_SHALLOW;
						log = true;
					}

					limit = SHALLOW;

				} else if (s_paceTimeout != Properties::DEFAULT_PACE_NEUTRAL) {
					s_paceTimeout = Properties::DEFAULT_PACE_NEUTRAL;
					log = true;
				}
			}

			if (log == true) {
				logging(cache, consumed, freelist, limit);
			}

			return limit;
		}

		void throttle(boolean flag) {
			if (s_exit == false) {
				if (((flag == false) && (s_pace == true)) || ((s_lastTimeStamp - s_lastTimeLimit) > Properties::DEFAULT_CACHE_LIMIT)) {
					s_pace = flag;

					synchronized(s_theLimit) {
						s_theLimit.notifyAll();
					}

					// XXX: due to wondering system clocks, ensure mutex is not blocked (i.e. stuck)
					s_lastTimeLimit = s_lastTimeStamp;

				} else {
					s_pace = flag;
				}
			}
		}

		void limit(boolean trigger /* logging and reorganize */) {
			if (s_exit == false) {

				/* Check to see if the memory size has changed. */
				DynamicUtils::getInstance()->checkForMemoryChange();

				/* Check to see if the processor count has changed. */
				if (true == Properties::getDynamicResources()) {
					inttype iNew   = 0;
					inttype iCur   = Properties::getWorkThreads();

					iNew = DynamicUtils::getInstance()->checkForProcessorChange();

					if ((s_exit == false) && (s_rtReadWriteLock.writeLock()->tryLock() == true)) {

						if (iCur < iNew) {
							/* If the thread count increases then add these additional threads */
							DynamicUtils::getInstance()->setThreadChanges(iNew);

							for (int i = iCur; i < iNew; i++) {
								Thread thread(&s_theTasks[i]);
								s_exitNotify.incrementAndGet();
								thread.start();
								DEEP_LOG(DEBUG, OTHER, "Starting workThread: %d max: %d\n", i, iNew);
							}

						} else if (iCur > iNew) {
							/* A reduction in the number of available cores. */
							DynamicUtils::getInstance()->setThreadChanges(iNew);

							DEEP_LOG(DEBUG, OTHER, "A reduction in the number of available cores, %d -> %d\n", iCur, iNew);

							for (int i = iNew; i < iCur; i++) {
								DEEP_LOG(DEBUG, OTHER, "Flag WorkThread %d for exit\n", i);
								s_theTasks[i].m_exitThread = true;
							}
						}

						s_rtReadWriteLock.writeLock()->unlock();
					}

				}

				/* Now calculate the amount of cache we have. */
				ulongtype cacheSize = Properties::getCacheSize();
				boolean   pace      = s_pace;

				s_cacheLimit = usage(cacheSize, trigger);
				if (s_cacheLimit > NEUTRAL) {
					if (pace == false) {
						pace = (s_lastTimeStamp - s_lastTimePurge) > Properties::DEFAULT_CACHE_PURGE;

					} else {
						s_lastTimePurge = s_lastTimeStamp;
					}

				} else if ((s_lastTimeStamp - s_lastTimePurge) > Properties::DEFAULT_CACHE_PURGE) {
					s_lastTimePurge = 0;
					pace = false;
				}

				if ((trigger == true) && (s_cacheLimit < IMMENSE)) {
					inttype working = 0;
					inttype reorging = 0;
					for (int i = 0; i < Properties::getWorkThreads(); i++) {

						// XXX: reorganizing threads can merge and that is intended
						if (s_theTasks[i].m_reorganize == true) {
							if ((i + 1) < Properties::getWorkThreads()) {
								working = i + 1;

							} else {
								working = 0;
							}

							reorging++;
						}
					}

					if (reorging < Properties::getReorgThreads()) {
						s_theTasks[working].m_reorganize = true;
					}
				}

				throttle(pace);
			}
		}

		#ifdef DEEP_IO_STATS
		void ioStats(boolean log) {
			if (log == true) {
				if ((s_exit == false) && (s_rtReadWriteLock.readLock()->tryLock() == true)) {
					copylist();

					ulongtype oR = 0, oC = 0, oU = 0, oD = 0, fV = 0, fH = 0, dV = 0, dH = 0;

					for (int i = 0; (s_exit == false) && (i < m_rtObjects.ArrayList<RealTime*>::size()); i++) {
						RealTime* rt = m_rtObjects.ArrayList<RealTime*>::get(i);

						rt->ioStatistics(&oR, &oC, &oU, &oD, &fV, &fH, &dV, &dH);
						const char* path = rt->getFilePath();

						DEEP_LOG(DEBUG, STATS, "crudl: %s, oR: %lld, oC: %lld, oU: %lld, oD: %lld\n", path, oR, oC, oU, oD);
						DEEP_LOG(DEBUG, STATS, "files: %s, fV: %lld, fH: %lld, dV: %lld, dH: %lld\n", path, fV, fH, dV, dH);
					}

					s_rtReadWriteLock.readLock()->unlock();
				}
			}
		}
		#endif

		void seekStats(boolean log, boolean reset) {
			if (log == true) {
				if ((s_exit == false) && (s_rtReadWriteLock.readLock()->tryLock() == true)) {
					copylist();

					ulongtype oT = 0, oI = 0;

					for (int i = 0; (s_exit == false) && (i < m_rtObjects.ArrayList<RealTime*>::size()); i++) {
						RealTime* rt = m_rtObjects.ArrayList<RealTime*>::get(i);

						rt->seekStatistics(&oT, &oI, reset);
					}

					s_rtReadWriteLock.readLock()->unlock();

					DEEP_LOG(DEBUG, SEEKS, "total: %lld, interval: %lld - reset: %d\n", oT, oI, reset);
				}
			}
		}

		void workTasks(boolean cycle, inttype thread) {
			boolean force = (cycle == true) || (s_theTasks[thread].m_continue == true) || (s_theTasks[thread].m_reorganize == true);

			boolean gCont = s_theTasks[thread].m_continue;
			boolean gReorg = s_theTasks[thread].m_reorganize;
			inttype gThreads = Properties::getWorkThreads();

			s_theTasks[thread].m_continue = false;
			s_theTasks[thread].m_reorganize = false;

			if ((s_cacheLimit > NEUTRAL) || (force == true) || ((s_lastTimeStamp - s_lastTimeTasks[thread]) > Properties::DEFAULT_CACHE_CYCLE)) {
				s_lastTimeTasks[thread] = s_lastTimeStamp;

				if ((s_exit == false) && (false == s_theTasks[thread].m_exitThread) && (s_rtReadWriteLock.readLock()->tryLock() == true)) {

					copylist();

					for (int i = 0, j = 0; (s_exit == false) && (i < m_rtObjects.ArrayList<RealTime*>::size()); i++, j++) {

						// XXX: indexing can take a while (e.g. reorg), use flag to speed up shutdown
						if (s_shutdown == true) {
							break;
						}

						// XXX: this number is dynamic
						gThreads = Properties::getWorkThreads();

						// XXX: This thread has been flagged for exit.
						if (true == s_theTasks[thread].m_exitThread) {
							s_theTasks[thread].m_reorganize = false;
							DEEP_LOG(DEBUG, OTHER, "Thread %d breaking out of workTasks (thread reduction)\n", thread);
							break;
						}

						if (j >= gThreads) {
							j = 0;
						}

						if (j != thread) {
							continue;
						}

						boolean lCont = gCont;
						boolean lReorg = gReorg;

						// XXX: indexing interally might purge due to cache pressure for create, update or delete
						m_rtObjects.ArrayList<RealTime*>::get(i)->indexCache(cycle, &lCont, &lReorg /* i.e. file */);
						{
							if (lCont == true) {
								s_theTasks[thread].m_continue = true;
							}

							if (lReorg == true) {
								s_reorganize = true;
							}
						}

						// XXX: indexing might purge due to cache pressure for heavy analytical reads (e.g. scans)
						if (s_cacheLimit > IMMENSE) {
							m_rtObjects.ArrayList<RealTime*>::get(i)->purgeCache(true /* index */, true /* deep */, false /* log */);
						}
					}

					s_rtReadWriteLock.readLock()->unlock();
				}
			}

			// XXX: start next instance to execute reorganize
			if (gReorg == true) {

				if ((thread + 1) < gThreads) {
					s_theTasks[thread + 1].m_reorganize = true;

				} else if (s_reorganize == true) {
					s_theTasks[0].m_reorganize = true;
					s_reorganize = false;
				}
			}
		}

		void purge(boolean log) {
			if ((s_cacheLimit > NEUTRAL) || (s_fragmented == true)) {
				if ((s_exit == false) && (s_rtReadWriteLock.readLock()->tryLock() == true)) {
					copylist();

					// XXX: sort by largest to smallest cache size (see compareTo)
					{
						for (int i = 0; (s_exit == false) && (i < m_rtObjects.ArrayList<RealTime*>::size()); i++) {
							m_rtObjects.ArrayList<RealTime*>::get(i)->sizeCache();
						}

						Collections::sort(&m_rtObjects);
					}

					for (int i = m_rtObjects.ArrayList<RealTime*>::size() - 1; (s_exit == false) && (i >= 0); i--) {
						m_rtObjects.ArrayList<RealTime*>::get(i)->purgeCache(false /* index */, s_cacheLimit > SHALLOW, log);

						// XXX: break when both memory pressure and memory fragmentation minimums have been achieved
						if ((s_cacheLimit == NEUTRAL) && (s_fragmented == false)) {
							break;
						}
					}

					s_rtReadWriteLock.readLock()->unlock();
				}
			}
		}

		void garbage(boolean force) {
			if (s_skipGarbage.get() == 0) {
				if ((force == true) || (s_gcObjects.ArrayList<Object*>::size() > Properties::DEFAULT_CACHE_GARBAGE)) {
					RETRY:
					synchronized(s_theGarbage) {
						boolean flag = s_gcObjects.getDeleteValue();
						s_gcObjects.setDeleteValue(false);

						for (int i = 0; (s_gc == true) && (s_gcObjects.ArrayList<Object*>::size() != 0); i++) {
							delete s_gcObjects.ArrayList<Object*>::remove(s_gcObjects.ArrayList<Object*>::size() - 1);

							if (i > Properties::DEFAULT_CACHE_GARBAGE) {
								break;
							}
						}

						s_gcObjects.setDeleteValue(flag);
					}

					if ((s_gc == true) && (s_gcObjects.ArrayList<Object*>::size() != 0)) {
						Thread::sleep(Properties::DEFAULT_PACE_NEUTRAL);
						goto RETRY;
					}
				}
			}
		}

		void fsusage(boolean log) {
			static const double GB = 1024 * 1024 * 1024;

			if ((log == true) && (s_exit == false) && (s_rtReadWriteLock.readLock()->tryLock() == true)) {
				copylist();

				Comparator<FileStore*> cmp;
				TreeSet<FileStore*> filesystems(&cmp, TreeSet<FileStore*>::INITIAL_ORDER, true);

				for (int i = 0; (s_exit == false) && (i < m_rtObjects.ArrayList<RealTime*>::size()); i++) {
					RealTime* rt = m_rtObjects.ArrayList<RealTime*>::get(i);

					try {
						FileStore fs = FileStore::getFileStore(File(rt->getFilePath()).getParentFile());

						double totalSpace = fs.getTotalSpace();
						double usableSpace = fs.getUsableSpace();
						double percent = usableSpace / totalSpace;

						s_fsLimits.lock();
						{
							if (percent <= Properties::DEFAULT_FS_EXTREME_PERCENT) {
								s_cacheFsUsageMode = Properties::DEFAULT_CACHE_FSUSAGE_FAST_MODE;
								s_fsLimits.HashMap<RealTime*,Limit>::put(rt, EXTREME);

							} else if (percent <= Properties::DEFAULT_FS_IMMENSE_PERCENT) {
								s_cacheFsUsageMode = Properties::DEFAULT_CACHE_FSUSAGE_FAST_MODE;
								s_fsLimits.HashMap<RealTime*,Limit>::put(rt, IMMENSE);

							} else if (percent <= Properties::DEFAULT_FS_SHALLOW_PERCENT) {
								s_cacheFsUsageMode = Properties::DEFAULT_CACHE_FSUSAGE_SLOW_MODE;
								s_fsLimits.HashMap<RealTime*,Limit>::put(rt, SHALLOW);

							} else {
								s_cacheFsUsageMode = Properties::DEFAULT_CACHE_FSUSAGE_SLOW_MODE;
								s_fsLimits.HashMap<RealTime*,Limit>::put(rt, NEUTRAL);
							}

							if (filesystems.contains(&fs) == false) {
								filesystems.add(new FileStore(fs));
								DEEP_LOG(INFO, FSMGT, "mount point: \"%s\", usage, total space: %.2fG, %c[%d;%dmavailable space:%c[%d;%dm %.2fG\n", fs.name()->data(), totalSpace / GB, 27,1,color(s_fsLimits.HashMap<RealTime*,Limit>::get(rt)),27,0, 25, usableSpace / GB);
							}
						}
						s_fsLimits.unlock();

					} catch(...) {
						DEEP_LOG(WARN, FSMGT, "unable to get mount point for: %s\n", File(rt->getFilePath()).getParentFile().data());
					}
				}

				s_rtReadWriteLock.readLock()->unlock();
			}
		}

		void sync(/* TODO: longtype delay */) {

			longtype interval = Properties::getDurableSyncInterval();
			if ((Properties::getDurable() == false) || (interval == 0)) {
				Thread::sleep(1000 /* delay */);
				return;
			}

			Thread::sleep(interval /* durable sync interval */);

			MeasuredRandomAccessFile::performSynchronizeGlobally(false /* decrement */);
		}

		void run(void) {

			if (s_inCleanup.get() == false) {
				boolean theSync = (this == &s_theSync);
				boolean theLimit = (this == &s_theLimit);
				boolean thePurge = (this == &s_thePurge);
				boolean theGarbage = (this == &s_theGarbage);
				boolean theFsusage = (this == &s_theFsusage);
				boolean theStats = (this == &s_theStats);

				inttype thread = 0;

				for (int i = 0; i < Properties::getWorkThreads(); i++) {
					if (this == &s_theTasks[i]) {
						Thread::sleep(Properties::DEFAULT_CACHE_SLEEP);
						s_theTasks[i].m_reorganize = false;
						thread = i;
						break;
					}
				}

				for (unsigned i = 1; (s_exit == false) && (s_shutdown == false); i++) {

					Thread::sleep(Properties::DEFAULT_CACHE_SLEEP);

					if (theGarbage == true) {
						garbage((i % 10) == 0);

					} else if (theSync == true) {
						sync(/* TODO: Properties::DEFAULT_SYNC_DELAY */);

					} else if (theLimit == true) {
						s_lastTimeStamp = System::currentTimeMillis();

						if ((s_cacheLimit > IGNORE) || ((i % 10) == 0) /* 1 sec */) {
							Memory::releaseAvailableBytes();
						}

						if ((s_cacheLimit > IGNORE) || ((i % 50) == 0) /* 5 sec */) {
							Transaction::minimum((i % Properties::DEFAULT_CACHE_TRANSACTION_DEBUG) == 0);
						}

						limit((i % Properties::DEFAULT_CACHE_LIMIT_MODE) == 0);

					} else if (thePurge == true) {
						purge((i % Properties::DEFAULT_CACHE_PURGE_MODE) == 0);

					} else if (theFsusage == true) {
						fsusage((i % s_cacheFsUsageMode /* dynamic interval */) == 0);

					} else if (theStats == true) {
						#ifdef DEEP_IO_STATS
						ioStats((i % Properties::DEFAULT_CACHE_STATS_MODE) == 0);
						#endif

						if (Properties::getSeekStatistics() == true) {
							seekStats((i % Properties::getSeekStatisticsDisplayMode()) == 0, (i % Properties::getSeekStatisticsResetMode()) == 0);
						}

					} else /* if (indexing, reorging, etc...) */ {
						if (true == s_theTasks[thread].m_exitThread) {
							// XXX: worker thread count has been reduced, drop off from execution

							DEEP_LOG(DEBUG, OTHER, "Thread %d leaving work cycle execution, starting cleanup.\n", thread);

							s_exitNotify.decrementAndGet();

							s_theTasks[thread].m_modified   = true;
							s_theTasks[thread].m_continue   = false;
							s_theTasks[thread].m_reorganize = false;
							s_theTasks[thread].m_exitThread = false;

							m_rtObjects.ArrayList<RealTime*>::clear();

							DEEP_LOG(INFO, OTHER, "Thread %d leaving work cycle execution.\n", thread);
							return;
						}
						else {
							workTasks((i % Properties::DEFAULT_CACHE_LIMIT_MODE) == 0, thread);
						}
					}
				}
			}

			// XXX: ignore synchronized syntax for performance (see: Synchronizable)
			s_theGarbage.lock();
			{
				longtype count = s_exitNotify.decrementAndGet();
				if (count != 0) {
					s_theGarbage.wait();

				} else {
					s_exit = false;
					s_shutdown = false;

					s_theGarbage.notifyAll();
				}
			}
			s_theGarbage.unlock();

		}

	public:
		FORCE_INLINE static Limit getCacheUsage(void) {
			return s_cacheLimit;
		}

		FORCE_INLINE static Limit getFSUsage(RealTime* rt) {
			Limit usage = IGNORE;

			s_fsLimits.lock();
			{
				usage = s_fsLimits.HashMap<RealTime*,Limit>::get(rt);
			}
			s_fsLimits.unlock();

			return usage;
		}

		FORCE_INLINE static boolean getPaceFlag(void) {
			return s_pace;
		}

		FORCE_INLINE static boolean getPurgeFlag(void) {
			return (s_cacheLimit > IGNORE);
		}

		FORCE_INLINE static boolean getFragmented(void) {
			return s_fragmented;
		}

		FORCE_INLINE static void immediateShutdown(void) {
			s_shutdown = true;

			#if 0
			s_rtReadWriteLock.readLock()->lock();
			s_rtObjects.lock();
			if (s_rtObjects.ArrayList<RealTime*>::size() == 0) {
				s_exit = true;
			}
			s_rtObjects.unlock();
			s_rtReadWriteLock.readLock()->unlock();
			#endif
		}

		FORCE_INLINE static longtype currentTimeMillis(void) {
			return s_lastTimeStamp;
		}

		FORCE_INLINE static void setInfinitelimit(boolean flag) { 
			if (flag == true) {
				s_infinitelimit = 1;

			} else {
				s_infinitelimit = 0;
			}
		}

		FORCE_INLINE static boolean getInfinitelimit(void) {
			return (s_infinitelimit != 0);
		}

		FORCE_INLINE void gc(Object* object) {
			// XXX: ignore synchronized syntax for performance (see: Synchronizable)
			s_theGarbage.lock();
			{
				s_gcObjects.ArrayList<Object*>::add(object);
			}
			s_theGarbage.unlock();
		}

		FORCE_INLINE static void pace(boolean factor, boolean wait, inttype infinite = 100) {
			do {
				// XXX: ignore synchronized syntax for performance (see: Synchronizable)
				s_theLimit.lock();
				{
					s_theLimit.wait((factor == true) || (s_cacheLimit == EXTREME) ? s_paceTimeout * 10 : s_paceTimeout);
				}
				s_theLimit.unlock();

			} while ((wait == true) && (s_infinitelimit == 1) && ((s_cacheLimit == EXTREME)) && (--infinite > 0));

			if (infinite == 0) {
				DEEP_LOG(WARN, CACHE, "memory pressure not relieved: %.2fG\n", Memory::getProcessAllocatedBytes() / GB);
			}
		}

		FORCE_INLINE static void gcRuntime(boolean flag /* i.e. on/off */) {
			if (Memory::hasManagement() == true) {
				s_gc = flag;
			}
		}

		FORCE_INLINE static void gcShutdown(boolean flag /* i.e. on/off */) {
			if (Memory::hasManagement() == true) {
				synchronized(s_theGarbage) {
					s_gcObjects.setDeleteValue(flag);
				}
			}
		}

		FORCE_INLINE static boolean checkMemoryRequirements() {
			boolean status = Memory::checkManagement();

			if (status == false) {
				DEEP_LOG(ERROR, OTHER, "(MEMORY): %s memory management library not found... exiting\n", Memory::getManagementName());
			}

			return status;
		}

	public:
		inline RealTimeResource(void):
			m_modified(true),
			m_reorganize(false),
			m_exitThread(false),
			m_rtObjects(Properties::LIST_CAP) {
		}

		inline void add(RealTime* rtObject) {
			s_rtReadWriteLock.readLock()->lock();
			{
				if (RealTimeResource::checkMemoryRequirements() == false) {
					exit(1);
				}

				s_rtObjects.lock();
				{
					if (s_rtObjects.ArrayList<RealTime*>::size() == 0) {
						boolean thread = true;

						if (s_infinitelimit == -1) {
							s_infinitelimit = 1;
						}

						synchronized(s_theGarbage) {
							thread = (s_lastTimeStamp == 0);

							s_lastTimeStamp = System::currentTimeMillis();

							for (int i = 0; i < Properties::DEFAULT_CACHE_WORK_THREADS_MAX; i++) {
								s_lastTimeTasks[i] = s_lastTimeStamp;
							}
						}

						if (thread == true) {
							Thread syncThread(&s_theSync);
							s_exitNotify.incrementAndGet();
							syncThread.start();

							Thread limitThread(&s_theLimit);
							s_exitNotify.incrementAndGet();
							limitThread.start();

							Thread purgeThread(&s_thePurge);
							s_exitNotify.incrementAndGet();
							purgeThread.start();

							Thread garbageThread(&s_theGarbage);
							s_exitNotify.incrementAndGet();
							garbageThread.start();

							Thread fsusageThread(&s_theFsusage);
							s_exitNotify.incrementAndGet();
							fsusageThread.start();

							Thread statsThread(&s_theStats);
							s_exitNotify.incrementAndGet();
							statsThread.start();

							inttype gThreads = Properties::getWorkThreads();

							for (int i = 0; i < gThreads; i++) {
								Thread thread(&s_theTasks[i]);
								s_exitNotify.incrementAndGet();
								thread.start();
								DEEP_LOG(DEBUG, OTHER, "Starting workThread: %d max: %d\n", i, gThreads);
							}
						}
					}

					if (s_rtObjects.ArrayList<RealTime*>::indexOf(rtObject) == -1) {
						s_rtObjects.ArrayList<RealTime*>::add(rtObject);

					#ifdef DEEP_DEBUG
					} else {
						DEEP_LOG(ERROR, OTHER, "Invalid register: Map already added to cache: %s\n", rtObject->getFilePath());

						throw InvalidException("Invalid register: Map already added to cache");
					#endif
					}

					s_thePurge.m_modified = true;
					s_theFsusage.m_modified = true;
					for (int i = 0; i < Properties::getWorkThreads(); i++) {
						s_theTasks[i].m_modified = true;
					}
				}
				s_rtObjects.unlock();

			}
			s_rtReadWriteLock.readLock()->unlock();
		}

		inline void remove(RealTime* rtObject) {

			// XXX: notify objects to postpone long duration logic (e.g. file reorganization)
			s_rtReadWriteLock.readLock()->lock();
			{
				s_rtObjects.lock();
				{
					for (int i = 0; i < s_rtObjects.ArrayList<RealTime*>::size(); i++) {
						RealTime* rt = s_rtObjects.ArrayList<RealTime*>::get(i);
						if (rt->getMount() == RealTime::MOUNT_OPENED) {
							rt->setMount(RealTime::MOUNT_PAUSED);
						}
					}
				}
				s_rtObjects.unlock();
			}
			s_rtReadWriteLock.readLock()->unlock();

			s_rtReadWriteLock.writeLock()->lock();
			{
				// XXX: notify objects all clear on long duration logic
				for (int i = 0; i < s_rtObjects.ArrayList<RealTime*>::size(); i++) {
					RealTime* rt = s_rtObjects.ArrayList<RealTime*>::get(i);
					if (rt->getMount() == RealTime::MOUNT_PAUSED) {
						rt->setMount(RealTime::MOUNT_OPENED);
					}
				}

				s_rtObjects.ArrayList<RealTime*>::remove(rtObject);

				s_fsLimits.lock();
				{
					s_fsLimits.HashMap<RealTime*,Limit>::remove(rtObject);
				}
				s_fsLimits.unlock();

				s_thePurge.m_modified = true;
				s_theFsusage.m_modified = true;
				for (int i = 0; i < Properties::getWorkThreads(); i++) {
					s_theTasks[i].m_modified = true;
				}

				if (s_rtObjects.ArrayList<RealTime*>::size() == 0) {
					synchronized(s_theGarbage) {
						s_pace = false;
						s_cacheLimit = IGNORE;

						if (s_infinitelimit == 1) {
							s_infinitelimit = -1;
						}
					}
				}
			}
			s_rtReadWriteLock.writeLock()->unlock();
		}

		static void setSkipGarbage(boolean skip) {
			if (skip == true) {
				s_skipGarbage.incrementAndGet();

			} else {
				s_skipGarbage.decrementAndGet();
			}
		}
};

// XXX: This must be the first static variable
AtomicBoolean RealTimeResource::s_inCleanup(false);

boolean RealTimeResource::s_gc = true;
boolean RealTimeResource::s_exit = false;
boolean RealTimeResource::s_pace = false;
boolean RealTimeResource::s_shutdown = false;
boolean RealTimeResource::s_fragmented = false;
boolean RealTimeResource::s_reorganize = false;
inttype RealTimeResource::s_infinitelimit = 1; /* while loop on cache pressure */
inttype RealTimeResource::s_cacheFsUsageMode = Properties::DEFAULT_CACHE_FSUSAGE_SLOW_MODE;

RealTimeResource::Limit RealTimeResource::s_cacheLimit = IGNORE;
longtype RealTimeResource::s_paceTimeout = Properties::DEFAULT_PACE_NEUTRAL;
longtype RealTimeResource::s_maxCacheSize = 0;

longtype RealTimeResource::s_lastTimeStamp = 0;
longtype RealTimeResource::s_lastTimePurge = 0;
longtype RealTimeResource::s_lastTimeLimit = 0;
longtype RealTimeResource::s_lastTimeTasks[Properties::DEFAULT_CACHE_WORK_THREADS_MAX];

AtomicLong RealTimeResource::s_exitNotify(0);
AtomicInteger RealTimeResource::s_skipGarbage(0);

ArrayList<Object*> RealTimeResource::s_gcObjects(Properties::LIST_CAP, true);
LockableArrayList<RealTime*> RealTimeResource::s_rtObjects(Properties::LIST_CAP, false);
LockableHashMap<RealTime*,RealTimeResource::Limit> RealTimeResource::s_fsLimits(Properties::HASH_CAP, false);

ReentrantReadWriteLock RealTimeResource::s_rtReadWriteLock;

// XXX: order of static initializer requires garbage instance first
RealTimeResource RealTimeResource::s_theGarbage;

RealTimeResource RealTimeResource::s_theSync;
RealTimeResource RealTimeResource::s_theLimit;
RealTimeResource RealTimeResource::s_thePurge;
RealTimeResource RealTimeResource::s_theFsusage;
RealTimeResource RealTimeResource::s_theTasks[Properties::DEFAULT_CACHE_WORK_THREADS_MAX];
RealTimeResource RealTimeResource::s_theStats;

// XXX: This must be the last static variable
RealTimeResource::Cleanup RealTimeResource::s_cleanup;

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECACHE_H_*/
