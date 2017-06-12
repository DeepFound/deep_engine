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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_PROPERTIES_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_PROPERTIES_H_ 

#include "cxx/lang/types.h"
#include "cxx/lang/String.h"

using namespace cxx::lang;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

class Properties {

	private:
		static inttype s_threadLimit;
		static longtype s_cacheSize;
		static uinttype s_fileSize;
		static uinttype s_trtFileSize;

		static inttype s_fileLimit;
		static inttype s_workThreads;
		static inttype s_reorgThreads;

		static inttype s_transChunk;
		static inttype s_transTimeout;
		static boolean s_transStream;
		static inttype s_segmentSize;

		static ulongtype s_debugOptions;
		static ulongtype s_logOptions;
		static ulongtype s_profilingOptions;
		static inttype s_profilingTriggerInterval;
		static longtype s_statisticsFlushInterval;

		static boolean s_memoryFragment;
		static boolean s_memoryAnalytics;

		static boolean s_recoveryReplay;
		static boolean s_recoveryRealign;
		static boolean s_recoverySafe;
		static boolean s_dynamicResources;

		static boolean s_durable;
		static longtype s_durableSyncInterval;
		#ifdef DEEP_SYNCHRONIZATION_GROUPING
		static longtype s_durableHoldDownTime;
		static longtype s_durableHoldDownThreshold;
		#endif
		static doubletype s_valueCompressPercent;

		static doubletype s_reorgWorkPercent;
		static doubletype s_fragmentationPercent;
		static boolean s_semiPurge;
		static boolean s_dynamicSummarization;
		static boolean s_rangeSync;
		static boolean s_allowLrtVrtMismatch;
		static boolean s_cardinalityRecalculateRecovery;

		static boolean s_seekStatistics;
		static inttype s_seekStatisticsResetInterval;
		static inttype s_seekStatisticsDisplayInterval;

		static uinttype s_automaticCheckpointInterval;
		static inttype s_fileRefCheckMod;
		
		static String s_activation_key;
		static int s_system_uid;
		static bool s_activation_key_updated;

	public:
		static const inttype LIST_CAP = 100;
		static const inttype ARRAY_CAP = 25;
		static const inttype HASH_CAP = 256;

		static const inttype FIXED_KEY_SIZE = 767;
		static const inttype FIXED_INDEX_CAP = 64 + 1 /* +1 hidden key */;
		static const bytetype FIXED_KEY_PARTS = 32; /* secondary can include primary */

		static const inttype DEFAULT_THREAD_LIMIT = 1024 + 1;
		static const inttype DEFAULT_STORYLINE_MAXIMUM = 10; /* see StoryLock ubytetype */

		static const inttype DEFAULT_CYCLE_LIMIT = 1;
		static const inttype DEFAULT_CYCLE_QUANTA_LIMIT = 3;

		static const inttype DEFAULT_CACHE_SLEEP = 100;
		static const inttype DEFAULT_CACHE_PURGE = 500;
		static const inttype DEFAULT_CACHE_FLUSH = 100;
		static const inttype DEFAULT_CACHE_LIMIT = 10000;
		static const inttype DEFAULT_CACHE_CYCLE = 15000;
		static const inttype DEFAULT_CACHE_GARBAGE = 1000;
		static const inttype DEFAULT_CACHE_FINALIZE_IDLE = 120000;
		static const doubletype DEFAULT_CACHE_PERCENT_DEFAULT = 0.70;

		static const inttype DEFAULT_CACHE_FINAL_CHECK_RETRY = 3;
		static const inttype DEFAULT_CACHE_FINAL_MAXIMUM_RETRY = 25;

		static const inttype DEFAULT_CACHE_WORK_THREADS_MIN = 5;
		static const inttype DEFAULT_CACHE_WORK_THREADS_MAX = 32;
		static const inttype DEFAULT_CACHE_REORG_THREADS_MIN = DEFAULT_CACHE_WORK_THREADS_MIN / 2;
		static const inttype DEFAULT_CACHE_REORG_THREADS_MAX = DEFAULT_CACHE_WORK_THREADS_MAX / 2;
		static const inttype DEFAULT_CACHE_CHECK_THREADS_RETRY = 50;

		static const longtype DEFAULT_CACHE_SIZE = 1073741824L;
		static const longtype DEFAULT_CACHE_DIVISER = DEFAULT_CACHE_SIZE * 10;

		#ifdef DEEP_IO_STATS
		static const inttype DEFAULT_CACHE_STATS_MODE = 300 /* 30 seconds */;
		#endif
		static const inttype DEFAULT_CACHE_LIMIT_MODE = 600 /* 60 seconds */;
		static const inttype DEFAULT_CACHE_PURGE_MODE = 150 /* 15 seconds */;
		static const inttype DEFAULT_CACHE_FSUSAGE_FAST_MODE = 3000 /* 5 minutes */;
		static const inttype DEFAULT_CACHE_FSUSAGE_SLOW_MODE = 6000 /* 15 minutes */;
		static const inttype DEFAULT_CACHE_TRANSACTION_DEBUG = 6000 /* 15 minutes */;
		static const inttype DEFAULT_CACHE_SEEK_RESET_INTERVAL = 600000 /* 10 minutes */;
		static const inttype DEFAULT_CACHE_SEEK_DISPLAY_INTERVAL = 30000 /* 30 seconds */;

		static const inttype DEFAULT_PACE_NEUTRAL = 5;
		static const inttype DEFAULT_PACE_SHALLOW = 25;
		static const inttype DEFAULT_PACE_IMMENSE = 50;
		static const inttype DEFAULT_PACE_EXTREME = 100;
		static const inttype DEFAULT_PACE_INFINITE = -1;

		static const ubytetype DIR_SIZE_WITHOUT_TABLE_FILES = 4; /* up,current,frm,trt */
		static const uinttype DEFAULT_FILE_HEADER = 50; /* at lease Versions.h */
		static const inttype DEFAULT_FILE_LIMIT = 512; /* file open limit */
		static const inttype DEFAULT_FILE_ARRAY = 65535;
		static const inttype DEFAULT_FILE_BUFFER = 8192;
		static const inttype DEFAULT_FILE_NAME_SIZE = 25;
		static const inttype DEFAULT_FILE_DATA_SIZE = 1073741824;
		static const inttype DEFAULT_FILE_FETCH_MIN = 67108864;
		static const inttype DEFAULT_FILE_FETCH_FACTOR = 1;
		static const inttype DEFAULT_FILE_MIN_REORG_UPTIME = 450000;
		static const ushorttype DEFAULT_FILE_MAX_REORG_IGNORE = 25;
		static const inttype DEFAULT_FILE_CLEANUP_INTERVAL = 120; /* 2 min */

		static const uinttype DEFAULT_FILE_MIN_SIZE = 104857600; /* 100MB */

		static const inttype DEFAULT_TRANSACTION_SIZE = 1024 * 10;
		static const inttype DEFAULT_TRANSACTION_RETRY = 2;
		static const inttype DEFAULT_TRANSACTION_ORDER = 37;
		static const inttype DEFAULT_TRANSACTION_DEPTH = 128;
		static const inttype MAXIMUM_TRANSACTION_DEPTH = 10000;
		static const inttype DEFAULT_TRANSACTION_CHUNK = 1500; /* see DEFAULT_SEGMENT_SIZE */
		static const boolean DEFAULT_TRANSACTION_STREAM = false;
		static const inttype DEFAULT_TRANSACTION_TIMEOUT = 50000;
		static const inttype DEFAULT_TRANSACTION_INFINITE = DEFAULT_TRANSACTION_DEPTH / 2;

		static const inttype DEFAULT_SEGMENT_FILE_RANGE = 8; /* char 8 bits */
		static const inttype DEFAULT_SEGMENT_LEAF_ORDER = 37;
		static const inttype DEFAULT_SEGMENT_BRANCH_ORDER = 11;

		static const ushorttype DEFAULT_SEGMENT_SIZE = 1500;
		static const inttype DEFAULT_SEGMENT_BYTE_SIZE = 26214400; /* 25M */
		static const inttype DEFAULT_SEGMENT_SUMMARY_SIZE = DEFAULT_SEGMENT_SIZE;
		static const inttype DEFAULT_MAX_SUMMARY_FILE_REFS = 10;
		static const inttype DEFAULT_SEGMENT_MAXIMUM_SIZE = 50000; /* 50K is well over conceivable limit */
		static const ulongtype DEFAULT_SEGMENT_INDEXING_MIN = 100000;
		static const ulongtype DEFAULT_SEGMENT_INDEXING_MAX = 1000000;
		static const inttype DEFAULT_SEGMENT_SUMMARIZATION_LIMIT = 100;

		static const inttype DEFAULT_DURABLE_SYNC_INTERVAL = 0;
		static const inttype DEFAULT_FILE_RANGE_SYNC_CHUNK = 10000000;
		static const longtype DEFAULT_STATISTICS_FLUSH_INTERVAL = 60; /* 60 seconds */
		static const bytetype DEFAULT_VALUE_STATISTIC_PERCENT_REWRITE = 2;
		static const bytetype DEFAULT_SEGMENT_MINIMUM_KEY_BLOCK_DIVISOR = 2;
		static const bytetype DEFAULT_MINIMUM_MEMORY_COMPRESSION_KEY = 2;
		static const bytetype DEFAULT_MINIMUM_MEMORY_COMPRESSION_KEY_VALUE = 5;
		static const bytetype DEFAULT_MEMORY_COMPRESSION_SEGMENT_SIZE_DIVISOR = 5;
		static const floattype DEFAULT_KEY_COMPRESSION_RATIO /* see Properties.cxx */;
		static const doubletype DEFAULT_SECONDARY_COMPRESSION_FACTOR /* see Properties.cxx */;

		static const doubletype DEFAULT_FILE_SIZE_PERCENT /* see Properties::cxx */;
		static const doubletype DEFAULT_CACHE_SIZE_PERCENT /* see Properties::cxx */;

		static const doubletype DEFAULT_CACHE_SHALLOW_PERCENT /* see Properties::cxx */;
		static const doubletype DEFAULT_CACHE_IMMENSE_PERCENT /* see Properties::cxx */;
		static const doubletype DEFAULT_CACHE_EXTREME_PERCENT /* see Properties::cxx */;

		static const doubletype DEFAULT_FS_SHALLOW_PERCENT /* see Properties::cxx */;
		static const doubletype DEFAULT_FS_IMMENSE_PERCENT /* see Properties::cxx */;
		static const doubletype DEFAULT_FS_EXTREME_PERCENT /* see Properties::cxx */;

		static const doubletype DEFAULT_FILE_FRAG_IDLE_DIVISOR /* see Properties::cxx */;
		static const doubletype DEFAULT_FILE_FRAG_SIZE_RATIO_HIGH /* see Properties::cxx */;
		static const doubletype DEFAULT_FILE_FRAG_SIZE_RATIO_MED_HIGH /* see Properties::cxx */;
		static const doubletype DEFAULT_FILE_FRAG_SIZE_RATIO_MED_LOW /* see Properties::cxx */;
		static const doubletype DEFAULT_FILE_FRAG_SIZE_RATIO_LOW /* see Properties::cxx */;

		static const uinttype DEFAULT_FILE_FRAG_DIFF_HIGH = 50;
		static const uinttype DEFAULT_FILE_FRAG_DIFF_MED_HIGH = 30;
		static const uinttype DEFAULT_FILE_FRAG_DIFF_MED_LOW = 15;
		static const uinttype DEFAULT_FILE_FRAG_DIFF_LOW = 5;

		static const uinttype DEFAULT_SUMMARY_TARGET = 3;
		static const uinttype DEFAULT_SUMMARY_ANCHOR = 2;
		static const ulongtype DEFAULT_SUMMARY_STORAGE_THRESHOLD = 2;

		static const uinttype COLD_POINT_READ_DIVISOR = 400;
		static const uinttype CHECKPOINT_MIN_FILE_SIZE_FACTOR = 3;

		// see ct_plugin.cc : ct_datastore_debug_option_names
		enum DebugOption {
			KEY_RANGE = 0,
			KEY_PRIMARY = 1,
			KEY_SECONDARY = 2,
			MEMORY_STATISTICS = 3,
			NO_RECOVERY_REPLAY = 4,
			NO_PARTIAL_REPLAY = 5,
			ONLY_PARTIAL_REPLAY = 6,
			FILE_REF_CHECKS = 7
		};

		// see ct_plugin.cc : ct_datastore_log_option_names
		enum LogOption {
			VERBOSE_INDEX = 0,
			VERBOSE_PURGE = 1
		};

		// see ct_plugin.cc : ct_datastore_profiling_option_names
		enum ProfilingOption {
			COMMIT = 0,
			QUERY = 1
		};

		enum CheckpointMode {
			CHECKPOINT_OFF = 0,
			CHECKPOINT_AUTO = 1,
			CHECKPOINT_MANUAL = 2
		};

	private:
		static CheckpointMode s_checkpointMode;

	public:
		FORCE_INLINE static void setThreadLimit(inttype limit) {
			s_threadLimit = limit;
		}

		FORCE_INLINE static inttype getThreadLimit(void) {
			if (s_threadLimit == 0) {
				s_threadLimit = DEFAULT_THREAD_LIMIT;
			}

			return s_threadLimit;
		}

		FORCE_INLINE static void setCacheSize(longtype size) {
			s_cacheSize = size;
		}

		FORCE_INLINE static longtype getCacheSizeActual(void) {
			return s_cacheSize;
		}

		FORCE_INLINE static longtype getCacheSize(void) {
			if (s_cacheSize == 0) {
				s_cacheSize = DEFAULT_CACHE_SIZE;
			}

			doubletype size(s_cacheSize);
			if (s_cacheSize > DEFAULT_CACHE_SIZE) {
				size = (doubletype) DEFAULT_CACHE_SIZE;
			}

			// XXX: % above size since datastore's seem to over subscribe
			return s_cacheSize + (size * DEFAULT_CACHE_SIZE_PERCENT);
		}

		FORCE_INLINE static void setFileSize(uinttype size) {
			s_fileSize = size;
		}

		FORCE_INLINE static uinttype getFileSize(void) {
			if (s_fileSize == 0) {
				s_fileSize = DEFAULT_FILE_DATA_SIZE;
			}

			// XXX: percent to fit concurrent work in file
			doubletype size(s_fileSize);
			return s_fileSize - (size * DEFAULT_FILE_SIZE_PERCENT);
		}

		FORCE_INLINE static void setTrtFileSize(uinttype size) {
			s_trtFileSize = size;
		}

		FORCE_INLINE static uinttype getTrtFileSize() {
			return s_trtFileSize;
		}

		FORCE_INLINE static inttype getFileCleanupInterval(void) {
			return DEFAULT_FILE_CLEANUP_INTERVAL;
		}

		FORCE_INLINE static uinttype getMinFileSize(void) {
			const uinttype fileSize = getFileSize();
			if (fileSize < DEFAULT_FILE_MIN_SIZE) {
				return fileSize;
			}

			return DEFAULT_FILE_MIN_SIZE;
		}

		FORCE_INLINE static void setFileLimit(inttype limit) {
			s_fileLimit = limit;
		}

		FORCE_INLINE static inttype getFileLimit(void) {
			if (s_fileLimit == 0) {
				s_fileLimit = DEFAULT_FILE_LIMIT;
			}

			return s_fileLimit;
		}

		FORCE_INLINE static void setWorkThreads(inttype threads) {
			if (threads < DEFAULT_CACHE_WORK_THREADS_MIN) {
				threads = DEFAULT_CACHE_WORK_THREADS_MIN;

			} else if (threads > DEFAULT_CACHE_WORK_THREADS_MAX) {
				threads = DEFAULT_CACHE_WORK_THREADS_MAX;
			}

			s_workThreads = threads;
		}

		FORCE_INLINE static inttype getWorkThreads(void) {
			if (s_workThreads == 0) {
				s_workThreads = DEFAULT_CACHE_WORK_THREADS_MIN;
			}

			return s_workThreads;
		}

		FORCE_INLINE static void setReorgThreads(inttype threads) {
			// XXX: reorg threads are hijacked index threads
			if (threads < DEFAULT_CACHE_REORG_THREADS_MIN) {
				threads = DEFAULT_CACHE_REORG_THREADS_MIN;

			} else if (threads > DEFAULT_CACHE_REORG_THREADS_MAX) {
				threads = DEFAULT_CACHE_REORG_THREADS_MAX;
			}

			s_reorgThreads = threads;
		}

		FORCE_INLINE static inttype getReorgThreads(void) {
			if (s_reorgThreads == 0) {
				s_reorgThreads = DEFAULT_CACHE_REORG_THREADS_MIN;
			}

			return s_reorgThreads;
		}

		FORCE_INLINE static void setTransactionTimeout(inttype timeout) {
			s_transTimeout = timeout;
		}

		FORCE_INLINE static inttype getTransactionTimeout(void) {
			if (s_transTimeout == 0) {
				s_transTimeout = DEFAULT_TRANSACTION_TIMEOUT;
			}

			return s_transTimeout;
		}

		FORCE_INLINE static void setTransactionChunk(inttype chunk) {
			s_transChunk = chunk;
		}

		FORCE_INLINE static inttype getTransactionChunk(void) {
			if (s_transChunk == 0) {
				s_transChunk = DEFAULT_TRANSACTION_CHUNK;
			}

			return s_transChunk;
		}

		FORCE_INLINE static void setTransactionStream(boolean stream) {
			s_transStream = stream;
		}

		FORCE_INLINE static boolean getTransactionStream(void) {
			return s_transStream;
		}

		FORCE_INLINE static void setSegmentSize(inttype size) {
			s_segmentSize = size;
		}

		FORCE_INLINE static inttype getSegmentSize(void) {
			if (s_segmentSize == 0) {
				s_segmentSize = DEFAULT_SEGMENT_SIZE;
			}

			return s_segmentSize;
		}

		FORCE_INLINE static void setDebugEnabled(DebugOption option, boolean enabled) {
			if (enabled == true) {
				s_debugOptions |= (1 << option);

			} else {
				s_debugOptions &= ~(1 << option);
			}
		}

		FORCE_INLINE static boolean getDebugEnabled(DebugOption option) {
			// TODO: Force this on for now but leave as an option. Can be removed completely if we don't see any problems
			if (option == FILE_REF_CHECKS) {
				return true;
			}

			return s_debugOptions & (1 << option);
		}

		FORCE_INLINE static void setLogOption(LogOption option, boolean enabled) {
			if (enabled == true) {
				s_logOptions |= (1 << option);

			} else {
				s_logOptions &= ~(1 << option);
			}
		}

		FORCE_INLINE static boolean getLogOption(LogOption option) {
			return s_logOptions & (1 << option);
		}

		FORCE_INLINE static void setProfilingEnabled(ProfilingOption option, boolean enabled) {
			if (enabled == true) {
				s_profilingOptions |= (1 << option);

			} else {
				s_profilingOptions &= ~(1 << option);
			}
		}

		FORCE_INLINE static boolean getProfilingEnabled(ProfilingOption option) {
			return s_profilingOptions & (1 << option);
		}

		FORCE_INLINE static void setProfilingTriggerInterval(inttype interval) {
			s_profilingTriggerInterval = interval;
		}
	
		FORCE_INLINE static inttype getProfilingTriggerInterval(void) {
			return s_profilingTriggerInterval;
		}

		FORCE_INLINE static void setStatisticsFlushInterval(inttype interval) {
			if (interval < DEFAULT_STATISTICS_FLUSH_INTERVAL) {
				interval = DEFAULT_STATISTICS_FLUSH_INTERVAL;
			}
				
			s_statisticsFlushInterval = interval;
		}

		FORCE_INLINE static longtype getStatisticsFlushInterval(void) {
			return s_statisticsFlushInterval;
		}

		FORCE_INLINE static void setMemoryFragment(boolean enabled) {
			s_memoryFragment = enabled;
		}

		FORCE_INLINE static boolean getMemoryFragment(void) {
			return s_memoryFragment;
		}

		FORCE_INLINE static void setMemoryAnalytics(boolean enabled) {
			s_memoryAnalytics = enabled;
		}

		FORCE_INLINE static boolean getMemoryAnalytics(void) {
			return s_memoryAnalytics;
		}

		FORCE_INLINE static void setRecoveryReplay(boolean enabled) {
			s_recoveryReplay = enabled;
		}

		FORCE_INLINE static boolean getRecoveryReplay(void) {
			return s_recoveryReplay;
		}

		FORCE_INLINE static void setRecoveryRealign(boolean enabled) {
			s_recoveryRealign = enabled;
		}

		FORCE_INLINE static boolean getRecoveryRealign(void) {
			return s_recoveryRealign;
		}

		FORCE_INLINE static void setRecoverySafe(boolean enabled) {
			s_recoverySafe = enabled;
		}

		FORCE_INLINE static boolean getRecoverySafe(void) {
			return s_recoverySafe;
		}

		FORCE_INLINE static void setDynamicResources(boolean dynamic) {
			s_dynamicResources = dynamic;
		}

		FORCE_INLINE static boolean getDynamicResources(void) {
			return s_dynamicResources;
		}

		FORCE_INLINE static void setDurable(boolean enabled) {
			s_durable = enabled;
		}

		FORCE_INLINE static boolean getDurable(void) {
			return s_durable;
		}

		FORCE_INLINE static void setDurableSyncInterval(longtype interval) {
			s_durableSyncInterval = interval;
		}

		FORCE_INLINE static longtype getDurableSyncInterval(void) {
			return s_durableSyncInterval;
		}

		#ifdef DEEP_SYNCHRONIZATION_GROUPING
		FORCE_INLINE static void setDurableHoldDownTime(longtype hold) {
			s_durableHoldDownTime = hold;
		}

		FORCE_INLINE static longtype getDurableHoldDownTime(void) {
			return s_durableHoldDownTime;
		}

		FORCE_INLINE static void setDurableHoldDownThreshold(longtype threshold) {
			s_durableHoldDownThreshold = threshold;
		}

		FORCE_INLINE static longtype getDurableHoldDownThreshold(void) {
			return s_durableHoldDownThreshold;
		}
		#endif

		FORCE_INLINE static void setValueCompressPercent(double percent) {
			s_valueCompressPercent = percent;
		}
	
		FORCE_INLINE static double getValueCompressPercent(void) {
			return s_valueCompressPercent;
		}

		FORCE_INLINE static void setSeekStatistics(boolean enabled) {
			s_seekStatistics = enabled;
		}
	
		FORCE_INLINE static boolean getSeekStatistics(void) {
			return s_seekStatistics;
		}

		FORCE_INLINE static void setSeekStatisticsResetInterval(inttype interval) {
			s_seekStatisticsResetInterval = interval;
		}
	
		FORCE_INLINE static inttype getSeekStatisticsResetInterval(void) {
			return s_seekStatisticsResetInterval;
		}

		FORCE_INLINE static inttype getSeekStatisticsResetMode(void) {
			return s_seekStatisticsResetInterval / DEFAULT_CACHE_SLEEP;
		}

		FORCE_INLINE static void setSeekStatisticsDisplayInterval(inttype interval) {
			s_seekStatisticsDisplayInterval = interval;
		}
	
		FORCE_INLINE static inttype getSeekStatisticsDisplayInterval(void) {
			return s_seekStatisticsDisplayInterval;
		}

		FORCE_INLINE static inttype getSeekStatisticsDisplayMode(void) {
			return s_seekStatisticsDisplayInterval / DEFAULT_CACHE_SLEEP;
		}

		FORCE_INLINE static void setCheckpointMode(CheckpointMode mode) {
			s_checkpointMode = mode;
		}

		FORCE_INLINE static CheckpointMode getCheckpointMode(void) {
			return s_checkpointMode;
		}

		FORCE_INLINE static void setAutomaticCheckpointInterval(uinttype interval) {
			s_automaticCheckpointInterval = interval;
		}

		FORCE_INLINE static uinttype getAutomaticCheckpointInterval(void) {
			return s_automaticCheckpointInterval;
		}

		FORCE_INLINE static void setFileRefCheckMod(inttype mod) {
			s_fileRefCheckMod = mod;
		}

		FORCE_INLINE static inttype getFileRefCheckMod(void) {
			return s_fileRefCheckMod;
		}

		FORCE_INLINE static void setReorgWorkPercent(doubletype percent) {
			s_reorgWorkPercent = percent;
		}

		FORCE_INLINE static doubletype getReorgWorkPercent(void) {
			return s_reorgWorkPercent;
		}

		FORCE_INLINE static void setFragmentationPercent(doubletype percent) {
			s_fragmentationPercent = percent;
		}

		FORCE_INLINE static doubletype getFragmentationPercent(void) {
			return s_fragmentationPercent;
		}

		FORCE_INLINE static void setSemiPurge(boolean enabled) {
			s_semiPurge = enabled;
		}

		FORCE_INLINE static boolean getSemiPurge(void) {
			return s_semiPurge;
		}

		FORCE_INLINE static void setDynamicSummarization(boolean enabled) {
			s_dynamicSummarization = enabled;
		}

		FORCE_INLINE static boolean getDynamicSummarization(void) {
			return s_dynamicSummarization;
		}

		FORCE_INLINE static void setRangeSync(boolean enabled) {
			s_rangeSync = enabled;
		}

		FORCE_INLINE static boolean getRangeSync(void) {
			return s_rangeSync;
		}

		FORCE_INLINE static void setAllowLrtVrtMismatch(boolean enabled) {
			s_allowLrtVrtMismatch = enabled;
		}

		FORCE_INLINE static boolean getAllowLrtVrtMismatch(void) {
			return s_allowLrtVrtMismatch;
		}

		FORCE_INLINE static void setCardinalityRecalculateRecovery(boolean enabled) {
			s_cardinalityRecalculateRecovery = enabled;
		}

		FORCE_INLINE static boolean getCardinalityRecalculateRecovery(void) {
			return s_cardinalityRecalculateRecovery;
		}

		FORCE_INLINE static uinttype getSummaryTarget(void) {
			return getCheckpointMode() != CHECKPOINT_OFF ? DEFAULT_SUMMARY_TARGET : 0;
		}

		FORCE_INLINE static uinttype getSummaryAnchor(void) {
			return getCheckpointMode() != CHECKPOINT_OFF ? DEFAULT_SUMMARY_ANCHOR : 0;
		}

		FORCE_INLINE static ulongtype getSummaryStorageThreshold(void) {
			return getCheckpointMode() != CHECKPOINT_OFF ? DEFAULT_SUMMARY_STORAGE_THRESHOLD : 0;
		}

		FORCE_INLINE static void setSystemUid(int system_uid) {
			s_system_uid = system_uid;
		}

		FORCE_INLINE static int getSystemUid(void) {
			return s_system_uid;
		}

		FORCE_INLINE static void setActivationKey(const char* activation_key) {
			s_activation_key = activation_key;
			s_activation_key_updated = true;
		}

		FORCE_INLINE static const char* getActivationKey(void) {
			return s_activation_key.data();
		}
		FORCE_INLINE static bool getActivationKeyChange(void) {
			return s_activation_key_updated;
		}
		FORCE_INLINE static void setActivationKeyChange(bool activation_key_updated) {
			s_activation_key_updated = activation_key_updated;
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_PROPERTIES_H_*/
