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
#include "com/deepis/db/store/relative/core/Properties.h"

using namespace com::deepis::db::store::relative::core;

inttype Properties::s_threadLimit = DEFAULT_THREAD_LIMIT;
longtype Properties::s_cacheSize = DEFAULT_CACHE_SIZE;
uinttype Properties::s_fileSize = DEFAULT_FILE_DATA_SIZE;
#if 0
uinttype Properties::s_trtFileSize = 104857600 /* 100M */;
#else
uinttype Properties::s_trtFileSize = 0 /* OFF */;
#endif

inttype Properties::s_fileLimit = 0;
inttype Properties::s_workThreads = 0;
inttype Properties::s_reorgThreads = 0;

inttype Properties::s_transChunk = 0;
inttype Properties::s_transTimeout = 0;
boolean Properties::s_transStream = false;
inttype Properties::s_segmentSize = 0;

ulongtype Properties::s_debugOptions = 0;
ulongtype Properties::s_logOptions = 0;
ulongtype Properties::s_profilingOptions = 0;
inttype Properties::s_profilingTriggerInterval = 10; /* 10 millis */;

longtype Properties::s_statisticsFlushInterval = 3600; /* 1 hour in secs */

boolean Properties::s_memoryFragment = false;
boolean Properties::s_memoryAnalytics = true;

boolean Properties::s_recoveryReplay = true;
boolean Properties::s_recoveryRealign = true;
boolean Properties::s_recoverySafe = true;
boolean Properties::s_dynamicResources = false;

boolean Properties::s_durable = true;
longtype Properties::s_durableSyncInterval = 0;
#ifdef DEEP_SYNCHRONIZATION_GROUPING
longtype Properties::s_durableHoldDownTime = 1; /* > 0 is on */
longtype Properties::s_durableHoldDownThreshold = 25; /* 25 millis */
#endif
doubletype Properties::s_valueCompressPercent = 0.15;

doubletype Properties::s_reorgWorkPercent = 0.10;
doubletype Properties::s_fragmentationPercent = 0.80;
boolean Properties::s_semiPurge = false;
boolean Properties::s_dynamicSummarization = false;
boolean Properties::s_rangeSync = false;
boolean Properties::s_allowLrtVrtMismatch = false;
boolean Properties::s_cardinalityRecalculateRecovery = false;
inttype Properties::s_fileRefCheckMod = 0;

boolean Properties::s_seekStatistics = false;
inttype Properties::s_seekStatisticsResetInterval = DEFAULT_CACHE_SEEK_RESET_INTERVAL;
inttype Properties::s_seekStatisticsDisplayInterval = DEFAULT_CACHE_SEEK_DISPLAY_INTERVAL;

Properties::CheckpointMode Properties::s_checkpointMode = Properties::CHECKPOINT_AUTO;
uinttype Properties::s_automaticCheckpointInterval = 900; /* 15 minutes */

String Properties::s_activation_key = "xxxxxxxxxxxxxxx"; //Default value, never used
int Properties::s_system_uid = 334565; //Default value, never used
bool Properties::s_activation_key_updated = false;
//
// XXX: following double definitions are here due to compilers
//
const doubletype Properties::DEFAULT_FILE_SIZE_PERCENT = 0.025;
const doubletype Properties::DEFAULT_CACHE_SIZE_PERCENT = 0.125;

const doubletype Properties::DEFAULT_CACHE_SHALLOW_PERCENT = 0.05;
const doubletype Properties::DEFAULT_CACHE_IMMENSE_PERCENT = 0.10;
const doubletype Properties::DEFAULT_CACHE_EXTREME_PERCENT = 0.15;

const doubletype Properties::DEFAULT_FS_SHALLOW_PERCENT = 0.45;
const doubletype Properties::DEFAULT_FS_IMMENSE_PERCENT = 0.30;
const doubletype Properties::DEFAULT_FS_EXTREME_PERCENT = 0.15;

const floattype Properties::DEFAULT_KEY_COMPRESSION_RATIO = 1.5;
const doubletype Properties::DEFAULT_SECONDARY_COMPRESSION_FACTOR = 0.05;

const doubletype Properties::DEFAULT_FILE_FRAG_IDLE_DIVISOR = 3.0;
const doubletype Properties::DEFAULT_FILE_FRAG_SIZE_RATIO_HIGH = 0.90;
const doubletype Properties::DEFAULT_FILE_FRAG_SIZE_RATIO_MED_HIGH = 0.75;
const doubletype Properties::DEFAULT_FILE_FRAG_SIZE_RATIO_MED_LOW = 0.50;
const doubletype Properties::DEFAULT_FILE_FRAG_SIZE_RATIO_LOW = 0.25;
