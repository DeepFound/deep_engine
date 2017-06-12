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
#ifndef CXX_UTIL_LOGGER_H_
#define CXX_UTIL_LOGGER_H_

#include <time.h>
#include <stdio.h>
#include <string.h>

#include "cxx/lang/types.h"
#include "cxx/lang/RuntimeException.h"

namespace cxx { namespace util {

#ifdef CXX_UTIL_LOGGING

// XXX: new logging formats

	#define DEEP_LOG_LEVEL(LEVEL,TOPIC,format,...)															\
		if ((Logger::isLevelEnabled(LEVEL) == true) && (Logger::isTopicEnabled(Logger::TOPIC) == true)) {						\
			switch (LEVEL) { 															\
				case Logger::INFO : DEEP_LOG_INFO(TOPIC,format,##__VA_ARGS__); break;									\
				case Logger::WARN : DEEP_LOG_WARN(TOPIC,format,##__VA_ARGS__); break;									\
				case Logger::ERROR : DEEP_LOG_ERROR(TOPIC,format,##__VA_ARGS__); break;									\
				case Logger::DEBUG : DEEP_LOG_DEBUG(TOPIC,format,##__VA_ARGS__); break;									\
			}																		\
		}

	#define DEEP_LOG(LEVEL,TOPIC,format,...)															\
		DEEP_LOG_LEVEL(Logger::LEVEL,TOPIC,format,##__VA_ARGS__)

	#define DEEP_LOG_INFO(TOPIC,format,...)																\
		if (Logger::isLevelEnabled(Logger::INFO) == true) {													\
			char date[25];																	\
			Logger::getDate(date, 25);															\
			fprintf(stdout, "%s %c[%d;%dm[INFO]%c[%dm  %25.*s:%06d - (%s): " format, date, 27,1,32,27,0, 25, Logger::getName(__FILE__), __LINE__, Logger::topicToString(Logger::TOPIC), ##__VA_ARGS__);	\
		}

	#define DEEP_LOG_WARN(TOPIC,format,...)																\
		if (Logger::isLevelEnabled(Logger::WARN) == true) {													\
			char date[25];																	\
			Logger::getDate(date, 25);															\
			fprintf(stdout, "%s %c[%d;%dm[WARN]%c[%dm  %25.*s:%06d - (%s): " format, date, 27,1,35,27,0, 25, Logger::getName(__FILE__), __LINE__, Logger::topicToString(Logger::TOPIC), ##__VA_ARGS__);	\
		}

	#define DEEP_LOG_ERROR(TOPIC,format,...)															\
		if (Logger::isLevelEnabled(Logger::ERROR) == true) {													\
			char date[25];																	\
			Logger::getDate(date, 25);															\
			fprintf(stdout, "%s %c[%d;%dm[ERROR]%c[%dm %25.*s:%06d - (%s): " format, date, 27,1,31,27,0, 25, Logger::getName(__FILE__), __LINE__, Logger::topicToString(Logger::TOPIC), ##__VA_ARGS__);	\
		}

	#define DEEP_LOG_DEBUG(TOPIC,format,...)															\
		if (Logger::isLevelEnabled(Logger::DEBUG) == true) {													\
			char date[25];																	\
			Logger::getDate(date, 25);															\
			fprintf(stdout, "%s %c[%d;%dm[DEBUG]%c[%dm %25.*s:%06d - (%s): " format, date, 27,1,33,27,0, 25, Logger::getName(__FILE__), __LINE__, Logger::topicToString(Logger::TOPIC), ##__VA_ARGS__);	\
		}

// XXX: old logging formats

	#define LOGGING_INFO(format,...) 																\
		if (Logger::isLevelEnabled(Logger::INFO) == true) {													\
			char date[25];																	\
			Logger::getDate(date, 25);															\
			fprintf(stdout, "%s %c[%d;%dm[INFO]%c[%dm  %25.*s:%06d - " format, date, 27,1,32,27,0, 25, Logger::getName(__FILE__), __LINE__, ##__VA_ARGS__);	\
		}

	#define LOGGING_WARN(format,...) 																\
		if (Logger::isLevelEnabled(Logger::WARN) == true) {													\
			char date[25];																	\
			Logger::getDate(date, 25);															\
			fprintf(stdout, "%s %c[%d;%dm[WARN]%c[%dm  %25.*s:%06d - " format, date, 27,1,35,27,0, 25, Logger::getName(__FILE__), __LINE__, ##__VA_ARGS__);	\
		}

	#define LOGGING_ERROR(format,...) 																\
		if (Logger::isLevelEnabled(Logger::ERROR) == true) {													\
			char date[25];																	\
			Logger::getDate(date, 25);															\
			fprintf(stdout, "%s %c[%d;%dm[ERROR]%c[%dm %25.*s:%06d - " format, date, 27,1,31,27,0, 25, Logger::getName(__FILE__), __LINE__, ##__VA_ARGS__);	\
		}

	#define LOGGING_DEBUG(format,...) 																\
		if (Logger::isLevelEnabled(Logger::DEBUG) == true) {													\
			char date[25];																	\
			Logger::getDate(date, 25);															\
			fprintf(stdout, "%s %c[%d;%dm[DEBUG]%c[%dm %25.*s:%06d - " format, date, 27,1,33,27,0, 25, Logger::getName(__FILE__), __LINE__, ##__VA_ARGS__);	\
		}

#else
	#define LOGGING_INFO(format,...)
	#define LOGGING_WARN(format,...)
	#define LOGGING_ERROR(format,...)
	#define LOGGING_DEBUG(format,...)
#endif

class Logger {

	public:
		static boolean getDate(char* buffer, int size);
		static const char* getName(const char* filepath);
	
		// XXX: levels

		enum Level {
			ERROR = 0,
			WARN,
			INFO,
			DEBUG
		};

		static ubytetype levelFlags;
		static const char* levelStrings[];

		static const char* levelToString(Level level);
		static void setLevelFlags(ubytetype flags);
		static boolean isLevelEnabled(Level level);
		static void enableLevel(Level level);
		static void disableLevel(Level level);

		// XXX: topics ( TODO: make more dynamic )
	
		enum Topic {
			INDEX, // = 0
			REORG, // = 1
			STATS, // = 2
			SEEKS, // = 3
			CLSRE, // = 4
			BREAK, // = 5
			PURGE, // = 6
			CMPRS, // = 7
			UNMNT, // = 8
			LPRIM, // = 9
			FETCH, // = 10
			CHECK, // = 11
			F_VRT, // = 12
			F_IRT, // = 13
			CEASE, // = 14
			CLEAR, // = 15
			FINAL, // = 16
			SUMIZ, // = 17
			READC, // = 18
			OPTIZ, // = 19
			DCVRY, // = 20
			COMMT, // = 21
			RCPOS, // = 22
			RCVRY, // = 23
			PAUSE, // = 24
			VIEWS, // = 25
			CACHE, // = 26
			DROP_, // = 27
			RENAM, // = 28
			AFFIX, // = 29
			FAULT, // = 30
			ISUMM, // = 31
			VERSN, // = 32
			TRACE, // = 33
			FSMGT, // = 34
			OTHER, // = 35
			LCNSE, // = 36
		};		

		static ulongtype topicFlags;
		// XXX: this is made available to ct_plugin via DeepStore to avoid duplicating the list of topics
		static const char* topicStrings[];

		static const char* topicToString(Topic topic);
		static void setTopicFlags(ulongtype flags);
		static boolean isTopicEnabled(Topic topic);
		static void enableTopic(Topic topic);
		static void disableTopic(Topic topic);	
};

// XXX: levels

inline const char* Logger::levelToString(Level level) {
	return levelStrings[(ubytetype)level];
}

inline void Logger::setLevelFlags(ubytetype flags) {
	levelFlags = flags;
}

inline boolean Logger::isLevelEnabled(Level level) {
	return levelFlags & (((ubytetype)1) << ((ubytetype)level));
}

inline void Logger::enableLevel(Level level) {
	ubytetype enable = 0; //ERROR
	while (enable <= level) {
		levelFlags |= (((ubytetype)1) << ((ubytetype)enable++));
	}
}

inline void Logger::disableLevel(Level level) {
	ubytetype disable = level;
	while (disable <= (ubytetype)DEBUG) { 
		levelFlags &= ~(((ubytetype)1) << ((ubytetype)disable++));
	}
}

// XXX: topics

inline const char* Logger::topicToString(Topic topic) {
	return topicStrings[(ubytetype)topic];
}

inline void Logger::setTopicFlags(ulongtype flags) {
	topicFlags = flags;
}

	// XXX: note that OTHER is always ON
inline boolean Logger::isTopicEnabled(Topic topic) {
	return (topic == OTHER) || (topicFlags & (1L << ((bytetype)topic)));
}

inline void Logger::enableTopic(Topic topic) {
	topicFlags |= (1L << ((bytetype)topic));
}

inline void Logger::disableTopic(Topic topic) {
	topicFlags &= ~(1L << ((bytetype)topic));
}

inline boolean Logger::getDate(char* buffer, int size) {

	time_t tv;
	struct tm* tm;

	tv = time(null);
	tm = localtime(&tv);
	if (tm == null) {
		return false;
	}

	if (strftime(buffer, size, "%b %d %Y %H:%M:%S", tm) == 0) {
		return false;
	}

	return true;
}

inline const char* Logger::getName(const char* filepath) {
	const char* name = strrchr(filepath, '/');
	if (name != null) {
		name += 1;

	} else {
		name = filepath;
	}

	return name;
}

} } // namespace

#endif /*CXX_UTIL_LOGGER_H_*/
