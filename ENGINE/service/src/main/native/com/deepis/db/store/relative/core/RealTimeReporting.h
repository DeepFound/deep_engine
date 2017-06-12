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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEREPORTING_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEREPORTING_H_ 

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

struct RealTimeReporting {

	struct PurgeReport {
		typedef ulongtype counttype;

		counttype purgeFlag;
		counttype summary;
		union {
			counttype purged;
			counttype notPaged;
		};
		counttype reindexing;
		counttype indexState;
		union {
			counttype fragmentedKey;
			counttype fragmentedValue;
		};
		union {
			counttype reseeded;
			counttype altered;
			counttype relocated;
			counttype viewed;
			counttype rolling;
		};
		counttype dirty;

		FORCE_INLINE PurgeReport() : 
			purgeFlag(0),
			summary(0),
			purged(0),
			reindexing(0),
			indexState(0),
			fragmentedKey(0),
			reseeded(0),
			dirty(0) {
		}

		FORCE_INLINE ulongtype total() const {
			return purgeFlag + summary + purged + reindexing + indexState + fragmentedKey + reseeded + dirty;
		}

		FORCE_INLINE String toString() const {
			char buf[256];
			char inner[252];
			buf[0] = 0;
			inner[0] = 0;
			uinttype total = 0;

			const char* sep = " ";
			appendString(inner, sizeof(inner), sep, total, "C", purgeFlag);
			appendString(inner, sizeof(inner), sep, total, "S", summary);
			appendString(inner, sizeof(inner), sep, total, "P", purged);
			appendString(inner, sizeof(inner), sep, total, "R", reindexing);
			appendString(inner, sizeof(inner), sep, total, "I", indexState);
			appendString(inner, sizeof(inner), sep, total, "F", fragmentedKey);
			appendString(inner, sizeof(inner), sep, total, "X", reseeded);
			appendString(inner, sizeof(inner), sep, total, "D", dirty);

			const inttype count = snprintf(buf, sizeof(buf), "{%s }", inner);
			return String(buf, count);
		}

		FORCE_INLINE static void appendString(char inner[], const uinttype size, const char*& sep, uinttype& total, const char* prefix, counttype v) {
			if ((v > 0) && (total < size)) {
				const inttype count = snprintf(inner+total, size-total, "%s%s:%llu", sep, prefix, (ulongtype)v);
				if (count > 0) {
					sep = ", ";
					total += count;
				}
			}
		}
	};

	struct IndexReport {
		typedef PurgeReport::counttype counttype;

		counttype filled;
		counttype dirty;
		counttype viewed;
		counttype rebuild;
		counttype retry;
		PurgeReport purgeReport;

		FORCE_INLINE IndexReport() :
			filled(0),
			dirty(0),
			viewed(0),
			rebuild(0),
			retry(0),
			purgeReport() {
		}

		FORCE_INLINE String toString() const {
			char buf[256];
			char inner[252];
			buf[0] = 0;
			inner[0] = 0;
			uinttype total = 0;

			const char* sep = " ";
			appendString(inner, sizeof(inner), sep, total, "F", filled);
			appendString(inner, sizeof(inner), sep, total, "D", dirty);
			appendString(inner, sizeof(inner), sep, total, "V", viewed);
			appendString(inner, sizeof(inner), sep, total, "R", rebuild);
			appendString(inner, sizeof(inner), sep, total, "A", retry);
			appendString(inner, sizeof(inner), sep, total, "P", purgeReport.total());

			const inttype count = snprintf(buf, sizeof(buf), "{%s }", inner);
			return String(buf, count);
		}

		FORCE_INLINE static void appendString(char inner[], const uinttype size, const char*& sep, uinttype& total, const char* prefix, counttype v) {
			return PurgeReport::appendString(inner, size, sep, total, prefix, v);
		}
	};
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMEREPORTING_H_*/
