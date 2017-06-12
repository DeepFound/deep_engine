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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMELOCALITY_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMELOCALITY_H_

#include "cxx/util/Comparator.h"
#include "cxx/util/Converter.h"
#include "cxx/util/Logger.h"

#include "com/deepis/db/store/relative/core/Properties.h"
#include "com/deepis/db/store/relative/util/MapFileSet.h"
#include "com/deepis/db/store/relative/util/InvalidException.h"

using namespace cxx::util;
using namespace com::deepis::db::store::relative::util;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace core {

template<typename T>
class SafeCompare{
	public:
	FORCE_INLINE static inttype compare(T val1, T val2) {
		return (val1 < val2) ? -1 : ((val1 == val2) ? 0 : 1);
	}
};

struct RealTimeLocality {
	public:
		static const uinttype VIEWPOINT_NONE = 0;
		static const longtype TIME_NONE = 0;
		static const RealTimeLocality LOCALITY_NONE;

	private:
		ushorttype m_index;
		uinttype m_length;
		uinttype m_viewpoint;
		longtype m_time;

		FORCE_INLINE RealTimeLocality() :
			m_index(0),
			m_length(0),
			m_viewpoint(VIEWPOINT_NONE),
			m_time(TIME_NONE) {
		}

	public:
		FORCE_INLINE RealTimeLocality(ushorttype index, uinttype length, uinttype viewpoint, longtype time = TIME_NONE) :
			m_index(index),
			m_length(length),
			m_viewpoint(viewpoint),
			m_time(time) {
			#ifdef DEEP_DEBUG
			if (length < Properties::DEFAULT_FILE_HEADER) {
				DEEP_LOG(ERROR, OTHER, "Invalid length for locality: %u\n", length);
				throw InvalidException("Invalid length for locality");
			}
			#endif
		}

		FORCE_INLINE RealTimeLocality(const RealTimeLocality& l) : 
			m_index(l.m_index),
			m_length(l.m_length),
			m_viewpoint(l.m_viewpoint),
			m_time(l.m_time) {
		}

		FORCE_INLINE RealTimeLocality(const RealTimeLocality& l, longtype time) : 
			m_index(l.m_index),
			m_length(l.m_length),
			m_viewpoint(l.m_viewpoint),
			m_time(time) {
		}

		FORCE_INLINE virtual ~RealTimeLocality() {
		}

		FORCE_INLINE static RealTimeLocality safeLocality(ushorttype index, uinttype length, uinttype viewpoint, longtype time = TIME_NONE) {
			if (length < Properties::DEFAULT_FILE_HEADER) {
				return LOCALITY_NONE;
			}

			return RealTimeLocality(index, length, viewpoint, time);
		}

		FORCE_INLINE RealTimeLocality updateViewpoint(uinttype viewpoint) {
			return RealTimeLocality(m_index, m_length, viewpoint, m_time);
		}

		FORCE_INLINE ushorttype getIndex() const {
			return m_index;
		}

		FORCE_INLINE uinttype getLength() const {
			return m_length;
		}

		FORCE_INLINE uinttype getViewpoint() const {
			return m_viewpoint;
		}

		FORCE_INLINE longtype getTime() const {
			return m_time;
		}

		FORCE_INLINE boolean isNone() const {
			return m_length == 0;
		}

		FORCE_INLINE String toString() const {
			if (isNone() == true) {
				return String("{ _ : _ }");
			}

			char buf[128];
			char vpbuf[32];
			char dtbuf[32];
			vpbuf[0] = 0;
			dtbuf[0] = 0;

			if (m_viewpoint != VIEWPOINT_NONE) {
				snprintf(vpbuf, sizeof(vpbuf), " => %u", m_viewpoint);
			}

			if (m_time != TIME_NONE) {
				char tbuf[29];
				tbuf[0] = 0;
				time_t time = (time_t)(m_time / 1000);
				struct tm tm;
				localtime_r(&time, &tm);
				strftime(tbuf, sizeof(tbuf), "%Y_%m_%d_%H_%M_%S", &tm);
				snprintf(dtbuf, sizeof(dtbuf), " @ %s", tbuf);
			}

			const inttype count = snprintf(buf, sizeof(buf), "{ %u : %u%s%s }", m_index, m_length, vpbuf, dtbuf);
			return String(buf, count);
		}

		FORCE_INLINE boolean operator==(const RealTimeLocality& l) const {
			return (m_index == l.m_index) && (m_length == l.m_length) && (m_viewpoint == l.m_viewpoint);
		}

		FORCE_INLINE boolean operator!=(const RealTimeLocality& l) const {
			return (*this == l) == false;
		}
};

const RealTimeLocality RealTimeLocality::LOCALITY_NONE;

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIME_H_*/

#include "com/deepis/db/store/relative/util/MeasuredRandomAccessFile.h"

#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMELOCALITY_H_CMP_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMELOCALITY_H_CMP_

namespace cxx { namespace util {

template<>
class Comparator<RealTimeLocality&>  {
	private:
		MapFileSet<MeasuredRandomAccessFile>* m_lrtWriteFileList;
		boolean m_compareViewpoint;

		FORCE_INLINE longtype getCreationTime(inttype lrtFileIndex) const {
			if (m_lrtWriteFileList == null) {
				return 0;
			}

			MeasuredRandomAccessFile* lwfile = m_lrtWriteFileList->get(lrtFileIndex);
			if (lwfile == null) {
				#if 0
				DEEP_LOG(ERROR, OTHER, "Invalid locality comparison: no such file for index %d\n", lrtFileIndex);
				throw InvalidException("Invalid locality comparison: no such file for index");
				#else
				return 0;
				#endif
			}

			return lwfile->getFileCreationTime();
		}
	public:
		FORCE_INLINE Comparator(MapFileSet<MeasuredRandomAccessFile>* lrtWriteFileList, boolean compareViewpoint = true) :
			m_lrtWriteFileList(lrtWriteFileList),
			m_compareViewpoint(compareViewpoint) {
			// nothing to do
		}

		FORCE_INLINE virtual ~Comparator() {
			// nothing to do
		}

		FORCE_INLINE int compareNoViewpoint(const RealTimeLocality& o1, const RealTimeLocality& o2) const {
			inttype delta = 0;

			// XXX: only compare indexes for "real" localities
			if ((o1.isNone() == false) && (o2.isNone() == false)) {
				const longtype t1 = getCreationTime(o1.getIndex());
				const longtype t2 = getCreationTime(o2.getIndex());
				if ((t1 != 0) && (t2 != 0)) {
					delta = SafeCompare<longtype>::compare(t1, t2);
					if (delta != 0) {
						return delta;
					}
				}

				// XXX: index comparison is really just to ensure equality; ordering of indexes is not exact
				delta = o1.getIndex() - o2.getIndex();
				if (delta != 0) {
					return delta;
				}
			}

			return SafeCompare<uinttype>::compare(o1.getLength(), o2.getLength());
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const RealTimeLocality& o1, const RealTimeLocality& o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const RealTimeLocality& o1, const RealTimeLocality& o2) const {
		#endif
			inttype delta = compareNoViewpoint(o1, o2);
			if ((delta != 0) || (m_compareViewpoint == false)) {
				return delta;
			}

			// XXX: viewpoint comparison is really just to ensure equality; ordering of viewpoints is not exact
			return SafeCompare<uinttype>::compare(o1.getViewpoint(), o2.getViewpoint());
		}
};

template<>
class Comparator<RealTimeLocality>  {
	private:
		Comparator<RealTimeLocality&> m_comparator;

	public:
		FORCE_INLINE Comparator() :
			m_comparator(null) {
		}
		FORCE_INLINE Comparator(MapFileSet<MeasuredRandomAccessFile>* lrtWriteFileList, boolean compareViewpoint = true) :
			m_comparator(lrtWriteFileList, compareViewpoint) {
			// nothing to do
		}

		FORCE_INLINE virtual ~Comparator() {
			// nothing to do
		}

		FORCE_INLINE int compareNoViewpoint(const RealTimeLocality o1, const RealTimeLocality o2) const {
			return m_comparator.compareNoViewpoint(o1, o2);
		}

		#ifdef COM_DEEPIS_DB_CARDINALITY
		FORCE_INLINE int compare(const RealTimeLocality o1, const RealTimeLocality o2, inttype* pos = null) const {
		#else
		FORCE_INLINE int compare(const RealTimeLocality o1, const RealTimeLocality o2) const {
		#endif
			return m_comparator.compare(o1, o2);
		}
};

template<>
class Converter<RealTimeLocality> {
	public:
		static const RealTimeLocality NULL_VALUE;
		static const RealTimeLocality RESERVE;

		Converter() {
		}

		FORCE_INLINE static inttype hashCode(const RealTimeLocality o) {
 			return o.getLength();
		}

		FORCE_INLINE static boolean equals(RealTimeLocality o1, RealTimeLocality o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static const bytearray toData(const RealTimeLocality o) {
			throw UnsupportedOperationException("Invalid locality: toData");
		}

		FORCE_INLINE static void destroy(RealTimeLocality o) {
			// XXX: nothing to do
		}

		FORCE_INLINE static void validate(RealTimeLocality o) {
			// XXX: nothing to do
		}
};

const RealTimeLocality Converter<RealTimeLocality>::NULL_VALUE = RealTimeLocality::LOCALITY_NONE;
const RealTimeLocality Converter<RealTimeLocality>::RESERVE = Converter<RealTimeLocality>::NULL_VALUE;

} } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIME_H_CMP_*/
