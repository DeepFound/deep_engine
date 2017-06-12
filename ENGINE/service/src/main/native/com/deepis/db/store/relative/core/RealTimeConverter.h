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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECONVERTER_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECONVERTER_H_

#include "cxx/lang/nbyte.h"
#include "cxx/lang/UnsupportedOperationException.h"

#include "cxx/util/Converter.h"

#include "com/deepis/db/store/relative/core/Segment.h"
#include "com/deepis/db/store/relative/core/Information.h"

using namespace cxx::lang;
using namespace cxx::util;

using namespace com::deepis::db::store::relative::core;

namespace cxx { namespace util {

template<>
class Converter<voidptr> {
	public:
		static const voidptr NULL_VALUE;
		static const voidptr RESERVE;

		Converter() {
		}

		FORCE_INLINE static inttype hashCode(const voidptr o) {
			throw UnsupportedOperationException("Invalid voidptr hashCode");
		}

		FORCE_INLINE static boolean equals(voidptr o1, voidptr o2) {
			throw UnsupportedOperationException("Invalid voidptr equals");
		}

		FORCE_INLINE static const bytearray toData(const voidptr o) {
			throw UnsupportedOperationException("Invalid voidptr toData");
		}

		FORCE_INLINE static void destroy(voidptr o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(voidptr o) {
			// nothing to do
		}
};

const voidptr Converter<voidptr>::NULL_VALUE = null;
const voidptr Converter<voidptr>::RESERVE = (voidptr) &Converter<voidptr>::NULL_VALUE;

template<>
class Converter<RealTimeResource::Limit> {
	public:
		static const RealTimeResource::Limit NULL_VALUE;
		static const RealTimeResource::Limit RESERVE;

		Converter() {
		}

		FORCE_INLINE static inttype hashCode(const RealTimeResource::Limit o) {
			throw UnsupportedOperationException("Invalid limit hashCode");
		}

		FORCE_INLINE static boolean equals(RealTimeResource::Limit o1, RealTimeResource::Limit o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static const bytearray toData(const RealTimeResource::Limit& o) {
			return (const bytearray) &o;
		}

		FORCE_INLINE static void destroy(RealTimeResource::Limit o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(RealTimeResource::Limit o) {
			// nothing to do
		}
};

const RealTimeResource::Limit Converter<RealTimeResource::Limit>::NULL_VALUE = RealTimeResource::IGNORE;;
const RealTimeResource::Limit Converter<RealTimeResource::Limit>::RESERVE = (RealTimeResource::Limit) Converter<RealTimeResource::Limit>::NULL_VALUE;

template<>
class Converter<StoryLock*> {
	public:
		static const StoryLock* NULL_VALUE;
		static const StoryLock* RESERVE;

		Converter() {
		}

		FORCE_INLINE static inttype hashCode(const StoryLock* o) {
			long address = (long) o;
 			return address ^ ((long) address >> 32);
		}

		FORCE_INLINE static boolean equals(StoryLock* o1, StoryLock* o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static const bytearray toData(const StoryLock* o) {
			throw UnsupportedOperationException("Invalid information: toData");
		}

		FORCE_INLINE static void destroy(StoryLock* o) {
			// nothing to do
		}

		FORCE_INLINE static void validate(StoryLock* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}
};

const StoryLock* Converter<StoryLock*>::NULL_VALUE = null;
const StoryLock* Converter<StoryLock*>::RESERVE = (StoryLock*) &Converter<StoryLock*>::NULL_VALUE;

template<>
class Converter<Information*> {
	public:
		static const Information* NULL_VALUE;
		static const Information* RESERVE;

		Converter() {
		}

		FORCE_INLINE static inttype hashCode(const Information* o) {
			long address = (long) o;
 			return address ^ ((long) address >> 32);
		}

		FORCE_INLINE static boolean equals(Information* o1, Information* o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static const bytearray toData(const Information* o) {
			throw UnsupportedOperationException("Invalid information: toData");
		}

		FORCE_INLINE static void destroy(Information* o) {
			if ((o != null) && (o->decref() == 0)) {
				Information::freeInfo(o);
			}
		}

		FORCE_INLINE static void validate(Information* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}
};

const Information* Converter<Information*>::NULL_VALUE = null;
const Information* Converter<Information*>::RESERVE = (Information*) &Converter<Information*>::NULL_VALUE;

template<typename K>
class Converter<Segment<K>*> {
	public:
		static const Segment<K>* NULL_VALUE;
		static const Segment<K>* RESERVE;

		Converter() {
		}

		FORCE_INLINE static inttype hashCode(const Segment<K>* o) {
			long address = (long) o;
 			return address ^ ((long) address >> 32);
		}

		FORCE_INLINE static boolean equals(Segment<K>* o1, Segment<K>* o2) {
			return (o1 == o2);
		}

		FORCE_INLINE static const bytearray toData(const Segment<K>* o) {
			throw UnsupportedOperationException("Invalid segment: toData");
		}

		FORCE_INLINE static void destroy(Segment<K>* o) {
			delete o;
		}

		FORCE_INLINE static void validate(Segment<K>* o) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(o);
		}
};

template<typename K>
const Segment<K>* Converter<Segment<K>*>::NULL_VALUE = null;

template<typename K>
const Segment<K>* Converter<Segment<K>*>::RESERVE = (Segment<K>*) &Converter<Segment<K>*>::NULL_VALUE;

} } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_CORE_REALTIMECONVERTER_H_*/
