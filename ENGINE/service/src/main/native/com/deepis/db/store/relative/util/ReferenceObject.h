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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_REFERENCEOBJECT_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_REFERENCEOBJECT_H_ 

#include "cxx/lang/types.h"

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

using namespace cxx::lang;

template<typename T, T LIMIT>
struct ReferenceObject {

	CXX_LANG_MEMORY_DEBUG_DECL()

	protected:
		volatile T m_refcnt;

	public:
		ReferenceObject(void):
			m_refcnt(1) {

			// XXX: subclass inits memory debug
			#if 0
			CXX_LANG_MEMORY_DEBUG_INIT()
			#endif
		}

		// XXX: subclass clears memory debug
		#if 0
		~ReferenceObject(void) {
			CXX_LANG_MEMORY_DEBUG_CLEAR()
		}
		#endif

		FORCE_INLINE T ref(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);

			return m_refcnt;
		}

		FORCE_INLINE T incref(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);

			return __sync_add_and_fetch(&m_refcnt, 1);
		}

		FORCE_INLINE T decref(void) {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);

			return __sync_sub_and_fetch(&m_refcnt, 1);
		}

		FORCE_INLINE boolean limit(void) const {
			CXX_LANG_MEMORY_DEBUG_ASSERT(this);

			return (m_refcnt > (LIMIT - 1));
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_REFERENCEOBJECT_H_*/
