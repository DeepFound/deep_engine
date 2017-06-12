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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_CONCURRENTOBJECT_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_CONCURRENTOBJECT_H_ 

#include "cxx/lang/types.h"

using namespace cxx::lang;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

class ConcurrentObject {

	private:
		longtype m_instance;
		boolean m_deleteFlag;

	public:
		ConcurrentObject(void):
			m_instance(-1),
			m_deleteFlag(false) {
		}

		virtual ~ConcurrentObject(void) {
			// nothing to do for now
		}

		FORCE_INLINE void setInstance(longtype instance) {
			m_instance = instance;
		}

		FORCE_INLINE longtype getInstance(void) const {
			return m_instance;
		}

		FORCE_INLINE void setDeleteFlag(boolean flag) {
			m_deleteFlag = flag;
		}

		FORCE_INLINE boolean getDeleteFlag(void) const {
			return m_deleteFlag;
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_CONCURRENTOBJECT_H_*/
