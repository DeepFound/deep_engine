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
#ifndef COM_DEEPIS_DB_STORE_RELATIVE_UTIL_LOCKABLEARRAYLIST_H_
#define COM_DEEPIS_DB_STORE_RELATIVE_UTIL_LOCKABLEARRAYLIST_H_ 

#include "cxx/util/ArrayList.h"

//#ifdef DEEP_USERLOCK
	//#include "cxx/util/concurrent/locks/UserSpaceLock.h"
//#else
	#include "cxx/util/concurrent/locks/ReentrantLock.h"
//#endif

using namespace cxx::util;
using namespace cxx::util::concurrent::locks;

namespace com { namespace deepis { namespace db { namespace store { namespace relative { namespace util {

template<typename E>
class LockableArrayList : public ArrayList<E> /*, public Lockable */ {

	private:
		//#ifdef DEEP_USERLOCK
		//UserSpaceLock m_lock;
		//#else
		ReentrantLock m_lock;
		//#endif

	public:
		LockableArrayList(void):
			m_lock(false) {
		}

		LockableArrayList(int initialCapacity, boolean deleteValue):
			ArrayList<E>(initialCapacity, deleteValue),
			m_lock(false) {
		}

		FORCE_INLINE void lock() {
			m_lock.lock();
		}

		FORCE_INLINE void unlock() {
			m_lock.unlock();
		}
};

} } } } } } // namespace

#endif /*COM_DEEPIS_DB_STORE_RELATIVE_UTIL_LOCKABLEARRAYLIST_H_*/
