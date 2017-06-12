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
#ifndef CXX_UTIL_CONCURRENT_COUNTDOWNLATCH_H_
#define CXX_UTIL_CONCURRENT_COUNTDOWNLATCH_H_ 

#include "cxx/util/concurrent/Synchronize.h"
#include "cxx/util/concurrent/atomic/AtomicLong.h"

using namespace cxx::lang;
using namespace cxx::util::concurrent;
using namespace cxx::util::concurrent::atomic;

namespace cxx { namespace util { namespace concurrent {

class CountDownLatch : public Object {
	private:
		uinttype m_count;
		Synchronizable m_sync;

	public:
		FORCE_INLINE CountDownLatch(uinttype count) :
			m_count(count),
			m_sync() {

		}

		FORCE_INLINE virtual ~CountDownLatch() {
			/* XXX: nothing to do */
		}

		FORCE_INLINE uinttype getCount() {
			m_sync.lock();
			uinttype ret = m_count;
			m_sync.unlock();
			return ret;
		}

		FORCE_INLINE void await() {
			m_sync.lock();
			while (m_count > 0) {
				if (m_sync.wait() == false) {
					m_sync.lock();
				}
			}
			m_sync.unlock();
		}

		FORCE_INLINE void countDown() {
			m_sync.lock();
			if ((m_count > 0) && (--m_count == 0)) {
				m_sync.notifyAll();
			}
			m_sync.unlock();
		}
};

} } } // namespace

#endif /*CXX_UTIL_CONCURRENT_COUNTDOWNLATCH_H_*/
