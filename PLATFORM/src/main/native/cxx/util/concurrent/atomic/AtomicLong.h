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
#ifndef CXX_UTIL_CONCURRENT_ATOMIC_ATOMICLONG_H_
#define CXX_UTIL_CONCURRENT_ATOMIC_ATOMICLONG_H_ 

#include "cxx/lang/Object.h"

using namespace cxx::lang;

namespace cxx { namespace util { namespace concurrent { namespace atomic {

class AtomicLong : public Object {

	private:
		volatile longtype m_value;

	public:
		AtomicLong(longtype initialValue = 0):
			m_value(initialValue) {
		}

		FORCE_INLINE void set(longtype newValue) {
			m_value = newValue;
		}

		FORCE_INLINE longtype get(void) const {
			return m_value;
		}

		FORCE_INLINE longtype addAndGet(longtype delta) {
			return __sync_add_and_fetch(&m_value, delta);
		}

		FORCE_INLINE longtype getAndAdd(longtype delta) {
			return __sync_fetch_and_add(&m_value, delta);
		}

		FORCE_INLINE longtype getAndIncrement() {
			return __sync_fetch_and_add(&m_value, 1);
		}

		FORCE_INLINE longtype getAndDecrement() {
			return __sync_fetch_and_sub(&m_value, 1);
		}

		FORCE_INLINE longtype incrementAndGet() {
			return __sync_add_and_fetch(&m_value, 1);
		}

		FORCE_INLINE longtype decrementAndGet() {
			return __sync_sub_and_fetch(&m_value, 1);
		}
};

} } } } // namespace

#endif /*CXX_UTIL_CONCURRENT_ATOMIC_ATOMICLONG_H_*/
