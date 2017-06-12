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
#ifndef CXX_UTIL_SET_H_
#define CXX_UTIL_SET_H_

#include "cxx/util/Converter.h"
#include "cxx/util/Collection.h"

using namespace cxx::lang;

namespace cxx { namespace util {

template<typename E>
class Set: public Collection<E> {
	public:
		Set() {
		}

		virtual ~Set() = 0;
	/*
	public:
		virtual boolean removeAll(const Collection* c) = 0;
		virtual boolean retainAll(const Collection* c) = 0;
		virtual boolean containAll(const Collection* c) = 0;
	*/

	protected:
		static const E NULL_VALUE;
};

template<typename E>
const E Set<E>::NULL_VALUE = (const E)Converter<E>::NULL_VALUE;

template<typename E>
inline Set<E>::~Set() {}

} } // namespace

#endif /*CXX_UTIL_SET_H_*/
