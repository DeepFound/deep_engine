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
#ifndef CXX_LANG_ARRAY_H_
#define CXX_LANG_ARRAY_H_

#include "cxx/lang/basearray.h"
#include "cxx/lang/ArrayIndexOutOfBoundsException.h"

namespace cxx { namespace lang {

template<typename T>
class array : public basearray<T> {

	protected:
		virtual array<T>* newInstance(int length) const {
			return new array<T>(length);
		}

	public:
		explicit array(int length) : basearray<T>(length) {
			memset(this->m_elements, 0, length*sizeof(T));
		}

		virtual T& operator[](const int index) {
			if (index < 0 || index >= this->length) {
				throw ArrayIndexOutOfBoundsException((String("Index: ")+=index+", Size: "+this->length).c_str());
			}

			return this->m_elements[index];
		}
};

} } // namespace

#endif /*CXX_LANG_ARRAY_H_*/
