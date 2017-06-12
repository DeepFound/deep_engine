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
#ifndef CXX_LANG_BASEARRAY_H_
#define CXX_LANG_BASEARRAY_H_

#include "cxx/lang/Object.h"

namespace cxx { namespace lang {

template<typename T>
class basearray : public Object {

	private:
		basearray(const basearray& array) {
		}

		basearray& operator=(const basearray& array) {
		}

	protected:
		T* m_elements;
		virtual basearray<T>* newInstance(int length) const {
			return new basearray<T>(length);
		}

	public:
		const int length;

		explicit basearray(int length) : m_elements(new T[length]), length(length) {
		}

		virtual ~basearray() {
			delete [] m_elements;
		}

		virtual T& operator[](const int index) {
			return m_elements[index];
		}

		virtual basearray<T>* clone() const {
			basearray<T>* clonedArray = newInstance(length);
			for (int i=0; i<length; i++) {
				clonedArray->m_elements[i] = m_elements[i];
			}

			return clonedArray;
		}

		virtual boolean operator==(const basearray<T>* array) const {
			if (this == array) {
				return true;
			}

			if (length != array->length) {
				return false;
			}

			for (int i = 0; i < length; i++) {
				if (m_elements[i] != array->m_elements[i]) {
					return false;
				}
			}

			return true;
		}

		virtual boolean equals(const basearray<T>* obj) const {
			return *this == obj;
		}
};

} } // namespace

#endif /*CXX_LANG_BASEARRAY_H_*/
