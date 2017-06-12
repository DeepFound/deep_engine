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
#ifndef CXX_LANG_SYSTEM_H_
#define CXX_LANG_SYSTEM_H_

#include <string.h>

#include "cxx/lang/nbyte.h"
#include "cxx/lang/array.h"

using namespace cxx::io;

namespace cxx { namespace lang {

class System {

	public:
		static long long currentTimeMillis(void);
		static long long nanoTime(void);

		template<typename T>
		FORCE_INLINE static void arraycopy(T* src, int srcPos, T* dest, int destPos, int length) {
			if (src == dest) {
				memmove(dest + destPos, src + srcPos, length*sizeof(T));

			} else {
				memcpy(dest + destPos, src + srcPos, length*sizeof(T));
			}
		}

		template<typename T>
		FORCE_INLINE static void arraycopy(array<T>* src, int srcPos, array<T>* dest, int destPos, int length) {
			if (src == dest) {
				T* temp = new T[length];
				for (int i=0; i<length; i++) {
					temp[i] = (*src)[srcPos+i];
				}
				for (int i=0; i<length; i++) {
					(*dest)[destPos+i] = temp[i];
				}
				delete temp;

			} else {
				for (int i=0; i<length; i++) {
					(*dest)[destPos+i] = (*src)[srcPos+i];
				}
			}
		}

		FORCE_INLINE static void arraycopy(nbyte* src, int srcPos, nbyte* dest, int destPos, int length) {
			memcpy(*dest + destPos, *src + srcPos, length);
		}

		// XXX: ensure arraycopy for nbyte* gets called
		FORCE_INLINE static void arraycopy(const nbyte* src, int srcPos, nbyte* dest, int destPos, int length) {
			arraycopy((nbyte*) src, srcPos, dest, destPos, length);
		}

		// XXX: ensure arraycopy for nbyte* gets called
		FORCE_INLINE static void arraycopy(const nbyte* src, int srcPos, const nbyte* dest, int destPos, int length) {
			arraycopy((nbyte*) src, srcPos, (nbyte*) dest, destPos, length);
		}

		FORCE_INLINE static void arraycopy(const bytearray src, int srcPos, bytearray dest, int destPos, int length) {
			memcpy(dest + destPos, src + srcPos, length);
		}
};

} } // namespace

#endif /*CXX_LANG_SYSTEM_H_*/
