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
#ifndef CXX_LANG_OBJECT_H_
#define CXX_LANG_OBJECT_H_

#include <stdlib.h>
#include <typeinfo>

#include "cxx/lang/types.h"

namespace cxx { namespace lang {

class Object {

	CXX_LANG_MEMORY_DEBUG_DECL()

	public:
		Object(void) {
			CXX_LANG_MEMORY_DEBUG_INIT()
		}

		virtual ~Object(void) {
			CXX_LANG_MEMORY_DEBUG_CLEAR()
		}

		virtual void notify();

		virtual void notifyAll();

		virtual boolean wait(void);

		virtual boolean wait(long timeout);

		virtual boolean wait(long timeout, int nanos);

		virtual Object* clone() const;

		virtual long hashCode(void) const {
			long address = (long) this;
			return address ^ ((long) address >> 32);
		}

		virtual boolean equals(const Object* obj) const {
			return (this == obj);
		}

		virtual const char* getClass() const {
#ifdef __GXX_RTTI
			return typeid(*this).name();
#else
			return "RTTI Not Active";
#endif
		}

		/*
		virtual boolean instanceOf(const Object* obj) const {
			return typeid(*this) == typeid(*obj);
		}

		virtual const Class* getClass() const {
			return null;
		}

		virtual const String* toString() const {
			return null;
		}
		*/
};

} } // namespace

#endif /*CXX_LANG_OBJECT_H_*/
