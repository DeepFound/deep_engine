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
#ifndef CXX_LANG_TYPES_H_
#define CXX_LANG_TYPES_H_

// XXX: for now types manual (i.e. more control)
#if 0
#include <inttypes.h>
#endif

#ifndef boolean
#define boolean bool
#endif

#ifndef null
#define null NULL
#endif

#ifndef voidtype 
typedef void voidtype;
#endif

#ifndef voidptr 
typedef void* voidptr;
#endif

#ifndef voidarray 
typedef void* voidarray;
#endif

#ifndef bytetype 
typedef char bytetype;
#endif

#ifndef ubytetype 
typedef unsigned char ubytetype;
#endif

#ifndef bytearray 
typedef char* bytearray;
#endif

#ifndef ubytearray 
typedef unsigned char* ubytearray;
#endif

#ifndef chararray 
typedef char* chararray;
#endif

#ifndef uchararray 
typedef unsigned char* uchararray;
#endif

#ifndef longtype 
typedef long long longtype;
#endif

#ifndef ulongtype 
typedef unsigned long long ulongtype;
#endif

#ifndef doubletype
typedef double doubletype;
#endif

#ifndef floattype
typedef float floattype;
#endif

#ifndef inttype 
typedef int inttype;
#endif

#ifndef uinttype 
typedef unsigned int uinttype;
#endif

#ifndef shorttype 
typedef short shorttype;
#endif

#ifndef ushorttype 
typedef unsigned short ushorttype;
#endif

#ifndef chartype 
typedef char chartype;
#endif

#ifndef uchartype 
typedef unsigned char uchartype;
#endif

#ifdef DEEP_DEBUG
	#define FORCE_INLINE inline
#else
	#define FORCE_INLINE __attribute__((always_inline)) inline
#endif

#ifdef CXX_LANG_MEMORY_DEBUG
	#define CXX_LANG_MEMORY_DEBUG_DECL() public: inttype DEEP_MAGIC;

	#define CXX_LANG_MEMORY_DEBUG_INIT() DEEP_MAGIC = 123456789;

	#define CXX_LANG_MEMORY_DEBUG_CLEAR() DEEP_MAGIC = 0;

	#define CXX_LANG_MEMORY_DEBUG_ASSERT(o) if ((o != null) && (o->DEEP_MAGIC != 123456789)) abort();

	#define CXX_LANG_MEMORY_DEBUG_TYPE_ASSERT(T, o) { T p = o; if ((p != null) && (p->DEEP_MAGIC != 123456789)) abort(); }

	#define CXX_LANG_MEMORY_DEBUG_VALIDATE(T, o) Converter<T>::validate(o);
#else
	#define CXX_LANG_MEMORY_DEBUG_DECL()

	#define CXX_LANG_MEMORY_DEBUG_INIT()

	#define CXX_LANG_MEMORY_DEBUG_CLEAR()

	#define CXX_LANG_MEMORY_DEBUG_ASSERT(o)

	#define CXX_LANG_MEMORY_DEBUG_TYPE_ASSERT(T, o)

	#define CXX_LANG_MEMORY_DEBUG_VALIDATE(T, o)
#endif

#endif /*CXX_LANG_TYPES_H_*/
