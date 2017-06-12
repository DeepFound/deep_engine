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
#ifndef CXX_UTIL_CONVERTER_CXX_
#define CXX_UTIL_CONVERTER_CXX_

#include "cxx/util/Converter.h"

namespace cxx { namespace util {

// XXX: place holders
const boolean Converter<boolean>::NULL_VALUE = false;
const boolean Converter<boolean>::RESERVE = false;

const chartype Converter<chartype>::NULL_VALUE = -1;
const chartype Converter<chartype>::RESERVE = CHAR_MAX;

// XXX: uchartype equivalent to the boolean type
const uchartype Converter<uchartype>::NULL_VALUE = 0;
const uchartype Converter<uchartype>::RESERVE = UCHAR_MAX;

const shorttype Converter<shorttype>::NULL_VALUE = -1;
const shorttype Converter<shorttype>::RESERVE = Short::MAX_VALUE;

const ushorttype Converter<ushorttype>::NULL_VALUE = 0;
const ushorttype Converter<ushorttype>::RESERVE = USHRT_MAX;

const inttype Converter<inttype>::NULL_VALUE = -1;
const inttype Converter<inttype>::RESERVE = Integer::MAX_VALUE;

const uinttype Converter<uinttype>::NULL_VALUE = 0;
const uinttype Converter<uinttype>::RESERVE = UINT_MAX;

const floattype Converter<floattype>::NULL_VALUE = 0.0;
const floattype Converter<floattype>::RESERVE = FLT_MAX;

const longtype Converter<longtype>::NULL_VALUE = -1;
const longtype Converter<longtype>::RESERVE = Long::MAX_VALUE;

const ulongtype Converter<ulongtype>::NULL_VALUE = 0;
const ulongtype Converter<ulongtype>::RESERVE = ULONG_MAX;

const doubletype Converter<doubletype>::NULL_VALUE = 0.0;
const doubletype Converter<doubletype>::RESERVE = DBL_MAX;

const Object* Converter<Object*>::NULL_VALUE = null;
const Object* Converter<Object*>::RESERVE = (Object*)&Converter<Short*>::NULL_VALUE;

const Short* Converter<Short*>::NULL_VALUE = null;
const Short* Converter<Short*>::RESERVE = (Short*)&Converter<Short*>::NULL_VALUE;

const Integer* Converter<Integer*>::NULL_VALUE = null;
const Integer* Converter<Integer*>::RESERVE = (Integer*)&Converter<Integer*>::NULL_VALUE;

const Float* Converter<Float*>::NULL_VALUE = null;
const Float* Converter<Float*>::RESERVE = (Float*)&Converter<Float*>::NULL_VALUE;

const Long* Converter<Long*>::NULL_VALUE = null;
const Long* Converter<Long*>::RESERVE = (Long*)&Converter<Long*>::NULL_VALUE;

const String* Converter<String*>::NULL_VALUE = null;
const String* Converter<String*>::RESERVE = (String*)&Converter<String*>::NULL_VALUE;

const nbyte* Converter<nbyte*>::NULL_VALUE = null;
const nbyte* Converter<nbyte*>::RESERVE = (nbyte*)&Converter<nbyte*>::NULL_VALUE;

const CompositeKey* Converter<CompositeKey*>::NULL_VALUE = null;
const CompositeKey* Converter<CompositeKey*>::RESERVE = (CompositeKey*)&Converter<CompositeKey*>::NULL_VALUE;

} } // namespace

#endif /* CXX_UTIL_CONVERTER_CXX_ */
