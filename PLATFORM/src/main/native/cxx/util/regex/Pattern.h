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
#ifndef CXX_UTIL_REGEX_PATTERN_H_
#define CXX_UTIL_REGEX_PATTERN_H_

#include "cxx/lang/Object.h"
#include "cxx/lang/RuntimeException.h"
#include "cxx/lang/String.h"

#include "regex.h"

using namespace std;
using namespace cxx::lang;

namespace cxx { namespace util { namespace regex {

class Matcher;

class Pattern : public Object {
	private:
		const String m_regexStr;
		const int m_flags;
		regex_t m_regex;

		FORCE_INLINE void compile() {
			if (regcomp(&m_regex, m_regexStr.data(), m_flags) != 0) {
				throw RuntimeException("Unable to compile regexp");
			}
		}

		inline Pattern(const String& regexStr, inttype flags = REG_EXTENDED) :
			m_regexStr(regexStr),
			m_flags(flags) {
			compile();
		}

	public:
		inline Pattern(const Pattern& pattern):
			m_regexStr(pattern.m_regexStr),
			m_flags(pattern.m_flags) {
			compile();
		}

		virtual ~Pattern() {
			regfree(&m_regex);
		}

		FORCE_INLINE static Pattern compile(String& regex, inttype flags = REG_EXTENDED) {
			return Pattern(regex, flags);
		}

		FORCE_INLINE Matcher matcher(String& input);

		friend class Matcher;
};

} } } // namespace

#include "cxx/util/regex/Matcher.h"

namespace cxx { namespace util { namespace regex {

FORCE_INLINE Matcher Pattern::matcher(String& input) {
	return Matcher(*this, input);
}

} } } // namespace

#endif /*CXX_UTIL_REGEX_PATTERN_H_*/
